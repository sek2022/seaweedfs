package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/storage/volume_info"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

/*

Steps to apply erasure coding to .dat .idx files
0. ensure the volume is readonly
1. client call VolumeEcShardsGenerate to generate the .ecx and .ec00 ~ .ec13 files
2. client ask master for possible servers to hold the ec files
3. client call UploadFile on above target servers to upload ec files from the source server
4. target servers report the new ec files to the master
5.   master stores vid -> [14]*DataNode
6. client checks master. If all 14 slices are ready, delete the original .idx, .idx files

*/

// VolumeEcShardsGenerate generates the .ecx and .ec00 ~ .ec13 files
func (vs *VolumeServer) VolumeEcShardsGenerate(ctx context.Context, req *volume_server_pb.VolumeEcShardsGenerateRequest) (*volume_server_pb.VolumeEcShardsGenerateResponse, error) {

	glog.V(0).Infof("VolumeEcShardsGenerate: %v", req)
	v := vs.store.GetVolume(needle.VolumeId(req.VolumeId))
	if v == nil {
		return nil, fmt.Errorf("volume %d not found", req.VolumeId)
	}
	baseFileName := v.DataFileName()

	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	clientMap, clients, err := openEcUploadClients(req.AllocatedEcIds, vs.grpcDialOption)

	defer func(clients []volume_info.UploadFileClient) {
		err := closeEcUploadClients(clients)
		if err != nil {
			fmt.Println("close ec client error:", err)
		}
	}(clients)

	if err != nil {
		return nil, err
	}
	//set volume server is coding
	if !vs.isECoding {
		vs.isECoding = true
		err = vs.notifyMasterVolumeServerECoding(true)
		if err != nil {
			return nil, err
		}
	}
	defer func() {
		if vs.isECoding {
			//set volume server is not coding
			vs.isECoding = false
			err := vs.notifyMasterVolumeServerECoding(false)
			if err != nil {
				return
			}
		}
	}()

	// upload .ec00 ~ .ec13 files
	if err := erasure_coding.UploadEcFiles(baseFileName, clientMap, req.Collection, req.VolumeId); err != nil {
		return nil, fmt.Errorf("WriteEcFiles %s: %v", baseFileName, err)
	}

	// upload .ecx file
	if err := erasure_coding.UploadSortedFileFromIdx(v.IndexFileName(), req.Collection, req.VolumeId, ".ecx", clients); err != nil {
		return nil, fmt.Errorf("WriteSortedFileFromIdx %s: %v", v.IndexFileName(), err)
	}

	// upload .vif files
	var expireAtSec uint64
	if v.Ttl != nil {
		ttlSecond := v.Ttl.ToSeconds()
		if ttlSecond > 0 {
			expireAtSec = uint64(time.Now().Unix()) + ttlSecond //calculated expiration time
		}
	}
	volumeInfo := &volume_server_pb.VolumeInfo{Version: uint32(v.Version())}
	volumeInfo.ExpireAtSec = expireAtSec

	datSize, _, _ := v.FileStat()
	volumeInfo.DatFileSize = int64(datSize)
	if err := volume_info.UploadVolumeInfo(baseFileName+".vif", req.Collection, req.VolumeId, ".vif", volumeInfo, clients); err != nil {
		return nil, fmt.Errorf("SaveVolumeInfo %s: %v", baseFileName, err)
	}

	return &volume_server_pb.VolumeEcShardsGenerateResponse{}, nil
}

func openEcUploadClients(ecIds map[string]*volume_server_pb.EcShardIds, dialOption grpc.DialOption) (map[uint32]volume_info.UploadFileClient, []volume_info.UploadFileClient, error) {
	var clientMap = make(map[uint32]volume_info.UploadFileClient, erasure_coding.TotalShardsCount)
	var clients = make([]volume_info.UploadFileClient, 0)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var errors = make([]error, 0)
	wg.Add(len(ecIds))

	for key, ids := range ecIds {
		go func() {
			err := operation.WithVolumeServerClient(true, pb.ServerAddress(key), dialOption, func(client volume_server_pb.VolumeServerClient) error {
				uploadClient, err := client.UploadFile(context.Background())
				if err != nil {
					return fmt.Errorf("failed to Upload File : %v", err)
				}
				ufClient := volume_info.UploadFileClient{Client: uploadClient, Address: key, IsRun: true, Close: make(chan bool)}
				mu.Lock()
				for _, id := range ids.ShardIds {
					clientMap[id] = ufClient
				}
				clients = append(clients, ufClient)
				mu.Unlock()

				wg.Done()

				<-ufClient.Close
				//fmt.Printf("openEcClients client destroy, client: %s \n", ufClient.Address)
				return nil
			})
			if err != nil {
				wg.Done()
			}
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		}()
	}

	wg.Wait()

	if len(errors) > 0 {
		return nil, clients, errors[0]
	}

	return clientMap, clients, nil
}

func closeEcUploadClients(clients []volume_info.UploadFileClient) error {
	//fmt.Printf("clients:%v \n", clients)
	if clients == nil {
		return nil
	}
	for _, client := range clients {
		reply, err := client.Client.CloseAndRecv()
		if err != nil {
			fmt.Printf("failed to recv: %v \n", err)
		}

		fmt.Printf("client replay,client:%s, reply:%v \n", client.Address, reply.String())
		client.Close <- true
		client.IsRun = false
	}
	return nil
}

// VolumeEcShardsRebuild generates the any of the missing .ec00 ~ .ec13 files
func (vs *VolumeServer) VolumeEcShardsRebuild(ctx context.Context, req *volume_server_pb.VolumeEcShardsRebuildRequest) (*volume_server_pb.VolumeEcShardsRebuildResponse, error) {

	glog.V(0).Infof("VolumeEcShardsRebuild: %v", req)

	baseFileName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	// Check if files exist in temp directory (SSD) for faster rebuild
	tempDir := fmt.Sprintf("%s/%d", erasure_coding.EcRebuildTempDir, req.VolumeId)
	tempBaseFileName := path.Join(tempDir, baseFileName)

	// Check file integrity in temp directory, similar to checkEcVolumeStatus
	_, _, existingShardCount, err := checkEcTempDirStatus(baseFileName, tempDir)
	if err != nil {
		glog.V(0).Infof("failed to check temp directory status: %v", err)
		existingShardCount = 0
	}
	hasTempFiles := existingShardCount > 0

	var rebuiltShardIds []uint32

	if hasTempFiles {
		// Rebuild directly in temp directory (SSD) for faster I/O
		glog.V(0).Infof("rebuilding EC shards in temp directory %s", tempDir)
		var sourceLocation *storage.DiskLocation
		for _, loc := range vs.store.Locations {
			sourceLocation = loc
		}

		if sourceLocation != nil {
			// Copy existing EC files from local location to temp directory
			if err := copyFilesFromLocationToTempDir(sourceLocation, baseFileName, tempDir); err != nil {
				glog.Warningf("failed to copy files from location to temp dir: %v, will use normal rebuild", err)
			}
		}

		_, _, _, err := checkEcTempDirStatus(baseFileName, tempDir)
		if err != nil {
			glog.V(0).Infof("failed to check temp directory status: %v", err)
			return nil, fmt.Errorf("failed to check temp directory status: %v", err)
		}

		if generatedShardIds, err := erasure_coding.RebuildEcFiles(tempBaseFileName); err != nil {
			return nil, fmt.Errorf("RebuildEcFiles in temp dir %s: %v", tempBaseFileName, err)
		} else {
			rebuiltShardIds = generatedShardIds
		}

		//fmt.Printf("--------- rebuiltShardIds: %v\n", rebuiltShardIds)

		// Rebuild ecx file if needed
		if util.FileExists(tempBaseFileName + ".ecx") {
			if err := erasure_coding.RebuildEcxFile(tempBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcxFile in temp dir %s: %v", tempBaseFileName, err)
			}
		}

		// Copy generated shard files and ecx file from temp directory to final location
		if len(rebuiltShardIds) > 0 || util.FileExists(tempBaseFileName+".ecx") {
			// Find a location to copy files to
			if sourceLocation != nil {
				if err := copyFilesFromTempDirToLocation(tempDir, baseFileName, sourceLocation, rebuiltShardIds); err != nil {
					return nil, fmt.Errorf("failed to copy files from temp dir to location: %v", err)
				}
			}
		}
	} else {
		// Normal rebuild process from final location
		for _, loc := range vs.store.Locations {
			_, _, existingShardCount, err := checkEcVolumeStatus(baseFileName, loc)
			if err != nil {
				return nil, err
			}

			if existingShardCount == 0 {
				continue
			}

			if util.FileExists(path.Join(loc.IdxDirectory, baseFileName+".ecx")) {
				// write .ec00 ~ .ec13 files
				dataBaseFileName := path.Join(loc.Directory, baseFileName)
				if generatedShardIds, err := erasure_coding.RebuildEcFiles(dataBaseFileName); err != nil {
					return nil, fmt.Errorf("RebuildEcFiles %s: %v", dataBaseFileName, err)
				} else {
					rebuiltShardIds = generatedShardIds
				}

				indexBaseFileName := path.Join(loc.IdxDirectory, baseFileName)
				if err := erasure_coding.RebuildEcxFile(indexBaseFileName); err != nil {
					return nil, fmt.Errorf("RebuildEcxFile %s: %v", dataBaseFileName, err)
				}

				break
			}
		}
	}

	//fmt.Printf("-------222-- rebuiltShardIds: %v\n", rebuiltShardIds)

	return &volume_server_pb.VolumeEcShardsRebuildResponse{
		RebuiltShardIds: rebuiltShardIds,
	}, nil
}

// VolumeEcShardsCopy copy the .ecx and some ec data slices
func (vs *VolumeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {

	glog.V(0).Infof("VolumeEcShardsCopy: %v", req)

	// Check if temp directory should be used and has enough free space
	tempDir := fmt.Sprintf("%s/%d", erasure_coding.EcRebuildTempDir, req.VolumeId)
	useTempDir := false

	if erasure_coding.EcRebuildUseTempDir {
		if err := os.MkdirAll(tempDir, 0755); err == nil {
			diskStatus := stats.NewDiskStatus(tempDir)
			if diskStatus.Free >= erasure_coding.EcRebuildTempDirMinFreeSpace {
				useTempDir = true
				//glog.V(0).Infof("using temp directory %s for faster copy (free space: %d bytes)", tempDir, diskStatus.Free)
			} else {
				glog.V(0).Infof("temp directory %s has insufficient free space: %d bytes (required: %d bytes), using normal location", tempDir, diskStatus.Free, erasure_coding.EcRebuildTempDirMinFreeSpace)
			}
		} else {
			glog.Warningf("failed to create temp directory %s: %v, using normal location", tempDir, err)
		}
	}

	var dataBaseFileName, indexBaseFileName string
	if useTempDir {
		// Use temp directory for base file names
		dataBaseFileName = path.Join(tempDir, erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)))
		indexBaseFileName = path.Join(tempDir, erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId)))
		glog.V(0).Infof("using temp directory %s for faster copy", tempDir)
	} else {
		glog.V(0).Infof("using normal location for copy")
		// Use normal location
		var location *storage.DiskLocation
		if req.CopyEcxFile {
			location = vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
				return location.DiskType == types.HardDriveType
			})
		} else {
			location = vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
				return true
			})
		}
		if location == nil {
			return nil, fmt.Errorf("no space left")
		}
		dataBaseFileName = storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))
		indexBaseFileName = storage.VolumeFileName(location.IdxDirectory, req.Collection, int(req.VolumeId))
	}

	err := operation.WithVolumeServerClient(true, pb.ServerAddress(req.SourceDataNode), vs.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy ec data slices
		for _, shardId := range req.ShardIds {
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, erasure_coding.ToExt(int(shardId)), false, false, nil); err != nil {
				return err
			}
		}

		if req.CopyEcxFile {

			// copy ecx file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecx", false, false, nil); err != nil {
				return err
			}
		}

		if req.CopyEcjFile {
			// copy ecj file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, indexBaseFileName, ".ecj", true, true, nil); err != nil {
				return err
			}
		}

		if req.CopyVifFile {
			// copy vif file
			if _, err := vs.doCopyFile(client, true, req.Collection, req.VolumeId, math.MaxUint32, math.MaxInt64, dataBaseFileName, ".vif", false, true, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("VolumeEcShardsCopy volume %d: %v", req.VolumeId, err)
	}

	return &volume_server_pb.VolumeEcShardsCopyResponse{}, nil
}

// VolumeEcShardsDelete local delete the .ecx and some ec data slices if not needed
// the shard should not be mounted before calling this.
func (vs *VolumeServer) VolumeEcShardsDelete(ctx context.Context, req *volume_server_pb.VolumeEcShardsDeleteRequest) (*volume_server_pb.VolumeEcShardsDeleteResponse, error) {

	bName := erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))

	glog.V(0).Infof("ec volume %s shard delete %v", bName, req.ShardIds)

	// Delete from normal locations
	for _, location := range vs.store.Locations {
		if err := deleteEcShardIdsForEachLocation(bName, location, req.ShardIds); err != nil {
			glog.Errorf("deleteEcShards from %s %s.%v: %v", location.Directory, bName, req.ShardIds, err)
			return nil, err
		}
	}

	// Delete from temp directory if exists
	tempDir := fmt.Sprintf("%s/%d", erasure_coding.EcRebuildTempDir, req.VolumeId)
	if err := deleteEcShardIdsFromTempDir(bName, tempDir, req.ShardIds); err != nil {
		glog.Errorf("deleteEcShards from temp dir %s %s.%v: %v", tempDir, bName, req.ShardIds, err)
		// Don't return error, continue with normal locations
	}

	return &volume_server_pb.VolumeEcShardsDeleteResponse{}, nil
}

func deleteEcShardIdsForEachLocation(bName string, location *storage.DiskLocation, shardIds []uint32) error {

	found := false

	indexBaseFilename := path.Join(location.IdxDirectory, bName)
	dataBaseFilename := path.Join(location.Directory, bName)

	//if util.FileExists(path.Join(location.IdxDirectory, bName+".ecx")) {
	for _, shardId := range shardIds {
		shardFileName := dataBaseFilename + erasure_coding.ToExt(int(shardId))
		if util.FileExists(shardFileName) {
			found = true
			os.Remove(shardFileName)
		}
	}
	//}

	if !found {
		return nil
	}

	hasEcxFile, hasIdxFile, existingShardCount, err := checkEcVolumeStatus(bName, location)
	if err != nil {
		return err
	}

	if hasEcxFile && existingShardCount == 0 {
		if err := os.Remove(indexBaseFilename + ".ecx"); err != nil {
			return err
		}
		os.Remove(indexBaseFilename + ".ecj")

		if !hasIdxFile {
			// .vif is used for ec volumes and normal volumes
			os.Remove(dataBaseFilename + ".vif")
		}
	}

	return nil
}

func deleteEcShardIdsFromTempDir(bName string, tempDir string, shardIds []uint32) error {
	if !util.FileExists(tempDir) {
		return nil
	}

	found := false
	dataBaseFilename := path.Join(tempDir, bName)
	indexBaseFilename := path.Join(tempDir, bName)

	// Delete shard files
	for _, shardId := range shardIds {
		shardFileName := dataBaseFilename + erasure_coding.ToExt(int(shardId))
		if util.FileExists(shardFileName) {
			found = true
			if err := os.Remove(shardFileName); err != nil {
				return fmt.Errorf("failed to remove shard file %s: %v", shardFileName, err)
			}
		}
	}

	if !found {
		return nil
	}

	// Check if there are any remaining shard files
	hasEcxFile, _, existingShardCount, err := checkEcTempDirStatus(bName, tempDir)
	if err != nil {
		return err
	}

	// If no shard files remain, clean up index files
	if hasEcxFile && existingShardCount == 0 {
		if err := os.Remove(indexBaseFilename + ".ecx"); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove .ecx file: %v", err)
		}
		if err := os.Remove(indexBaseFilename + ".ecj"); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove .ecj file: %v", err)
		}
		if err := os.Remove(dataBaseFilename + ".vif"); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove .vif file: %v", err)
		}
	}

	// Remove temp directory at the end
	if err := os.RemoveAll(tempDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove temp directory %s: %v", tempDir, err)
	}

	return nil
}

func copyFilesFromLocationToTempDir(location *storage.DiskLocation, baseFileName string, tempDir string) error {
	// Ensure temp directory exists
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory %s: %v", tempDir, err)
	}

	sourceDataBaseFileName := path.Join(location.Directory, baseFileName)
	sourceIndexBaseFileName := path.Join(location.IdxDirectory, baseFileName)

	tempDataBaseFileName := path.Join(tempDir, baseFileName)
	tempIndexBaseFileName := path.Join(tempDir, baseFileName)

	// Copy all shard files (.ec00 ~ .ec13)
	for shardId := 0; shardId < erasure_coding.TotalShardsCount; shardId++ {
		ext := erasure_coding.ToExt(shardId)
		sourceFile := sourceDataBaseFileName + ext
		tempFile := tempDataBaseFileName + ext

		if util.FileExists(sourceFile) && !util.FileExists(tempFile) {
			if err := copyFile(sourceFile, tempFile); err != nil {
				return fmt.Errorf("failed to copy shard file %s to %s: %v", sourceFile, tempFile, err)
			}
		}
	}

	// Copy index files (.ecx, .ecj)
	if util.FileExists(sourceIndexBaseFileName+".ecx") && !util.FileExists(tempIndexBaseFileName+".ecx") {
		if err := copyFile(sourceIndexBaseFileName+".ecx", tempIndexBaseFileName+".ecx"); err != nil {
			return fmt.Errorf("failed to copy .ecx file: %v", err)
		}
	}
	if util.FileExists(sourceIndexBaseFileName+".ecj") && !util.FileExists(tempIndexBaseFileName+".ecj") {
		if err := copyFile(sourceIndexBaseFileName+".ecj", tempIndexBaseFileName+".ecj"); err != nil {
			return fmt.Errorf("failed to copy .ecj file: %v", err)
		}
	}

	// Copy .vif file if exists
	if util.FileExists(sourceDataBaseFileName+".vif") && !util.FileExists(tempDataBaseFileName+".vif") {
		if err := copyFile(sourceDataBaseFileName+".vif", tempDataBaseFileName+".vif"); err != nil {
			return fmt.Errorf("failed to copy .vif file: %v", err)
		}
	}

	return nil
}

func copyFilesFromTempDirToLocation(tempDir string, baseFileName string, location *storage.DiskLocation, generatedShardIds []uint32) error {
	tempDataBaseFileName := path.Join(tempDir, baseFileName)
	tempIndexBaseFileName := path.Join(tempDir, baseFileName)

	finalDataBaseFileName := path.Join(location.Directory, baseFileName)
	finalIndexBaseFileName := path.Join(location.IdxDirectory, baseFileName)

	// Ensure target directories exist
	if err := os.MkdirAll(location.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create data directory %s: %v", location.Directory, err)
	}
	if err := os.MkdirAll(location.IdxDirectory, 0755); err != nil {
		return fmt.Errorf("failed to create index directory %s: %v", location.IdxDirectory, err)
	}

	// Copy generated shard files
	for _, shardId := range generatedShardIds {
		ext := erasure_coding.ToExt(int(shardId))
		tempFile := tempDataBaseFileName + ext
		finalFile := finalDataBaseFileName + ext

		if util.FileExists(tempFile) {
			if err := copyFile(tempFile, finalFile); err != nil {
				return fmt.Errorf("failed to copy shard file %s to %s: %v", tempFile, finalFile, err)
			}
		}
	}

	// Copy index files (.ecx, .ecj)
	if util.FileExists(tempIndexBaseFileName + ".ecx") {
		if err := copyFile(tempIndexBaseFileName+".ecx", finalIndexBaseFileName+".ecx"); err != nil {
			return fmt.Errorf("failed to copy .ecx file: %v", err)
		}
	}
	if util.FileExists(tempIndexBaseFileName + ".ecj") {
		if err := copyFile(tempIndexBaseFileName+".ecj", finalIndexBaseFileName+".ecj"); err != nil {
			return fmt.Errorf("failed to copy .ecj file: %v", err)
		}
	}

	// Copy .vif file if exists
	if util.FileExists(tempDataBaseFileName + ".vif") {
		if err := copyFile(tempDataBaseFileName+".vif", finalDataBaseFileName+".vif"); err != nil {
			return fmt.Errorf("failed to copy .vif file: %v", err)
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

func checkEcVolumeStatus(bName string, location *storage.DiskLocation) (hasEcxFile bool, hasIdxFile bool, existingShardCount int, err error) {
	// check whether to delete the .ecx and .ecj file also
	fileInfos, err := os.ReadDir(location.Directory)
	if err != nil {
		return false, false, 0, err
	}
	if location.IdxDirectory != location.Directory {
		idxFileInfos, err := os.ReadDir(location.IdxDirectory)
		if err != nil {
			return false, false, 0, err
		}
		fileInfos = append(fileInfos, idxFileInfos...)
	}
	for _, fileInfo := range fileInfos {
		if fileInfo.Name() == bName+".ecx" || fileInfo.Name() == bName+".ecj" {
			hasEcxFile = true
			continue
		}
		if fileInfo.Name() == bName+".idx" {
			hasIdxFile = true
			continue
		}
		if strings.HasPrefix(fileInfo.Name(), bName+".ec") {
			existingShardCount++
		}
	}
	return hasEcxFile, hasIdxFile, existingShardCount, nil
}

func checkEcTempDirStatus(baseFileName string, tempDir string) (hasEcxFile bool, hasEcjFile bool, existingShardCount int, err error) {
	// check file integrity in temp directory
	fileInfos, err := os.ReadDir(tempDir)
	if err != nil {
		return false, false, 0, err
	}

	baseName := path.Base(baseFileName)
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			continue
		}
		fileName := fileInfo.Name()

		if fileName == baseName+".ecx" {
			hasEcxFile = true
			continue
		}
		if fileName == baseName+".ecj" {
			hasEcjFile = true
			continue
		}
		if strings.HasPrefix(fileName, baseName+".ec") {
			// Check if file size is valid (not empty)
			filePath := path.Join(tempDir, fileName)
			if fileStat, statErr := os.Stat(filePath); statErr == nil && fileStat.Size() > 0 {
				existingShardCount++
			}
		}
	}
	return hasEcxFile, hasEcjFile, existingShardCount, nil
}

func (vs *VolumeServer) VolumeEcShardsMount(ctx context.Context, req *volume_server_pb.VolumeEcShardsMountRequest) (*volume_server_pb.VolumeEcShardsMountResponse, error) {

	glog.V(0).Infof("VolumeEcShardsMount: %v", req)

	for _, shardId := range req.ShardIds {
		err := vs.store.MountEcShards(req.Collection, needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard mount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard mount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("mount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsMountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardsUnmount(ctx context.Context, req *volume_server_pb.VolumeEcShardsUnmountRequest) (*volume_server_pb.VolumeEcShardsUnmountResponse, error) {

	glog.V(0).Infof("VolumeEcShardsUnmount: %v", req)

	for _, shardId := range req.ShardIds {
		err := vs.store.UnmountEcShards(needle.VolumeId(req.VolumeId), erasure_coding.ShardId(shardId))

		if err != nil {
			glog.Errorf("ec shard unmount %v: %v", req, err)
		} else {
			glog.V(2).Infof("ec shard unmount %v", req)
		}

		if err != nil {
			return nil, fmt.Errorf("unmount %d.%d: %v", req.VolumeId, shardId, err)
		}
	}

	return &volume_server_pb.VolumeEcShardsUnmountResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardRead(req *volume_server_pb.VolumeEcShardReadRequest, stream volume_server_pb.VolumeServer_VolumeEcShardReadServer) error {

	ecVolume, found := vs.store.FindEcVolume(needle.VolumeId(req.VolumeId))
	if !found {
		return fmt.Errorf("VolumeEcShardRead not found ec volume id %d", req.VolumeId)
	}
	ecShard, found := ecVolume.FindEcVolumeShard(erasure_coding.ShardId(req.ShardId))
	if !found {
		return fmt.Errorf("not found ec shard %d.%d", req.VolumeId, req.ShardId)
	}

	if req.FileKey != 0 {
		_, size, _ := ecVolume.FindNeedleFromEcx(types.Uint64ToNeedleId(req.FileKey))
		if size.IsDeleted() {
			return stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				IsDeleted: true,
			})
		}
	}

	bufSize := req.Size
	if bufSize > BufferSizeLimit {
		bufSize = BufferSizeLimit
	}
	buffer := make([]byte, bufSize)

	startOffset, bytesToRead := req.Offset, req.Size

	for bytesToRead > 0 {
		// min of bytesToRead and bufSize
		bufferSize := bufSize
		if bufferSize > bytesToRead {
			bufferSize = bytesToRead
		}
		bytesread, err := ecShard.ReadAt(buffer[0:bufferSize], startOffset)

		// println("read", ecShard.FileName(), "startOffset", startOffset, bytesread, "bytes, with target", bufferSize)
		if bytesread > 0 {

			if int64(bytesread) > bytesToRead {
				bytesread = int(bytesToRead)
			}
			err = stream.Send(&volume_server_pb.VolumeEcShardReadResponse{
				Data: buffer[:bytesread],
			})
			if err != nil {
				// println("sending", bytesread, "bytes err", err.Error())
				return err
			}

			startOffset += int64(bytesread)
			bytesToRead -= int64(bytesread)

		}

		if err != nil {
			if err != io.EOF {
				return err
			}
			return nil
		}

	}

	return nil

}

func (vs *VolumeServer) VolumeEcBlobDelete(ctx context.Context, req *volume_server_pb.VolumeEcBlobDeleteRequest) (*volume_server_pb.VolumeEcBlobDeleteResponse, error) {

	glog.V(0).Infof("VolumeEcBlobDelete: %v", req)

	resp := &volume_server_pb.VolumeEcBlobDeleteResponse{}

	for _, location := range vs.store.Locations {
		if localEcVolume, found := location.FindEcVolume(needle.VolumeId(req.VolumeId)); found {

			_, size, _, err := localEcVolume.LocateEcShardNeedle(types.NeedleId(req.FileKey), needle.Version(req.Version))
			if err != nil {
				return nil, fmt.Errorf("locate in local ec volume: %v", err)
			}
			if size.IsDeleted() {
				return resp, nil
			}

			err = localEcVolume.DeleteNeedleFromEcx(types.NeedleId(req.FileKey))
			if err != nil {
				return nil, err
			}

			break
		}
	}

	return resp, nil
}

// VolumeEcShardsToVolume generates the .idx, .dat files from .ecx, .ecj and .ec01 ~ .ec14 files
func (vs *VolumeServer) VolumeEcShardsToVolume(ctx context.Context, req *volume_server_pb.VolumeEcShardsToVolumeRequest) (*volume_server_pb.VolumeEcShardsToVolumeResponse, error) {

	glog.V(0).Infof("VolumeEcShardsToVolume: %v", req)

	// collect .ec00 ~ .ec09 files
	shardFileNames := make([]string, erasure_coding.DataShardsCount)
	v, found := vs.store.CollectEcShards(needle.VolumeId(req.VolumeId), shardFileNames)
	if !found {
		return nil, fmt.Errorf("ec volume %d not found", req.VolumeId)
	}

	if v.Collection != req.Collection {
		return nil, fmt.Errorf("existing collection:%v unexpected input: %v", v.Collection, req.Collection)
	}

	for shardId := 0; shardId < erasure_coding.DataShardsCount; shardId++ {
		if shardFileNames[shardId] == "" {
			return nil, fmt.Errorf("ec volume %d missing shard %d", req.VolumeId, shardId)
		}
	}

	dataBaseFileName, indexBaseFileName := v.DataBaseFileName(), v.IndexBaseFileName()
	// calculate .dat file size
	datFileSize, err := erasure_coding.FindDatFileSize(dataBaseFileName, indexBaseFileName)
	if err != nil {
		return nil, fmt.Errorf("FindDatFileSize %s: %v", dataBaseFileName, err)
	}

	// write .dat file from .ec00 ~ .ec09 files
	if err := erasure_coding.WriteDatFile(dataBaseFileName, datFileSize, shardFileNames); err != nil {
		return nil, fmt.Errorf("WriteDatFile %s: %v", dataBaseFileName, err)
	}

	// write .idx file from .ecx and .ecj files
	if err := erasure_coding.WriteIdxFileFromEcIndex(indexBaseFileName); err != nil {
		return nil, fmt.Errorf("WriteIdxFileFromEcIndex %s: %v", v.IndexBaseFileName(), err)
	}

	return &volume_server_pb.VolumeEcShardsToVolumeResponse{}, nil
}

func (vs *VolumeServer) VolumeEcShardDataNodesForFileId(ctx context.Context, req *volume_server_pb.VolumeEcShardDataNodesForFileIdRequest) (*volume_server_pb.VolumeEcShardDataNodesForFileIdResponse, error) {
	if len(req.FileIds) <= 0 {
		return nil, fmt.Errorf("request fileIds is null")
	}

	fileDataNodesMap := make(map[string]*volume_server_pb.EcShardIds)
	for _, fileId := range req.FileIds {
		commaSep := strings.Index(fileId, ",")
		if commaSep < 0 {
			return nil, fmt.Errorf("fileId format error: %s, fileIds:%v", fileId, req.FileIds)
		}
		vid := fileId[0:commaSep]
		fid := fileId[commaSep+1:]

		volumeId, err := needle.NewVolumeId(vid)
		if err != nil {
			glog.V(2).Infof("parsing vid %s: %v", vid, err)
			return nil, fmt.Errorf("parsing vid %s: %v", vid, err)
		}
		n := new(needle.Needle)
		err = n.ParsePath(fid)
		if err != nil {
			glog.V(2).Infof("parsing fid %s: %v", fileId, err)
			return nil, fmt.Errorf("parsing fid %s: %v", fileId, err)
		}

		ecVolume, hasEcVolume := vs.store.FindEcVolume(volumeId)

		if !hasEcVolume {
			return nil, fmt.Errorf("not found ec volumes for fileId: %s", fileId)
		}
		//if ecVolume.Collection != req.Collection {
		//	return nil, fmt.Errorf("existing collection:%v unexpected input: %v", ecVolume.Collection, req.Collection)
		//}
		_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
		if err != nil {
			return nil, fmt.Errorf("FileId error:%s, %v", fileId, err)
		}
		fileEcIds := volume_server_pb.EcShardIds{}
		//fmt.Println("-----intervals:", intervals, ",volumeId:", volumeId)
		for _, interval := range intervals {
			shardId, _ := interval.ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)
			//fmt.Println("-----shardId:", shardId, ",ecVolume.ShardLocations:", ecVolume.ShardLocations)
			fileEcIds.ShardIds = append(fileEcIds.ShardIds, uint32(shardId))
			//if shard, found := ecVolume.FindEcVolumeShard(shardId); found {
			//
			//}
			//ecVolume.ShardLocationsLock.RLock()
			//sourceDataNodes, hasShardIdLocation := ecVolume.ShardLocations[shardId]
			//ecVolume.ShardLocationsLock.RUnlock()
			//if hasShardIdLocation && len(sourceDataNodes) > 0 {
			//	for _, node := range sourceDataNodes {
			//		fileDataNodes.DataNode = append(fileDataNodes.DataNode, node.ToHttpAddress())
			//	}
			//}
		}
		//fmt.Println("-----fileDataNodes:", fileEcIds.ShardIds)
		fileDataNodesMap[fileId] = &fileEcIds
	}
	return &volume_server_pb.VolumeEcShardDataNodesForFileIdResponse{FileShardIds: fileDataNodesMap}, nil
}
