package weed_server

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
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

	var rebuiltShardIds []uint32

	for _, location := range vs.store.Locations {
		_, _, existingShardCount, err := checkEcVolumeStatus(baseFileName, location)
		if err != nil {
			return nil, err
		}

		if existingShardCount == 0 {
			continue
		}

		if util.FileExists(path.Join(location.IdxDirectory, baseFileName+".ecx")) {
			// write .ec00 ~ .ec13 files
			dataBaseFileName := path.Join(location.Directory, baseFileName)
			if generatedShardIds, err := erasure_coding.RebuildEcFiles(dataBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcFiles %s: %v", dataBaseFileName, err)
			} else {
				rebuiltShardIds = generatedShardIds
			}

			indexBaseFileName := path.Join(location.IdxDirectory, baseFileName)
			if err := erasure_coding.RebuildEcxFile(indexBaseFileName); err != nil {
				return nil, fmt.Errorf("RebuildEcxFile %s: %v", dataBaseFileName, err)
			}

			break
		}
	}

	return &volume_server_pb.VolumeEcShardsRebuildResponse{
		RebuiltShardIds: rebuiltShardIds,
	}, nil
}

// VolumeEcShardsCopy copy the .ecx and some ec data slices
func (vs *VolumeServer) VolumeEcShardsCopy(ctx context.Context, req *volume_server_pb.VolumeEcShardsCopyRequest) (*volume_server_pb.VolumeEcShardsCopyResponse, error) {

	glog.V(0).Infof("VolumeEcShardsCopy: %v", req)

	var location *storage.DiskLocation
	if req.CopyEcxFile {
		location = vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
			return location.DiskType == types.HardDriveType
		})
	} else {
		location = vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
			//(location.FindEcVolume) This method is error, will cause location is nil, redundant judgment
			// _, found := location.FindEcVolume(needle.VolumeId(req.VolumeId))
			// return found
			return true
		})
	}
	if location == nil {
		return nil, fmt.Errorf("no space left")
	}

	dataBaseFileName := storage.VolumeFileName(location.Directory, req.Collection, int(req.VolumeId))
	indexBaseFileName := storage.VolumeFileName(location.IdxDirectory, req.Collection, int(req.VolumeId))

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

	for _, location := range vs.store.Locations {
		if err := deleteEcShardIdsForEachLocation(bName, location, req.ShardIds); err != nil {
			glog.Errorf("deleteEcShards from %s %s.%v: %v", location.Directory, bName, req.ShardIds, err)
			return nil, err
		}
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
