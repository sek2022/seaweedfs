package weed_server

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// UploadFile save stream to file
func (vs *VolumeServer) UploadFile(stream volume_server_pb.VolumeServer_UploadFileServer) error {
	wt := util.NewWriteThrottler(vs.compactionBytePerSecond)
	location := vs.store.FindFreeLocation(func(location *storage.DiskLocation) bool {
		//(location.FindEcVolume) This method is error, will cause location is nil, redundant judgment
		// _, found := location.FindEcVolume(needle.VolumeId(req.VolumeId))
		// return found
		return true
	})

	if location == nil {
		return fmt.Errorf("no space left")
	}

	var destFiles = make(map[string]*os.File)
	// Use buffered writers to improve write performance
	bufWriters := make(map[string]*bufio.Writer)
	defer func() {
		// First flush all buffered data
		for _, writer := range bufWriters {
			writer.Flush()
		}
		// Then close all files
		for _, file := range destFiles {
			err := file.Close()
			if err != nil {
				return
			}
		}
	}()
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC

	// Add buffer for batch processing
	const batchSize = 1024 * 1024 // 1MB
	buffer := make([]byte, 0, batchSize)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			glog.V(0).Infof("stream close to req:%v", req)
			stream.SendAndClose(&volume_server_pb.UploadFileResponse{Message: "success"})
			return nil
		}
		if req == nil {
			fmt.Println("UploadFile request is nil")
			break
		}
		baseFileName := util.Join(location.Directory, erasure_coding.EcShardBaseFileName(req.Collection, int(req.VolumeId))+req.Ext)

		if _, b := destFiles[baseFileName]; !b {
			glog.V(0).Infof("writing to %s", baseFileName)
			needAppend := req.Ext == ".ecj"
			if needAppend {
				flags = os.O_WRONLY | os.O_CREATE
			}
			file, fileErr := os.OpenFile(baseFileName, flags, 0644)
			if fileErr != nil {
				fmt.Printf("writing file error:%s, %v \n", baseFileName, fileErr)
				return fileErr
			}
			destFiles[baseFileName] = file
			// Create buffered writer with 4MB buffer size
			bufWriters[baseFileName] = bufio.NewWriterSize(file, 4*1024*1024)
		}
		//destFiles[baseFileName].Write(req.FileContent)
		// Use buffered write instead of direct file write
		if _, err := bufWriters[baseFileName].Write(req.FileContent); err != nil {
			return err
		}

		// Accumulate data and only call MaybeSlowdown when reaching batch size
		buffer = append(buffer, req.FileContent...)
		if len(buffer) >= batchSize {
			wt.MaybeSlowdown(int64(len(buffer)))
			buffer = buffer[:0]
		}
		// wt.MaybeSlowdown(int64(len(req.FileContent)))
	}
	return nil
}
