package volume_info

import (
	"fmt"
	"os"

	jsonpb "google.golang.org/protobuf/encoding/protojson"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/rclone_backend"
	_ "github.com/seaweedfs/seaweedfs/weed/storage/backend/s3_backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// MaybeLoadVolumeInfo load the file data as *volume_server_pb.VolumeInfo, the returned volumeInfo will not be nil
func MaybeLoadVolumeInfo(fileName string) (volumeInfo *volume_server_pb.VolumeInfo, hasRemoteFile bool, hasVolumeInfoFile bool, err error) {

	volumeInfo = &volume_server_pb.VolumeInfo{}

	glog.V(1).Infof("maybeLoadVolumeInfo checks %s", fileName)
	if exists, canRead, _, _, _ := util.CheckFile(fileName); !exists || !canRead {
		if !exists {
			return
		}
		hasVolumeInfoFile = true
		if !canRead {
			glog.Warningf("can not read %s", fileName)
			err = fmt.Errorf("can not read %s", fileName)
			return
		}
		return
	}

	hasVolumeInfoFile = true

	glog.V(1).Infof("maybeLoadVolumeInfo reads %s", fileName)
	fileData, readErr := os.ReadFile(fileName)
	if readErr != nil {
		glog.Warningf("fail to read %s : %v", fileName, readErr)
		err = fmt.Errorf("fail to read %s : %v", fileName, readErr)
		return

	}

	glog.V(1).Infof("maybeLoadVolumeInfo Unmarshal volume info %v", fileName)
	if err = jsonpb.Unmarshal(fileData, volumeInfo); err != nil {
		if oldVersionErr := tryOldVersionVolumeInfo(fileData, volumeInfo); oldVersionErr != nil {
			glog.Warningf("unmarshal error: %v oldFormat: %v", err, oldVersionErr)
			err = fmt.Errorf("unmarshal error: %v oldFormat: %v", err, oldVersionErr)
			return
		} else {
			err = nil
		}
	}

	if len(volumeInfo.GetFiles()) == 0 {
		return
	}

	hasRemoteFile = true

	return
}

func SaveVolumeInfo(fileName string, volumeInfo *volume_server_pb.VolumeInfo) error {

	if exists, _, canWrite, _, _ := util.CheckFile(fileName); exists && !canWrite {
		return fmt.Errorf("failed to check %s not writable", fileName)
	}

	m := jsonpb.MarshalOptions{
		AllowPartial:    true,
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, marshalErr := m.Marshal(volumeInfo)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal %s: %v", fileName, marshalErr)
	}

	if err := util.WriteFile(fileName, text, 0644); err != nil {
		return fmt.Errorf("failed to write %s: %v", fileName, err)
	}

	return nil
}

func tryOldVersionVolumeInfo(data []byte, volumeInfo *volume_server_pb.VolumeInfo) error {
	oldVersionVolumeInfo := &volume_server_pb.OldVersionVolumeInfo{}
	if err := jsonpb.Unmarshal(data, oldVersionVolumeInfo); err != nil {
		return fmt.Errorf("failed to unmarshal old version volume info: %v", err)
	}
	volumeInfo.Files = oldVersionVolumeInfo.Files
	volumeInfo.Version = oldVersionVolumeInfo.Version
	volumeInfo.Replication = oldVersionVolumeInfo.Replication
	volumeInfo.BytesOffset = oldVersionVolumeInfo.BytesOffset
	volumeInfo.DatFileSize = oldVersionVolumeInfo.DatFileSize
	volumeInfo.ExpireAtSec = oldVersionVolumeInfo.DestroyTime
	volumeInfo.ReadOnly = oldVersionVolumeInfo.ReadOnly

	return nil
}

func UploadVolumeInfo(fileName string, collection string, volumeId uint32, ext string, volumeInfo *volume_server_pb.VolumeInfo, clients []UploadFileClient) error {

	if exists, _, canWrite, _, _ := util.CheckFile(fileName); exists && !canWrite {
		return fmt.Errorf("failed to check %s not writable", fileName)
	}

	m := jsonpb.MarshalOptions{
		AllowPartial:    true,
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, marshalErr := m.Marshal(volumeInfo)
	if marshalErr != nil {
		return fmt.Errorf("failed to marshal %s: %v", fileName, marshalErr)
	}
	for _, client := range clients {
		if !client.IsRun {
			return fmt.Errorf("client is close:%s", client.Address)
		}
		fmt.Printf("vif send,client:%s, vid:%d, ext:%s \n", client.Address, volumeId, ext)
		clientErr := client.Client.Send(&volume_server_pb.UploadFileRequest{
			Collection:  collection,
			VolumeId:    volumeId,
			Ext:         ext,
			FileContent: text,
		})
		if clientErr != nil {
			return fmt.Errorf("client send error ext: %s: %v", ext, clientErr)
		}
	}

	return nil
}

type UploadFileClient struct {
	Client  volume_server_pb.VolumeServer_UploadFileClient
	Address string
	IsRun   bool
	Close   chan bool
}
