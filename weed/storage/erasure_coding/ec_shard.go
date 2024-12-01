package erasure_coding

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"golang.org/x/sys/unix"
)

type ShardId uint8

type EcVolumeShard struct {
	VolumeId           needle.VolumeId
	ShardId            ShardId
	Collection         string
	dir                string
	ecdFile            *os.File
	ecdFileSize        int64
	DiskType           types.DiskType
	DataFileAccessLock sync.RWMutex
	bufferPool         *sync.Pool
}

func NewEcVolumeShard(diskType types.DiskType, dirname string, collection string, id needle.VolumeId, shardId ShardId) (v *EcVolumeShard, e error) {

	v = &EcVolumeShard{dir: dirname, Collection: collection, VolumeId: id, ShardId: shardId, DiskType: diskType}

	baseFileName := v.FileName()

	// open ecd file
	if v.ecdFile, e = os.OpenFile(baseFileName+ToExt(int(shardId)), os.O_RDONLY, 0644); e != nil {
		if e == os.ErrNotExist || strings.Contains(e.Error(), "no such file or directory") {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("cannot read ec volume shard %s%s: %v", baseFileName, ToExt(int(shardId)), e)
	}
	ecdFi, statErr := v.ecdFile.Stat()
	if statErr != nil {
		_ = v.ecdFile.Close()
		return nil, fmt.Errorf("can not stat ec volume shard %s%s: %v", baseFileName, ToExt(int(shardId)), statErr)
	}
	v.ecdFileSize = ecdFi.Size()

	stats.VolumeServerVolumeGauge.WithLabelValues(v.Collection, "ec_shards").Inc()
	v.bufferPool = &sync.Pool{
		New: func() interface{} {
			// 增加到 1MB 的缓冲区
			buf := make([]byte, 1<<20)
			return &buf
		},
	}
	return
}

func (shard *EcVolumeShard) Size() int64 {
	return shard.ecdFileSize
}

func (shard *EcVolumeShard) String() string {
	return fmt.Sprintf("ec shard %v:%v, dir:%s, Collection:%s", shard.VolumeId, shard.ShardId, shard.dir, shard.Collection)
}

func (shard *EcVolumeShard) FileName() (fileName string) {
	return EcShardFileName(shard.Collection, shard.dir, int(shard.VolumeId))
}

func EcShardFileName(collection string, dir string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}

func EcShardBaseFileName(collection string, id int) (baseFileName string) {
	baseFileName = strconv.Itoa(id)
	if collection != "" {
		baseFileName = collection + "_" + baseFileName
	}
	return
}

func (shard *EcVolumeShard) Close() {
	if shard.ecdFile != nil {
		_ = shard.ecdFile.Close()
		shard.ecdFile = nil
	}
}

func (shard *EcVolumeShard) Destroy() {
	os.Remove(shard.FileName() + ToExt(int(shard.ShardId)))
	stats.VolumeServerVolumeGauge.WithLabelValues(shard.Collection, "ec_shards").Dec()
}

func (shard *EcVolumeShard) ReadAt(buf []byte, offset int64) (int, error) {
	// 计算对齐读取
	alignedOffset := offset &^ 4095         // 4KB对齐
	readSize := ((len(buf) + 4095) &^ 4095) // 向上对齐到4KB

	// 直接分配所需大小的临时缓冲区
	tmpBuf := make([]byte, readSize)

	// 使用pread直接读取
	n, err := unix.Pread(int(shard.ecdFile.Fd()), tmpBuf, alignedOffset)
	if err != nil {
		return 0, fmt.Errorf("pread error: %v", err)
	}
	if n < len(buf) {
		return 0, fmt.Errorf("short read: got %d bytes, want %d bytes", n, len(buf))
	}

	// 计算实际需要复制的长度
	copyLen := len(buf)
	if offset-alignedOffset+int64(copyLen) > int64(n) {
		copyLen = int(int64(n) - (offset - alignedOffset))
	}

	// 安全复制
	if copyLen <= 0 {
		return 0, fmt.Errorf("invalid copy length: %d", copyLen)
	}
	copy(buf[:copyLen], tmpBuf[offset-alignedOffset:offset-alignedOffset+int64(copyLen)])

	return copyLen, nil
	//return shard.ecdFile.ReadAt(buf, offset)

}
