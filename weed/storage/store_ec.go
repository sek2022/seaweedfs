package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"golang.org/x/exp/slices"

	"github.com/klauspost/reedsolomon"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func (s *Store) CollectErasureCodingHeartbeat() *master_pb.Heartbeat {
	var ecShardMessages []*master_pb.VolumeEcShardInformationMessage
	collectionEcShardSize := make(map[string]int64)
	for _, location := range s.Locations {
		location.ecVolumesLock.RLock()
		for _, ecShards := range location.ecVolumes {
			ecShardMessages = append(ecShardMessages, ecShards.ToVolumeEcShardInformationMessage()...)

			for _, ecShard := range ecShards.Shards {
				collectionEcShardSize[ecShards.Collection] += ecShard.Size()
			}
		}
		location.ecVolumesLock.RUnlock()
	}

	for col, size := range collectionEcShardSize {
		stats.VolumeServerDiskSizeGauge.WithLabelValues(col, "ec").Set(float64(size))
	}

	return &master_pb.Heartbeat{
		EcShards:      ecShardMessages,
		HasNoEcShards: len(ecShardMessages) == 0,
	}

}

func (s *Store) MountEcShards(collection string, vid needle.VolumeId, shardId erasure_coding.ShardId) error {
	for _, location := range s.Locations {
		if ecVolume, err := location.LoadEcShard(collection, vid, shardId); err == nil {
			glog.V(0).Infof("MountEcShards %d.%d", vid, shardId)

			var shardBits erasure_coding.ShardBits

			s.NewEcShardsChan <- master_pb.VolumeEcShardInformationMessage{
				Id:          uint32(vid),
				Collection:  collection,
				EcIndexBits: uint32(shardBits.AddShardId(shardId)),
				DiskType:    string(location.DiskType),
				ExpireAtSec: ecVolume.ExpireAtSec,
			}
			return nil
		} else if err == os.ErrNotExist {
			continue
		} else {
			return fmt.Errorf("%s load ec shard %d.%d: %v", location.Directory, vid, shardId, err)
		}
	}

	return fmt.Errorf("MountEcShards %d.%d not found on disk", vid, shardId)
}

func (s *Store) UnmountEcShards(vid needle.VolumeId, shardId erasure_coding.ShardId) error {

	ecShard, found := s.findEcShard(vid, shardId)
	if !found {
		return nil
	}

	var shardBits erasure_coding.ShardBits
	message := master_pb.VolumeEcShardInformationMessage{
		Id:          uint32(vid),
		Collection:  ecShard.Collection,
		EcIndexBits: uint32(shardBits.AddShardId(shardId)),
		DiskType:    string(ecShard.DiskType),
	}

	for _, location := range s.Locations {
		if deleted := location.UnloadEcShard(vid, shardId); deleted {
			glog.V(0).Infof("UnmountEcShards %d.%d", vid, shardId)
			s.DeletedEcShardsChan <- message
			return nil
		}
	}

	return fmt.Errorf("UnmountEcShards %d.%d not found on disk", vid, shardId)
}

func (s *Store) findEcShard(vid needle.VolumeId, shardId erasure_coding.ShardId) (*erasure_coding.EcVolumeShard, bool) {
	for _, location := range s.Locations {
		if v, found := location.FindEcShard(vid, shardId); found {
			return v, found
		}
	}
	return nil, false
}

func (s *Store) FindEcVolume(vid needle.VolumeId) (*erasure_coding.EcVolume, bool) {
	for _, location := range s.Locations {
		if s, found := location.FindEcVolume(vid); found {
			return s, true
		}
	}
	return nil, false
}

// shardFiles is a list of shard files, which is used to return the shard locations
func (s *Store) CollectEcShards(vid needle.VolumeId, shardFileNames []string) (ecVolume *erasure_coding.EcVolume, found bool) {
	for _, location := range s.Locations {
		if s, foundShards := location.CollectEcShards(vid, shardFileNames); foundShards {
			ecVolume = s
			found = true
		}
	}
	return
}

func (s *Store) DestroyEcVolume(vid needle.VolumeId) {
	for _, location := range s.Locations {
		location.DestroyEcVolume(vid)
	}
}

func (s *Store) ReadEcShardNeedle(vid needle.VolumeId, n *needle.Needle, onReadSizeFn func(size types.Size)) (int, error) {
	return s.ReadEcShardNeedleWithReadOption(vid, n, nil, onReadSizeFn)
}

func (s *Store) ReadEcShardNeedleWithReadOption(vid needle.VolumeId, n *needle.Needle, readOption *ReadOption, onReadSizeFn func(size types.Size)) (int, error) {

	for _, location := range s.Locations {
		if localEcVolume, found := location.FindEcVolume(vid); found {
			offset, size, intervals, err := localEcVolume.LocateEcShardNeedle(n.Id, localEcVolume.Version)
			if err != nil {
				return 0, fmt.Errorf("locate in local ec volume: %v", err)
			}
			if size.IsDeleted() {
				return 0, ErrorDeleted
			}

			if onReadSizeFn != nil {
				onReadSizeFn(size)
			}

			var readPart = false
			if readOption != nil {
				readPart = readOption.ReadPart
			}
			adjustedIntervals := intervals
			if readPart {
				// 计算实际需要读取的范围
				readStart := int64(0)
				readSize := int64(size)
				readStart = readOption.Offset
				if readOption.Size > 0 {
					readSize = readOption.Size
				}

				// 验证读取范围的有效性
				if readStart < 0 {
					return 0, fmt.Errorf("invalid range: negative offset")
				}
				if readStart >= int64(size) {
					return 0, io.EOF
				}
				if readStart+readSize > int64(size) {
					readSize = int64(size) - readStart
				}
				if readSize <= 0 {
					return 0, fmt.Errorf("invalid range: non-positive size")
				}
				// 调整区间以匹配读取范围
				adjustedIntervals = adjustIntervalsForRange(intervals, readStart, readSize, localEcVolume)
			}

			if len(adjustedIntervals) > 1 {
				glog.V(3).Infof("ReadEcShardNeedle needle id %s intervals:%+v", n.String(), adjustedIntervals)
			}

			bytes, isDeleted, err := s.readEcShardIntervals(vid, n.Id, localEcVolume, adjustedIntervals, readOption)

			glog.V(3).Infof("read ec volume %d offset %d size %d intervals:%+v adjustedIntervals:%+v", vid, offset.ToActualOffset(), size, intervals, adjustedIntervals)
			if err != nil {
				return 0, fmt.Errorf("ReadEcShardIntervals: %v", err)
			}
			if isDeleted {
				return 0, ErrorDeleted
			}

			if readPart {
				// 直接设置needle的数据，因为我们已经读取了正确的部分
				n.Data = bytes
				n.DataSize = uint32(size)
				n.Size = size
			} else {
				err = n.ReadBytes(bytes, offset.ToActualOffset(), size, localEcVolume.Version)
			}
			if err != nil {
				return 0, fmt.Errorf("readbytes: %v", err)
			}

			return len(bytes), nil
		}
	}
	return 0, fmt.Errorf("ec shard %d not found", vid)
}

// adjustIntervalsForRange 根据读取范围调整区间
func adjustIntervalsForRange(intervals []erasure_coding.Interval, offset, size int64, ecVolume *erasure_coding.EcVolume) []erasure_coding.Interval {
	var adjusted []erasure_coding.Interval

	totalHeaderSize := int64(0)
	totalHeaderSize += types.NeedleHeaderSize
	if ecVolume.Version == needle.Version3 {
		totalHeaderSize += needle.NeedleChecksumSize
	}
	//
	noRemoveHeaderSize := totalHeaderSize
	var currentOffset int64 = 0
	remainingSize := size

	for _, interval := range intervals {
		intervalSize := int64(interval.Size)

		// 跳过目标范围之前的区间
		if currentOffset+intervalSize <= offset {
			currentOffset += intervalSize
			continue
		}

		// 如果已经读取完所需大小，退出
		if remainingSize <= 0 {
			break
		}

		newInterval := interval

		// 计算这个区间内实际要读取的数据
		var intervalReadStart int64 = 0
		var intervalReadSize = intervalSize

		// 如果这个区间包含起始偏移量
		if currentOffset < offset {
			intervalReadStart = offset - currentOffset
			intervalReadSize = intervalSize - intervalReadStart
		}
		//interval需要跳过header读取
		if noRemoveHeaderSize > 0 {
			intervalReadStart = intervalReadStart + noRemoveHeaderSize

			if intervalReadStart > intervalSize { //越界处理
				intervalReadStart = intervalSize
			}
			intervalReadSize = intervalSize - intervalReadStart
			if intervalReadSize < noRemoveHeaderSize { //剩余的数据不够header，到下个interval跳过剩余header
				noRemoveHeaderSize = noRemoveHeaderSize - intervalReadSize
				glog.V(0).Infof("------remove header need multi interval, noRemoveHeaderSize:%d", noRemoveHeaderSize)
			} else {
				noRemoveHeaderSize = 0
			}
		}

		// 如果这个区间的数据超出了所需大小
		if remainingSize < intervalReadSize {
			intervalReadSize = remainingSize
		}

		if intervalReadSize > 0 {
			newInterval.InnerBlockOffset += intervalReadStart
			newInterval.Size = types.Size(intervalReadSize)
			adjusted = append(adjusted, newInterval)
		}

		currentOffset += intervalSize
		remainingSize -= intervalReadSize
	}

	return adjusted
}

func (s *Store) readEcShardIntervals(vid needle.VolumeId, needleId types.NeedleId, ecVolume *erasure_coding.EcVolume, intervals []erasure_coding.Interval, readOption *ReadOption) (data []byte, is_deleted bool, err error) {

	if err = s.cachedLookupEcShardLocations(ecVolume); err != nil {
		return nil, false, fmt.Errorf("failed to locate shard via master grpc %s: %v", s.MasterAddress, err)
	}

	//totalHeaderSize := int64(0)
	//totalHeaderSize += types.NeedleHeaderSize
	//if ecVolume.Version == needle.Version3 {
	//	totalHeaderSize += needle.NeedleChecksumSize
	//}

	for i, interval := range intervals {
		//if readOption.ReadPart { //部分读取，去除头部信息
		//	if i == 0 {
		//		//跳过头信息
		//		interval.InnerBlockOffset += totalHeaderSize
		//		//第一个少读取totalHeaderSize 字节
		//		interval.Size = interval.Size - types.Size(totalHeaderSize)
		//	}
		//
		//	if i == len(intervals)-1 { //最后一个需要加回headerSize个字节
		//		interval.Size = interval.Size + types.Size(totalHeaderSize)
		//	}
		//}
		if d, isDeleted, e := s.readOneEcShardInterval(needleId, ecVolume, interval, readOption); e != nil {
			return nil, isDeleted, e
		} else {
			if isDeleted {
				is_deleted = true
			}
			if i == 0 {
				data = d
			} else {
				data = append(data, d...)
			}
		}
	}
	return
}

func (s *Store) readOneEcShardInterval(needleId types.NeedleId, ecVolume *erasure_coding.EcVolume, interval erasure_coding.Interval, readOption *ReadOption) (data []byte, is_deleted bool, err error) {
	shardId, actualOffset := interval.ToShardIdAndOffset(erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingSmallBlockSize)

	data = make([]byte, interval.Size)

	t1 := time.Now().UnixMilli()
	if shard, found := ecVolume.FindEcVolumeShard(shardId); found {
		shard.DataFileAccessLock.RLock()
		defer shard.DataFileAccessLock.RUnlock()
		t2 := time.Now().UnixMilli()
		if _, err = shard.ReadAt(data, actualOffset); err != nil {
			glog.V(0).Infof("read local ec shard %d.%d offset %d: %v", ecVolume.VolumeId, shardId, actualOffset, err)
			return
		}
		t3 := time.Now().UnixMilli()
		glog.V(0).Infof("read local ec shard and add cache %d.%d offset %d, total time: %d, read time: %d, data:%d", ecVolume.VolumeId, shardId, actualOffset, t3-t1, t3-t2, len(data))

	} else {
		ecVolume.ShardLocationsLock.RLock()
		sourceDataNodes, hasShardIdLocation := ecVolume.ShardLocations[shardId]
		ecVolume.ShardLocationsLock.RUnlock()

		// try reading directly
		if hasShardIdLocation {
			//fmt.Println("-----not found needleId", needleId, ",volume:", ecVolume.VolumeId, ",shardId:", shardId, ",interval:", interval.BlockIndex, ",sourceDataNodes:", sourceDataNodes)
			_, is_deleted, err = s.readRemoteEcShardInterval(sourceDataNodes, needleId, ecVolume.VolumeId, shardId, data, actualOffset)
			t2 := time.Now().UnixMilli()
			glog.V(0).Infof("read Remote ec shard %d.%d offset %d, time: %d, size:%d", ecVolume.VolumeId, shardId, actualOffset, t2-t1, interval.Size)

			if err == nil {
				return
			}
			glog.V(0).Infof("clearing ec shard %d.%d locations: %v", ecVolume.VolumeId, shardId, err)
		}

		// try reading by recovering from other shards
		_, is_deleted, err = s.recoverOneRemoteEcShardInterval(needleId, ecVolume, shardId, data, actualOffset)
		if err == nil {
			return
		}
		glog.V(0).Infof("recover ec shard %d.%d : %v", ecVolume.VolumeId, shardId, err)
	}
	return
}

func forgetShardId(ecVolume *erasure_coding.EcVolume, shardId erasure_coding.ShardId) {
	// failed to access the source data nodes, clear it up
	ecVolume.ShardLocationsLock.Lock()
	delete(ecVolume.ShardLocations, shardId)
	ecVolume.ShardLocationsLock.Unlock()
}

func (s *Store) cachedLookupEcShardLocations(ecVolume *erasure_coding.EcVolume) (err error) {

	shardCount := len(ecVolume.ShardLocations)
	if shardCount < erasure_coding.DataShardsCount &&
		ecVolume.ShardLocationsRefreshTime.Add(11*time.Second).After(time.Now()) ||
		shardCount == erasure_coding.TotalShardsCount &&
			ecVolume.ShardLocationsRefreshTime.Add(37*time.Minute).After(time.Now()) ||
		shardCount >= erasure_coding.DataShardsCount &&
			ecVolume.ShardLocationsRefreshTime.Add(7*time.Minute).After(time.Now()) {
		// still fresh
		return nil
	}

	glog.V(3).Infof("lookup and cache ec volume %d locations", ecVolume.VolumeId)

	err = operation.WithMasterServerClient(false, s.MasterAddress, s.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
		req := &master_pb.LookupEcVolumeRequest{
			VolumeId: uint32(ecVolume.VolumeId),
		}
		resp, err := masterClient.LookupEcVolume(context.Background(), req)
		if err != nil {
			return fmt.Errorf("lookup ec volume %d: %v", ecVolume.VolumeId, err)
		}
		if len(resp.ShardIdLocations) < erasure_coding.DataShardsCount {
			return fmt.Errorf("only %d shards found but %d required", len(resp.ShardIdLocations), erasure_coding.DataShardsCount)
		}

		ecVolume.ShardLocationsLock.Lock()
		for _, shardIdLocations := range resp.ShardIdLocations {
			shardId := erasure_coding.ShardId(shardIdLocations.ShardId)
			delete(ecVolume.ShardLocations, shardId)
			for _, loc := range shardIdLocations.Locations {
				ecVolume.ShardLocations[shardId] = append(ecVolume.ShardLocations[shardId], pb.NewServerAddressFromLocation(loc))
			}
		}
		ecVolume.ShardLocationsRefreshTime = time.Now()
		ecVolume.ShardLocationsLock.Unlock()

		return nil
	})
	return
}

func (s *Store) readRemoteEcShardInterval(sourceDataNodes []pb.ServerAddress, needleId types.NeedleId, vid needle.VolumeId, shardId erasure_coding.ShardId, buf []byte, offset int64) (n int, is_deleted bool, err error) {

	if len(sourceDataNodes) == 0 {
		return 0, false, fmt.Errorf("failed to find ec shard %d.%d", vid, shardId)
	}

	for _, sourceDataNode := range sourceDataNodes {
		glog.V(3).Infof("read remote ec shard %d.%d from %s", vid, shardId, sourceDataNode)
		n, is_deleted, err = s.doReadRemoteEcShardInterval(sourceDataNode, needleId, vid, shardId, buf, offset)
		if err == nil {
			return
		}
		glog.V(1).Infof("read remote ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
	}

	return
}

func (s *Store) doReadRemoteEcShardInterval(sourceDataNode pb.ServerAddress, needleId types.NeedleId, vid needle.VolumeId, shardId erasure_coding.ShardId, buf []byte, offset int64) (n int, is_deleted bool, err error) {

	err = operation.WithVolumeServerClient(false, sourceDataNode, s.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {

		// copy data slice
		shardReadClient, err := client.VolumeEcShardRead(context.Background(), &volume_server_pb.VolumeEcShardReadRequest{
			VolumeId: uint32(vid),
			ShardId:  uint32(shardId),
			Offset:   offset,
			Size:     int64(len(buf)),
			FileKey:  uint64(needleId),
		})
		if err != nil {
			return fmt.Errorf("failed to start reading ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
		}

		for {
			resp, receiveErr := shardReadClient.Recv()
			if receiveErr == io.EOF {
				break
			}
			if receiveErr != nil {
				return fmt.Errorf("receiving ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, receiveErr)
			}
			if resp.IsDeleted {
				is_deleted = true
			}
			copy(buf[n:n+len(resp.Data)], resp.Data)
			n += len(resp.Data)
		}

		return nil
	})
	if err != nil {
		return 0, is_deleted, fmt.Errorf("read ec shard %d.%d from %s: %v", vid, shardId, sourceDataNode, err)
	}

	return
}

func (s *Store) recoverOneRemoteEcShardInterval(needleId types.NeedleId, ecVolume *erasure_coding.EcVolume, shardIdToRecover erasure_coding.ShardId, buf []byte, offset int64) (n int, is_deleted bool, err error) {
	glog.V(3).Infof("recover ec shard %d.%d from other locations", ecVolume.VolumeId, shardIdToRecover)

	enc, err := reedsolomon.New(erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create encoder: %v", err)
	}

	bufs := make([][]byte, erasure_coding.TotalShardsCount)

	var wg sync.WaitGroup
	ecVolume.ShardLocationsLock.RLock()
	for shardId, locations := range ecVolume.ShardLocations {

		// skip current shard or empty shard
		if shardId == shardIdToRecover {
			continue
		}
		if len(locations) == 0 {
			glog.V(3).Infof("readRemoteEcShardInterval missing %d.%d from %+v", ecVolume.VolumeId, shardId, locations)
			continue
		}

		// read from remote locations
		wg.Add(1)
		go func(shardId erasure_coding.ShardId, locations []pb.ServerAddress) {
			defer wg.Done()
			data := make([]byte, len(buf))
			nRead, isDeleted, readErr := s.readRemoteEcShardInterval(locations, needleId, ecVolume.VolumeId, shardId, data, offset)
			if readErr != nil {
				glog.V(3).Infof("recover: readRemoteEcShardInterval %d.%d %d bytes from %+v: %v", ecVolume.VolumeId, shardId, nRead, locations, readErr)
				forgetShardId(ecVolume, shardId)
			}
			if isDeleted {
				is_deleted = true
			}
			if nRead == len(buf) {
				bufs[shardId] = data
			}
		}(shardId, locations)
	}
	ecVolume.ShardLocationsLock.RUnlock()

	wg.Wait()

	if err = enc.ReconstructData(bufs); err != nil {
		glog.V(3).Infof("recovered ec shard %d.%d failed: %v", ecVolume.VolumeId, shardIdToRecover, err)
		return 0, false, err
	}
	glog.V(4).Infof("recovered ec shard %d.%d from other locations", ecVolume.VolumeId, shardIdToRecover)

	copy(buf, bufs[shardIdToRecover])

	return len(buf), is_deleted, nil
}

func (s *Store) EcVolumes() (ecVolumes []*erasure_coding.EcVolume) {
	for _, location := range s.Locations {
		location.ecVolumesLock.RLock()
		for _, v := range location.ecVolumes {
			ecVolumes = append(ecVolumes, v)
		}
		location.ecVolumesLock.RUnlock()
	}
	slices.SortFunc(ecVolumes, func(a, b *erasure_coding.EcVolume) int {
		return int(b.VolumeId - a.VolumeId)
	})
	return ecVolumes
}
