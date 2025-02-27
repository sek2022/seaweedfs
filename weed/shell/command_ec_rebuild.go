package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandEcRebuild{})
}

type commandEcRebuild struct {
}

func (c *commandEcRebuild) Name() string {
	return "ec.rebuild"
}

func (c *commandEcRebuild) Help() string {
	return `find and rebuild missing ec shards among volume servers

	ec.rebuild [-c EACH_COLLECTION|<collection_name>] [-force] [-concurrent maximum_concurrent_rebuilding]

	Algorithm:

	For each type of volume server (different max volume count limit){
		for each collection {
			rebuildEcVolumes()
		}
	}

	func rebuildEcVolumes(){
		idealWritableVolumes = totalWritableVolumes / numVolumeServers
		for {
			sort all volume servers ordered by the number of local writable volumes
			pick the volume server A with the lowest number of writable volumes x
			pick the volume server B with the highest number of writable volumes y
			if y > idealWritableVolumes and x +1 <= idealWritableVolumes {
				if B has a writable volume id v that A does not have {
					move writable volume v from A to B
				}
			}
		}
	}

`
}

func (c *commandEcRebuild) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcRebuild) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	fixCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := fixCommand.String("collection", "EACH_COLLECTION", "collection name, or \"EACH_COLLECTION\" for each collection")
	applyChanges := fixCommand.Bool("force", false, "apply the changes")
	concurrent := fixCommand.Int("concurrent", 3, "maximum number of concurrent rebuilding")

	if err = fixCommand.Parse(args); err != nil {
		return nil
	}
	infoAboutSimulationMode(writer, *applyChanges, "-force")
	maxConcurrent := *concurrent
	if maxConcurrent < 1 {
		return fmt.Errorf("concurrent must be greater than 0")
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// collect all ec nodes
	allEcNodes, _, err := collectEcNodes(commandEnv)
	if err != nil {
		return err
	}

	if *collection == "EACH_COLLECTION" {
		collections, err := ListCollectionNames(commandEnv, false, true)
		if err != nil {
			return err
		}
		fmt.Printf("rebuildEcVolumes collections %+v\n", len(collections))
		for _, c := range collections {
			fmt.Printf("rebuildEcVolumes collection %+v\n", c)
			if err = rebuildEcVolumes(commandEnv, allEcNodes, c, writer, *applyChanges, maxConcurrent); err != nil {
				return err
			}
		}
	} else {
		if err = rebuildEcVolumes(commandEnv, allEcNodes, *collection, writer, *applyChanges, maxConcurrent); err != nil {
			return err
		}
	}

	return nil
}

//func rebuildEcVolumes(commandEnv *CommandEnv, allEcNodes []*EcNode, collection string, writer io.Writer, applyChanges bool) error {
//
//	fmt.Printf("rebuildEcVolumes %s\n", collection)
//
//	// collect vid => each shard locations, similar to ecShardMap in topology.go
//	ecShardMap := make(EcShardMap)
//	for _, ecNode := range allEcNodes {
//		ecShardMap.registerEcNode(ecNode, collection)
//	}
//
//	for vid, locations := range ecShardMap {
//		shardCount := locations.shardCount()
//		if shardCount == erasure_coding.TotalShardsCount {
//			continue
//		}
//		if shardCount < erasure_coding.DataShardsCount {
//			return fmt.Errorf("ec volume %d is unrepairable with %d shards\n", vid, shardCount)
//		}
//
//		sortEcNodesByFreeslotsDescending(allEcNodes)
//
//		if allEcNodes[0].freeEcSlot < erasure_coding.TotalShardsCount {
//			return fmt.Errorf("disk space is not enough")
//		}
//
//		if err := rebuildOneEcVolume(commandEnv, allEcNodes[0], collection, vid, locations, writer, applyChanges); err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

func rebuildEcVolumes(commandEnv *CommandEnv, allEcNodes []*EcNode, collection string, writer io.Writer, applyChanges bool, maxConcurrent int) error {
	fmt.Printf("rebuildEcVolumes %s with max concurrent %d \n", collection, maxConcurrent)

	// collect vid => each shard locations
	ecShardMap := make(EcShardMap)
	for _, ecNode := range allEcNodes {
		ecShardMap.registerEcNode(ecNode, collection)
	}

	if len(allEcNodes) == 0 {
		return fmt.Errorf("no nodes available for rebuilding")
	}

	type rebuildTask struct {
		vid       needle.VolumeId
		locations EcShardLocations
	}

	var mapMu sync.Mutex // 添加互斥锁保护map操作

	// 获取需要重建的任务
	getTasksToRebuild := func() ([]rebuildTask, error) {
		mapMu.Lock()
		defer mapMu.Unlock()

		var tasks []rebuildTask
		for vid, locations := range ecShardMap {
			shardCount := locations.shardCount()
			if shardCount == erasure_coding.TotalShardsCount {
				continue
			}
			if shardCount < erasure_coding.DataShardsCount {
				return nil, fmt.Errorf("ec volume %d is unrepairable with %d shards\n", vid, shardCount)
			}
			tasks = append(tasks, rebuildTask{vid: vid, locations: locations})
		}
		return tasks, nil
	}

	var writerMu sync.Mutex
	safeWriter := &syncWriter{writer: writer, mu: &writerMu}

	// 循环处理所有任务
	for {
		// 对节点进行排序
		sortEcNodesByFreeslotsDescending(allEcNodes)

		// 计算可用节点数
		nodeCount := 0
		for i := 0; i < len(allEcNodes); i++ {
			if allEcNodes[i].freeEcSlot >= erasure_coding.TotalShardsCount {
				nodeCount++
				if nodeCount >= maxConcurrent {
					break
				}
			} else {
				break
			}
		}
		if nodeCount == 0 {
			return fmt.Errorf("no nodes have enough space for rebuilding")
		}

		// 获取待处理任务
		tasks, err := getTasksToRebuild()
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			break // 所有任务处理完成
		}

		// 创建错误通道和等待组
		errChan := make(chan error, nodeCount)
		var wg sync.WaitGroup

		// 并发处理任务，最多使用nodeCount个节点
		for i := 0; i < len(tasks) && i < nodeCount; i++ {
			wg.Add(1)
			go func(task rebuildTask, nodeIndex int) {
				defer wg.Done()
				if err := rebuildOneEcVolume(commandEnv, allEcNodes[nodeIndex], collection, task.vid, task.locations, safeWriter, applyChanges); err != nil {
					errChan <- err
				} else {
					// 处理完成后从ecShardMap中移除
					mapMu.Lock()
					delete(ecShardMap, task.vid)
					mapMu.Unlock()
				}
			}(tasks[i], i)
		}

		// 等待当前批次完成
		wg.Wait()
		close(errChan)

		// 检查错误
		for err := range errChan {
			if err != nil {
				return err
			}
		}
	}

	return nil
}

type syncWriter struct {
	writer io.Writer
	mu     *sync.Mutex
}

func (w *syncWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(p)
}

func rebuildOneEcVolume(commandEnv *CommandEnv, rebuilder *EcNode, collection string, volumeId needle.VolumeId, locations EcShardLocations, writer io.Writer, applyChanges bool) error {

	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	fmt.Printf("rebuildOneEcVolume %s %d\n", collection, volumeId)

	// collect shard files to rebuilder local disk
	var generatedShardIds []uint32
	copiedShardIds, _, err := prepareDataToRecover(commandEnv, rebuilder, collection, volumeId, locations, writer, applyChanges)
	if err != nil {
		return err
	}
	defer func() {
		// clean up working files

		// ask the rebuilder to delete the copied shards
		err = sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info), copiedShardIds)
		if err != nil {
			fmt.Fprintf(writer, "%s delete copied ec shards %s %d.%v\n", rebuilder.info.Id, collection, volumeId, copiedShardIds)
		}

	}()

	if !applyChanges {
		return nil
	}

	// generate ec shards, and maybe ecx file
	generatedShardIds, err = generateMissingShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info))
	if err != nil {
		return err
	}

	// mount the generated shards
	err = mountEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(rebuilder.info), generatedShardIds)
	if err != nil {
		return err
	}

	rebuilder.addEcVolumeShards(volumeId, collection, generatedShardIds)

	return nil
}

func generateMissingShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress) (rebuiltShardIds []uint32, err error) {

	err = operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		resp, rebuildErr := volumeServerClient.VolumeEcShardsRebuild(context.Background(), &volume_server_pb.VolumeEcShardsRebuildRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
		})
		if rebuildErr == nil {
			rebuiltShardIds = resp.RebuiltShardIds
		}
		return rebuildErr
	})
	return
}

func prepareDataToRecover(commandEnv *CommandEnv, rebuilder *EcNode, collection string, volumeId needle.VolumeId, locations EcShardLocations, writer io.Writer, applyBalancing bool) (copiedShardIds []uint32, localShardIds []uint32, err error) {

	needEcxFile := true
	var localShardBits erasure_coding.ShardBits
	for _, diskInfo := range rebuilder.info.DiskInfos {
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			if ecShardInfo.Collection == collection && needle.VolumeId(ecShardInfo.Id) == volumeId {
				needEcxFile = false
				localShardBits = erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
			}
		}
	}

	for shardId, ecNodes := range locations {

		if len(ecNodes) == 0 {
			fmt.Fprintf(writer, "missing shard %d.%d\n", volumeId, shardId)
			continue
		}

		if localShardBits.HasShardId(erasure_coding.ShardId(shardId)) {
			localShardIds = append(localShardIds, uint32(shardId))
			fmt.Fprintf(writer, "use existing shard %d.%d\n", volumeId, shardId)
			continue
		}

		var copyErr error
		if applyBalancing {
			copyErr = operation.WithVolumeServerClient(false, pb.NewServerAddressFromDataNode(rebuilder.info), commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
				_, copyErr := volumeServerClient.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
					VolumeId:       uint32(volumeId),
					Collection:     collection,
					ShardIds:       []uint32{uint32(shardId)},
					CopyEcxFile:    needEcxFile,
					CopyEcjFile:    true,
					CopyVifFile:    needEcxFile,
					SourceDataNode: ecNodes[0].info.Id,
				})
				return copyErr
			})
			if copyErr == nil && needEcxFile {
				needEcxFile = false
			}
		}
		if copyErr != nil {
			fmt.Fprintf(writer, "%s failed to copy %d.%d from %s: %v\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id, copyErr)
		} else {
			fmt.Fprintf(writer, "%s copied %d.%d from %s\n", rebuilder.info.Id, volumeId, shardId, ecNodes[0].info.Id)
			copiedShardIds = append(copiedShardIds, uint32(shardId))
		}

	}

	if len(copiedShardIds)+len(localShardIds) >= erasure_coding.DataShardsCount {
		return copiedShardIds, localShardIds, nil
	}

	return nil, nil, fmt.Errorf("%d shards are not enough to recover volume %d", len(copiedShardIds)+len(localShardIds), volumeId)

}

type EcShardMap map[needle.VolumeId]EcShardLocations
type EcShardLocations [][]*EcNode

func (ecShardMap EcShardMap) registerEcNode(ecNode *EcNode, collection string) {
	for _, diskInfo := range ecNode.info.DiskInfos {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if shardInfo.Collection == collection {
				existing, found := ecShardMap[needle.VolumeId(shardInfo.Id)]
				if !found {
					existing = make([][]*EcNode, erasure_coding.TotalShardsCount)
					ecShardMap[needle.VolumeId(shardInfo.Id)] = existing
				}
				for _, shardId := range erasure_coding.ShardBits(shardInfo.EcIndexBits).ShardIds() {
					existing[shardId] = append(existing[shardId], ecNode)
				}
			}
		}
	}
}

func (ecShardLocations EcShardLocations) shardCount() (count int) {
	for _, locations := range ecShardLocations {
		if len(locations) > 0 {
			count++
		}
	}
	return
}
