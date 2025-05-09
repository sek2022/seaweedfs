package shell

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
)

type DataCenterId string
type EcNodeId string
type RackId string

type EcNode struct {
	info       *master_pb.DataNodeInfo
	dc         DataCenterId
	rack       RackId
	freeEcSlot int
}
type CandidateEcNode struct {
	ecNode     *EcNode
	shardCount int
}

type EcRack struct {
	ecNodes    map[EcNodeId]*EcNode
	freeEcSlot int
}

type EcBusyServerProcessor struct {
	busyServers     map[string]needle.VolumeId
	busyServersLock sync.RWMutex
}

type VolumeEcAllocator struct {
	VolumeId       needle.VolumeId
	Choose         wdclient.Location                       //coding server
	Locations      []wdclient.Location                     //all location for volume
	AllocatedEcIds map[string]*volume_server_pb.EcShardIds //ec shards 分配
	AllocatedNodes map[string]*EcNode                      //分配到的node
}

func (ecBs *EcBusyServerProcessor) add(server string, volumeId needle.VolumeId) {
	ecBs.busyServersLock.Lock()
	defer ecBs.busyServersLock.Unlock()
	ecBs.busyServers[server] = volumeId
}

func (ecBs *EcBusyServerProcessor) inUse(server string) bool {
	ecBs.busyServersLock.RLock()
	defer ecBs.busyServersLock.RUnlock()
	if v, b := ecBs.busyServers[server]; b {
		if v > 0 {
			return true
		}
	}

	return false
}

func (ecBs *EcBusyServerProcessor) remove(server string) {
	ecBs.busyServersLock.Lock()
	defer ecBs.busyServersLock.Unlock()
	ecBs.busyServers[server] = 0

}

func NewEcBusyServerProcessor() *EcBusyServerProcessor {
	processor := &EcBusyServerProcessor{busyServers: make(map[string]needle.VolumeId)}
	return processor
}

var (
	ecBalanceAlgorithmDescription = `
	func EcBalance() {
		for each collection:
			balanceEcVolumes(collectionName)
		for each rack:
			balanceEcRack(rack)
	}

	func balanceEcVolumes(collectionName){
		for each volume:
			doDeduplicateEcShards(volumeId)

		tracks rack~shardCount mapping
		for each volume:
			doBalanceEcShardsAcrossRacks(volumeId)

		for each volume:
			doBalanceEcShardsWithinRacks(volumeId)
	}

	// spread ec shards into more racks
	func doBalanceEcShardsAcrossRacks(volumeId){
		tracks rack~volumeIdShardCount mapping
		averageShardsPerEcRack = totalShardNumber / numRacks  // totalShardNumber is 14 for now, later could varies for each dc
		ecShardsToMove = select overflown ec shards from racks with ec shard counts > averageShardsPerEcRack
		for each ecShardsToMove {
			destRack = pickOneRack(rack~shardCount, rack~volumeIdShardCount, ecShardReplicaPlacement)
			destVolumeServers = volume servers on the destRack
			pickOneEcNodeAndMoveOneShard(destVolumeServers)
		}
	}

	func doBalanceEcShardsWithinRacks(volumeId){
		racks = collect all racks that the volume id is on
		for rack, shards := range racks
			doBalanceEcShardsWithinOneRack(volumeId, shards, rack)
	}

	// move ec shards
	func doBalanceEcShardsWithinOneRack(volumeId, shards, rackId){
		tracks volumeServer~volumeIdShardCount mapping
		averageShardCount = len(shards) / numVolumeServers
		volumeServersOverAverage = volume servers with volumeId's ec shard counts > averageShardsPerEcRack
		ecShardsToMove = select overflown ec shards from volumeServersOverAverage
		for each ecShardsToMove {
			destVolumeServer = pickOneVolumeServer(volumeServer~shardCount, volumeServer~volumeIdShardCount, ecShardReplicaPlacement)
			pickOneEcNodeAndMoveOneShard(destVolumeServers)
		}
	}

	// move ec shards while keeping shard distribution for the same volume unchanged or more even
	func balanceEcRack(rack){
		averageShardCount = total shards / numVolumeServers
		for hasMovedOneEcShard {
			sort all volume servers ordered by the number of local ec shards
			pick the volume server A with the lowest number of ec shards x
			pick the volume server B with the highest number of ec shards y
			if y > averageShardCount and x +1 <= averageShardCount {
				if B has a ec shard with volume id v that A does not have {
					move one ec shard v from B to A
					hasMovedOneEcShard = true
				}
			}
		}
	}
	`
	// Overridable functions for testing.
	getDefaultReplicaPlacement = _getDefaultReplicaPlacement

	ecBusyServerProcessor = NewEcBusyServerProcessor()
)

type ErrorWaitGroup struct {
	maxConcurrency int
	wg             *sync.WaitGroup
	wgSem          chan bool
	errors         []error
	errorsMu       sync.Mutex
}
type ErrorWaitGroupTask func() error

func NewErrorWaitGroup(maxConcurrency int) *ErrorWaitGroup {
	if maxConcurrency <= 0 {
		// No concurrency = one task at the time
		maxConcurrency = 1
	}
	return &ErrorWaitGroup{
		maxConcurrency: maxConcurrency,
		wg:             &sync.WaitGroup{},
		wgSem:          make(chan bool, maxConcurrency),
	}
}

func (ewg *ErrorWaitGroup) Add(f ErrorWaitGroupTask) {
	if ewg.maxConcurrency <= 1 {
		// Keep run order deterministic when parallelization is off
		ewg.errors = append(ewg.errors, f())
		return
	}

	ewg.wg.Add(1)
	go func() {
		ewg.wgSem <- true

		err := f()
		ewg.errorsMu.Lock()
		ewg.errors = append(ewg.errors, err)
		ewg.errorsMu.Unlock()

		<-ewg.wgSem
		ewg.wg.Done()
	}()
}

func (ewg *ErrorWaitGroup) Wait() error {
	ewg.wg.Wait()
	return errors.Join(ewg.errors...)
}

func _getDefaultReplicaPlacement(commandEnv *CommandEnv) (*super_block.ReplicaPlacement, error) {
	var resp *master_pb.GetMasterConfigurationResponse
	var err error

	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
		return err
	})
	if err != nil {
		return nil, err
	}

	return super_block.NewReplicaPlacementFromString(resp.DefaultReplication)
}

func parseReplicaPlacementArg(commandEnv *CommandEnv, replicaStr string) (*super_block.ReplicaPlacement, error) {
	var rp *super_block.ReplicaPlacement
	var err error

	if replicaStr != "" {
		rp, err = super_block.NewReplicaPlacementFromString(replicaStr)
		if err != nil {
			return rp, err
		}
		fmt.Printf("using replica placement %q for EC volumes\n", rp.String())
	} else {
		// No replica placement argument provided, resolve from master default settings.
		rp, err = getDefaultReplicaPlacement(commandEnv)
		if err != nil {
			return rp, err
		}
		fmt.Printf("using master default replica placement %q for EC volumes\n", rp.String())
	}

	return rp, nil
}

func collectTopologyInfo(commandEnv *CommandEnv, delayBeforeCollecting time.Duration) (topoInfo *master_pb.TopologyInfo, volumeSizeLimitMb uint64, err error) {

	if delayBeforeCollecting > 0 {
		time.Sleep(delayBeforeCollecting)
	}

	var resp *master_pb.VolumeListResponse
	err = commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return
	}

	return resp.TopologyInfo, resp.VolumeSizeLimitMb, nil

}

func collectEcNodesForDC(commandEnv *CommandEnv, selectedDataCenter string) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {
	// list all possible locations
	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots = collectEcVolumeServersByDc(topologyInfo, selectedDataCenter)

	sortEcNodesByFreeslotsDescending(ecNodes)

	return
}

func collectEcNodes(commandEnv *CommandEnv) (ecNodes []*EcNode, totalFreeEcSlots int, err error) {
	return collectEcNodesForDC(commandEnv, "")
}

func moveMountedShardToEcNode(commandEnv *CommandEnv, existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, destinationEcNode *EcNode, applyBalancing bool) (err error) {

	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	copiedShardIds := []uint32{uint32(shardId)}

	if applyBalancing {

		existingServerAddress := pb.NewServerAddressFromDataNode(existingLocation.info)

		// ask destination node to copy shard and the ecx file from source node, and mount it
		copiedShardIds, err = oneServerCopyAndMountEcShardsFromSource(commandEnv.option.GrpcDialOption, destinationEcNode, []uint32{uint32(shardId)}, vid, collection, existingServerAddress)
		if err != nil {
			return err
		}

		// unmount the to be deleted shards
		err = unmountEcShards(commandEnv.option.GrpcDialOption, vid, existingServerAddress, copiedShardIds)
		if err != nil {
			return err
		}

		// ask source node to delete the shard, and maybe the ecx file
		err = sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, vid, existingServerAddress, copiedShardIds)
		if err != nil {
			return err
		}

		fmt.Printf("moved ec shard %d.%d %s => %s\n", vid, shardId, existingLocation.info.Id, destinationEcNode.info.Id)

	}

	destinationEcNode.addEcVolumeShards(vid, collection, copiedShardIds)
	existingLocation.deleteEcVolumeShards(vid, copiedShardIds)

	return nil

}

func oneServerCopyAndMountEcShardsFromSource(grpcDialOption grpc.DialOption,
	targetServer *EcNode, shardIdsToCopy []uint32,
	volumeId needle.VolumeId, collection string, existingLocation pb.ServerAddress) (copiedShardIds []uint32, err error) {

	fmt.Printf("allocate %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id)

	targetAddress := pb.NewServerAddressFromDataNode(targetServer.info)
	err = operation.WithVolumeServerClient(false, targetAddress, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {

		if targetAddress != existingLocation {
			fmt.Printf("copy %d.%v %s => %s\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id)
			_, copyErr := volumeServerClient.VolumeEcShardsCopy(context.Background(), &volume_server_pb.VolumeEcShardsCopyRequest{
				VolumeId:       uint32(volumeId),
				Collection:     collection,
				ShardIds:       shardIdsToCopy,
				CopyEcxFile:    true,
				CopyEcjFile:    true,
				CopyVifFile:    true,
				SourceDataNode: string(existingLocation),
			})
			if copyErr != nil {
				return fmt.Errorf("copy %d.%v %s => %s : %v\n", volumeId, shardIdsToCopy, existingLocation, targetServer.info.Id, copyErr)
			}
		}

		fmt.Printf("mount %d.%v on %s\n", volumeId, shardIdsToCopy, targetServer.info.Id)
		_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   shardIdsToCopy,
		})
		if mountErr != nil {
			return fmt.Errorf("mount %d.%v on %s : %v\n", volumeId, shardIdsToCopy, targetServer.info.Id, mountErr)
		}

		if targetAddress != existingLocation {
			copiedShardIds = shardIdsToCopy
			glog.V(0).Infof("%s ec volume %d deletes shards %+v", existingLocation, volumeId, copiedShardIds)
		}

		return nil
	})

	if err != nil {
		return
	}

	return
}

func oneServerMountEcShards(grpcDialOption grpc.DialOption, targetServer *EcNode, shardIdsToCopy []uint32,
	volumeId needle.VolumeId, collection string) (err error) {

	fmt.Printf("allocate %d.%v => %s\n", volumeId, shardIdsToCopy, targetServer.info.Id)

	targetAddress := pb.NewServerAddressFromDataNode(targetServer.info)
	err = operation.WithVolumeServerClient(false, targetAddress, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		fmt.Printf("mount %d.%v on %s\n", volumeId, shardIdsToCopy, targetServer.info.Id)
		_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   shardIdsToCopy,
		})
		if mountErr != nil {
			return fmt.Errorf("mount %d.%v on %s : %v\n", volumeId, shardIdsToCopy, targetServer.info.Id, mountErr)
		}
		return nil
	})

	if err != nil {
		return
	}

	return
}

// func eachDataNode(topo *master_pb.TopologyInfo, fn func(dc string, rack RackId, dn *master_pb.DataNodeInfo)) {
func eachDataNode(topo *master_pb.TopologyInfo, fn func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo)) {
	for _, dc := range topo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			for _, dn := range rack.DataNodeInfos {
				fn(DataCenterId(dc.Id), RackId(rack.Id), dn)
			}
		}
	}
}

func sortEcNodesByFreeslotsDescending(ecNodes []*EcNode) {
	slices.SortFunc(ecNodes, func(a, b *EcNode) int {
		return b.freeEcSlot - a.freeEcSlot
	})
}

func sortEcNodesByFreeslotsAscending(ecNodes []*EcNode) {
	slices.SortFunc(ecNodes, func(a, b *EcNode) int {
		return a.freeEcSlot - b.freeEcSlot
	})
}

// volume id sort
func sortVolumeIdsAscending(volumeIds []needle.VolumeId) {
	slices.SortFunc(volumeIds, func(a, b needle.VolumeId) int {
		return int(a) - int(b)
	})
}

//type CandidateEcNode struct {
//	ecNode     *EcNode
//	shardCount int
//}

// if the index node changed the freeEcSlot, need to keep every EcNode still sorted
func ensureSortedEcNodes(data []*CandidateEcNode, index int, lessThan func(i, j int) bool) {
	for i := index - 1; i >= 0; i-- {
		if lessThan(i+1, i) {
			swap(data, i, i+1)
		} else {
			break
		}
	}
	for i := index + 1; i < len(data); i++ {
		if lessThan(i, i-1) {
			swap(data, i, i-1)
		} else {
			break
		}
	}
}

func swap(data []*CandidateEcNode, i, j int) {
	t := data[i]
	data[i] = data[j]
	data[j] = t
}

func countShards(ecShardInfos []*master_pb.VolumeEcShardInformationMessage) (count int) {
	for _, ecShardInfo := range ecShardInfos {
		shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
		count += shardBits.ShardIdCount()
	}
	return
}

func countFreeShardSlots(dn *master_pb.DataNodeInfo, diskType types.DiskType) (count int) {
	if dn.DiskInfos == nil {
		return 0
	}
	diskInfo := dn.DiskInfos[string(diskType)]
	if diskInfo == nil {
		return 0
	}

	slots := int(diskInfo.MaxVolumeCount-diskInfo.VolumeCount)*erasure_coding.DataShardsCount - countShards(diskInfo.EcShardInfos)
	if slots < 0 {
		return 0
	}

	return slots
}

//type RackId string
//type EcNodeId string
//
//type EcNode struct {
//	info       *master_pb.DataNodeInfo
//	dc         string
//	rack       RackId
//	freeEcSlot int
//}

func (ecNode *EcNode) String() string {
	return fmt.Sprintf("dc:%s, rack:%s, freeEcSlot:%d, info id:%+v", ecNode.dc, ecNode.rack, ecNode.freeEcSlot, ecNode.info.Id)
}

func (ecNode *EcNode) localShardIdCount(vid uint32) int {
	for _, diskInfo := range ecNode.info.DiskInfos {
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			if vid == ecShardInfo.Id {
				shardBits := erasure_coding.ShardBits(ecShardInfo.EcIndexBits)
				return shardBits.ShardIdCount()
			}
		}
	}
	return 0
}

func collectEcVolumeServersByDc(topo *master_pb.TopologyInfo, selectedDataCenter string) (ecNodes []*EcNode, totalFreeEcSlots int) {
	eachDataNode(topo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		if selectedDataCenter != "" && selectedDataCenter != string(dc) {
			return
		}

		freeEcSlots := countFreeShardSlots(dn, types.HardDriveType)
		ecNodes = append(ecNodes, &EcNode{
			info:       dn,
			dc:         dc,
			rack:       rack,
			freeEcSlot: int(freeEcSlots),
		})
		totalFreeEcSlots += freeEcSlots
	})
	return
}

func sourceServerDeleteEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeDeletedShardIds []uint32) error {

	fmt.Printf("delete %d.%v from %s\n", volumeId, toBeDeletedShardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   toBeDeletedShardIds,
		})
		return deleteErr
	})

}

func unmountEcShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeUnmountedhardIds []uint32) error {

	fmt.Printf("unmount %d.%v from %s\n", volumeId, toBeUnmountedhardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, deleteErr := volumeServerClient.VolumeEcShardsUnmount(context.Background(), &volume_server_pb.VolumeEcShardsUnmountRequest{
			VolumeId: uint32(volumeId),
			ShardIds: toBeUnmountedhardIds,
		})
		return deleteErr
	})
}

func mountEcShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceLocation pb.ServerAddress, toBeMountedhardIds []uint32) error {

	fmt.Printf("mount %d.%v on %s\n", volumeId, toBeMountedhardIds, sourceLocation)

	return operation.WithVolumeServerClient(false, sourceLocation, grpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, mountErr := volumeServerClient.VolumeEcShardsMount(context.Background(), &volume_server_pb.VolumeEcShardsMountRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   toBeMountedhardIds,
		})
		return mountErr
	})
}

func ceilDivide(a, b int) int {
	var r int
	if (a % b) != 0 {
		r = 1
	}
	return (a / b) + r
}

func findEcVolumeShards(ecNode *EcNode, vid needle.VolumeId) erasure_coding.ShardBits {

	if diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]; found {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if needle.VolumeId(shardInfo.Id) == vid {
				return erasure_coding.ShardBits(shardInfo.EcIndexBits)
			}
		}
	}

	return 0
}

func (ecNode *EcNode) addEcVolumeShards(vid needle.VolumeId, collection string, shardIds []uint32) *EcNode {

	foundVolume := false
	diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]
	if found {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if needle.VolumeId(shardInfo.Id) == vid {
				oldShardBits := erasure_coding.ShardBits(shardInfo.EcIndexBits)
				newShardBits := oldShardBits
				for _, shardId := range shardIds {
					newShardBits = newShardBits.AddShardId(erasure_coding.ShardId(shardId))
				}
				shardInfo.EcIndexBits = uint32(newShardBits)
				ecNode.freeEcSlot -= newShardBits.ShardIdCount() - oldShardBits.ShardIdCount()
				foundVolume = true
				break
			}
		}
	} else {
		diskInfo = &master_pb.DiskInfo{
			Type: string(types.HardDriveType),
		}
		ecNode.info.DiskInfos[string(types.HardDriveType)] = diskInfo
	}

	if !foundVolume {
		var newShardBits erasure_coding.ShardBits
		for _, shardId := range shardIds {
			newShardBits = newShardBits.AddShardId(erasure_coding.ShardId(shardId))
		}
		diskInfo.EcShardInfos = append(diskInfo.EcShardInfos, &master_pb.VolumeEcShardInformationMessage{
			Id:          uint32(vid),
			Collection:  collection,
			EcIndexBits: uint32(newShardBits),
			DiskType:    string(types.HardDriveType),
		})
		ecNode.freeEcSlot -= len(shardIds)
	}

	return ecNode
}

func (ecNode *EcNode) deleteEcVolumeShards(vid needle.VolumeId, shardIds []uint32) *EcNode {

	if diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]; found {
		for _, shardInfo := range diskInfo.EcShardInfos {
			if needle.VolumeId(shardInfo.Id) == vid {
				oldShardBits := erasure_coding.ShardBits(shardInfo.EcIndexBits)
				newShardBits := oldShardBits
				for _, shardId := range shardIds {
					newShardBits = newShardBits.RemoveShardId(erasure_coding.ShardId(shardId))
				}
				shardInfo.EcIndexBits = uint32(newShardBits)
				ecNode.freeEcSlot -= newShardBits.ShardIdCount() - oldShardBits.ShardIdCount()
			}
		}
	}

	return ecNode
}

func groupByCount(data []*EcNode, identifierFn func(*EcNode) (id string, count int)) map[string]int {
	countMap := make(map[string]int)
	for _, d := range data {
		id, count := identifierFn(d)
		countMap[id] += count
	}
	return countMap
}

func groupBy(data []*EcNode, identifierFn func(*EcNode) (id string)) map[string][]*EcNode {
	groupMap := make(map[string][]*EcNode)
	for _, d := range data {
		id := identifierFn(d)
		groupMap[id] = append(groupMap[id], d)
	}
	return groupMap
}

type ecBalancer struct {
	commandEnv         *CommandEnv
	ecNodes            []*EcNode
	replicaPlacement   *super_block.ReplicaPlacement
	applyBalancing     bool
	maxMoveShards      int        // 最大移动分片数量限制
	maxParallelization int        // 最大并行任务数量限制
	movedShardsLock    sync.Mutex // 添加互斥锁来保护 movedShards
}

func (ecb *ecBalancer) errorWaitGroup() *ErrorWaitGroup {
	return NewErrorWaitGroup(ecb.maxParallelization)
}

func (ecb *ecBalancer) racks() map[RackId]*EcRack {
	racks := make(map[RackId]*EcRack)
	for _, ecNode := range ecb.ecNodes {
		if racks[ecNode.rack] == nil {
			racks[ecNode.rack] = &EcRack{
				ecNodes: make(map[EcNodeId]*EcNode),
			}
		}
		racks[ecNode.rack].ecNodes[EcNodeId(ecNode.info.Id)] = ecNode
		racks[ecNode.rack].freeEcSlot += ecNode.freeEcSlot
	}
	return racks
}

func (ecb *ecBalancer) balanceEcVolumes(collection string) error {

	fmt.Printf("balanceEcVolumes %s, maxParallelization: %d, maxMoveShards: %d\n", collection, ecb.maxParallelization, ecb.maxMoveShards)

	// 跟踪已移动的分片数量
	movedShards := 0
	if err := ecb.deleteDuplicatedEcShards(collection); err != nil {
		return fmt.Errorf("delete duplicated collection %s ec shards: %v", collection, err)
	}
	// 如果设置了最大移动分片数量限制，并且已移动的分片数量达到限制，则提前结束
	if ecb.maxMoveShards > 0 && movedShards >= ecb.maxMoveShards {
		fmt.Printf("已达到最大移动分片数量限制 %d，停止平衡操作\n", ecb.maxMoveShards)
		return nil
	}
	// 添加计数器，限制机架间分片移动
	if err := ecb.balanceEcShardsAcrossRacksWithLimit(collection, &movedShards); err != nil {
		return fmt.Errorf("balance across racks collection %s ec shards: %v", collection, err)
	}
	// 如果设置了最大移动分片数量限制，并且已移动的分片数量达到限制，则提前结束
	if ecb.maxMoveShards > 0 && movedShards >= ecb.maxMoveShards {
		fmt.Printf("已达到最大移动分片数量限制 %d，停止平衡操作\n", ecb.maxMoveShards)
		return nil
	}

	// 添加计数器，限制机架内分片移动
	if err := ecb.balanceEcShardsWithinRacksWithLimit(collection, &movedShards); err != nil {
		return fmt.Errorf("balance within racks collection %s ec shards: %v", collection, err)
	}
	return nil
}

func (ecb *ecBalancer) deleteDuplicatedEcShards(collection string) error {
	// vid => []ecNode
	vidLocations := ecb.collectVolumeIdToEcNodes(collection)
	// deduplicate ec shards
	for vid, locations := range vidLocations {
		if err := ecb.doDeduplicateEcShards(collection, vid, locations); err != nil {
			return err
		}
	}
	return nil
}

func (ecb *ecBalancer) doDeduplicateEcShards(collection string, vid needle.VolumeId, locations []*EcNode) error {
	// check whether this volume has ecNodes that are over average
	shardToLocations := make([][]*EcNode, erasure_coding.TotalShardsCount)
	for _, ecNode := range locations {
		shardBits := findEcVolumeShards(ecNode, vid)
		for _, shardId := range shardBits.ShardIds() {
			shardToLocations[shardId] = append(shardToLocations[shardId], ecNode)
		}
	}
	for shardId, ecNodes := range shardToLocations {
		if len(ecNodes) <= 1 {
			continue
		}
		sortEcNodesByFreeslotsAscending(ecNodes)
		fmt.Printf("ec shard %d.%d has %d copies, keeping %v\n", vid, shardId, len(ecNodes), ecNodes[0].info.Id)
		if !ecb.applyBalancing {
			continue
		}

		duplicatedShardIds := []uint32{uint32(shardId)}
		for _, ecNode := range ecNodes[1:] {
			if err := unmountEcShards(ecb.commandEnv.option.GrpcDialOption, vid, pb.NewServerAddressFromDataNode(ecNode.info), duplicatedShardIds); err != nil {
				return err
			}
			if err := sourceServerDeleteEcShards(ecb.commandEnv.option.GrpcDialOption, collection, vid, pb.NewServerAddressFromDataNode(ecNode.info), duplicatedShardIds); err != nil {
				return err
			}
			ecNode.deleteEcVolumeShards(vid, duplicatedShardIds)
		}
	}
	return nil
}

func (ecb *ecBalancer) balanceEcShardsAcrossRacks(collection string) error {
	// collect vid => []ecNode, since previous steps can change the locations
	vidLocations := ecb.collectVolumeIdToEcNodes(collection)
	// spread the ec shards evenly
	for vid, locations := range vidLocations {
		if err := ecb.doBalanceEcShardsAcrossRacks(collection, vid, locations); err != nil {
			return err
		}
	}
	return nil
}

func countShardsByRack(vid needle.VolumeId, locations []*EcNode) map[string]int {
	return groupByCount(locations, func(ecNode *EcNode) (id string, count int) {
		shardBits := findEcVolumeShards(ecNode, vid)
		return string(ecNode.rack), shardBits.ShardIdCount()
	})
}

func (ecb *ecBalancer) doBalanceEcShardsAcrossRacks(collection string, vid needle.VolumeId, locations []*EcNode) error {
	racks := ecb.racks()

	// calculate average number of shards an ec rack should have for one volume
	averageShardsPerEcRack := ceilDivide(erasure_coding.TotalShardsCount, len(racks))

	// see the volume's shards are in how many racks, and how many in each rack
	rackToShardCount := countShardsByRack(vid, locations)
	rackEcNodesWithVid := groupBy(locations, func(ecNode *EcNode) string {
		return string(ecNode.rack)
	})

	// ecShardsToMove = select overflown ec shards from racks with ec shard counts > averageShardsPerEcRack
	ecShardsToMove := make(map[erasure_coding.ShardId]*EcNode)
	for rackId, count := range rackToShardCount {
		if count <= averageShardsPerEcRack {
			continue
		}
		possibleEcNodes := rackEcNodesWithVid[rackId]
		for shardId, ecNode := range pickNEcShardsToMoveFrom(possibleEcNodes, vid, count-averageShardsPerEcRack) {
			ecShardsToMove[shardId] = ecNode
		}
	}

	for shardId, ecNode := range ecShardsToMove {
		rackId, err := ecb.pickRackToBalanceShardsInto(racks, rackToShardCount, averageShardsPerEcRack)
		if err != nil {
			fmt.Printf("ec shard %d.%d at %s can not find a destination rack:\n%s\n", vid, shardId, ecNode.info.Id, err.Error())
			continue
		}

		var possibleDestinationEcNodes []*EcNode
		for _, n := range racks[rackId].ecNodes {
			possibleDestinationEcNodes = append(possibleDestinationEcNodes, n)
		}
		err = ecb.pickOneEcNodeAndMoveOneShard(ecNode, collection, vid, shardId, possibleDestinationEcNodes)
		if err != nil {
			return err
		}
		rackToShardCount[string(rackId)] += 1
		rackToShardCount[string(ecNode.rack)] -= 1
		racks[rackId].freeEcSlot -= 1
		racks[ecNode.rack].freeEcSlot += 1
	}

	return nil
}

func (ecb *ecBalancer) pickRackToBalanceShardsInto(rackToEcNodes map[RackId]*EcRack, rackToShardCount map[string]int, averageShardsPerEcRack int) (RackId, error) {
	targets := []RackId{}
	targetShards := -1
	for _, shards := range rackToShardCount {
		if shards > targetShards {
			targetShards = shards
		}
	}

	details := ""
	for rackId, rack := range rackToEcNodes {
		shards := rackToShardCount[string(rackId)]

		if rack.freeEcSlot <= 0 {
			details += fmt.Sprintf("  Skipped %s because it has no free slots\n", rackId)
			continue
		}

		//fmt.Printf("rack %s has %d shards, diffRackCount: %d\n", rackId, shards, ecb.replicaPlacement.DiffRackCount)
		if ecb.replicaPlacement != nil && shards > ecb.replicaPlacement.DiffRackCount && shards >= averageShardsPerEcRack {
			details += fmt.Sprintf("  Skipped %s because shards %d >= replica placement limit for other racks (%d)\n", rackId, shards, ecb.replicaPlacement.DiffRackCount)
			continue
		}

		if shards < targetShards {
			// Favor racks with less shards, to ensure an uniform distribution.
			targets = nil
			targetShards = shards
		}
		if shards == targetShards {
			targets = append(targets, rackId)
		}
	}

	if len(targets) == 0 {
		return "", errors.New(details)
	}
	return targets[rand.IntN(len(targets))], nil
}

func (ecb *ecBalancer) balanceEcShardsWithinRacks(collection string) error {
	// collect vid => []ecNode, since previous steps can change the locations
	vidLocations := ecb.collectVolumeIdToEcNodes(collection)
	racks := ecb.racks()

	// spread the ec shards evenly
	for vid, locations := range vidLocations {

		// see the volume's shards are in how many racks, and how many in each rack
		rackToShardCount := countShardsByRack(vid, locations)
		rackEcNodesWithVid := groupBy(locations, func(ecNode *EcNode) string {
			return string(ecNode.rack)
		})

		for rackId, _ := range rackToShardCount {

			var possibleDestinationEcNodes []*EcNode
			for _, n := range racks[RackId(rackId)].ecNodes {
				if _, found := n.info.DiskInfos[string(types.HardDriveType)]; found {
					possibleDestinationEcNodes = append(possibleDestinationEcNodes, n)
				}
			}
			sourceEcNodes := rackEcNodesWithVid[rackId]
			averageShardsPerEcNode := ceilDivide(rackToShardCount[rackId], len(possibleDestinationEcNodes))
			if err := ecb.doBalanceEcShardsWithinOneRack(averageShardsPerEcNode, collection, vid, sourceEcNodes, possibleDestinationEcNodes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (ecb *ecBalancer) doBalanceEcShardsWithinOneRack(averageShardsPerEcNode int, collection string, vid needle.VolumeId, existingLocations, possibleDestinationEcNodes []*EcNode) error {
	for _, ecNode := range existingLocations {

		shardBits := findEcVolumeShards(ecNode, vid)
		overLimitCount := shardBits.ShardIdCount() - averageShardsPerEcNode

		for _, shardId := range shardBits.ShardIds() {

			if overLimitCount <= 0 {
				break
			}

			fmt.Printf("%s has %d overlimit, moving ec shard %d.%d\n", ecNode.info.Id, overLimitCount, vid, shardId)

			err := ecb.pickOneEcNodeAndMoveOneShard(ecNode, collection, vid, shardId, possibleDestinationEcNodes)
			if err != nil {
				return err
			}

			overLimitCount--
		}
	}

	return nil
}

func (ecb *ecBalancer) balanceEcRacks() error {
	// balance one rack for all ec shards
	for _, ecRack := range ecb.racks() {
		if err := ecb.doBalanceEcRack(ecRack); err != nil {
			return err
		}
	}
	return nil
}

func (ecb *ecBalancer) doBalanceEcRack(ecRack *EcRack) error {
	if len(ecRack.ecNodes) <= 1 {
		return nil
	}

	var rackEcNodes []*EcNode
	for _, node := range ecRack.ecNodes {
		rackEcNodes = append(rackEcNodes, node)
	}

	ecNodeIdToShardCount := groupByCount(rackEcNodes, func(ecNode *EcNode) (id string, count int) {
		diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]
		if !found {
			return
		}
		for _, ecShardInfo := range diskInfo.EcShardInfos {
			count += erasure_coding.ShardBits(ecShardInfo.EcIndexBits).ShardIdCount()
		}
		return ecNode.info.Id, count
	})

	var totalShardCount int
	for _, count := range ecNodeIdToShardCount {
		totalShardCount += count
	}

	averageShardCount := ceilDivide(totalShardCount, len(rackEcNodes))

	hasMove := true
	for hasMove {
		hasMove = false
		slices.SortFunc(rackEcNodes, func(a, b *EcNode) int {
			return b.freeEcSlot - a.freeEcSlot
		})
		emptyNode, fullNode := rackEcNodes[0], rackEcNodes[len(rackEcNodes)-1]
		emptyNodeShardCount, fullNodeShardCount := ecNodeIdToShardCount[emptyNode.info.Id], ecNodeIdToShardCount[fullNode.info.Id]
		if fullNodeShardCount > averageShardCount && emptyNodeShardCount+1 <= averageShardCount {

			emptyNodeIds := make(map[uint32]bool)
			if emptyDiskInfo, found := emptyNode.info.DiskInfos[string(types.HardDriveType)]; found {
				for _, shards := range emptyDiskInfo.EcShardInfos {
					emptyNodeIds[shards.Id] = true
				}
			}
			if fullDiskInfo, found := fullNode.info.DiskInfos[string(types.HardDriveType)]; found {
				for _, shards := range fullDiskInfo.EcShardInfos {
					if _, found := emptyNodeIds[shards.Id]; !found {
						for _, shardId := range erasure_coding.ShardBits(shards.EcIndexBits).ShardIds() {

							fmt.Printf("%s moves ec shards %d.%d to %s\n", fullNode.info.Id, shards.Id, shardId, emptyNode.info.Id)

							err := moveMountedShardToEcNode(ecb.commandEnv, fullNode, shards.Collection, needle.VolumeId(shards.Id), shardId, emptyNode, ecb.applyBalancing)
							if err != nil {
								return err
							}

							ecNodeIdToShardCount[emptyNode.info.Id]++
							ecNodeIdToShardCount[fullNode.info.Id]--
							hasMove = true
							break
						}
						break
					}
				}
			}
		}
	}

	return nil
}

func (ecb *ecBalancer) getAverageShardsPerEcRack() int {
	racks := ecb.racks()
	// calculate average number of shards an ec rack should have for one volume
	averageShardsPerEcRack := ceilDivide(erasure_coding.TotalShardsCount, len(racks))
	return averageShardsPerEcRack
}
func (ecb *ecBalancer) pickEcNodeToBalanceShardsInto(vid needle.VolumeId, existingLocation *EcNode, possibleDestinations []*EcNode) (*EcNode, error) {
	if existingLocation == nil {
		return nil, fmt.Errorf("INTERNAL: missing source nodes")
	}
	if len(possibleDestinations) == 0 {
		return nil, fmt.Errorf("INTERNAL: missing destination nodes")
	}

	nodeShards := map[*EcNode]int{}
	for _, node := range possibleDestinations {
		vs := findEcVolumeShards(node, vid)
		nodeShards[node] = vs.ShardIdCount()
		//fmt.Printf("+++ vid: %d, node: %s, shards: %d, vs: %v，info: %v\n", vid, node.info.Id, nodeShards[node], vs, node.info.DiskInfos)
	}

	targets := []*EcNode{}
	targetShards := -1
	for _, shards := range nodeShards {
		if shards > targetShards {
			targetShards = shards
		}
	}

	//fmt.Printf("+++ nodeShards: %v \n", nodeShards)
	averageShardsPerEcRack := ecb.getAverageShardsPerEcRack()
	details := ""
	for _, node := range possibleDestinations {
		if node.info.Id == existingLocation.info.Id {
			continue
		}
		if node.freeEcSlot <= 0 {
			details += fmt.Sprintf("  Skipped %s because it has no free slots\n", node.info.Id)
			continue
		}

		shards := nodeShards[node]
		if ecb.replicaPlacement != nil && shards > ecb.replicaPlacement.SameRackCount && shards >= averageShardsPerEcRack {
			details += fmt.Sprintf("  Skipped %s because shards %d > replica placement limit for the rack (%d), rp:%v \n", node.info.Id, shards, ecb.replicaPlacement.SameRackCount, ecb.replicaPlacement)
			continue
		}

		if shards < targetShards {
			// Favor nodes with less shards, to ensure an uniform distribution.
			targets = nil
			targetShards = shards
		}
		if shards == targetShards {
			targets = append(targets, node)
		}
	}

	if len(targets) == 0 {
		return nil, errors.New(details)
	}
	return targets[rand.IntN(len(targets))], nil
}

func (ecb *ecBalancer) pickOneEcNodeAndMoveOneShard(existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, possibleDestinationEcNodes []*EcNode) error {
	destNode, err := ecb.pickEcNodeToBalanceShardsInto(vid, existingLocation, possibleDestinationEcNodes)
	if err != nil {
		fmt.Printf("WARNING: Could not find suitable taget node for %d.%d:\n%s", vid, shardId, err.Error())
		return nil
	}

	fmt.Printf("%s moves ec shard %d.%d to %s\n", existingLocation.info.Id, vid, shardId, destNode.info.Id)
	return moveMountedShardToEcNode(ecb.commandEnv, existingLocation, collection, vid, shardId, destNode, ecb.applyBalancing)
}

func pickNEcShardsToMoveFrom(ecNodes []*EcNode, vid needle.VolumeId, n int) map[erasure_coding.ShardId]*EcNode {
	picked := make(map[erasure_coding.ShardId]*EcNode)
	var candidateEcNodes []*CandidateEcNode
	for _, ecNode := range ecNodes {
		shardBits := findEcVolumeShards(ecNode, vid)
		if shardBits.ShardIdCount() > 0 {
			candidateEcNodes = append(candidateEcNodes, &CandidateEcNode{
				ecNode:     ecNode,
				shardCount: shardBits.ShardIdCount(),
			})
		}
	}
	slices.SortFunc(candidateEcNodes, func(a, b *CandidateEcNode) int {
		return b.shardCount - a.shardCount
	})
	for i := 0; i < n; i++ {
		selectedEcNodeIndex := -1
		for i, candidateEcNode := range candidateEcNodes {
			shardBits := findEcVolumeShards(candidateEcNode.ecNode, vid)
			if shardBits > 0 {
				selectedEcNodeIndex = i
				for _, shardId := range shardBits.ShardIds() {
					candidateEcNode.shardCount--
					picked[shardId] = candidateEcNode.ecNode
					candidateEcNode.ecNode.deleteEcVolumeShards(vid, []uint32{uint32(shardId)})
					break
				}
				break
			}
		}
		if selectedEcNodeIndex >= 0 {
			ensureSortedEcNodes(candidateEcNodes, selectedEcNodeIndex, func(i, j int) bool {
				return candidateEcNodes[i].shardCount > candidateEcNodes[j].shardCount
			})
		}

	}
	return picked
}

func (ecb *ecBalancer) collectVolumeIdToEcNodes(collection string) map[needle.VolumeId][]*EcNode {
	vidLocations := make(map[needle.VolumeId][]*EcNode)
	for _, ecNode := range ecb.ecNodes {
		diskInfo, found := ecNode.info.DiskInfos[string(types.HardDriveType)]
		if !found {
			continue
		}
		for _, shardInfo := range diskInfo.EcShardInfos {
			// ignore if not in current collection
			if shardInfo.Collection == collection {
				vidLocations[needle.VolumeId(shardInfo.Id)] = append(vidLocations[needle.VolumeId(shardInfo.Id)], ecNode)
			}
		}
	}
	return vidLocations
}

func EcBalance(commandEnv *CommandEnv, collections []string, dc string, ecReplicaPlacement *super_block.ReplicaPlacement, applyBalancing bool, maxMoveShards int, maxParallelization int) (err error) {
	if len(collections) == 0 {
		return fmt.Errorf("no collections to balance")
	}

	// collect all ec nodes
	allEcNodes, totalFreeEcSlots, err := collectEcNodesForDC(commandEnv, dc)
	if err != nil {
		return err
	}
	if totalFreeEcSlots < 1 {
		return fmt.Errorf("no free ec shard slots. only %d left", totalFreeEcSlots)
	}

	ecb := &ecBalancer{
		commandEnv:         commandEnv,
		ecNodes:            allEcNodes,
		replicaPlacement:   ecReplicaPlacement,
		applyBalancing:     applyBalancing,
		maxMoveShards:      maxMoveShards,
		maxParallelization: maxParallelization,
	}

	for _, c := range collections {
		if err = ecb.balanceEcVolumes(c); err != nil {
			return err
		}
	}
	if err := ecb.balanceEcRacks(); err != nil {
		return fmt.Errorf("balance ec racks: %v", err)
	}

	return nil
}

// 新增方法，支持限制移动数量的机架间分片平衡
func (ecb *ecBalancer) balanceEcShardsAcrossRacksWithLimit(collection string, movedShards *int) error {
	// collect vid => []ecNode, since previous steps can change the locations
	vidLocations := ecb.collectVolumeIdToEcNodes(collection)

	// spread the ec shards evenly
	ewg := ecb.errorWaitGroup()
	// spread the ec shards evenly
	for vid, locations := range vidLocations {
		ewg.Add(func() error {
			if err := ecb.doBalanceEcShardsAcrossRacksWithLimit(collection, vid, locations, movedShards); err != nil {
				return err
			}
			return nil
		})

		// if err := ecb.doBalanceEcShardsAcrossRacksWithLimit(collection, vid, locations, movedShards); err != nil {
		// 	return err
		// }

	}
	err := ewg.Wait()
	if err != nil {
		return err
	}
	// 检查是否达到最大移动分片数量限制
	if ecb.maxMoveShards > 0 && *movedShards >= ecb.maxMoveShards {
		return nil
	}

	return nil
}

// 新增方法，支持限制移动数量的机架间分片平衡实现
func (ecb *ecBalancer) doBalanceEcShardsAcrossRacksWithLimit(collection string, vid needle.VolumeId, locations []*EcNode, movedShards *int) error {
	racks := ecb.racks()

	// calculate average number of shards an ec rack should have for one volume
	averageShardsPerEcRack := ceilDivide(erasure_coding.TotalShardsCount, len(racks))

	// see the volume's shards are in how many racks, and how many in each rack
	rackToShardCount := countShardsByRack(vid, locations)
	rackEcNodesWithVid := groupBy(locations, func(ecNode *EcNode) string {
		return string(ecNode.rack)
	})

	// ecShardsToMove = select overflown ec shards from racks with ec shard counts > averageShardsPerEcRack
	ecShardsToMove := make(map[erasure_coding.ShardId]*EcNode)
	for rackId, count := range rackToShardCount {
		//fmt.Printf("collection: %s, vid: %d, rackId: %s, count: %d, averageShardsPerEcRack: %d\n", collection, vid, rackId, count, averageShardsPerEcRack)
		if count <= averageShardsPerEcRack {
			continue
		}
		possibleEcNodes := rackEcNodesWithVid[rackId]
		for shardId, ecNode := range pickNEcShardsToMoveFrom(possibleEcNodes, vid, count-averageShardsPerEcRack) {
			ecShardsToMove[shardId] = ecNode
		}
	}

	for shardId, ecNode := range ecShardsToMove {
		// 检查是否达到最大移动分片数量限制
		if ecb.maxMoveShards > 0 && *movedShards >= ecb.maxMoveShards {
			return nil
		}

		rackId, err := ecb.pickRackToBalanceShardsInto(racks, rackToShardCount, averageShardsPerEcRack)
		if err != nil {
			fmt.Printf("ec shard %d.%d at %s can not find a destination rack:\n%s\n", vid, shardId, ecNode.info.Id, err.Error())
			continue
		}

		var possibleDestinationEcNodes []*EcNode
		for _, n := range racks[rackId].ecNodes {
			possibleDestinationEcNodes = append(possibleDestinationEcNodes, n)
		}
		err = ecb.pickOneEcNodeAndMoveShard(ecNode, collection, vid, shardId, possibleDestinationEcNodes, movedShards)
		if err != nil {
			return err
		}
		rackToShardCount[string(rackId)] += 1
		rackToShardCount[string(ecNode.rack)] -= 1
		racks[rackId].freeEcSlot -= 1
		racks[ecNode.rack].freeEcSlot += 1
	}

	return nil
}

// 新增方法，支持限制移动数量的机架内分片平衡
func (ecb *ecBalancer) balanceEcShardsWithinRacksWithLimit(collection string, movedShards *int) error {
	// collect vid => []ecNode, since previous steps can change the locations
	vidLocations := ecb.collectVolumeIdToEcNodes(collection)
	racks := ecb.racks()

	// spread the ec shards evenly
	for vid, locations := range vidLocations {
		// 检查是否达到最大移动分片数量限制
		if ecb.maxMoveShards > 0 && *movedShards >= ecb.maxMoveShards {
			return nil
		}

		// see the volume's shards are in how many racks, and how many in each rack
		rackToShardCount := countShardsByRack(vid, locations)
		rackEcNodesWithVid := groupBy(locations, func(ecNode *EcNode) string {
			return string(ecNode.rack)
		})

		for rackId, _ := range rackToShardCount {
			// 检查是否达到最大移动分片数量限制
			if ecb.maxMoveShards > 0 && *movedShards >= ecb.maxMoveShards {
				return nil
			}

			var possibleDestinationEcNodes []*EcNode
			for _, n := range racks[RackId(rackId)].ecNodes {
				if _, found := n.info.DiskInfos[string(types.HardDriveType)]; found {
					possibleDestinationEcNodes = append(possibleDestinationEcNodes, n)
				}
			}
			sourceEcNodes := rackEcNodesWithVid[rackId]
			averageShardsPerEcNode := ceilDivide(rackToShardCount[rackId], len(possibleDestinationEcNodes))
			if err := ecb.doBalanceEcShardsWithinOneRackWithLimit(averageShardsPerEcNode, collection, vid, sourceEcNodes, possibleDestinationEcNodes, movedShards); err != nil {
				return err
			}
		}
	}
	return nil
}

// 新增方法，支持限制移动数量的机架内分片平衡实现
func (ecb *ecBalancer) doBalanceEcShardsWithinOneRackWithLimit(averageShardsPerEcNode int, collection string, vid needle.VolumeId, existingLocations, possibleDestinationEcNodes []*EcNode, movedShards *int) error {
	for _, ecNode := range existingLocations {
		// 检查是否达到最大移动分片数量限制
		if ecb.maxMoveShards > 0 && *movedShards >= ecb.maxMoveShards {
			return nil
		}

		shardBits := findEcVolumeShards(ecNode, vid)
		overLimitCount := shardBits.ShardIdCount() - averageShardsPerEcNode

		for _, shardId := range shardBits.ShardIds() {
			// 检查是否达到最大移动分片数量限制
			if ecb.maxMoveShards > 0 && *movedShards >= ecb.maxMoveShards {
				return nil
			}

			if overLimitCount <= 0 {
				break
			}

			fmt.Printf("%s has %d overlimit, moving ec shard %d.%d\n", ecNode.info.Id, overLimitCount, vid, shardId)

			err := ecb.pickOneEcNodeAndMoveShard(ecNode, collection, vid, shardId, possibleDestinationEcNodes, movedShards)
			if err != nil {
				return err
			}

			overLimitCount--
		}
	}

	return nil
}

// 新增方法，用于追踪分片移动次数的版本
func (ecb *ecBalancer) pickOneEcNodeAndMoveShard(existingLocation *EcNode, collection string, vid needle.VolumeId, shardId erasure_coding.ShardId, possibleDestinationEcNodes []*EcNode, movedShards *int) error {
	destNode, err := ecb.pickEcNodeToBalanceShardsInto(vid, existingLocation, possibleDestinationEcNodes)
	if err != nil {
		fmt.Printf("WARNING: Could not find suitable taget node for %d.%d:\n%s", vid, shardId, err.Error())
		return nil
	}

	fmt.Printf("%s moves ec shard %d.%d to %s\n", existingLocation.info.Id, vid, shardId, destNode.info.Id)
	err = moveMountedShardToEcNode(ecb.commandEnv, existingLocation, collection, vid, shardId, destNode, ecb.applyBalancing)
	if err == nil && ecb.applyBalancing {
		ecb.movedShardsLock.Lock()
		*movedShards++
		fmt.Printf("已移动 %d 个分片，限制为 %d\n", *movedShards, ecb.maxMoveShards)
		ecb.movedShardsLock.Unlock()
	}
	return err
}
