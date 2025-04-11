package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

func init() {
	Commands = append(Commands, &commandEcEncode{})
}

type commandEcEncode struct {
}

func (c *commandEcEncode) Name() string {
	return "ec.encode"
}

func (c *commandEcEncode) Help() string {
	return `apply erasure coding to a volume

	ec.encode [-collection=""] [-fullPercent=95 -quietFor=1h]
	ec.encode [-collection=""] [-volumeId=<volume_id>]

	This command will:
	1. freeze one volume
	2. apply erasure coding to the volume
	3. move the encoded shards to multiple volume servers

	The erasure coding is 10.4. So ideally you have more than 14 volume servers, and you can afford
	to lose 4 volume servers.

	If the number of volumes are not high, the worst case is that you only have 4 volume servers,
	and the shards are spread as 4,4,3,3, respectively. You can afford to lose one volume server.

	If you only have less than 4 volume servers, with erasure coding, at least you can afford to
	have 4 corrupted shard files.

`
}

func (c *commandEcEncode) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcEncode) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	encodeCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	//volumeId := encodeCommand.Int("volumeId", 0, "the volume id")
	collection := encodeCommand.String("collection", "", "the collection name")
	fullPercentage := encodeCommand.Float64("fullPercent", 95, "the volume reaches the percentage of max volume size")
	quietPeriod := encodeCommand.Duration("quietFor", time.Hour, "select volumes without no writes for this period")
	parallelCopy := encodeCommand.Bool("parallelCopy", true, "copy shards in parallel")
	forceChanges := encodeCommand.Bool("force", false, "force the encoding even if the cluster has less than recommended 4 nodes")
	concurrency := encodeCommand.Int("concurrency", 3, "limit total concurrent ec.encode volume number (default 3)")
	maxVolumesId := encodeCommand.Int("max", 0, "limit max volumeId ec.encode can process (default 0, no limit)")
	if err = encodeCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	startTime := time.Now()
	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	if !*forceChanges {
		var nodeCount int
		eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
			nodeCount++
		})
		if nodeCount < erasure_coding.ParityShardsCount {
			glog.V(0).Infof("skip erasure coding with %d nodes, less than recommended %d nodes", nodeCount, erasure_coding.ParityShardsCount)
			return nil
		}
	}

	//vid := needle.VolumeId(*volumeId)

	// volumeId is provided
	//if vid != 0 {
	//	return doEcEncode(commandEnv, *collection, vid, nil, *parallelCopy)
	//}

	// apply to all volumes in the collection
	volumeIds, err := collectVolumeIdsForEcEncode(commandEnv, *collection, *fullPercentage, *quietPeriod)
	if err != nil {
		return err
	}

	//A maximum of 3 volumes can be processed at one time
	maxProcessNum := *concurrency
	//has process volumes

	if len(volumeIds) == 0 {
		fmt.Printf("no need ec encode volumes: %v, concurrent number:%d \n", volumeIds, maxProcessNum)
		return nil
	}
	//Loop until encode completed all volumes or max volumes
	for len(volumeIds) > 0 {
		if *maxVolumesId > 0 && uint32(volumeIds[0]) > uint32(*maxVolumesId) {
			glog.V(0).Infof("End loop process volumes, max volumeId:%d, min volumeId:%d", *maxVolumesId, volumeIds[0])
			break
		}

		// 限制日志中只显示前50个volumeIds
		displayVolumeIds := volumeIds
		if len(volumeIds) > 50 {
			displayVolumeIds = volumeIds[:50]
			glog.V(0).Infof("ec encode volumes: %v... (total %d volumes), concurrent number:%d, ec info:%d:%d \n", displayVolumeIds, len(volumeIds), maxProcessNum, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
		} else {
			glog.V(0).Infof("ec encode volumes: %v (total %d volumes), concurrent number:%d, ec info:%d:%d \n", volumeIds, len(volumeIds), maxProcessNum, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
		}

		processVolumeIds := volumeIds
		if len(volumeIds) > maxProcessNum {
			processVolumeIds = volumeIds[0:maxProcessNum]
		}
		//choose encode location for volumes
		//volumeLocationsMap, volumeChooseLocationMap := chooseEncodeLocationForAllVolumes(commandEnv, processVolumeIds)
		allocatorMap, err0 := chooseAllocatorForAllVolumes(commandEnv, processVolumeIds)
		if err0 != nil {
			glog.V(0).Infof("choose allocator for volumn fail, volumes:%v, error:%v", processVolumeIds, err0)
			break
		}

		if len(allocatorMap) <= 0 {
			glog.V(0).Infof("choose allocator for volumns fail, volumes:%v, no storage node", processVolumeIds)
			break
		}

		var wg sync.WaitGroup
		var mu sync.Mutex
		var errors = make([]error, 0)
		wg.Add(len(allocatorMap))
		for vidKey, allocator := range allocatorMap {
			//locations := volumeLocationsMap[vid]
			//chooseLoc := volumeChooseLocationMap[vid]
			//locations := allocator.Locations
			//chooseLoc := allocator.Choose
			go func(env *CommandEnv, coll string, id needle.VolumeId, allocator *VolumeEcAllocator, pc bool) {
				defer wg.Done()
				if *maxVolumesId > 0 && uint32(id) > uint32(*maxVolumesId) {
					return
				}
				if err = doEcEncode(env, coll, id, allocator, pc); err != nil {
					mu.Lock()
					errors = append(errors, err)
					fmt.Printf("doEcEncode error:%v \n", err)
					mu.Unlock()
				}
			}(commandEnv, *collection, vidKey, allocator, *parallelCopy)
		}
		wg.Wait()

		//sleep 5s
		time.Sleep(5 * time.Second)
		volumeIds, err = collectVolumeIdsForEcEncode(commandEnv, *collection, *fullPercentage, *quietPeriod)
		if err != nil {
			return err
		}

		if len(errors) > 0 {
			return errors[0]
		}

	}
	glog.V(0).Infof("ec end, total second time:%ds!", (time.Now().UnixMilli()-startTime.UnixMilli())/1000)
	return nil
}

// ensure every volume allocate a master server location for ec encode
func chooseAllocatorForAllVolumes(commandEnv *CommandEnv, volumeIds []needle.VolumeId) (map[needle.VolumeId]*VolumeEcAllocator, error) {
	var volumeLocationsMap = make(map[needle.VolumeId][]wdclient.Location)
	var volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)

	var volumeAllocatorMap = make(map[needle.VolumeId]*VolumeEcAllocator)
	for _, vid := range volumeIds {
		locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		if !found {
			continue
		}
		volumeLocationsMap[vid] = locations
	}
	remainVolumeIds := volumeIds
	for len(remainVolumeIds) > 0 {
		locationMap, rem := chooseMasterServerForVolumes(commandEnv, remainVolumeIds)
		for key, value := range locationMap {
			volumeChooseLocationMap[key] = value
		}
		remainVolumeIds = rem
	}

	//glog.V(0).Infof("generateEcShards %s %d on %s ...\n", collection, volumeId, sourceVolumeServer)
	allEcNodes, totalFreeEcSlots, err := collectEcNodes(commandEnv)
	if err != nil {
		return nil, err
	}

	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		return nil, fmt.Errorf("not enough free ec shard slots. only %d left", totalFreeEcSlots)
	}

	ecb := &ecBalancer{
		commandEnv: commandEnv,
		ecNodes:    allEcNodes,
	}

	for _, vid := range volumeIds {
		//Ensure EcNodes come from different rack, Prevent uneven distribution
		allocatedDataNodes := getEcNodesMustDifferentRacks(allEcNodes, ecb)
		if allocatedDataNodes == nil || len(allocatedDataNodes) <= 0 {
			glog.V(0).Infof("allocated data nodes Insufficient quantity, volumeId ec failed,vid: %d", vid)
			break
		}

		if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
			allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
		}

		// calculate how many shards to allocate for these servers，分配存储ec的server
		//allocatedEcIds {"192.168.10.1:1001": [1,2,9], "192.168.10.1:1002": [3,4,8], "192.168.10.1:1003": [5,6,7]}
		//allocatedNodes {"192.168.10.1:1001": node, "192.168.10.1:1002": node, "192.168.10.1:1003": node}
		allocatedEcIds, allocatedNodes := balancedEcDistribution2(allocatedDataNodes, vid)

		ecAllocator := &VolumeEcAllocator{VolumeId: vid, AllocatedEcIds: allocatedEcIds, AllocatedNodes: allocatedNodes, Choose: volumeChooseLocationMap[vid], Locations: volumeLocationsMap[vid]}
		volumeAllocatorMap[vid] = ecAllocator
	}

	for _, allocator := range volumeAllocatorMap {
		//释放当前server
		for key, _ := range allocator.AllocatedEcIds {
			ecBusyServerProcessor.remove(key)
		}
	}

	return volumeAllocatorMap, nil
}

// choose a master server for volume
func chooseMasterServerForVolumes(commandEnv *CommandEnv, volumeIds []needle.VolumeId) (map[needle.VolumeId]wdclient.Location, []needle.VolumeId) {
	var serversWithVolumes = make(map[string][]uint32)
	var volumeLocationsMap = make(map[needle.VolumeId][]wdclient.Location)
	var volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)
	//1-192.168.3.74 = []
	for _, vid := range volumeIds {
		locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		if !found {
			continue
		}
		volumeLocationsMap[vid] = locations
		for _, loc := range locations {
			serverIp := splitIP(loc.Url)
			if len(serverIp) <= 0 {
				fmt.Printf("loc url is err:%s", loc.Url)
				continue
			}
			//init 1 times
			var ipVolumeIds []uint32
			var b bool
			if ipVolumeIds, b = serversWithVolumes[serverIp]; !b {
				ipVolumeIds = make([]uint32, 0)
			}
			ipVolumeIds = append(ipVolumeIds, uint32(vid))
			serversWithVolumes[serverIp] = ipVolumeIds
		}
	}

	keys := make([]string, 0, len(serversWithVolumes))
	for k := range serversWithVolumes {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var combinations = make([][]uint32, len(serversWithVolumes))
	for i, key := range keys {
		combinations[i] = serversWithVolumes[key]
	}
	volumeCombinations := getCombination(combinations...)
	maxCount := 0
	var maxVolumeChooseLocationMap map[needle.VolumeId]wdclient.Location

	for _, currentVolumeIds := range volumeCombinations {
		volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)
		for index, vid := range currentVolumeIds {
			locations := volumeLocationsMap[needle.VolumeId(vid)]
			if len(locations) == 0 {
				continue
			}
			var chooseLoc = wdclient.Location{}
			for _, loc := range locations {
				serverIp := splitIP(loc.Url)
				if len(serverIp) <= 0 {
					fmt.Printf("loc url is err:%s", loc.Url)
					continue
				}
				if keys[index] == serverIp {
					chooseLoc = loc
				}
			}
			volumeChooseLocationMap[needle.VolumeId(vid)] = chooseLoc
		}

		count := fillServerCount(volumeChooseLocationMap)
		if count > maxCount {
			maxCount = count
			maxVolumeChooseLocationMap = volumeChooseLocationMap
		}

		if maxCount >= len(keys) {
			break
		}
	}
	remainVolumeIds := make([]needle.VolumeId, 0)
	for _, vid := range volumeIds {
		if _, b := maxVolumeChooseLocationMap[vid]; !b {
			remainVolumeIds = append(remainVolumeIds, vid)
		}
	}

	return maxVolumeChooseLocationMap, remainVolumeIds
}

func fillServerCount(mapLocation map[needle.VolumeId]wdclient.Location) int {
	var servers = make(map[string]uint32)
	for _, loc := range mapLocation {
		serverIp := splitIP(loc.Url)
		servers[serverIp] = uint32(1)
	}

	return len(servers)
}

func splitIP(url string) string {
	parts := strings.Split(url, ":")
	if len(parts) != 2 {
		return ""
	}
	return parts[0]
}

func doEcEncode(commandEnv *CommandEnv, collection string, vid needle.VolumeId, allocator *VolumeEcAllocator, parallelCopy bool) (err error) {
	if !commandEnv.isLocked() {
		return fmt.Errorf("lock is lost")
	}

	var locations []wdclient.Location
	var chooseLoc wdclient.Location
	// If volumeId is provided -> (ec.encode -volumeId=<volume_id>)
	locations = allocator.Locations
	chooseLoc = allocator.Choose

	// mark the volume as readonly
	err = markVolumeReplicasWritable(commandEnv.option.GrpcDialOption, vid, locations, false, false)
	if err != nil {
		return fmt.Errorf("mark volume %d as readonly on %s: %v", vid, chooseLoc.Url, err)
	}

	// generate and mount ec shards
	err = generateAndMountEcShards(commandEnv, vid, collection, chooseLoc.ServerAddress(), allocator)
	if err != nil {
		return fmt.Errorf("generate ec shards for volume %d on %s: %v", vid, chooseLoc.Url, err)
	}

	// balance the ec shards to current cluster
	err = spreadEcShards(commandEnv, vid, collection, locations, chooseLoc, parallelCopy)
	if err != nil {
		return fmt.Errorf("spread ec shards for volume %d from %s: %v", vid, chooseLoc.Url, err)
	}

	return nil
}

func generateAndMountEcShards(commandEnv *CommandEnv, volumeId needle.VolumeId, collection string, sourceVolumeServer pb.ServerAddress, allocator *VolumeEcAllocator) error {

	//glog.V(0).Infof("generateEcShards %s %d on %s ...\n", collection, volumeId, sourceVolumeServer)
	//allEcNodes, totalFreeEcSlots, err := collectEcNodes(commandEnv)
	//if err != nil {
	//	return err
	//}
	//
	//if totalFreeEcSlots < erasure_coding.TotalShardsCount {
	//	return fmt.Errorf("not enough free ec shard slots. only %d left", totalFreeEcSlots)
	//}
	//
	//ecb := &ecBalancer{
	//	commandEnv: commandEnv,
	//	ecNodes:    allEcNodes,
	//}
	//
	////Ensure EcNodes come from different rack, Prevent uneven distribution
	//allocatedDataNodes := getEcNodesMustDifferentRacks(allEcNodes, ecb)
	//if allocatedDataNodes == nil || len(allocatedDataNodes) <= 0 {
	//	glog.V(0).Infof("allocated data nodes Insufficient quantity, volumeId ec failed,vid: %d", volumeId)
	//	return fmt.Errorf("allocated data nodes Insufficient quantity, volumeId ec failed,vid: %d", volumeId)
	//}
	//
	//if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
	//	allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	//}
	//
	//// calculate how many shards to allocate for these servers，分配存储ec的server
	////allocatedEcIds {"192.168.10.1:1001": [1,2,9], "192.168.10.1:1002": [3,4,8], "192.168.10.1:1003": [5,6,7]}
	////allocatedNodes {"192.168.10.1:1001": node, "192.168.10.1:1002": node, "192.168.10.1:1003": node}
	//allocatedEcIds, allocatedNodes := balancedEcDistribution2(allocatedDataNodes, volumeId)
	allocatedEcIds, allocatedNodes := allocator.AllocatedEcIds, allocator.AllocatedNodes

	glog.V(0).Infof("choose:%v, vid: %d, ec info：%d:%d", allocator.Choose, volumeId, erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)

	err2 := operation.WithVolumeServerClient(false, sourceVolumeServer, commandEnv.option.GrpcDialOption, func(volumeServerClient volume_server_pb.VolumeServerClient) error {
		_, genErr := volumeServerClient.VolumeEcShardsGenerate(context.Background(), &volume_server_pb.VolumeEcShardsGenerateRequest{
			VolumeId:       uint32(volumeId),
			Collection:     collection,
			AllocatedEcIds: allocatedEcIds,
		})
		return genErr
	})

	if err2 == nil {
		//mount
		for key, _ := range allocatedNodes {
			mountErr := mountEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.ServerAddress(key), allocatedEcIds[key].GetShardIds())
			if mountErr != nil {
				err2 = fmt.Errorf("mount %d.%v on %s : %v\n", volumeId, allocatedEcIds[key].GetShardIds(), key, mountErr)
			}
		}
		//add ec shards
		for key, server := range allocatedNodes {
			server.addEcVolumeShards(volumeId, collection, allocatedEcIds[key].GetShardIds())
		}
	}

	cleanupFunc := func(server *EcNode, allocatedEcShardIds []uint32) {
		if err := unmountEcShards(commandEnv.option.GrpcDialOption, volumeId, pb.NewServerAddressFromDataNode(server.info), allocatedEcShardIds); err != nil {
			fmt.Printf("unmount aborted shards %d.%v on %s: %v\n", volumeId, allocatedEcShardIds, server.info.Id, err)
		}
		if err := sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(server.info), allocatedEcShardIds); err != nil {
			fmt.Printf("remove aborted shards %d.%v on %s: %v\n", volumeId, allocatedEcShardIds, server.info.Id, err)
		}
	}

	if err2 != nil {
		for i, server := range allocatedNodes {
			if len(allocatedEcIds[i].GetShardIds()) <= 0 {
				continue
			}
			cleanupFunc(server, allocatedEcIds[i].GetShardIds())
		}
	}

	return err2

}

func getEcNodesMustDifferentRacks(allEcNodes []*EcNode, ecb *ecBalancer) []*EcNode {
	racks := ecb.racks()
	if len(racks) <= 0 {
		return nil
	}
	// calculate average number of shards an ec rack should have for one volume
	averageShardsPerEcRack := ceilDivide(erasure_coding.TotalShardsCount, len(racks))
	var rackEcNodes = make(map[string][]*EcNode)
	for _, node := range allEcNodes {
		serverAddress := pb.NewServerAddressWithGrpcPort(node.info.Id, int(node.info.GrpcPort))
		key := serverAddress.String()
		//查看当前server是否正在被使用
		if ecBusyServerProcessor.inUse(key) {
			continue
		}

		if _, b := rackEcNodes[string(node.rack)]; !b {
			arr := make([]*EcNode, 0)
			rackEcNodes[string(node.rack)] = arr
		}
		if len(rackEcNodes[string(node.rack)]) >= averageShardsPerEcRack {
			continue
		}
		rackEcNodes[string(node.rack)] = append(rackEcNodes[string(node.rack)], node)
	}
	minRackSize := len(racks) - 1
	//如果可用机柜不足则返回空
	if len(rackEcNodes) < minRackSize {
		glog.V(0).Infof("rack insuffic size:%d, min racks:%d", len(rackEcNodes), minRackSize)
		return nil
	}

	rackNodesSlice := make([]*EcNode, 0)
	for _, value := range rackEcNodes {
		for _, node := range value {
			rackNodesSlice = append(rackNodesSlice, node)
		}
	}

	sortEcNodesByFreeslotsDescending(rackNodesSlice)

	return rackNodesSlice
}

func spreadEcShards(commandEnv *CommandEnv, volumeId needle.VolumeId, collection string, existingLocations []wdclient.Location, chooseLoc wdclient.Location, parallelCopy bool) (err error) {

	//allEcNodes, totalFreeEcSlots, err := collectEcNodes(commandEnv, "")
	//if err != nil {
	//	return err
	//}
	//
	//if totalFreeEcSlots < erasure_coding.TotalShardsCount {
	//	return fmt.Errorf("not enough free ec shard slots. only %d left", totalFreeEcSlots)
	//}
	//Ensure EcNodes come from different rack, Prevent uneven distribution
	//allocatedDataNodes := getEcNodesMustDifferentRacks(allEcNodes)
	//if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
	//	allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	//}

	// calculate how many shards to allocate for these servers
	//allocatedEcIds := balancedEcDistribution(allocatedDataNodes)

	// ask the data nodes to copy from the source volume server
	//copiedShardIds, err := parallelCopyEcShardsFromSource(commandEnv.option.GrpcDialOption, allocatedDataNodes, allocatedEcIds, volumeId, collection, chooseLoc, parallelCopy)
	//if err != nil {
	//	return err
	//}

	// unmount the to be deleted shards
	//err = unmountEcShards(commandEnv.option.GrpcDialOption, volumeId, chooseLoc.ServerAddress(), copiedShardIds)
	//if err != nil {
	//	return err
	//}

	// ask the source volume server to clean up copied ec shards
	//err = sourceServerDeleteEcShards(commandEnv.option.GrpcDialOption, collection, volumeId, chooseLoc.ServerAddress(), copiedShardIds)
	//if err != nil {
	//	return fmt.Errorf("source delete copied ecShards %s %d.%v: %v", chooseLoc.Url, volumeId, copiedShardIds, err)
	//}

	// ask the source volume server to delete the original volume
	for _, location := range existingLocations {
		fmt.Printf("delete volume %d from %s\n", volumeId, location.Url)
		//can't delete vif file after ec volume delete
		err = deleteVolumeAfterEc(commandEnv.option.GrpcDialOption, volumeId, location.ServerAddress(), true)
		if err != nil {
			return fmt.Errorf("deleteVolume %s volume %d: %v", location.Url, volumeId, err)
		}
	}

	return err

}

func parallelCopyEcShardsFromSource(grpcDialOption grpc.DialOption, targetServers []*EcNode, allocatedEcIds [][]uint32, volumeId needle.VolumeId, collection string, existingLocation wdclient.Location, parallelCopy bool) (actuallyCopied []uint32, err error) {

	fmt.Printf("parallelCopyEcShardsFromSource, %d %s, targetServers: %+v, allocatedEcIds: %v\n", volumeId, existingLocation.Url, targetServers, allocatedEcIds)

	var wg sync.WaitGroup
	shardIdChan := make(chan []uint32, len(targetServers))
	copyFunc := func(server *EcNode, allocatedEcShardIds []uint32) {
		defer wg.Done()
		copiedShardIds, copyErr := oneServerCopyAndMountEcShardsFromSource(grpcDialOption, server,
			allocatedEcShardIds, volumeId, collection, existingLocation.ServerAddress())
		if copyErr != nil {
			err = copyErr
		} else {
			shardIdChan <- copiedShardIds
			server.addEcVolumeShards(volumeId, collection, copiedShardIds)
		}
	}
	cleanupFunc := func(server *EcNode, allocatedEcShardIds []uint32) {
		if err := unmountEcShards(grpcDialOption, volumeId, pb.NewServerAddressFromDataNode(server.info), allocatedEcShardIds); err != nil {
			fmt.Printf("unmount aborted shards %d.%v on %s: %v\n", volumeId, allocatedEcShardIds, server.info.Id, err)
		}
		if err := sourceServerDeleteEcShards(grpcDialOption, collection, volumeId, pb.NewServerAddressFromDataNode(server.info), allocatedEcShardIds); err != nil {
			fmt.Printf("remove aborted shards %d.%v on target server %s: %v\n", volumeId, allocatedEcShardIds, server.info.Id, err)
		}
		if err := sourceServerDeleteEcShards(grpcDialOption, collection, volumeId, existingLocation.ServerAddress(), allocatedEcShardIds); err != nil {
			fmt.Printf("remove aborted shards %d.%v on existing server %s: %v\n", volumeId, allocatedEcShardIds, existingLocation.ServerAddress(), err)
		}
	}

	// maybe parallelize
	for i, server := range targetServers {
		if len(allocatedEcIds[i]) <= 0 {
			continue
		}

		wg.Add(1)
		if parallelCopy {
			go copyFunc(server, allocatedEcIds[i])
		} else {
			copyFunc(server, allocatedEcIds[i])
		}
	}
	wg.Wait()
	close(shardIdChan)

	if err != nil {
		for i, server := range targetServers {
			if len(allocatedEcIds[i]) <= 0 {
				continue
			}
			cleanupFunc(server, allocatedEcIds[i])
		}
		return nil, err
	}

	for shardIds := range shardIdChan {
		actuallyCopied = append(actuallyCopied, shardIds...)
	}

	return
}

func balancedEcDistribution(servers []*EcNode) (allocated [][]uint32) {
	allocated = make([][]uint32, len(servers))
	allocatedShardIdIndex := uint32(0)
	serverIndex := rand.Intn(len(servers))
	for allocatedShardIdIndex < erasure_coding.TotalShardsCount {
		if servers[serverIndex].freeEcSlot > 0 {
			allocated[serverIndex] = append(allocated[serverIndex], allocatedShardIdIndex)
			allocatedShardIdIndex++
		}
		serverIndex++
		if serverIndex >= len(servers) {
			serverIndex = 0
		}
	}

	return allocated
}

// allocatedEcIds {"192.168.10.1:1001": [1,2,9], "192.168.10.1:1002": [3,4,8], "192.168.10.1:1003": [5,6,7]}
// allocatedNodes {"192.168.10.1:1001": node, "192.168.10.1:1002": node, "192.168.10.1:1003": node}
func balancedEcDistribution2(servers []*EcNode, volumeId needle.VolumeId) (map[string]*volume_server_pb.EcShardIds, map[string]*EcNode) {
	allocated := make(map[string]*volume_server_pb.EcShardIds, len(servers))
	allocatedNodes := make(map[string]*EcNode, len(servers))
	allocatedShardIdIndex := uint32(0)
	serverIndex := rand.Intn(len(servers))
	for allocatedShardIdIndex < erasure_coding.TotalShardsCount {
		node := servers[serverIndex]
		serverAddress := pb.NewServerAddressWithGrpcPort(node.info.Id, int(node.info.GrpcPort))
		key := serverAddress.String()
		if node.freeEcSlot > 0 {
			if _, b := allocated[key]; !b {
				allocated[key] = &volume_server_pb.EcShardIds{ShardIds: make([]uint32, 0)}
			}
			allocated[key].ShardIds = append(allocated[key].ShardIds, allocatedShardIdIndex)
			allocatedNodes[key] = node
			allocatedShardIdIndex++
			//添加到管理器
			ecBusyServerProcessor.add(key, volumeId)
		}
		serverIndex++
		if serverIndex >= len(servers) {
			serverIndex = 0
		}
	}

	return allocated, allocatedNodes
}

func collectVolumeIdsForEcEncode(commandEnv *CommandEnv, selectedCollection string, fullPercentage float64, quietPeriod time.Duration) (vids []needle.VolumeId, err error) {

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return
	}

	quietSeconds := int64(quietPeriod / time.Second)
	nowUnixSeconds := time.Now().Unix()

	fmt.Printf("collect volumes quiet for: %d seconds and %.1f%% full\n", quietSeconds, fullPercentage)

	vidMap := make(map[uint32]bool)
	eachDataNode(topologyInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		//eachDataNode(topologyInfo, func(dc string, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, v := range diskInfo.VolumeInfos {
				// ignore remote volumes
				if v.RemoteStorageName != "" && v.RemoteStorageKey != "" {
					continue
				}
				if v.Collection == selectedCollection && v.ModifiedAtSecond+quietSeconds < nowUnixSeconds {
					if float64(v.Size) > fullPercentage/100*float64(volumeSizeLimitMb)*1024*1024 {
						if good, found := vidMap[v.Id]; found {
							if good {
								if diskInfo.FreeVolumeCount < 2 {
									glog.V(0).Infof("skip %s %d on %s, no free disk", v.Collection, v.Id, dn.Id)
									vidMap[v.Id] = false
								}
							}
						} else {
							if diskInfo.FreeVolumeCount < 2 {
								glog.V(0).Infof("skip %s %d on %s, no free disk", v.Collection, v.Id, dn.Id)
								vidMap[v.Id] = false
							} else {
								vidMap[v.Id] = true
							}
						}
					}
				}
			}
		}
	})

	for vid, good := range vidMap {
		if good {
			vids = append(vids, needle.VolumeId(vid))
		}
	}

	sortVolumeIdsAscending(vids)

	return
}

// Product
func getCombination(sets ...[]uint32) [][]uint32 {
	lens := func(i int) int { return len(sets[i]) }
	product := make([][]uint32, 0)
	for ix := make([]int, len(sets)); ix[0] < lens(0); nextIndex(ix, lens) {
		var r []uint32
		for j, k := range ix {
			r = append(r, sets[j][k])
		}
		product = append(product, r)
	}
	return product
}

func nextIndex(ix []int, lens func(i int) int) {
	for j := len(ix) - 1; j >= 0; j-- {
		ix[j]++
		if j == 0 || ix[j] < lens(j) {
			return
		}
		ix[j] = 0
	}
}
