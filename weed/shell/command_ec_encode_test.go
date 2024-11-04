package shell

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"golang.org/x/exp/maps"
	"sort"
	"sync"
	"testing"
)

func TestEcDistribution(t *testing.T) {

	topologyInfo := parseOutput(topoData)

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots := collectEcVolumeServersByDc(topologyInfo, "")

	sortEcNodesByFreeslotsDescending(ecNodes)

	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		println("not enough free ec shard slots", totalFreeEcSlots)
	}
	allocatedDataNodes := ecNodes
	if len(allocatedDataNodes) > erasure_coding.TotalShardsCount {
		allocatedDataNodes = allocatedDataNodes[:erasure_coding.TotalShardsCount]
	}

	for _, dn := range allocatedDataNodes {
		// fmt.Printf("info %+v %+v\n", dn.info, dn)
		fmt.Printf("=> %+v %+v\n", dn.info.Id, dn.freeEcSlot)
	}

}

var vMap = map[needle.VolumeId][]wdclient.Location{
	needle.VolumeId(1):  {wdclient.Location{Url: "192.168.3.74:10001"}, wdclient.Location{Url: "192.168.3.75:10001"}},
	needle.VolumeId(2):  {wdclient.Location{Url: "192.168.3.76:10001"}, wdclient.Location{Url: "192.168.3.77:10001"}},
	needle.VolumeId(3):  {wdclient.Location{Url: "192.168.3.78:10001"}, wdclient.Location{Url: "192.168.3.74:10002"}},
	needle.VolumeId(4):  {wdclient.Location{Url: "192.168.3.75:10002"}, wdclient.Location{Url: "192.168.3.76:10002"}},
	needle.VolumeId(5):  {wdclient.Location{Url: "192.168.3.77:10002"}, wdclient.Location{Url: "192.168.3.78:10002"}},
	needle.VolumeId(6):  {wdclient.Location{Url: "192.168.3.74:10003"}, wdclient.Location{Url: "192.168.3.77:10003"}},
	needle.VolumeId(7):  {wdclient.Location{Url: "192.168.3.74:10003"}, wdclient.Location{Url: "192.168.3.79:10003"}},
	needle.VolumeId(8):  {wdclient.Location{Url: "192.168.3.79:10003"}, wdclient.Location{Url: "192.168.3.76:10003"}},
	needle.VolumeId(9):  {wdclient.Location{Url: "192.168.3.80:10003"}, wdclient.Location{Url: "192.168.3.75:10003"}},
	needle.VolumeId(10): {wdclient.Location{Url: "192.168.3.81:10003"}, wdclient.Location{Url: "192.168.3.79:10003"}},
	needle.VolumeId(11): {wdclient.Location{Url: "192.168.3.82:10003"}, wdclient.Location{Url: "192.168.3.81:10003"}},
	needle.VolumeId(12): {wdclient.Location{Url: "192.168.3.83:10003"}, wdclient.Location{Url: "192.168.3.84:10003"}},
	needle.VolumeId(13): {wdclient.Location{Url: "192.168.3.81:10003"}, wdclient.Location{Url: "192.168.3.83:10003"}},
	needle.VolumeId(14): {wdclient.Location{Url: "192.168.3.78:10003"}, wdclient.Location{Url: "192.168.3.81:10003"}},
	needle.VolumeId(15): {wdclient.Location{Url: "192.168.3.76:10003"}, wdclient.Location{Url: "192.168.3.80:10003"}},
}

func GetLocationsClone(vid needle.VolumeId) (v []wdclient.Location, b bool) {
	v, b = vMap[vid]
	return
}

// serversDisplayTimesInVolumes: map[192.168.3.74:4 192.168.3.75:4 192.168.3.76:3 192.168.3.77:4 192.168.3.78:3]
// volumeLocationsMap: map[1:[{Url:192.168.3.74:10001} {Url:192.168.3.75:10001}] 2:[{Url:192.168.3.76:10001} {Url:192.168.3.77:10001}] 3:[{Url:192.168.3.78:10001} {Url:192.168.3.74:10002}] 4:[{Url:192.168.3.75:10002} {Url:192.168.3.76:10002}] 5:[{Url:192.168.3.77:10002} {Url:192.168.3.78:10002}] 6:[{Url:192.168.3.74:10003} {Url:192.168.3.77:10003}]]
// volumeChooseLocationMap: map[1:{Url:192.168.3.75:10001} 2:{Url:192.168.3.76:10001} 3:{Url:192.168.3.78:10001} 4:{Url:192.168.3.75:10002} 5:{Url:192.168.3.78:10002} 6:{Url:192.168.3.74:10003}]
func Test_getServerDisplayTimesInVolumes(t *testing.T) {
	volumeIds := maps.Keys(vMap)
	sort.Slice(volumeIds, func(i, j int) bool {
		return volumeIds[i] < volumeIds[j]
	})
	commandEnv := CommandEnv{}
	commandEnv.MasterClient = &wdclient.MasterClient{}

	volumeLocationsMap, volumeChooseLocationMap := chooseMasterForAllVolumesTest(&commandEnv, volumeIds)

	fmt.Printf("volumeLocationsMap: %v \n", volumeLocationsMap)
	fmt.Printf("volumeChooseLocationMap: %v \n", volumeChooseLocationMap)

	var wg sync.WaitGroup
	wg.Add(len(volumeIds))
	for _, vid := range volumeIds {
		chooseLoc := volumeChooseLocationMap[vid]
		fmt.Printf("vid:%d, location:%v \n", vid, chooseLoc)
		go func() {
			defer wg.Done()
			//fmt.Printf("vid:%d, locations:%v \n", vid, locations)

		}()
	}
	wg.Wait()

}

func chooseMasterForAllVolumesTest(commandEnv *CommandEnv, volumeIds []needle.VolumeId) (map[needle.VolumeId][]wdclient.Location, map[needle.VolumeId]wdclient.Location) {
	var volumeLocationsMap = make(map[needle.VolumeId][]wdclient.Location)
	var volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)
	for _, vid := range volumeIds {
		//locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		locations, found := GetLocationsClone(vid)
		if !found {
			continue
		}
		volumeLocationsMap[vid] = locations
	}
	remainVolumeIds := volumeIds
	for len(remainVolumeIds) > 0 {
		locationMap, rem := chooseMasterServerForVolumesTest(remainVolumeIds)
		for key, value := range locationMap {
			volumeChooseLocationMap[key] = value
		}
		remainVolumeIds = rem
	}

	return volumeLocationsMap, volumeChooseLocationMap
}

// choose a master server for volume
func chooseMasterServerForVolumesTest(volumeIds []needle.VolumeId) (map[needle.VolumeId]wdclient.Location, []needle.VolumeId) {
	var serversWithVolumes = make(map[string][]uint32)
	var volumeLocationsMap = make(map[needle.VolumeId][]wdclient.Location)
	var volumeChooseLocationMap = make(map[needle.VolumeId]wdclient.Location)
	//1-192.168.3.74 = []
	for _, vid := range volumeIds {
		//locations, found := commandEnv.MasterClient.GetLocationsClone(uint32(vid))
		locations, found := GetLocationsClone(vid)
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

func TestGetCombinations(t *testing.T) {
	var result [][]int
	arr := []int{222, 223, 224, 225, 226, 227}
	permute(arr, len(arr), &result)

	fmt.Printf("result:%v \n", result)
}

func TestSyscall(t *testing.T) {
	//err := syscall.Fallocate(int(file.Fd()), FallocFlPunchHole, offset, length)
	//syscall.Fall
}
