package shell

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	topology1  = parseOutput(topoData)
	topology2  = parseOutput(topoData2)
	topologyEc = parseOutput(topoDataEc)
)

func errorCheck(got error, want string) error {
	if got == nil && want == "" {
		return nil
	}
	if got != nil && want == "" {
		return fmt.Errorf("expected no error, got %q", got.Error())
	}
	if got == nil && want != "" {
		return fmt.Errorf("got no error, expected %q", want)
	}
	if !strings.Contains(got.Error(), want) {
		return fmt.Errorf("expected error %q, got %q", want, got.Error())
	}
	return nil
}

func TestParseReplicaPlacementArg(t *testing.T) {
	getDefaultReplicaPlacementOrig := getDefaultReplicaPlacement
	getDefaultReplicaPlacement = func(commandEnv *CommandEnv) (*super_block.ReplicaPlacement, error) {
		return super_block.NewReplicaPlacementFromString("123")
	}
	defer func() {
		getDefaultReplicaPlacement = getDefaultReplicaPlacementOrig
	}()

	testCases := []struct {
		argument string
		want     string
		wantErr  string
	}{
		{"lalala", "lal", "unexpected replication type"},
		{"", "123", ""},
		{"021", "021", ""},
	}

	for _, tc := range testCases {
		commandEnv := &CommandEnv{}
		got, gotErr := parseReplicaPlacementArg(commandEnv, tc.argument)

		if err := errorCheck(gotErr, tc.wantErr); err != nil {
			t.Errorf("argument %q: %s", tc.argument, err.Error())
			continue
		}

		want, _ := super_block.NewReplicaPlacementFromString(tc.want)
		if !got.Equals(want) {
			t.Errorf("got replica placement %q, want %q", got.String(), want.String())
		}
	}
}

func TestEcDistribution(t *testing.T) {

	// find out all volume servers with one slot left.
	ecNodes, totalFreeEcSlots := collectEcVolumeServersByDc(topology1, "")

	sortEcNodesByFreeslotsDescending(ecNodes)

	if totalFreeEcSlots < erasure_coding.TotalShardsCount {
		t.Errorf("not enough free ec shard slots: %d", totalFreeEcSlots)
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
func TestPickRackToBalanceShardsInto(t *testing.T) {
	testCases := []struct {
		topology         *master_pb.TopologyInfo
		vid              string
		replicaPlacement string
		wantOneOf        []string
		wantErr          string
	}{
		// Non-EC volumes. We don't care about these, but the function should return all racks as a safeguard.
		{topologyEc, "", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		{topologyEc, "6225", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		{topologyEc, "6226", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		{topologyEc, "6241", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		{topologyEc, "6242", "123", []string{"rack1", "rack2", "rack3", "rack4", "rack5", "rack6"}, ""},
		// EC volumes.
		{topologyEc, "9578", "", nil, "shards 1 > replica placement limit for other racks (0)"},
		{topologyEc, "9578", "111", []string{"rack1", "rack2", "rack3"}, ""},
		{topologyEc, "9578", "222", []string{"rack1", "rack2", "rack3"}, ""},
		{topologyEc, "10457", "222", []string{"rack1"}, ""},
		{topologyEc, "12737", "222", []string{"rack2"}, ""},
		{topologyEc, "14322", "222", []string{"rack3"}, ""},
	}

	for _, tc := range testCases {
		vid, _ := needle.NewVolumeId(tc.vid)
		ecNodes, _ := collectEcVolumeServersByDc(tc.topology, "")
		rp, _ := super_block.NewReplicaPlacementFromString(tc.replicaPlacement)

		ecb := &ecBalancer{
			ecNodes:          ecNodes,
			replicaPlacement: rp,
		}

		racks := ecb.racks()
		rackToShardCount := countShardsByRack(vid, ecNodes)
		averageShardsPerEcRack := 0 //ceilDivide(erasure_coding.TotalShardsCount, len(racks))
		got, gotErr := ecb.pickRackToBalanceShardsInto(racks, rackToShardCount, averageShardsPerEcRack)
		if err := errorCheck(gotErr, tc.wantErr); err != nil {
			t.Errorf("volume %q: %s, averageShardsPerEcRack: %d", tc.vid, err.Error(), averageShardsPerEcRack)
			continue
		}

		if string(got) == "" && len(tc.wantOneOf) == 0 {
			continue
		}
		found := false
		for _, want := range tc.wantOneOf {
			if got := string(got); got == want {
				found = true
				break
			}
		}
		if !(found) {
			t.Errorf("expected one of %v for volume %q, got %q", tc.wantOneOf, tc.vid, got)
		}
	}
}
func TestPickEcNodeToBalanceShardsInto(t *testing.T) {
	testCases := []struct {
		topology  *master_pb.TopologyInfo
		nodeId    string
		vid       string
		wantOneOf []string
		wantErr   string
	}{
		{topologyEc, "", "", nil, "INTERNAL: missing source nodes"},
		{topologyEc, "idontexist", "12737", nil, "INTERNAL: missing source nodes"},
		// Non-EC nodes. We don't care about these, but the function should return all available target nodes as a safeguard.
		{
			topologyEc, "172.19.0.10:8702", "6225", []string{
				"172.19.0.13:8701", "172.19.0.14:8711", "172.19.0.16:8704", "172.19.0.17:8703",
				"172.19.0.19:8700", "172.19.0.20:8706", "172.19.0.21:8710", "172.19.0.3:8708",
				"172.19.0.4:8707", "172.19.0.5:8705", "172.19.0.6:8713", "172.19.0.8:8709",
				"172.19.0.9:8712"},
			"",
		},
		{
			topologyEc, "172.19.0.8:8709", "6226", []string{
				"172.19.0.10:8702", "172.19.0.13:8701", "172.19.0.14:8711", "172.19.0.16:8704",
				"172.19.0.17:8703", "172.19.0.19:8700", "172.19.0.20:8706", "172.19.0.21:8710",
				"172.19.0.3:8708", "172.19.0.4:8707", "172.19.0.5:8705", "172.19.0.6:8713",
				"172.19.0.9:8712"},
			"",
		},
		// EC volumes.
		{topologyEc, "172.19.0.10:8702", "14322", []string{
			"172.19.0.14:8711", "172.19.0.5:8705", "172.19.0.6:8713"},
			""},
		{topologyEc, "172.19.0.13:8701", "10457", []string{
			"172.19.0.10:8702", "172.19.0.6:8713"},
			""},
		{topologyEc, "172.19.0.17:8703", "12737", []string{
			"172.19.0.13:8701"},
			""},
		{topologyEc, "172.19.0.20:8706", "14322", []string{
			"172.19.0.14:8711", "172.19.0.5:8705", "172.19.0.6:8713"},
			""},
	}

	for _, tc := range testCases {
		vid, _ := needle.NewVolumeId(tc.vid)
		allEcNodes, _ := collectEcVolumeServersByDc(tc.topology, "")

		ecb := &ecBalancer{
			ecNodes: allEcNodes,
		}

		// Resolve target node by name
		var ecNode *EcNode
		for _, n := range allEcNodes {
			if n.info.Id == tc.nodeId {
				ecNode = n
				break
			}
		}

		got, gotErr := ecb.pickEcNodeToBalanceShardsInto(vid, ecNode, allEcNodes)
		if err := errorCheck(gotErr, tc.wantErr); err != nil {
			t.Errorf("node %q, volume %q: %s", tc.nodeId, tc.vid, err.Error())
			continue
		}

		if got == nil {
			if len(tc.wantOneOf) == 0 {
				continue
			}
			t.Errorf("node %q, volume %q: got no node, want %q", tc.nodeId, tc.vid, tc.wantOneOf)
			continue
		}
		found := false
		for _, want := range tc.wantOneOf {
			if got := got.info.Id; got == want {
				found = true
				break
			}
		}
		if !(found) {
			t.Errorf("expected one of %v for volume %q, got %q", tc.wantOneOf, tc.vid, got.info.Id)
		}
	}
}

func Test_ceilDivide(t *testing.T) {
	ast := assert.New(t)
	//3
	ast.Equal(3, ceilDivide(14, 5))

	//5
	ast.Equal(5, ceilDivide(14, 3))

	//2
	ast.Equal(2, ceilDivide(14, 7))
}

func Test_VolumeIdSort(t *testing.T) {
	ast := assert.New(t)
	volumeIds := []needle.VolumeId{12, 19, 9, 6, 18}
	fmt.Println("before sort:", volumeIds)
	destVolumeIds := []needle.VolumeId{6, 9, 12, 18, 19}
	sortVolumeIdsAscending(volumeIds)
	fmt.Println("after sort:", volumeIds)
	//
	ast.Equal(destVolumeIds, volumeIds)
}

func Test_EcNode_GetShardIds(t *testing.T) {
	// shardId := "07"
	baseFileName := "123"
	fileName := "123.ecj"
	shardId := strings.TrimPrefix(fileName, baseFileName+".ec")
	fmt.Println("shardId:", shardId)
	usid := uint32(util.ParseInt(shardId, 0))
	fmt.Println("usid:", usid)
}
