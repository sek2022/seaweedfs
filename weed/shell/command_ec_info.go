package shell

import (
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

func init() {
	Commands = append(Commands, &commandEcInfo{})
}

type commandEcInfo struct {
	volumeId *uint32
}

func (c *commandEcInfo) Name() string {
	return "ec.info"
}

func (c *commandEcInfo) Help() string {
	return `print out ec shard distribution

	ec.info [volumeId=xxx]
		If volumeId is provided, print out the shard distribution for that specific volume.
		Otherwise, print out the overall EC shard distribution.

	Example:
		ec.info
		ec.info volumeId=27
`
}

func (c *commandEcInfo) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcInfo) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	infoCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeIdFlag := infoCommand.Uint("volumeId", 0, "指定要查询的卷ID")
	if err = infoCommand.Parse(args); err != nil {
		return nil
	}

	var volumeId *uint32
	if infoCommand.Parsed() && infoCommand.Lookup("volumeId").Value.String() != "0" {
		vid := uint32(*volumeIdFlag)
		volumeId = &vid
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	if volumeId != nil {
		// 显示特定卷的分片分布
		return c.showVolumeShardDistributionWithId(commandEnv, topologyInfo, writer, *volumeId)
	}

	// 显示整体 EC 分片分布
	return c.showEcDistribution(commandEnv, topologyInfo, writer)
}

func (c *commandEcInfo) showVolumeShardDistributionWithId(commandEnv *CommandEnv, topologyInfo *master_pb.TopologyInfo, writer io.Writer, volumeId uint32) error {
	// 按机柜和节点收集分片信息
	type nodeShards struct {
		nodeId   string
		shardIds []erasure_coding.ShardId
	}
	rackNodes := make(map[string]map[string]*nodeShards)

	for _, dc := range topologyInfo.DataCenterInfos {
		for _, rack := range dc.RackInfos {
			rackNodes[rack.Id] = make(map[string]*nodeShards)
			for _, dn := range rack.DataNodeInfos {
				if diskInfo, found := dn.DiskInfos[string(types.HardDriveType)]; found {
					for _, shardInfo := range diskInfo.EcShardInfos {
						if shardInfo.Id == volumeId {
							shardBits := erasure_coding.ShardBits(shardInfo.EcIndexBits)
							for _, shardId := range shardBits.ShardIds() {
								if _, exists := rackNodes[rack.Id][dn.Id]; !exists {
									rackNodes[rack.Id][dn.Id] = &nodeShards{
										nodeId:   dn.Id,
										shardIds: make([]erasure_coding.ShardId, 0),
									}
								}
								rackNodes[rack.Id][dn.Id].shardIds = append(rackNodes[rack.Id][dn.Id].shardIds, shardId)
							}
						}
					}
				}
			}
		}
	}

	// 按机柜名称排序
	var rackIds []string
	for rackId := range rackNodes {
		rackIds = append(rackIds, rackId)
	}
	sort.Strings(rackIds)

	fmt.Fprintf(writer, "Volume %d shard distribution:\n", volumeId)
	for _, rackId := range rackIds {
		fmt.Fprintf(writer, "%s:\n", rackId)

		// 获取该机柜下的所有节点
		var nodes []*nodeShards
		for _, node := range rackNodes[rackId] {
			nodes = append(nodes, node)
		}

		// 按节点ID排序
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].nodeId < nodes[j].nodeId
		})

		// 对每个节点的分片ID进行排序
		for _, node := range nodes {
			sort.Slice(node.shardIds, func(i, j int) bool {
				return node.shardIds[i] < node.shardIds[j]
			})

			// 将分片ID转换为字符串
			shardIdStrs := make([]string, len(node.shardIds))
			for i, id := range node.shardIds {
				shardIdStrs[i] = fmt.Sprintf("%d", id)
			}

			fmt.Fprintf(writer, "    %d.[%s] => %s\n", volumeId, strings.Join(shardIdStrs, ", "), node.nodeId)
		}
	}

	return nil
}

func (c *commandEcInfo) showEcDistribution(commandEnv *CommandEnv, topologyInfo *master_pb.TopologyInfo, writer io.Writer) error {
	fmt.Fprintf(writer, "Erasure Coding 配置信息:\n")
	fmt.Fprintf(writer, "--------------------------------\n")
	fmt.Fprintf(writer, "数据分片数量 (DataShardsCount): %d\n", erasure_coding.DataShardsCount)
	fmt.Fprintf(writer, "校验分片数量 (ParityShardsCount): %d\n", erasure_coding.ParityShardsCount)
	fmt.Fprintf(writer, "总分片数量 (TotalShardsCount): %d\n", erasure_coding.TotalShardsCount)
	fmt.Fprintf(writer, "EC比例: %d:%d\n", erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	fmt.Fprintf(writer, "--------------------------------\n")
	fmt.Fprintf(writer, "最大块大小: %d 字节 (%d MB)\n", erasure_coding.ErasureCodingLargeBlockSize, erasure_coding.ErasureCodingLargeBlockSize/1024/1024)
	fmt.Fprintf(writer, "小块大小: %d 字节 (%d MB)\n", erasure_coding.ErasureCodingSmallBlockSize, erasure_coding.ErasureCodingSmallBlockSize/1024/1024)
	fmt.Fprintf(writer, "--------------------------------\n")

	// 显示整体 EC 分片分布
	return nil
}
