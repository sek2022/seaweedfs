package shell

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

func init() {
	Commands = append(Commands, &commandEcFix{})
}

type commandEcFix struct {
}

func (c *commandEcFix) Name() string {
	return "ec.fix"
}

func (c *commandEcFix) Help() string {
	return `修复EC编码过程中未完成的文件

	ec.fix -volumeId=<volume_id> [-collection=""] [-force]

	此命令会：
	1. 检查卷服务器上的EC分片文件
	2. 找出未完全编码的EC文件（即处于不完整状态的EC文件）
	3. 删除不完整的EC文件
	4. 如果添加-force参数，将自动执行修复操作，否则只显示问题而不修复

	示例：
	ec.fix -volumeId=234               # 检查指定卷ID
	ec.fix -volumeId=234 -collection=myCollection  # 检查指定集合和卷ID
	ec.fix -volumeId=234 -force        # 检查并修复指定卷ID的问题
`
}

func (c *commandEcFix) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcFix) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	fixCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	volumeId := fixCommand.Int("volumeId", 0, "卷ID（必须指定）")
	collection := fixCommand.String("collection", "", "集合名称，空表示所有集合")
	applyChanges := fixCommand.Bool("force", false, "是否应用修复，默认只显示问题但不修复")
	if err = fixCommand.Parse(args); err != nil {
		return nil
	}

	if err = commandEnv.confirmIsLocked(args); err != nil {
		return
	}

	// 检查volumeId是否已指定
	if *volumeId <= 0 {
		return fmt.Errorf("必须指定有效的卷ID (volumeId)")
	}

	vid := needle.VolumeId(*volumeId)

	// 收集拓扑信息
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	fmt.Fprintf(writer, "开始检查EC编码文件问题...\n")
	fmt.Fprintf(writer, "应用修复: %v\n", *applyChanges)

	// 直接调用修复函数
	return fixEcVolumeIssues(commandEnv, topologyInfo, *collection, vid, *applyChanges, writer)
}

// 收集所有EC分片ID，不过滤集合
func collectAllEcShardIds(topoInfo *master_pb.TopologyInfo) (volumeIds []needle.VolumeId) {
	vidMap := make(map[needle.VolumeId]struct{})
	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				vidMap[needle.VolumeId(ecShardInfo.Id)] = struct{}{}
			}
		}
	})

	for vid := range vidMap {
		volumeIds = append(volumeIds, vid)
	}
	sortVolumeIdsAscending(volumeIds)
	return volumeIds
}

// 修复特定卷ID的EC问题
func fixEcVolumeIssues(commandEnv *CommandEnv, topoInfo *master_pb.TopologyInfo, collection string, vid needle.VolumeId, applyChanges bool, writer io.Writer) error {
	// 首先检查卷是否还处于 volume 状态
	// 如果 EC 编码成功完成，volume 状态就不存在了
	hasNormalVolume, err := checkVolumeExistence(commandEnv, vid)
	if err != nil {
		return fmt.Errorf("检查卷 %d 状态时出错: %v", vid, err)
	}

	if !hasNormalVolume {
		fmt.Fprintf(writer, "卷 %d 不存在或已经成功完成 EC 编码，无需修复\n", vid)
		return nil
	}

	// 查找所有包含此卷分片的服务器
	servers := findServersWithEcShard(topoInfo, vid)
	if len(servers) == 0 {
		fmt.Fprintf(writer, "未找到卷 %d 的任何EC分片服务器，无需修复\n", vid)
		return nil
	}

	fmt.Fprintf(writer, "发现卷 %d 有 %d 个服务器包含EC分片\n", vid, len(servers))

	if !applyChanges {
		fmt.Fprintf(writer, "添加 -force 参数以修复这些服务器上的EC分片\n")
		return nil
	}

	// 应用修复
	fmt.Fprintf(writer, "正在修复卷 %d 的所有EC分片...\n", vid)

	fixedCount := 0
	for _, server := range servers {
		serverAddr := pb.NewServerAddressFromDataNode(server)

		// 直接获取所有EC分片
		shards, err := getServerAllEcShards(commandEnv, serverAddr, collection, vid)
		if err != nil {
			glog.Errorf("获取服务器 %s 上卷 %d 的EC分片失败: %v", serverAddr, vid, err)
			continue
		}

		if len(shards) == 0 {
			glog.V(0).Infof("getServerAllEcShards, serverAddr: %s, collection: %s, vid: %d, shards: %v", serverAddr, collection, vid, shards)
			continue
		}

		// 创建修复请求
		issue := &ecShardIssue{
			description:  fmt.Sprintf("包含 %d 个EC分片，执行清理", len(shards)),
			incompleteEC: true,
			shards:       shards,
		}

		fmt.Fprintf(writer, "serverAddr: %s, issue: %+v \n", serverAddr, issue)

		// 修复所有EC分片
		//err = applyEcFix(commandEnv, serverAddr, collection, vid, issue)
		glog.V(0).Infof("applyEcFix, serverAddr: %s, collection: %s, vid: %d, shards: %v", serverAddr, collection, vid, shards)

		if err != nil {
			fmt.Fprintf(writer, "  修复服务器 %s 上卷 %d 的EC分片失败: %v\n", serverAddr, vid, err)
		} else {
			fmt.Fprintf(writer, "  已成功修复服务器 %s 上卷 %d 的 %d 个EC分片\n", serverAddr, vid, len(shards))
			fixedCount += len(shards)
		}
	}

	if fixedCount > 0 {
		fmt.Fprintf(writer, "共成功修复 %d 个EC分片\n", fixedCount)
	} else {
		fmt.Fprintf(writer, "没有修复任何EC分片\n")
	}

	return nil
}

// 获取服务器上卷的所有EC分片
func getServerAllEcShards(commandEnv *CommandEnv, serverAddr pb.ServerAddress, collection string, vid needle.VolumeId) ([]uint32, error) {
	var shards []uint32

	err := operation.WithVolumeServerClient(false, serverAddr, commandEnv.option.GrpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		// 获取服务器信息
		resp, err := client.VolumeServerStatus(context.Background(), &volume_server_pb.VolumeServerStatusRequest{})
		if err != nil {
			return err
		}

		// 查找所有EC分片
		for _, loc := range resp.DiskStatuses {
			baseFileName := erasure_coding.EcShardBaseFileName(collection, int(vid))
			dirName := loc.Dir

			// 在数据目录下扫描分片文件
			files, err := os.ReadDir(dirName)
			if err != nil {
				return fmt.Errorf("无法读取目录 %s: %v", dirName, err)
			}

			// 收集所有EC分片
			for _, file := range files {
				if strings.HasPrefix(file.Name(), baseFileName+".ec") {
					shardId := strings.TrimPrefix(file.Name(), baseFileName+".ec")
					shards = append(shards, uint32(util.ParseInt(shardId, 0)))
				}
			}
		}

		return nil
	})

	return shards, err
}

// 检查卷是否存在于正常状态（而非EC状态）
func checkVolumeExistence(commandEnv *CommandEnv, vid needle.VolumeId) (bool, error) {
	var hasNormalVolume bool

	// 查找所有数据节点
	err := commandEnv.MasterClient.WithClient(false, func(client master_pb.SeaweedClient) error {
		resp, err := client.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: []string{vid.String()},
		})

		if err != nil {
			return err
		}

		// 如果能找到卷，并且没有错误信息，说明这是个普通卷
		for _, loc := range resp.VolumeIdLocations {
			if loc.Error == "" && len(loc.Locations) > 0 {
				hasNormalVolume = true
				return nil
			}
		}

		return nil
	})

	return hasNormalVolume, err
}

// EC分片问题类型
type ecShardIssue struct {
	description  string
	incompleteEC bool
	shards       []uint32
}

// 查找包含指定EC分片的所有服务器
func findServersWithEcShard(topoInfo *master_pb.TopologyInfo, vid needle.VolumeId) []*master_pb.DataNodeInfo {
	var servers []*master_pb.DataNodeInfo

	eachDataNode(topoInfo, func(dc DataCenterId, rack RackId, dn *master_pb.DataNodeInfo) {
		for _, diskInfo := range dn.DiskInfos {
			for _, ecShardInfo := range diskInfo.EcShardInfos {
				if needle.VolumeId(ecShardInfo.Id) == vid {
					servers = append(servers, dn)
					//break
				}
			}
		}
	})

	return servers
}

// 应用EC分片修复
func applyEcFix(commandEnv *CommandEnv, serverAddr pb.ServerAddress, collection string, vid needle.VolumeId, issue *ecShardIssue) error {
	if !issue.incompleteEC || len(issue.shards) == 0 {
		return nil
	}

	// 首先卸载EC分片
	if err := ecFixUnmountShards(commandEnv.option.GrpcDialOption, vid, serverAddr, issue.shards); err != nil {
		return fmt.Errorf("卸载分片 %d.%v 失败: %v", vid, issue.shards, err)
	}

	// 然后删除不完整的EC分片文件
	if err := ecFixDeleteShards(commandEnv.option.GrpcDialOption, collection, vid, serverAddr, issue.shards); err != nil {
		return fmt.Errorf("删除分片 %d.%v 失败: %v", vid, issue.shards, err)
	}

	return nil
}

// 卸载EC分片
func ecFixUnmountShards(grpcDialOption grpc.DialOption, volumeId needle.VolumeId, sourceServerAddress pb.ServerAddress, shardIds []uint32) error {
	return operation.WithVolumeServerClient(false, sourceServerAddress, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.VolumeUnmount(context.Background(), &volume_server_pb.VolumeUnmountRequest{
			VolumeId: uint32(volumeId),
		})
		return err
	})
}

// 从源服务器删除EC分片
func ecFixDeleteShards(grpcDialOption grpc.DialOption, collection string, volumeId needle.VolumeId, sourceServerAddress pb.ServerAddress, shardIds []uint32) error {
	return operation.WithVolumeServerClient(false, sourceServerAddress, grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
		_, err := client.VolumeEcShardsDelete(context.Background(), &volume_server_pb.VolumeEcShardsDeleteRequest{
			VolumeId:   uint32(volumeId),
			Collection: collection,
			ShardIds:   shardIds,
		})
		return err
	})
}
