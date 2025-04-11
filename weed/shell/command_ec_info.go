package shell

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
)

func init() {
	Commands = append(Commands, &commandEcInfo{})
}

type commandEcInfo struct {
}

func (c *commandEcInfo) Name() string {
	return "ec.info"
}

func (c *commandEcInfo) Help() string {
	return `显示当前Erasure Coding配置信息

	ec.info
	ec.info [-collection=""] [-fullPercent=95 -quietFor=1h]

	此命令会显示：
	1. 数据分片数量 (DataShardsCount)
	2. 校验分片数量 (ParityShardsCount)
	3. 总分片数量 (TotalShardsCount)
	4. EC比例 (如10:4)
	5. 待编码的卷数量 (使用-collection指定集合)
`
}

func (c *commandEcInfo) HasTag(CommandTag) bool {
	return false
}

func (c *commandEcInfo) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {
	infoCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	collection := infoCommand.String("collection", "", "指定集合名称")
	fullPercentage := infoCommand.Float64("fullPercent", 95, "卷达到最大卷大小的百分比")
	quietPeriod := infoCommand.Duration("quietFor", time.Hour, "选择在此期间内没有写入操作的卷")
	if err = infoCommand.Parse(args); err != nil {
		return nil
	}

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

	// 统计待编码的卷数量
	if *collection != "" || len(args) > 0 {
		// 如果命令行中有collection参数或者其他参数
		volumeIds, err := collectVolumeIdsForEcEncode(commandEnv, *collection, *fullPercentage, *quietPeriod)
		if err != nil {
			return err
		}

		if len(volumeIds) == 0 {
			fmt.Fprintf(writer, "集合 %s 中没有需要EC编码的卷\n", *collection)
		} else {
			fmt.Fprintf(writer, "集合 [%s] 中待EC编码的卷数量: %d\n", *collection, len(volumeIds))

			// 显示部分卷ID（如果数量多就只显示前50个）
			displayVolumeIds := volumeIds
			if len(volumeIds) > 50 {
				displayVolumeIds = volumeIds[:50]
				fmt.Fprintf(writer, "前50个待编码的卷: %v... (共 %d 个)\n", displayVolumeIds, len(volumeIds))
			} else {
				fmt.Fprintf(writer, "待编码的卷: %v\n", volumeIds)
			}
		}
	} else {
		fmt.Fprintf(writer, "提示: 使用 -collection 参数查看特定集合中待EC编码的卷数量\n")
		fmt.Fprintf(writer, "示例: ec.info -collection=myCollection -fullPercent=95 -quietFor=1h\n")
	}

	return nil
}
