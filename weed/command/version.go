package command

import (
	"fmt"
	"runtime"

	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var cmdVersion = &Command{
	Run:       runVersion,
	UsageLine: "version",
	Short:     "print SeaweedFS version",
	Long:      `Version prints the SeaweedFS version`,
}

func runVersion(cmd *Command, args []string) bool {
	if len(args) != 0 {
		cmd.Usage()
	}

	fmt.Printf("version %s %s %s\n", util.Version(), runtime.GOOS, runtime.GOARCH)
	fmt.Printf("Erasure Coding 配置信息:\n")
	fmt.Printf("--------------------------------\n")
	fmt.Printf("数据分片数量 (DataShardsCount): %d\n", erasure_coding.DataShardsCount)
	fmt.Printf("校验分片数量 (ParityShardsCount): %d\n", erasure_coding.ParityShardsCount)
	fmt.Printf("总分片数量 (TotalShardsCount): %d\n", erasure_coding.TotalShardsCount)
	fmt.Printf("EC比例: %d:%d\n", erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
	fmt.Printf("--------------------------------\n")
	return true
}
