package weed_server

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"os"
	"testing"
)

func TestVolumeServer_UploadFile(t *testing.T) {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	baseFileName := util.Join(os.TempDir(), "t1.vif")

	ext := ".ecj"

	needAppend := ext == ".ecj"
	if needAppend {
		flags = os.O_WRONLY | os.O_CREATE
	}
	file, fileErr := os.OpenFile(baseFileName, flags, 0644)
	defer file.Close()
	if fileErr != nil {
		fmt.Printf("writing file error:%s, %v \n", baseFileName, fileErr)
	}
	fmt.Println(flags)
	fmt.Println(file.Name())

	file.WriteString("11122233")

}

func TestDecompress(t *testing.T) {
	data := []byte{31, 139, 163, 86, 159, 88, 126, 96}
	var err error
	if data, err = util.DecompressData(data); err != nil {
		fmt.Println("uncompress error:", err)
		return
	}

	fmt.Println(data)
}
