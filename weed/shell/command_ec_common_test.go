package shell

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/stretchr/testify/assert"
	"testing"
)

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
