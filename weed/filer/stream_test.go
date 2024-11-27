package filer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetLoopTimes(t *testing.T) {
	//loopTimes := int(math.Ceil(float64(31) / float64(6)))
	loopTimes := getLoopTimes(32, 6)
	assert.Equal(t, 6, loopTimes)

	loopTimes2 := getLoopTimes(1, 6)
	assert.Equal(t, 1, loopTimes2)

	loopTimes3 := getLoopTimes(0, 6)
	assert.Equal(t, 0, loopTimes3)
}

func TestGetLoopIndex(t *testing.T) {
	loopTimes := getLoopIndex(32, 6)
	assert.Equal(t, 5, loopTimes)

	loopTimes2 := getLoopIndex(1, 6)
	assert.Equal(t, 0, loopTimes2)

	loopTimes3 := getLoopIndex(0, 6)
	assert.Equal(t, 0, loopTimes3)

	loopTimes4 := getLoopIndex(5, 6)
	assert.Equal(t, 0, loopTimes4)

	loopTimes5 := getLoopIndex(6, 6)
	assert.Equal(t, 1, loopTimes5)
}
