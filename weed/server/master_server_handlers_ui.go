package weed_server

import (
	"fmt"
	"net/http"
	"time"

	hashicorpRaft "github.com/hashicorp/raft"
	"github.com/seaweedfs/raft"

	ui "github.com/seaweedfs/seaweedfs/weed/server/master_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (ms *MasterServer) Version() string {
	return fmt.Sprintf("%s ec:%d:%d", util.Version(), erasure_coding.DataShardsCount, erasure_coding.ParityShardsCount)
}
func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	infos["Max Volume Id"] = ms.Topo.GetMaxVolumeId()

	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	if ms.Topo.RaftServer != nil {
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        raft.Server
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			ms.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.RaftServer,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusTpl.Execute(w, args)
	} else if ms.Topo.HashicorpRaft != nil {
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        *hashicorpRaft.Raft
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			ms.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.HashicorpRaft,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusNewRaftTpl.Execute(w, args)
	}
}
