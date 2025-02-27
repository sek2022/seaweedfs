package topology

import (
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"golang.org/x/exp/slices"
	"sort"
	"strings"
)

type TopologyInfo struct {
	Max         int64              `json:"Max"`
	Free        int64              `json:"Free"`
	DataCenters []DataCenterInfo   `json:"DataCenters"`
	Layouts     []VolumeLayoutInfo `json:"Layouts"`
	Statistics  Statistics         `json:"Statistics"`
}

type Statistics struct {
	Dc                        int64                      `json:"Dc"`
	Rack                      int64                      `json:"Rack"`
	VolumeNode                int64                      `json:"VolumeNode"`
	VolumeCount               int64                      `json:"VolumeCount"`
	WriteableVolumeCount      int64                      `json:"WriteableVolumeCount"`
	CrowdedVolumeCount        int64                      `json:"CrowdedVolumeCount"`
	ErasureCodingShardsCount  int64                      `json:"ErasureCodingShardsCount"`
	DiskTotal                 uint64                     `json:"DiskTotal"`
	DiskFree                  uint64                     `json:"DiskFree"`
	DiskUsed                  uint64                     `json:"DiskUsed"`
	DiskUsages                string                     `json:"DiskUsages"`
	RackStatistics            []RackStatistics           `json:"RackStatistics"`
	WaitFixReplicationCount   int64                      `json:"WaitFixReplicationCount"`
	WaitFixReplicationVolumes []WaitFixReplicationVolume `json:"WaitFixReplicationVolumes"`
	WaitFixEcShardsCount      int64                      `json:"WaitFixEcShardsCount"`
	WaitFixEcShardsVolumes    []WaitFixEcShardsVolume    `json:"WaitFixEcShardsVolumes"`
	WriteableVolumes          []WriteableVolume          `json:"WriteableVolumes"`
}

type WaitFixReplicationVolume struct {
	VolumeId uint32
	Urls     []string
}

type WaitFixEcShardsVolume struct {
	VolumeId      uint32
	EcShardsCount int64
	Unrepairable  bool
}

type WriteableVolume struct {
	Collection string
	VolumeId   uint32
	Urls       []string
	IsCrowed   bool
}

type RackStatistics struct {
	DcName                   string `json:"DcName"`
	Name                     string `json:"Name"`
	VolumeNode               int64  `json:"VolumeNode"`
	VolumeCount              int64  `json:"VolumeCount"`
	ActiveVolumeCount        int64  `json:"ActiveVolumeCount"`
	ErasureCodingShardsCount int64  `json:"ErasureCodingShardsCount"`
}

type VolumeLayoutCollection struct {
	Collection   string
	VolumeLayout *VolumeLayout
}

func (t *Topology) ToInfo() (info TopologyInfo) {
	info.Max = t.diskUsages.GetMaxVolumeCount()
	info.Free = t.diskUsages.FreeSpace()
	var dcs []DataCenterInfo
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		dcs = append(dcs, dc.ToInfo())
	}

	slices.SortFunc(dcs, func(a, b DataCenterInfo) int {
		return strings.Compare(string(a.Id), string(b.Id))
	})

	info.DataCenters = dcs
	var layouts []VolumeLayoutInfo
	for _, col := range t.collectionMap.Items() {
		c := col.(*Collection)
		for _, layout := range c.storageType2VolumeLayout.Items() {
			if layout != nil {
				tmp := layout.(*VolumeLayout).ToInfo()
				tmp.Collection = c.Name
				layouts = append(layouts, tmp)
			}
		}
	}
	info.Layouts = layouts
	var statistics = Statistics{}
	racksStatistics := make([]RackStatistics, 0)
	for _, dc := range info.DataCenters {
		statistics.Dc++
		for _, rk := range dc.Racks {
			rackStatistics := RackStatistics{DcName: string(dc.Id), Name: string(rk.Id)}
			statistics.Rack++
			for _, node := range rk.DataNodes {
				statistics.VolumeNode++
				statistics.VolumeCount += node.Volumes
				statistics.ErasureCodingShardsCount += node.EcShards
				rackStatistics.VolumeNode++
				rackStatistics.VolumeCount += node.Volumes
				rackStatistics.ErasureCodingShardsCount += node.EcShards
			}
			racksStatistics = append(racksStatistics, rackStatistics)
		}
	}
	volumeFree := uint64(0)
	waitFixReplicationVolumes := make([]WaitFixReplicationVolume, 0)
	writeableVolumes := make([]WriteableVolume, 0)
	for _, vlc := range t.ListVolumeLayoutCollections() {
		vl := vlc.VolumeLayout
		writable, crowded := vl.GetWritableVolumeCount()

		if writable > 0 {
			stats := vl.Stats()
			volumeFree += stats.TotalSize - stats.UsedSize
		}
		vl.accessLock.RLock()
		for vid, location := range vl.vid2location {
			if vl.rp.GetCopyCount() > len(location.list) {
				urls := make([]string, 0)
				for _, dn := range location.list {
					urls = append(urls, dn.Url())
				}
				volume := WaitFixReplicationVolume{VolumeId: uint32(vid), Urls: urls}
				waitFixReplicationVolumes = append(waitFixReplicationVolumes, volume)
			}
			if vl.volumeIsWritable(vid) {
				urls := make([]string, 0)
				for _, dn := range location.list {
					urls = append(urls, dn.Url())
				}
				volume := WriteableVolume{Collection: vlc.Collection, VolumeId: uint32(vid), Urls: urls, IsCrowed: vl.volumeIsCrowed(vid)}
				writeableVolumes = append(writeableVolumes, volume)
			}
		}
		vl.accessLock.RUnlock()

		statistics.WriteableVolumeCount += int64(writable)
		statistics.CrowdedVolumeCount += int64(crowded)
	}

	statistics.WriteableVolumes = writeableVolumes

	sort.Slice(waitFixReplicationVolumes, func(i, j int) bool {
		return waitFixReplicationVolumes[i].VolumeId < waitFixReplicationVolumes[j].VolumeId
	})

	statistics.WaitFixReplicationCount = int64(len(waitFixReplicationVolumes))
	statistics.WaitFixReplicationVolumes = waitFixReplicationVolumes

	waitFixEcShardsVolumes := make([]WaitFixEcShardsVolume, 0)
	t.ecShardMapLock.RLock()
	for vid, ecShardLocations := range t.ecShardMap {
		shardCount := ecShardLocations.shardCount()
		if shardCount == erasure_coding.TotalShardsCount {
			continue
		}

		waitFixEcShardsVolume := WaitFixEcShardsVolume{VolumeId: uint32(vid), EcShardsCount: int64(shardCount), Unrepairable: false}
		if shardCount < erasure_coding.DataShardsCount {
			waitFixEcShardsVolume.Unrepairable = true
		}
		waitFixEcShardsVolumes = append(waitFixEcShardsVolumes, waitFixEcShardsVolume)
	}
	t.ecShardMapLock.RUnlock()

	statistics.WaitFixEcShardsCount = int64(len(waitFixEcShardsVolumes))
	statistics.WaitFixEcShardsVolumes = waitFixEcShardsVolumes

	statistics.DiskTotal = uint64(info.Max) * t.volumeSizeLimit
	statistics.DiskFree = uint64(info.Free)*t.volumeSizeLimit + volumeFree
	statistics.DiskUsed = statistics.DiskTotal - statistics.DiskFree
	result := float64(statistics.DiskUsed) / float64(statistics.DiskTotal) * float64(100)
	statistics.DiskUsages = fmt.Sprintf("%.2f", result)
	statistics.RackStatistics = racksStatistics
	info.Statistics = statistics
	return
}

func (t *Topology) ListVolumeLayoutCollections() (volumeLayouts []*VolumeLayoutCollection) {
	for _, col := range t.collectionMap.Items() {
		for _, volumeLayout := range col.(*Collection).storageType2VolumeLayout.Items() {
			volumeLayouts = append(volumeLayouts,
				&VolumeLayoutCollection{col.(*Collection).Name, volumeLayout.(*VolumeLayout)},
			)
		}
	}
	return volumeLayouts
}

func (t *Topology) ToVolumeMap() interface{} {
	m := make(map[string]interface{})
	m["Max"] = t.diskUsages.GetMaxVolumeCount()
	m["Free"] = t.diskUsages.FreeSpace()
	dcs := make(map[NodeId]interface{})
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		racks := make(map[NodeId]interface{})
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			dataNodes := make(map[NodeId]interface{})
			for _, d := range rack.Children() {
				dn := d.(*DataNode)
				var volumes []interface{}
				for _, v := range dn.GetVolumes() {
					volumes = append(volumes, v)
				}
				dataNodes[d.Id()] = volumes
			}
			racks[r.Id()] = dataNodes
		}
		dcs[dc.Id()] = racks
	}
	m["DataCenters"] = dcs
	return m
}

func (t *Topology) ToVolumeLocations() (volumeLocations []*master_pb.VolumeLocation) {
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		for _, r := range dc.Children() {
			rack := r.(*Rack)
			for _, d := range rack.Children() {
				dn := d.(*DataNode)
				volumeLocation := &master_pb.VolumeLocation{
					Url:        dn.Url(),
					PublicUrl:  dn.PublicUrl,
					DataCenter: dn.GetDataCenterId(),
					GrpcPort:   uint32(dn.GrpcPort),
				}
				for _, v := range dn.GetVolumes() {
					volumeLocation.NewVids = append(volumeLocation.NewVids, uint32(v.Id))
				}
				for _, s := range dn.GetEcShards() {
					volumeLocation.NewVids = append(volumeLocation.NewVids, uint32(s.VolumeId))
				}
				volumeLocations = append(volumeLocations, volumeLocation)
			}
		}
	}
	return
}

func (t *Topology) ToTopologyInfo() *master_pb.TopologyInfo {
	m := &master_pb.TopologyInfo{
		Id:        string(t.Id()),
		DiskInfos: t.diskUsages.ToDiskInfo(),
	}
	for _, c := range t.Children() {
		dc := c.(*DataCenter)
		m.DataCenterInfos = append(m.DataCenterInfos, dc.ToDataCenterInfo())
	}
	return m
}
