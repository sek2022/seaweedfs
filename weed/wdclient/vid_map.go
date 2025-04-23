package wdclient

import (
	"context"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

const (
	maxCursorIndex = 4096
)

type HasLookupFileIdFunction interface {
	GetLookupFileIdFunction() LookupFileIdFunctionType
}

type LookupFileIdFunctionType func(fileId string) (targetUrls []string, err error)

type Location struct {
	Url        string `json:"url,omitempty"`
	PublicUrl  string `json:"publicUrl,omitempty"`
	DataCenter string `json:"dataCenter,omitempty"`
	GrpcPort   int    `json:"grpcPort,omitempty"`
}

func (l Location) ServerAddress() pb.ServerAddress {
	return pb.NewServerAddressWithGrpcPort(l.Url, l.GrpcPort)
}

type vidMap struct {
	sync.RWMutex
	vid2Locations   map[uint32][]Location
	ecVid2Locations map[uint32][]Location
	fid2Locations   map[string][]Location
	DataCenter      string
	cursor          int32
	cache           *vidMap
}

func newVidMap(dataCenter string) *vidMap {
	return &vidMap{
		vid2Locations:   make(map[uint32][]Location),
		ecVid2Locations: make(map[uint32][]Location),
		fid2Locations:   make(map[string][]Location),
		DataCenter:      dataCenter,
		cursor:          -1,
	}
}

func (vc *vidMap) getLocationIndex(length int) (int, error) {
	if length <= 0 {
		return 0, fmt.Errorf("invalid length: %d", length)
	}
	if atomic.LoadInt32(&vc.cursor) == maxCursorIndex {
		atomic.CompareAndSwapInt32(&vc.cursor, maxCursorIndex, -1)
	}
	return int(atomic.AddInt32(&vc.cursor, 1)) % length, nil
}

func (vc *vidMap) isSameDataCenter(loc *Location) bool {
	if vc.DataCenter == "" || loc.DataCenter == "" || vc.DataCenter != loc.DataCenter {
		return false
	}
	return true
}

func (vc *vidMap) LookupVolumeServerUrl(vid string) (serverUrls []string, err error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil, err
	}

	locations, found := vc.GetLocations(uint32(id))
	if !found {
		return nil, fmt.Errorf("volume %d not found", id)
	}
	var sameDcServers, otherDcServers []string
	for _, loc := range locations {
		if vc.isSameDataCenter(&loc) {
			sameDcServers = append(sameDcServers, loc.Url)
		} else {
			otherDcServers = append(otherDcServers, loc.Url)
		}
	}
	rand.Shuffle(len(sameDcServers), func(i, j int) {
		sameDcServers[i], sameDcServers[j] = sameDcServers[j], sameDcServers[i]
	})
	rand.Shuffle(len(otherDcServers), func(i, j int) {
		otherDcServers[i], otherDcServers[j] = otherDcServers[j], otherDcServers[i]
	})
	// Prefer same data center
	serverUrls = append(sameDcServers, otherDcServers...)
	return
}

func (vc *vidMap) LookupFileLocations(fid string) (serverLocations []Location, err error) {
	locations, found := vc.GetFileLocations(fid)
	if !found {
		return nil, fmt.Errorf("fileId %s not found", fid)
	}
	var sameDcServers, otherDcServers []Location
	for _, loc := range locations {
		if vc.isSameDataCenter(&loc) {
			sameDcServers = append(sameDcServers, loc)
		} else {
			otherDcServers = append(otherDcServers, loc)
		}
	}
	rand.Shuffle(len(sameDcServers), func(i, j int) {
		sameDcServers[i], sameDcServers[j] = sameDcServers[j], sameDcServers[i]
	})
	rand.Shuffle(len(otherDcServers), func(i, j int) {
		otherDcServers[i], otherDcServers[j] = otherDcServers[j], otherDcServers[i]
	})
	// Prefer same data center
	serverLocations = append(sameDcServers, otherDcServers...)
	return
}

func (vc *vidMap) LookupFileId(fileId string) (fullUrls []string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return nil, errors.New("Invalid fileId " + fileId)
	}
	serverUrls, lookupError := vc.LookupVolumeServerUrl(parts[0])
	if lookupError != nil {
		return nil, lookupError
	}
	for _, serverUrl := range serverUrls {
		fullUrls = append(fullUrls, "http://"+serverUrl+"/"+fileId)
	}
	return
}

func (vc *vidMap) LookupFileIdUrls(fileId string) (fullUrls []string, err error) {
	locs, err := vc.LookupFileIdLocations(fileId)
	if err != nil {
		return nil, err
	}
	for _, loc := range locs {
		fullUrls = append(fullUrls, "http://"+loc.Url+"/"+fileId)
	}
	return
}

func (vc *vidMap) LookupFileIdLocations(fileId string) (locations []Location, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return nil, errors.New("Invalid fileId " + fileId)
	}

	serverLocations, lookupError := vc.LookupFileLocations(fileId)
	if lookupError != nil {
		return nil, lookupError
	}
	for _, loc := range serverLocations {
		locations = append(locations, loc)
	}
	return
}

func (vc *vidMap) GetVidLocations(vid string) (locations []Location, err error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil, fmt.Errorf("Unknown volume id %s", vid)
	}
	foundLocations, found := vc.GetLocations(uint32(id))
	if found {
		return foundLocations, nil
	}
	return nil, fmt.Errorf("volume id %s not found", vid)
}

func (vc *vidMap) IsEcVolume(vid string) (ec bool, err error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		return false, fmt.Errorf("unknown volume id %s", vid)
	}
	vc.RLock()
	defer vc.RUnlock()

	locations, found := vc.vid2Locations[uint32(id)]
	if found && len(locations) > 0 {
		ec = false
		return
	}
	locations, found = vc.ecVid2Locations[uint32(id)]
	if found && len(locations) > 0 {
		ec = true
		return
	}
	return
}

func VolumeId(fileId string) (string, bool) {
	lastCommaIndex := strings.LastIndex(fileId, ",")
	if lastCommaIndex > 0 {
		return fileId[:lastCommaIndex], true
	}
	return fileId, false
}

func (vc *vidMap) GetVolumeOrFileIdLocations(fileOrVolumeId string, grpcDialOption grpc.DialOption) (locations []Location, found bool) {
	// glog.V(4).Infof("~ lookup volume id %d: %+v ec:%+v", vid, vc.vid2Locations, vc.ecVid2Locations)
	var ec = false
	vidStr, isFile := VolumeId(fileOrVolumeId)
	vid, err := strconv.Atoi(vidStr)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %d", vid)
		return nil, false
	}
	locations, found, ec = vc.getLocations(uint32(vid))
	selectedLocs := locations
	if found && len(locations) > 0 {
		if ec && isFile {
			//fmt.Println("-----vid_map.go GetVolumeOrFileIdLocations")
			var fileEcShardIds map[string]*volume_server_pb.EcShardIds
			var hasLoopVolumeServerUrls = make(map[string]string)
			for _, loc := range locations {
				if _, b := hasLoopVolumeServerUrls[loc.Url]; b { //every volume server loop one times
					continue
				}

				hasLoopVolumeServerUrls[loc.Url] = loc.Url
				err2 := pb.WithVolumeServerClient(false, loc.ServerAddress(), grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
					resp, genErr := client.VolumeEcShardDataNodesForFileId(context.Background(), &volume_server_pb.VolumeEcShardDataNodesForFileIdRequest{
						FileIds:    []string{fileOrVolumeId},
						Collection: "",
					})

					if genErr != nil {
						return genErr
					}
					fileEcShardIds = resp.FileShardIds
					return nil
				})

				if err2 != nil {
					fmt.Println("-----loc:", loc.ServerAddress()+",error:", err2)
					continue
				}
				if len(fileEcShardIds) > 0 && fileEcShardIds[fileOrVolumeId] != nil && len(fileEcShardIds[fileOrVolumeId].ShardIds) > 0 {
					break
				}
			}

			if len(fileEcShardIds) > 0 && fileEcShardIds[fileOrVolumeId] != nil {
				//fmt.Println("-----fileId:", fileOrVolumeId, " is a ec file,fileDataNodes[fileOrVolumeId].DataNode:", fileEcShardIds[fileOrVolumeId].ShardIds)
				selectedLocs = make([]Location, 0)
				for sid, loc := range locations {
					for _, shardId := range fileEcShardIds[fileOrVolumeId].ShardIds {
						if uint32(sid) == shardId {
							selectedLocs = append(selectedLocs, loc)
							break
						}
					}
				}
			}
		}

		return locations, found
	}

	if vc.cache != nil {
		return vc.cache.GetVolumeOrFileIdLocations(fileOrVolumeId, grpcDialOption)
	}

	return nil, false
}

func (vc *vidMap) GetLocations(vid uint32) (locations []Location, found bool) {
	// glog.V(4).Infof("~ lookup volume id %d: %+v ec:%+v", vid, vc.vid2Locations, vc.ecVid2Locations)
	locations, found, _ = vc.getLocations(vid)

	if found && len(locations) > 0 {
		return locations, found
	}

	if vc.cache != nil {
		return vc.cache.GetLocations(vid)
	}

	return nil, false
}

func (vc *vidMap) GetFileLocations(fid string) (locations []Location, found bool) {
	// glog.V(4).Infof("~ lookup volume id %d: %+v ec:%+v", vid, vc.vid2Locations, vc.ecVid2Locations)
	locations, found = vc.getFileLocations(fid)

	if found && len(locations) > 0 {
		return locations, found
	}

	if vc.cache != nil {
		return vc.cache.GetFileLocations(fid)
	}

	return nil, false
}

func (vc *vidMap) GetLocationsClone(vid uint32) (locations []Location, found bool) {
	locations, found = vc.GetLocations(vid)

	if found {
		// clone the locations in case the volume locations are changed below
		existingLocations := make([]Location, len(locations))
		copy(existingLocations, locations)
		return existingLocations, found
	}

	return nil, false
}

func (vc *vidMap) getLocations(vid uint32) (locations []Location, found bool, ec bool) {
	vc.RLock()
	defer vc.RUnlock()
	ec = false
	locations, found = vc.vid2Locations[vid]
	if found && len(locations) > 0 {
		return
	}
	ec = true
	locations, found = vc.ecVid2Locations[vid]
	return
}

func (vc *vidMap) getFileLocations(fid string) (locations []Location, found bool) {
	vc.RLock()
	defer vc.RUnlock()
	locations, found = vc.fid2Locations[fid]
	if found && len(locations) > 0 {
		return
	}
	return
}

func (vc *vidMap) addLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("+ volume id %d: %+v", vid, location)

	locations, found := vc.vid2Locations[vid]
	if !found {
		vc.vid2Locations[vid] = []Location{location}
		return
	}

	for _, loc := range locations {
		if loc.Url == location.Url {
			return
		}
	}

	vc.vid2Locations[vid] = append(locations, location)

}

func (vc *vidMap) addFileLocation(fid string, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("+ file id %s: %+v", fid, location)
	locations, found := vc.fid2Locations[fid]
	if !found {
		vc.fid2Locations[fid] = []Location{location}
		return
	}

	for _, loc := range locations {
		if loc.Url == location.Url {
			return
		}
	}

	vc.fid2Locations[fid] = append(locations, location)

}

func (vc *vidMap) addEcLocation(vid uint32, location Location) {
	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("+ ec volume id %d: %+v", vid, location)

	locations, found := vc.ecVid2Locations[vid]
	if !found {
		vc.ecVid2Locations[vid] = []Location{location}
		return
	}

	for _, loc := range locations {
		if loc.Url == location.Url {
			return
		}
	}

	vc.ecVid2Locations[vid] = append(locations, location)

}

func (vc *vidMap) deleteLocation(vid uint32, location Location) {
	if vc.cache != nil {
		vc.cache.deleteLocation(vid, location)
	}

	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("- volume id %d: %+v", vid, location)

	locations, found := vc.vid2Locations[vid]
	if !found {
		return
	}

	for i, loc := range locations {
		if loc.Url == location.Url {
			vc.vid2Locations[vid] = append(locations[0:i], locations[i+1:]...)
			break
		}
	}
}

func (vc *vidMap) deleteEcLocation(vid uint32, location Location) {
	if vc.cache != nil {
		vc.cache.deleteLocation(vid, location)
	}

	vc.Lock()
	defer vc.Unlock()

	glog.V(4).Infof("- ec volume id %d: %+v", vid, location)

	locations, found := vc.ecVid2Locations[vid]
	if !found {
		return
	}

	for i, loc := range locations {
		if loc.Url == location.Url {
			vc.ecVid2Locations[vid] = append(locations[0:i], locations[i+1:]...)
			break
		}
	}
}
