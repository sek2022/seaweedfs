package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"

	"github.com/seaweedfs/seaweedfs/weed/glog"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/topology"
)

func (ms *MasterServer) lookupVolumeId(vids []string, collection string) (volumeLocations map[string]operation.LookupResult) {

	volumeLocations = make(map[string]operation.LookupResult)
	for _, vid := range vids {
		isFile := false
		fileId := vid
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			isFile = true
			vid = vid[0:commaSep]
		}

		if !isFile { //vid is a volumeId, key is vid
			if _, ok := volumeLocations[vid]; ok {
				continue
			}
			volumeLocations[vid] = ms.findVolumeLocation(collection, vid)
		}

		if isFile { //vid is a fileId, key is fileId
			volumeLocations[fileId] = ms.findVolumeLocation(collection, fileId)
		}
	}
	return
}

// If "fileId" is provided, this returns the fileId location and a JWT to update or delete the file.
// If "volumeId" is provided, this only returns the volumeId location
func (ms *MasterServer) dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	if vid != "" {
		// backward compatible
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
	}
	fileId := r.FormValue("fileId")
	if fileId != "" {
		commaSep := strings.Index(fileId, ",")
		if commaSep > 0 {
			vid = fileId[0:commaSep]
		}
	}
	collection := r.FormValue("collection") // optional, but can be faster if too many collections
	location := ms.findVolumeLocation(collection, vid)
	httpStatus := http.StatusOK
	if location.Error != "" || location.Locations == nil {
		httpStatus = http.StatusNotFound
	} else {
		forRead := r.FormValue("read")
		isRead := forRead == "yes"
		ms.maybeAddJwtAuthorization(w, fileId, !isRead)
	}
	writeJsonQuiet(w, r, httpStatus, location)
}

// findVolumeLocation finds the volume location from master topo if it is leader,
// or from master client if not leader
func (ms *MasterServer) findVolumeLocation(collection, fileOrVolumeId string) operation.LookupResult {
	var locations []operation.Location
	var err error
	var isEc bool
	var isEcBusy bool
	isFile := false
	vid := fileOrVolumeId
	commaSep := strings.Index(fileOrVolumeId, ",")
	if commaSep > 0 {
		isFile = true
		vid = fileOrVolumeId[0:commaSep]
	}
	if ms.Topo.IsLeader() {
		volumeId, newVolumeIdErr := needle.NewVolumeId(vid)
		if newVolumeIdErr != nil {
			err = fmt.Errorf("unknown volume id %s", vid)
		} else {
			machines, ec := ms.Topo.Lookup(collection, volumeId)
			isEc = ec
			for _, loc := range machines {
				if loc.IsErasureCoding {
					isEcBusy = true
					glog.V(3).Infof("-----master_server_handler:%s is ECoding......", loc.ServerAddress())
				}
				locations = append(locations, operation.Location{
					Url:        loc.Url(),
					PublicUrl:  loc.PublicUrl,
					DataCenter: loc.GetDataCenterId(),
					GrpcPort:   loc.GrpcPort,
					Coding:     loc.IsErasureCoding,
				})
			}
		}
	} else {
		isEc, err = ms.MasterClient.IsEcVolume(vid)
		machines, getVidLocationsErr := ms.MasterClient.GetVidLocations(vid)
		for _, loc := range machines {
			locations = append(locations, operation.Location{
				Url:        loc.Url,
				PublicUrl:  loc.PublicUrl,
				DataCenter: loc.DataCenter,
				GrpcPort:   loc.GrpcPort,
			})
		}
		err = getVidLocationsErr
	}
	if len(locations) == 0 && err == nil {
		err = fmt.Errorf("volume id %s not found", vid)
	}
	selectedLocs := locations
	//ec file process
	if isFile && isEc && err == nil {
		//fmt.Println("-----fileId:", fileOrVolumeId, " is a ec file,locations:", locations)
		//get locations from volume server
		var fileEcShardIds map[string]*volume_server_pb.EcShardIds
		//[
		//{192.168.68.50:10013 192.168.68.50:10013 DefaultDataCenter 20013}
		//{192.168.68.50:10014 192.168.68.50:10014 DefaultDataCenter 20014}
		//{192.168.68.50:10011 192.168.68.50:10011 DefaultDataCenter 20011}
		//{192.168.68.50:10012 192.168.68.50:10012 DefaultDataCenter 20012}
		//{192.168.68.50:10013 192.168.68.50:10013 DefaultDataCenter 20013}
		//{192.168.68.50:10014 192.168.68.50:10014 DefaultDataCenter 20014}
		//{192.168.68.50:10011 192.168.68.50:10011 DefaultDataCenter 20011}
		//{192.168.68.50:10012 192.168.68.50:10012 DefaultDataCenter 20012} {192.168.68.50:10013 192.168.68.50:10013 DefaultDataCenter 20013} {192.168.68.50:10014 192.168.68.50:10014 DefaultDataCenter 20014} {192.168.68.50:10011 192.168.68.50:10011 DefaultDataCenter 20011} {192.168.68.50:10012 192.168.68.50:10012 DefaultDataCenter 20012} {192.168.68.50:10013 192.168.68.50:10013 DefaultDataCenter 20013} {192.168.68.50:10014 192.168.68.50:10014 DefaultDataCenter 20014}]
		var hasLoopVolumeServerUrls = make(map[string]string)
		for _, loc := range locations {
			if _, b := hasLoopVolumeServerUrls[loc.Url]; b { //every volume server loop one times
				continue
			}

			hasLoopVolumeServerUrls[loc.Url] = loc.Url
			err2 := pb.WithVolumeServerClient(false, loc.ServerAddress(), ms.grpcDialOption, func(client volume_server_pb.VolumeServerClient) error {
				resp, genErr := client.VolumeEcShardDataNodesForFileId(context.Background(), &volume_server_pb.VolumeEcShardDataNodesForFileIdRequest{
					FileIds:    []string{fileOrVolumeId},
					Collection: collection,
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
			selectedLocs = make([]operation.Location, 0)
			for sid, loc := range locations {
				for _, shardId := range fileEcShardIds[fileOrVolumeId].ShardIds {
					if uint32(sid) == shardId && !containLoc(selectedLocs, loc) {
						selectedLocs = append(selectedLocs, loc)
						break
					}
				}
			}
		}
	}

	// 添加排序逻辑，将non-coding节点排在前面
	if len(selectedLocs) > 1 {
		sort.SliceStable(selectedLocs, func(i, j int) bool {
			return !selectedLocs[i].Coding && selectedLocs[j].Coding
		})
		if isEcBusy {
			glog.V(3).Infof("need sort,after sort:%v", selectedLocs)
		}
	}

	//fmt.Println("-----fileId:", fileOrVolumeId, " is a ec file,selectedLocs:", selectedLocs)
	ret := operation.LookupResult{
		VolumeOrFileId: vid,
		Locations:      selectedLocs,
	}
	if err != nil {
		ret.Error = err.Error()
	}
	return ret
}

func containLoc(locs []operation.Location, addLoc operation.Location) bool {
	for _, loc := range locs {
		if loc.Url == addLoc.Url {
			return true
		}
	}
	return false
}

func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	stats.AssignRequest()
	requestedCount, e := strconv.ParseUint(r.FormValue("count"), 10, 64)
	if e != nil || requestedCount == 0 {
		requestedCount = 1
	}

	writableVolumeCount, e := strconv.ParseUint(r.FormValue("writableVolumeCount"), 10, 32)
	if e != nil {
		writableVolumeCount = 0
	}

	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: err.Error()})
		return
	}

	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl, option.DiskType)

	var (
		lastErr    error
		maxTimeout = time.Second * 10
		startTime  = time.Now()
	)

	if !ms.Topo.DataCenterExists(option.DataCenter) {
		writeJsonQuiet(w, r, http.StatusBadRequest, operation.AssignResult{
			Error: fmt.Sprintf("data center %v not found in topology", option.DataCenter),
		})
		return
	}

	for time.Now().Sub(startTime) < maxTimeout {
		fid, count, dnList, shouldGrow, err := ms.Topo.PickForWrite(requestedCount, option, vl)
		if shouldGrow && !vl.HasGrowRequest() {
			glog.V(0).Infof("dirAssign volume growth %v from %v", option.String(), r.RemoteAddr)
			if err != nil && ms.Topo.AvailableSpaceFor(option) <= 0 {
				err = fmt.Errorf("%s and no free m1 volumes left for %s", err.Error(), option.String())
			}
			vl.AddGrowRequest()
			ms.volumeGrowthRequestChan <- &topology.VolumeGrowRequest{
				Option: option,
				Count:  uint32(writableVolumeCount),
				Reason: "http assign",
			}
		}
		if err != nil {
			stats.MasterPickForWriteErrorCounter.Inc()
			lastErr = err
			time.Sleep(200 * time.Millisecond)
			continue
		} else {
			ms.maybeAddJwtAuthorization(w, fid, true)
			dn := dnList.Head()
			if dn == nil {
				continue
			}
			writeJsonQuiet(w, r, http.StatusOK, operation.AssignResult{Fid: fid, Url: dn.Url(), PublicUrl: dn.PublicUrl, Count: count})
			return
		}
	}

	if lastErr != nil {
		writeJsonQuiet(w, r, http.StatusNotAcceptable, operation.AssignResult{Error: lastErr.Error()})
	} else {
		writeJsonQuiet(w, r, http.StatusRequestTimeout, operation.AssignResult{Error: "request timeout"})
	}
}

func (ms *MasterServer) maybeAddJwtAuthorization(w http.ResponseWriter, fileId string, isWrite bool) {
	if fileId == "" {
		return
	}
	var encodedJwt security.EncodedJwt
	if isWrite {
		encodedJwt = security.GenJwtForVolumeServer(ms.guard.SigningKey, ms.guard.ExpiresAfterSec, fileId)
	} else {
		encodedJwt = security.GenJwtForVolumeServer(ms.guard.ReadSigningKey, ms.guard.ReadExpiresAfterSec, fileId)
	}
	if encodedJwt == "" {
		return
	}

	w.Header().Set("Authorization", "BEARER "+string(encodedJwt))
}
