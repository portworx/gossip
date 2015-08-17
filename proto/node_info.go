package proto

import (
	log "github.com/Sirupsen/logrus"
	"reflect"
	"sync"
	"time"
)

type StoreKey string

// StoreValueMetaInfo is the meta info associated with
// store value
type StoreValueMetaInfo interface{}

// StoreValueIdInfo is the id info associated with
// store value
type StoreValueIdInfo interface{}

type StoreValueDiff struct {
	Ids interface{}
}

// StoreValue is the value store for each key
// in a store
type StoreValue interface {
	MetaInfo() StoreValueMetaInfo
	IdInfo() StoreValueIdInfo
	Update(StoreValue)
	UpdateSelfValue(interface{})
	Diff(StoreValueMetaInfo) (diffNew StoreValueDiff, selfNew StoreValueDiff)
	DiffValue(StoreValueDiff) StoreValue
}

type StoreValueMap map[StoreKey]StoreValue
type StoreValueIdInfoMap map[StoreKey]StoreValueDiff
type StoreValueMetaInfoMap map[StoreKey]StoreValueMetaInfo

type GossipStore interface {
	// Update updates the current state of the gossip data
	// with the newly available data
	Update(newData StoreValueMap)

	// UpdateStoreValue updates the value for the given key
	UpdateStoreValue(StoreKey, interface{})

	// GetStoreValue returns the StoreValue associated with
	// the given key
	GetStoreValue(key StoreKey) StoreValue

	// GetStoreKeys returns all the keys present in the store
	GetStoreKeys() []StoreKey

	// Subset returns the available gossip data for the given
	// nodes. Node data is returned if there is none available
	// for a given node
	Subset(nodes StoreValueIdInfoMap) StoreValueMap

	// MetaInfoMap returns meta information for the
	// current available data
	MetaInfo() StoreValueMetaInfoMap

	// Diff returns a tuple of lists, where
	// first list is of the names of node for which
	// the current data is newer as compared to the
	// given meta info, and old list is the names
	// of nodes for which the current data is older
	Diff(d StoreValueMetaInfoMap) (StoreValueIdInfoMap, StoreValueIdInfoMap)
}

/********************** StoreValue Implementation ***********************/

type NodeId uint16

type NodeInfo struct {
	Id           NodeId
	LastUpdateTs time.Time
	Value        interface{}
}

type NodeMetaInfo struct {
	Id           NodeId
	LastUpdateTs time.Time
}

// NodeValue implements the StoreValue interface
type NodeValue struct {
	Nodes []NodeInfo
}

// NodeMetaInfoList implements the StoreValueMetaInfo interface
type NodeMetaInfoList struct {
	MetaInfos []NodeMetaInfo
}

// NodeIdInfoList implements StoreValueIdInfo interface
type NodeIdInfoList struct {
	NodeIds []NodeId
}

func (s NodeValue) MetaInfo() StoreValueMetaInfo {
	var metaInfo NodeMetaInfoList
	metaInfo.MetaInfos = make([]NodeMetaInfo, len(s.Nodes)) // check for nil
	for i, data := range s.Nodes {
		metaInfo.MetaInfos[i] = NodeMetaInfo{data.Id, data.LastUpdateTs}
	}
	return metaInfo
}

func (s NodeValue) IdInfo() StoreValueIdInfo {
	var idInfo NodeIdInfoList
	idInfo.NodeIds = make([]NodeId, len(s.Nodes))
	for _, data := range s.Nodes {
		idInfo.NodeIds = append(idInfo.NodeIds, data.Id)
	}
	return idInfo
}

func (s NodeValue) UpdateSelfValue(val interface{}) {
	var nodeInfo NodeInfo
	nodeInfo, ok := val.(NodeInfo)
	if !ok {
		log.Error("Invalid type for value Update(): ", reflect.TypeOf(val))
		return
	}

	maxLen := NodeId(len(s.Nodes))
	if maxLen < nodeInfo.Id {
		for i := maxLen; i <= nodeInfo.Id; i++ {
			s.Nodes = append(s.Nodes, NodeInfo{})
		}
	}

	// Add asset len(s.Nodes) > nodeInfo.id
	nodeInfo.LastUpdateTs = time.Now()
	s.Nodes[nodeInfo.Id] = nodeInfo
}

func (s NodeValue) Update(newStore StoreValue) {
	var newValues NodeValue
	newValues, ok := newStore.(NodeValue)
	if !ok {
		log.Error("Invalid type for Update()", reflect.TypeOf(newStore))
		return
	}

	if newValues.Nodes == nil {
		log.Info("Nothing to update")
		return
	}

	oldLen := len(s.Nodes)
	for i, newData := range newValues.Nodes {
		switch {
		case newData.Id == 0:
			continue
		case i >= oldLen,
			s.Nodes[i].Id == 0:
			log.Info("Appended nil new value at position ", i)
			s.Nodes = append(s.Nodes, newData)
		case s.Nodes[i].LastUpdateTs.Before(newData.LastUpdateTs):
			log.Info("Appended new value at position ", i)
			s.Nodes[i] = newData
		case s.Nodes[i].LastUpdateTs.After(newData.LastUpdateTs):
			log.Info("Skipping value at position ", i,
				" since we have newer data")
		default:
			continue
		}
	}
}

func (s NodeValue) Diff(
	svMetaInfo StoreValueMetaInfo) (StoreValueDiff, StoreValueDiff) {

	maxLen := len(s.Nodes)
	var diffNew, selfNew StoreValueDiff
	newIds := make([]NodeId, maxLen)
	oldIds := make([]NodeId, maxLen)

	var metaInfo NodeMetaInfoList
	metaInfo, ok := svMetaInfo.(NodeMetaInfoList)
	if !ok {
		log.Error("Invalid type for Diff()", reflect.TypeOf(svMetaInfo))
		return diffNew, selfNew
	}

	for i, nodeIdInfo := range metaInfo.MetaInfos {
		if i >= maxLen {
			newIds = append(newIds, nodeIdInfo.Id)
			oldIds = append(oldIds, 0)
			continue
		}

		if nodeIdInfo.Id != 0 &&
			nodeIdInfo.LastUpdateTs.After(s.Nodes[i].LastUpdateTs) {
			newIds = append(newIds, nodeIdInfo.Id)
			oldIds = append(oldIds, 0)
			continue
		} else {
			oldIds = append(oldIds, nodeIdInfo.Id)
			newIds = append(newIds, 0)
			continue
		}
	}

	diffNew.Ids = newIds
	selfNew.Ids = oldIds
	return diffNew, selfNew
}

func (s NodeValue) DiffValue(
	svDiff StoreValueDiff) StoreValue {
	var sValue NodeValue
	if svDiff.Ids == nil {
		return sValue
	}

	newIds, ok := svDiff.Ids.([]NodeId)
	if !ok {
		log.Error("Invalid type for new id list ", reflect.TypeOf(svDiff.Ids))
	}

	sValue.Nodes = make([]NodeInfo, len(newIds))
	maxLen := len(s.Nodes)
	for i, newId := range newIds {
		if newId == 0 || i >= maxLen {
			// nothing to do
			continue
		}

		sValue.Nodes[i] = s.Nodes[i]
	}

	return sValue
}

type NodeValueMap struct {
	lock  sync.Mutex
	kvMap map[StoreKey]NodeValue
}

type NodeValueMetaMap map[StoreKey]NodeMetaInfo
type NodeIdInfoMap map[StoreKey]NodeIdInfoList

// NewOldList implements the StoreValueIdInfoMap interface
type NewOldList struct {
	NewerList StoreValueIdInfo
	OlderList StoreValueIdInfo
}

func (s NodeValueMap) Update(newStore StoreValueMap) {
	s.lock.Lock()
	defer s.lock.Unlock()

	for key, newValue := range newStore {
		newNodeValue, ok := newValue.(NodeValue)
		if !ok {
			log.Error("Invalid type for StoreValue.Update()", reflect.TypeOf(newNodeValue))
			return
		}
		selfValue, ok := s.kvMap[key]
		if !ok {
			s.kvMap[key] = newNodeValue
			continue
		}
		selfValue.Update(newNodeValue)
	}
}

func NewGossipStore() GossipStore {
	var n NodeValueMap
	n.kvMap = make(map[StoreKey]NodeValue)
	return n
}

func (s NodeValueMap) UpdateStoreValue(key StoreKey, val interface{}) {
	s.lock.Lock()
	defer s.lock.Unlock()

	var nodeValue NodeValue
	nodeValue, ok := s.kvMap[key]
	if !ok {
		nodeValue.UpdateSelfValue(val)
		s.kvMap[key] = nodeValue
	}
	nodeValue.UpdateSelfValue(val)
}

func (s NodeValueMap) GetStoreValue(key StoreKey) StoreValue {
	s.lock.Lock()
	defer s.lock.Unlock()

	var storeValue StoreValue
	storeValue, _ = s.kvMap[key]
	return storeValue
}

func (s NodeValueMap) GetStoreKeys() []StoreKey {
	s.lock.Lock()
	defer s.lock.Unlock()

	storeKeys := make([]StoreKey, len(s.kvMap))
	i := 0
	for key, _ := range s.kvMap {
		storeKeys[i] = key
		i++
	}
	return storeKeys
}

func (s NodeValueMap) MetaInfo() StoreValueMetaInfoMap {
	s.lock.Lock()
	defer s.lock.Unlock()

	mInfo := make(StoreValueMetaInfoMap, len(s.kvMap))

	for key, nodeValue := range s.kvMap {
		mInfo[key] = nodeValue.MetaInfo()
	}

	return mInfo
}

func (s NodeValueMap) Diff(
	d StoreValueMetaInfoMap) (StoreValueIdInfoMap, StoreValueIdInfoMap) {
	s.lock.Lock()
	defer s.lock.Unlock()

	diffNew := make(StoreValueIdInfoMap)
	selfNew := make(StoreValueIdInfoMap)
	for key, value := range d {
		selfValue, _ := s.kvMap[key]
		dN, sN := selfValue.Diff(value)
		diffNew[key] = dN
		selfNew[key] = sN
	}

	return diffNew, selfNew
}

func (s NodeValueMap) Subset(nodes StoreValueIdInfoMap) StoreValueMap {
	s.lock.Lock()
	defer s.lock.Unlock()

	subset := make(StoreValueMap)

	for key, nodeIdInfo := range nodes {
		node, ok := s.kvMap[key]
		if !ok {
			log.Error("No subset for key ", key)
			continue
		}
		subset[key] = node.DiffValue(nodeIdInfo)
	}

	return subset
}
