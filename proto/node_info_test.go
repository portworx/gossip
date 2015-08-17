package proto

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

const (
	CPU    string = "CPU"
	Memory string = "Memory"
)

func fillUpNodeInfo(nodes *NodeValue) {
	nodes.Nodes = make([]NodeInfo, 3)
	for i, nodeInfo := range nodes.Nodes {
		nodeInfo.Id = NodeId(i + 1)
		nodeInfo.LastUpdateTs = time.Now()

		value := make(map[string]NodeId)
		value[CPU] = nodeInfo.Id
		value[Memory] = nodeInfo.Id
		nodeInfo.Value = value
		fmt.Printf("Node: ", nodeInfo)
	}
}

func verifyMetaInfo(nodes *NodeValue,
	t *testing.T) {
	m := nodes.MetaInfo()
	metaInfo, ok := m.(NodeMetaInfoList)
	if !ok {
		t.Error("Invalid type returned for metaInfo ",
			reflect.TypeOf(metaInfo))
	}

	// check len
	if len(metaInfo.MetaInfos) != len(nodes.Nodes) {
		t.Error("MetaInfo len ", len(metaInfo.MetaInfos),
			" does not match nodes len ", len(nodes.Nodes))
	}

	// check empty node contents
	for i, metaInfo := range metaInfo.MetaInfos {
		if metaInfo.Id != nodes.Nodes[i].Id {
			t.Error("Invalid Node Id: Expected:",
				nodes.Nodes[i].Id, " , Got: ",
				metaInfo.Id)
		}
		fmt.Println("Id is :", metaInfo.Id)

		if metaInfo.LastUpdateTs !=
			nodes.Nodes[i].LastUpdateTs {
			t.Error("Invalid Node Id: Expected:",
				nodes.Nodes[i].Id, " , Got: ",
				metaInfo.Id)
		}
	}
}

func TestMetaInfo(t *testing.T) {
	var nodes NodeValue
	nodes.Nodes = make([]NodeInfo, 3)

	// Test empty nodes values
	m := nodes.MetaInfo()
	metaInfo, ok := m.(NodeMetaInfoList)
	if !ok {
		t.Error("Invalid type returned for metaInfo ",
			reflect.TypeOf(metaInfo))
	}

	// check len
	if len(metaInfo.MetaInfos) != len(nodes.Nodes) {
		t.Error("MetaInfo len ", len(metaInfo.MetaInfos),
			" does not match nodes len ", len(nodes.Nodes))
	}

	// check empty node contents
	for _, metaInfo := range metaInfo.MetaInfos {
		if metaInfo.Id != 0 {
			t.Error("Invalid nodeId for null node: ", metaInfo.Id)
		}
	}

	// fill it up with values
	fillUpNodeInfo(&nodes)
	for i, nodeInfo := range nodes.Nodes {
		fmt.Println("After i ", i, "\n")
		fmt.Println("Node: ", nodeInfo)
	}
	verifyMetaInfo(&nodes, t)
}
