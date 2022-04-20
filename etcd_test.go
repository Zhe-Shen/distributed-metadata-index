package main

import (
	"fmt"
	"testing"
)

func TestEtcd(t *testing.T) {
	tree := NewTagValueIndex()

	// add new tag value
	tree.AddTagValue("intel", 0)
	tree.AddTagValue("intel-i7", 1)
	tree.AddTagValue("intel-i7", 2)
	tree.AddTagValue("intel-i7", 5)
	tree.AddTagValue("intel-i9", 4)
	tree.AddTagValue("amd", 3)
	treeb := EncodeTagValueIndexToBytes(tree)
	err := PutIndex("cpu", treeb)
	if err != nil {
		t.Errorf(err.Error())
	}

	treeb, err = GetIndex("cpu")
	if err != nil {
		t.Errorf(err.Error())
	}

	// convert bytes to TagValueIndex
	treed := DecodeBytesToTagValueIndex(treeb)

	fmt.Printf("%-18s %-8s %s\n", "prefix", "data", "error")
	fmt.Printf("%-18s %-8s %s\n", "------", "----", "-----")

	for _, prefix := range []string{
		"a",
		"a*",
		"intel",
		"inte*",
		"intel-i7",
		"intel-i9",
	} {
		data, err := treed.FindAllMatchedNodes(prefix)
		fmt.Printf("%-18s %-8v %v\n", prefix, data, err)
	}
}
