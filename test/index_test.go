package test

import (
	dmi "distributed-metadata-index/pkg"
	"fmt"
	"testing"
)

func TestIndexBasic(t *testing.T) {

	tree := dmi.NewTagValueIndex()

	// add new tag value
	tree.AddTagValue("intel", 0)
	tree.AddTagValue("intel-i7", 1)
	tree.AddTagValue("intel-i7", 2)
	tree.AddTagValue("intel-i7", 5)
	tree.AddTagValue("intel-i9", 4)
	tree.AddTagValue("amd", 3)

	// convert TagValueIndex to bytes
	treeb := dmi.EncodeTagValueIndexToBytes(tree)
	err := dmi.PutIndex("cpu", treeb)
	if err != nil {
		t.Errorf(err.Error())
	}

	treeb, err = dmi.GetIndex("cpu")
	if err != nil {
		t.Errorf(err.Error())
	}
	// convert bytes to TagValueIndex
	treed := dmi.DecodeBytesToTagValueIndex(treeb)

	fmt.Printf("%-18s %-8s %s\n", "prefix", "data", "error")
	fmt.Printf("%-18s %-8s %s\n", "------", "----", "-----")

	for _, prefix := range []string{
		"c",
		"a",
		"a*",
		"amd",
		"int",
		"intel",
		"intel*",
		"intel-i7",
		"intel-i9",
	} {
		data, err := treed.FindAllMatchedNodes(prefix)
		fmt.Printf("%-18s %-8v %v\n", prefix, data, err)
	}
}
