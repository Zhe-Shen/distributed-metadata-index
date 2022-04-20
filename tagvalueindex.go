package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"sort"
)

type TagValueIndex struct {
	SubNodes []Node
	NodeList []uint32
	Data     string
	IsEnd    bool
}

type Node struct {
	Str  string
	Tree *TagValueIndex
}

type TagNodePair struct {
	str      string
	nodeList []uint32
}

// New returns an empty prefix Tree.
func NewTagValueIndex() *TagValueIndex {
	return new(TagValueIndex)
}

// FindAllMatchedNodes searches the prefix Tree for all strings that uniquely matches the prefix.
func (t *TagValueIndex) FindAllMatchedNodes(prefix string) (nodeList []TagNodePair, err error) {
outerLoop:
	for {
		if len(prefix) == 0 || prefix[0] == '*' {
			if t.IsEnd {
				nodeList = append(nodeList, TagNodePair{
					str:      t.Data,
					nodeList: t.NodeList,
				})
			}
			for _, n := range t.SubNodes {
				nodeList = append(nodeList, n.getAllSubNodeList()...)
			}
			return nodeList, nil
		}

		// Figure out which SubNodes to consider. use binary search for two candidate SubNodes
		var start, stop int

		ix := sort.Search(len(t.SubNodes),
			func(i int) bool { return t.SubNodes[i].Str >= prefix })
		start, stop = maxInt(0, ix-1), minInt(ix, len(t.SubNodes)-1)

		// Perform the check on all candidate SubNodes.
		for i := start; i <= stop; i++ {
			cur_node := &t.SubNodes[i]
			m := matchingChars(prefix, cur_node.Str)
			switch {
			case m == len(cur_node.Str):
				// Full Node match, so proceed down subtree.
				t, prefix = cur_node.Tree, prefix[m:]
				continue outerLoop
			case m == len(prefix) || (m == len(prefix)-1 && prefix[len(prefix)-1] == '*'):
				nodeList = cur_node.getAllSubNodeList()
				return nodeList, nil
			}
		}
		return nil, nil
	}
}

// AddTagValue a string and single one NodeList to the prefix Tree.
func (t *TagValueIndex) AddTagValue(tagValue string, nodeValue uint32) {
	originTag := tagValue
outerLoop:
	for {

		// consumed the entire string
		if len(tagValue) == 0 {
			t.IsEnd = true
			t.NodeList = append(t.NodeList, nodeValue)
			t.Data = originTag
			break outerLoop
		}

		// FindAllMatchedNodes the lexicographical Node insertion point.
		ix := sort.Search(len(t.SubNodes),
			func(i int) bool { return t.SubNodes[i].Str >= tagValue })

		// Check the SubNodes before and after the insertion point to see if we need to split one of them.
		var splitNode *Node
		var splitIndex int
	innerLoop:
		for li, lm := maxInt(ix-1, 0), minInt(ix, len(t.SubNodes)-1); li <= lm; li++ {
			sub_node := &t.SubNodes[li]
			m := matchingChars(sub_node.Str, tagValue)
			switch {
			case m == len(sub_node.Str):
				// full match, so proceed down the subtree.
				t, tagValue = sub_node.Tree, tagValue[m:]
				continue outerLoop
			case m > 0:
				// partial match need to split this Tree Node.
				splitNode, splitIndex = sub_node, m
				break innerLoop
			}
		}

		// No split necessary, insert a new Node and subtree.
		if splitNode == nil {
			subtree := &TagValueIndex{NodeList: []uint32{nodeValue}, IsEnd: true, Data: originTag}
			t.SubNodes = append(t.SubNodes[:ix],
				append([]Node{{tagValue, subtree}}, t.SubNodes[ix:]...)...)
			break outerLoop
		}

		// A split is necessary
		s1, s2 := splitNode.Str[:splitIndex], splitNode.Str[splitIndex:]
		child := &TagValueIndex{
			SubNodes: []Node{{s2, splitNode.Tree}},
		}
		splitNode.Str, splitNode.Tree = s1, child
		t, tagValue = child, tagValue[splitIndex:]
	}
}

// EncodeTagIndexToBytes convert a TagIndex struct to byte array
func EncodeTagValueIndexToBytes(p interface{}) []byte {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

// DecodeToTagIndex convert byte array to a TagIndex struct
func DecodeBytesToTagValueIndex(s []byte) TagValueIndex {
	p := TagValueIndex{}
	dec := gob.NewDecoder(bytes.NewReader(s))
	err := dec.Decode(&p)
	if err != nil {
		log.Fatal(err)
	}
	return p
}

////////////////////////////////////   Inner Functions   //////////////////////////////////////////////////

func (n *Node) getAllSubNodeList() (data []TagNodePair) {
	if n.Tree.IsEnd {
		data = append(data, TagNodePair{
			str:      n.Tree.Data,
			nodeList: n.Tree.NodeList,
		})
	}
	for _, subnode := range n.Tree.SubNodes {
		data = append(data, subnode.getAllSubNodeList()...)
	}
	return data
}

func minInt(a, b int) int {
	switch {
	case a < b:
		return a
	default:
		return b
	}
}

func maxInt(a, b int) int {
	switch {
	case a > b:
		return a
	default:
		return b
	}
}

// matchingChars returns the number of shared characters in s1 and s2,
// starting from the beginning of each string.
func matchingChars(s1, s2 string) int {
	i := 0
	for l := minInt(len(s1), len(s2)); i < l; i++ {
		if s1[i] != s2[i] {
			break
		}
	}
	return i
}
