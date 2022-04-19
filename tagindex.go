package main

import (
	"bytes"
	"encoding/gob"
	"log"
)

// TagIndex is a struct to store tag value and corresponding node list.
// TagIndex can be used in Etcd where key is tag name, value is TagIndex
type TagIndex struct {
	TagIndexMap map[string]string // key: tag value; value: node list
}

// CreateNewTagIndex creates a new TagIndex.
func CreateNewTagIndex() (*TagIndex, error) {
	return &TagIndex{
		TagIndexMap: make(map[string]string),
	}, nil
}

// GetAllTagPairs return all the kv pairs in the tag index
func (ti *TagIndex) GetAllTagPairs() map[string]string {
	return ti.TagIndexMap
}

// EncodeTagIndexToBytes convert a TagIndex struct to byte array
func EncodeTagIndexToBytes(p interface{}) []byte {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(p)
	if err != nil {
		log.Fatal(err)
	}
	//fmt.Println("encoded size (bytes): ", len(buf.Bytes()))
	return buf.Bytes()
}

// DecodeToTagIndex convert byte array to a TagIndex struct
func DecodeToTagIndex(s []byte) TagIndex {
	p := TagIndex{}
	dec := gob.NewDecoder(bytes.NewReader(s))
	err := dec.Decode(&p)
	if err != nil {
		log.Fatal(err)
	}
	return p
}

////////////////////////////////// Test For TagIndex ///////////////////////////////////

//func main() {
//	ti, _ := CreateNewTagIndex()
//	ti.TagIndexMap["intel"] = "1,2,3"
//	ti.TagIndexMap["amd"] = "1,3"
//	tib := EncodeTagIndexToBytes(ti)
//	tid := DecodeToTagIndex(tib)
//	fmt.Println(tid)
//}
