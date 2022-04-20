package main

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// CreateClient returns an etcd client
func CreateClient() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		return nil, err
	}
	return cli, err
}

// PutIndex stores tagName and index as key-value pair in etcd
func PutIndex(tagName string, index []byte) error {
	cli, err := CreateClient()
	if err != nil {
		return err
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Put(ctx, tagName, string(index))
	if err != nil {
		return err
	}
	return err
}

// GetIndex returns index bytes array with the specified tagName
func GetIndex(tagName string) ([]byte, error) {
	cli, err := CreateClient()
	if err != nil {
		return nil, err
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Get(ctx, tagName)
	if err != nil {
		return nil, err
	}
	var res []byte
	for _, ev := range resp.Kvs {
		res = ev.Value
		break
	}
	return res, nil
}

/*
// SearchWithTrueValue searches etcd with specified tagName and tagValue
func SearchWithTrueValue(tagName string, tagValue string) (string, error) {
	indexBytes, err := GetIndex(tagName)
	if err != nil {
		return "", err
	}
	tagIndex := DecodeToTagIndex(indexBytes)
	if res, ok := tagIndex.TagIndexMap[tagValue]; ok {
		res = tagName + "=" + tagValue + ": " + res
		return res, nil
	} else {
		return "", fmt.Errorf("Tag key-value pair %v=%v not found", tagName, tagIndex)
	}
}

// SearchWithTrueValue searches etcd with specified tagName and a prefix of tagValue
func SearchWithPrefix(tagName string, prefix string) ([]string, error) {
	indexBytes, err := GetIndex(tagName)
	res := make([]string, 0)
	if err != nil {
		return res, err
	}
	tagIndex := DecodeToTagIndex(indexBytes)
	for tagValue, nodeList := range tagIndex.TagIndexMap {
		if strings.HasPrefix(tagValue, prefix) {
			tmpStr := tagName + "=" + tagValue + ": " + nodeList
			res = append(res, tmpStr)
		}
	}
	return res, nil
}
*/
