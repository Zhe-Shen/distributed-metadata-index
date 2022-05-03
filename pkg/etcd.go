package pkg

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// CreateClient returns an etcd client
func CreateClient() (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{EtcdHost1, EtcdHost2, EtcdHost3},
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

// DeleteAll deletes all key-value pairs in etcd
func DeleteAll() error {
	cli, err := CreateClient()
	if err != nil {
		return err
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = cli.Delete(ctx, string(0), clientv3.WithRange(string(255)))
	if err != nil {
		return err
	}
	return nil
}
