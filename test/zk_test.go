package test

import (
	"sync"
	"testing"

	zk "distributed-metadata-index/pkg"
)

// CleanupZk ensures that tests can be run one after another by clearing
// the Zookeeper directory after each test.
func CleanupZk() {
	zkConn, _ := zk.ConnectZk(zk.ZkAddr)
	zkConn.Delete(zk.TagNameTriePath, -1)
	children, _, _ := zkConn.Children(zk.TagNameTriePath)
	for _, c := range children {
		zkConn.Delete(c, -1)
	}
}

func TestZkBasic(t *testing.T) {
	client, _ := zk.CreateZkClient()

	err := client.AddTagName("abc")
	if err != nil {
		t.Errorf("error while AddTagName, err: %v\n", err)
	}
	err = client.AddTagName("aiden")
	if err != nil {
		t.Errorf("error while AddTagName, err: %v\n", err)
	}
	err = client.AddTagName("efg")
	if err != nil {
		t.Errorf("error while AddTagName, err: %v\n", err)
	}

	results, err := client.SearchTagName("a")
	if err != nil {
		t.Errorf("error while SearchTagName, err: %v\n", err)
	}

	if len(results) != 1 || results[0] != "aiden" {
		t.Errorf("wrong result, expect: ['aiden'], actual: %v\n", results)
	}

	t.Cleanup(CleanupZk)
}

func TestWildCard(t *testing.T) {
	client, _ := zk.CreateZkClient()

	err := client.AddTagName("cpu")
	if err != nil {
		t.Errorf("error while AddTagName, err: %v\n", err)
	}
	err = client.AddTagName("cpa")
	if err != nil {
		t.Errorf("error while AddTagName, err: %v\n", err)
	}
	err = client.AddTagName("efg")
	if err != nil {
		t.Errorf("error while AddTagName, err: %v\n", err)
	}

	results, err := client.SearchTagName("cp*")
	if err != nil {
		t.Errorf("error while SearchTagName, err: %v\n", err)
	}

	if len(results) != 2 {
		t.Errorf("wrong result, expect: ['cpu', 'cpa], actual: %v\n", results)
	}

	t.Cleanup(CleanupZk)
}

func TestConcurrentAdd(t *testing.T) {
	numClients := 10
	tagNames := [10]string{"abc", "acd", "bde", "bdf", "aba", "abc", "bac", "cef", "caf", "def"}
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(idx int) {
			zc, err := zk.CreateZkClient()
			if err != nil {
				t.Error(err)
			}
			err = zc.AddTagName(tagNames[idx])
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	zc, _ := zk.CreateZkClient()
	allTagNames, err := zc.SearchAllTagName(zk.TagNameTriePath, nil)
	if err != nil {
		t.Error(err)
	}
	if len(allTagNames) != 9 {
		t.Errorf("number of all tags is incorrect, expect: 9, acutal: %v\n", len(allTagNames))
	}

	for _, actualTag := range tagNames {
		exists := false
		for _, tag := range allTagNames {
			if actualTag == tag {
				exists = true
				break
			}
		}
		if !exists {
			t.Errorf("cannot find %v in the trie\n", actualTag)
		}
	}

	t.Cleanup(CleanupZk)
}
