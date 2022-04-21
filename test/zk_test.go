package test

import (
	"fmt"
	"sync"
	"testing"

	dmi "distributed-metadata-index/pkg"
)

// CleanupZk ensures that tests can be run one after another by clearing
// the Zookeeper directory after each test.
func CleanupZk() {
	zkConn, _ := dmi.ConnectZk(dmi.ZkAddr)
	err := dmi.DeleteZkRoot(dmi.TagNameTriePath, zkConn)
	if err != nil {
		fmt.Printf("error while deleting root, err: %v\n", err)
	}
}

func TestZkBasic(t *testing.T) {
	client, _ := dmi.CreateZkClient()

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

	results, err := client.SearchTagName("aiden")
	if err != nil {
		t.Errorf("error while SearchTagName, err: %v\n", err)
	}

	if len(results) != 1 || results[0] != "aiden" {
		t.Errorf("wrong result, expect: ['aiden'], actual: %v\n", results)
	}

	t.Cleanup(CleanupZk)
}

func TestWildCard(t *testing.T) {
	client, _ := dmi.CreateZkClient()

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
			zc, err := dmi.CreateZkClient()
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

	zc, _ := dmi.CreateZkClient()
	allTagNames, err := zc.SearchAllTagName(dmi.TagNameTriePath, nil)
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
