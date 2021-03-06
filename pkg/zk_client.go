package pkg

import (
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
)

const endOfWordNode = "eow"

const ASTERISK_WILDCARD = '*' // matches zero or more characters
const DOT_WILDCARD = '?'      // matches any single character

// ConnectZk sets up a zookeeper connection
func ConnectZk(zkAddr string) (*zk.Conn, error) {
	conn, _, err := zk.Connect([]string{zkAddr}, 1*time.Second)
	return conn, err
}

func InitTagNameTriePath(zkConn *zk.Conn) (err error) {
	exists, _, err := zkConn.Exists(TagNameTriePath)
	if err != nil {
		return err
	}

	if !exists {
		_, err = zkConn.Create(TagNameTriePath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

type ZkClient struct {
	zkConn *zk.Conn
}

func CreateZkClient() (*ZkClient, error) {
	zkConn, err := ConnectZk(ZkAddr)
	if err != nil {
		return nil, err
	}

	client := &ZkClient{
		zkConn: zkConn,
	}

	rootlock, err := CreateDistLock("", zkConn)
	if err != nil {
		return nil, err
	}
	rootlock.Acquire()
	InitTagNameTriePath(zkConn)
	rootlock.Release()

	return client, nil
}

func (zc *ZkClient) AddTagName(tagName string) error {
	parent := TagNameTriePath
	parentLock, err := CreateDistLock(parent, zc.zkConn)
	if err != nil {
		return err
	}
	parentLock.Acquire()

	for i := 0; i < len(tagName); i++ {
		character := tagName[i]
		curPath := parent + fmt.Sprintf("/%c", character)

		exists, _, err := zc.zkConn.Exists(curPath)
		if err != nil {
			parentLock.Release()
			return err
		}
		if !exists {
			_, err = zc.zkConn.Create(curPath, nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				parentLock.Release()
				return err
			}
		}

		// Fine-grained Locking: lock-crabbing
		// release parentLock after childLock is acquired
		childLock, err := CreateDistLock(curPath, zc.zkConn)
		if err != nil {
			parentLock.Release()
			return err
		}
		err = childLock.Acquire()
		if err != nil {
			parentLock.Release()
			return err
		}
		err = parentLock.Release()
		if err != nil {
			childLock.Release()
			return err
		}

		parent = curPath
		parentLock = childLock
	}

	defer parentLock.Release()

	exists, _, err := zc.zkConn.Exists(JoinPath(parent, endOfWordNode))
	if err != nil {
		return err
	}

	if !exists {
		_, err = zc.zkConn.Create(JoinPath(parent, endOfWordNode), nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

func (zc *ZkClient) SearchTagName(regexp string) (results []string, err error) {
	return zc.searchTagNameFromParent(TagNameTriePath, nil, regexp)
}

// A recursive function that supports *-wildcard and ?-wildcard search in a Trie data structure
func (zc *ZkClient) searchTagNameFromParent(parent string, parentLock *DistLock, regexp string) (results []string, err error) {
	if parentLock == nil {
		parentLock, err = CreateDistLock(parent, zc.zkConn)
		if err != nil {
			return results, err
		}
		parentLock.Acquire()
	}

	if len(regexp) == 0 {
		exists, _, err := zc.zkConn.Exists(JoinPath(parent, endOfWordNode))
		if exists {
			results = append(results, GetTagNameFromPath(parent))
		}
		parentLock.Release()
		return results, err
	}

	character := regexp[0]
	switch character {
	case ASTERISK_WILDCARD:
		children, _, err := zc.zkConn.Children(parent)
		if err != nil {
			parentLock.Release()
			return results, err
		}

		wildCardIsEmptyResults, err := zc.searchTagNameFromParent(parent, parentLock, regexp[1:])
		if err != nil {
			parentLock.Release()
			return results, err
		}
		results = append(results, wildCardIsEmptyResults...)

		for _, child := range children {
			// future improvement: goroutine
			if child == lockParentNode || child == endOfWordNode {
				continue
			}

			curPath := JoinPath(parent, child)
			wildCardMatchesResults, err := zc.searchTagNameFromParent(curPath, nil, regexp)
			if err != nil {
				parentLock.Release()
				return results, err
			}
			results = append(results, wildCardMatchesResults...)
		}

		// for wildcards, we will not release parentLock until all children are traversed
		parentLock.Release()

	case DOT_WILDCARD:
		children, _, err := zc.zkConn.Children(parent)
		if err != nil {
			parentLock.Release()
			return results, err
		}

		for _, child := range children {
			// future improvement: goroutine
			if child == lockParentNode || child == endOfWordNode {
				continue
			}

			curPath := JoinPath(parent, child)
			childResults, err := zc.searchTagNameFromParent(curPath, nil, regexp[1:])
			if err != nil {
				parentLock.Release()
				return results, err
			}

			results = append(results, childResults...)
		}

		// for wildcards, we will not release parentLock until all children are traversed
		parentLock.Release()

	default:
		curPath := JoinPath(parent, string(character))
		exists, _, err := zc.zkConn.Exists(curPath)
		if err != nil {
			parentLock.Release()
			return results, err
		}

		if exists {
			childLock, err := CreateDistLock(curPath, zc.zkConn)
			if err != nil {
				parentLock.Release()
				return results, err
			}

			// Fine-grained Locking: lock-crabbing
			// release parentLock after childLock is acquired
			childLock.Acquire()
			parentLock.Release()

			childResults, err := zc.searchTagNameFromParent(curPath, childLock, regexp[1:])
			if err != nil {
				return results, err
			}

			results = append(results, childResults...)
		}

		parentLock.Release()
	}

	return results, err
}
