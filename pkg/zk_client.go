package pkg

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

const endOfWordNode = "/eow"

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

	exists, _, err := zc.zkConn.Exists(parent + endOfWordNode)
	if err != nil {
		return err
	}

	if !exists {
		_, err = zc.zkConn.Create(parent+endOfWordNode, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}

	return nil
}

func (zc *ZkClient) SearchTagName(regexp string) (results []string, err error) {
	parent := TagNameTriePath
	parentLock, err := CreateDistLock(parent, zc.zkConn)
	if err != nil {
		return nil, err
	}
	parentLock.Acquire()

	for i, character := range regexp {
		if character != '*' {
			curPath := parent + fmt.Sprintf("/%c", character)
			exists, _, err := zc.zkConn.Exists(curPath)
			if err != nil {
				parentLock.Release()
				return nil, err
			}

			if !exists {
				return results, nil
			}

			childLock, err := CreateDistLock(curPath, zc.zkConn)
			if err != nil {
				parentLock.Release()
				return nil, err
			}

			err = childLock.Acquire()
			if err != nil {
				parentLock.Release()
				return nil, err
			}
			err = parentLock.Release()
			if err != nil {
				childLock.Release()
				return nil, err
			}

			parent = curPath
			parentLock = childLock

		} else {
			if i != len(regexp)-1 {
				return results, errors.New("* wildcard needs to be the last character in regexp")
			}
			return zc.SearchAllTagName(parent, parentLock)
		}
	}

	defer parentLock.Release()
	exists, _, err := zc.zkConn.Exists(parent + endOfWordNode)
	if exists {
		results = append(results, regexp)
	}

	return results, err
}

func (zc *ZkClient) SearchAllTagName(parent string, parentLock *DistLock) (results []string, err error) {
	if parentLock == nil {
		parentLock, err = CreateDistLock(parent, zc.zkConn)
		if err != nil {
			return results, err
		}
		parentLock.Acquire()
	}

	defer parentLock.Release()

	children, _, err := zc.zkConn.Children(parent)
	if err != nil {
		return results, err
	}

	exists, _, err := zc.zkConn.Exists(parent + endOfWordNode)
	if err != nil {
		return results, err
	}

	if exists {
		results = append(results, getTagNameFromPath(parent))
	}

	for _, child := range children {
		// TODO: ignore /lock and /eow
		if child == "lock" || child == "eow" {
			continue
		}
		childResults, err := zc.SearchAllTagName(fmt.Sprintf("%s/%s", parent, child), nil)
		if err != nil {
			return results, err
		}

		results = append(results, childResults...)
	}

	return results, nil
}

func getTagNameFromPath(path string) string {
	return strings.Join(strings.Split(path, "/")[2:], "")
}
