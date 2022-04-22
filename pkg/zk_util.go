package pkg

import (
	"fmt"

	"github.com/go-zookeeper/zk"
)

func DeleteZkRoot(root string, zkConn *zk.Conn) error {
	children, _, err := zkConn.Children(root)
	if err != nil {
		return err
	}

	for _, child := range children {
		err = DeleteZkRoot(fmt.Sprintf("%s/%s", root, child), zkConn)
		if err != nil {
			return err
		}
	}

	return zkConn.Delete(root, -1)
}

func JoinPath(parent string, childName string) string {
	return fmt.Sprintf("%s/%s", parent, childName)
}
