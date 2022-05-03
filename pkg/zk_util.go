package pkg

import (
	"fmt"
	"strings"

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

func JoinPath(parent string, childNames ...string) string {
	for _, child := range childNames {
		parent = parent + "/" + child
	}
	return parent
}

func GetTagNameFromPath(path string) string {
	return strings.Join(strings.Split(path, "/")[2:], "")
}
