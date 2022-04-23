package pkg

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-zookeeper/zk"
)

const lockParentNode = "lock"
const lockPrefix = "lock-"

// DistLock is a distributed lock that can be initialized with a root Zookeeper
// path and a Zookeeper connection. It can write, via the Zookeeper connection,
// to the root path.
//
// CounterClients use DistLock to acquire control of certain paths in Zookeeper
// (most importantly /counter) and prevents other CounterClients from also modifying
// the data at t path concurrently.
type DistLock struct {
	root   string // root zk path in which the lock is placed
	path   string // full zk path of the lock
	zkConn *zk.Conn
}

// CreateDistLock creates a distributed lock
func CreateDistLock(root string, zkConn *zk.Conn) (*DistLock, error) {
	_, err := zkConn.Create(JoinPath(root, lockParentNode), nil, 0, zk.WorldACL(zk.PermAll))
	if err != nil && err.Error() != "zk: node already exists" {
		fmt.Println(err)
		return nil, err
	}

	dlock := &DistLock{
		root:   root,
		path:   "",
		zkConn: zkConn,
	}
	return dlock, nil
}

// Acquire tries acquire a distributed lock from Zookeeper. If another client already acquired the lock,
// it waits until the lock is released.
//
// The basic recipe is as follows:
// 1. Call Create() with a pathname of "<lock-root>/lock-" and the sequence and ephemeral flags set.
// 2. Call Children() on the lock node without setting the watch flag.
// 3. If the pathname created in step 1 has the lowest sequence number suffix,
//    the client has the lock and should exit the protocol.
// 4. The client calls Exists() with the watch flag set on the path in the lock directory
//    with the next lowest sequence number
// 5. if Exists() returns false, go to step 2.
//    Otherwise, wait for a notification for the pathname from the previous step before going to step 2.
func (d *DistLock) Acquire() (err error) {
	if d.path != "" {
		return errors.New("the lock is already acquired")
	}

	// 1. Call Create() with a pathname of "<lock-root>/lock-" and the sequence and ephemeral flags set.
	curPath, err := d.zkConn.Create(
		JoinPath(d.root, lockParentNode, lockPrefix),
		nil,
		zk.FlagSequence|zk.FlagEphemeral,
		zk.WorldACL(zk.PermAll),
	)
	if err != nil {
		return err
	}

	d.path = curPath
	curSeq, err := getSeqNumFromZkPath(d.path)
	if err != nil {
		return err
	}
	for {
		// 2. Call Children() on the lock node without setting the watch flag.
		children, _, err := d.zkConn.Children(JoinPath(d.root, lockParentNode))
		if err != nil {
			return err
		}

		minSeq := curSeq
		for _, child := range children {
			childSeq, err := getSeqNumFromZkPath(child)
			if err != nil {
				return err
			}

			if childSeq < minSeq {
				minSeq = childSeq
			}
		}

		// 3. If the pathname created in step 1 has the lowest sequence number suffix,
		//    the client has the lock and should exit the protocol.
		if curSeq == minSeq {
			return nil
		}

		// 4. The client calls Exists() with the watch flag set on the path in the lock directory
		//    with the next lowest sequence number
		exists, _, ech, err := d.zkConn.ExistsW(createZkPathFromSeqNum(JoinPath(d.root, lockParentNode, lockPrefix), minSeq))
		if err != nil {
			return err
		}

		// 5. if Exists() returns false, go to step 2.
		//    Otherwise, wait for a notification for the pathname from the previous step before going to step 2.
		if exists {
			<-ech
		}
	}
}

// The unlock protocol is very simple: clients wishing to release a lock simply delete the node they created in step 1.
func (d *DistLock) Release() (err error) {
	if d.path == "" {
		return errors.New("is not locked in the first place")
	}

	err = d.zkConn.Delete(d.path, -1)
	if err != nil {
		return err
	}

	d.path = ""
	return nil
}

func getSeqNumFromZkPath(path string) (int, error) {
	return strconv.Atoi(strings.Split(path, "-")[1])
}

func createZkPathFromSeqNum(root string, seqId int) string {
	return root + fmt.Sprintf("%010d", seqId)
}
