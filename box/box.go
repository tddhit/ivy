package main

import (
	"encoding/gob"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tddhit/ivy/raft"
	"github.com/tddhit/ivy/rpc"
	"log"
	"os"
	"sync"
	"time"
)

type Box struct {
	mutex    sync.Mutex
	raftNode *raft.Raft
	applyCh  chan raft.ApplyMsg
	appendCh map[int]chan Op
	db       *leveldb.DB
}

const (
	SET = iota
	GET
	DELETE
)

type Op struct {
	Kind  int
	Key   string
	Value string
}

func NewBox() *Box {
	peers := make([]string, 0)
	peers = append(peers, "127.0.0.1:3871")
	peers = append(peers, "127.0.0.1:4871")
	peers = append(peers, "127.0.0.1:5871")
	me := os.Args[1]
	meBytes := []byte(me)
	meBytes[len(meBytes)-1] = '1'
	me = string(meBytes)
	gob.Register(Op{})
	db, err := leveldb.OpenFile("/tmp/box.db_"+string(meBytes[len(meBytes)-4:]), nil)
	if err != nil {
		panic(err)
	}
	b := &Box{
		mutex:    sync.Mutex{},
		applyCh:  make(chan raft.ApplyMsg, 100),
		appendCh: make(map[int]chan Op),
		db:       db,
	}
	b.raftNode = raft.NewRaft(peers, me, b.applyCh)
	go func() {
		for {
			select {
			case applyMsg := <-b.applyCh:
				op := applyMsg.Command.(Op)
				if op.Kind == SET {
					b.set(op.Key, op.Value)
					_, ok := b.appendCh[applyMsg.LogIndex]
					if !ok {
						b.appendCh[applyMsg.LogIndex] = make(chan Op, 100)
					}
					b.appendCh[applyMsg.LogIndex] <- op
				}
			}
		}
	}()
	return b
}

func (b *Box) set(key, value string) {
	b.db.Put([]byte(key), []byte(value), nil)
}

func (b *Box) Set(key, value string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	op := Op{
		Kind:  SET,
		Key:   key,
		Value: value,
	}
	logIndex, isLeader := b.raftNode.AppendCommand(op)
	if isLeader {
		_, ok := b.appendCh[logIndex]
		if !ok {
			b.appendCh[logIndex] = make(chan Op, 100)
		}
		select {
		case <-b.appendCh[logIndex]:
			log.Println("set success")
		case <-time.After(3500 * time.Millisecond):
			log.Println("set failed")
		}
	}
}

func (b *Box) Get(key string) (string, bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	value, err := b.db.Get([]byte(key), nil)
	if err != nil {
		return "", false
	}
	return string(value), true
}

func (b *Box) Delete(key string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.db.Delete([]byte(key), nil)
}

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags | log.Lmicroseconds)
	b := NewBox()
	if len(os.Args) < 2 {
		panic("usage:./box ip:port")
	}
	s := rpc.NewServer(os.Args[1])
	s.Register(b)
	s.Start()
}
