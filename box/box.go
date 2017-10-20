package main

import (
	"encoding/gob"
	"errors"
	"github.com/tddhit/ivy/raft"
	"github.com/tddhit/ivy/rpc"
	"log"
	"os"
	"sync"
)

type Box struct {
	mutex    sync.Mutex
	raftNode *raft.Raft
	applyCh  chan *raft.ApplyMsg
	kv       map[string]string
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
	b := &Box{
		mutex:   sync.Mutex{},
		applyCh: make(chan *raft.ApplyMsg, 100),
		kv:      make(map[string]string),
	}
	b.raftNode = raft.NewRaft(peers, me, b.applyCh)
	gob.Register(raft.RequestVoteArgs{})
	gob.Register(raft.RequestVoteReply{})
	gob.Register(raft.AppendEntriesArgs{})
	gob.Register(raft.AppendEntriesReply{})
	go func() {
		select {
		case applyMsg := <-b.applyCh:
			op := applyMsg.Command.(Op)
			if op.Kind == SET {
				b.set(op.Key, op.Value)
			}
		}
	}()
	return b
}

func (b *Box) set(key, value string) {
	b.kv[key] = value
}

func (b *Box) Set(key, value string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	op := Op{
		Kind:  SET,
		Key:   key,
		Value: value,
	}
	b.raftNode.AppendCommand(op)
}

func (b *Box) Get(key string) (string, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if value, ok := b.kv[key]; ok {
		return value, nil
	} else {
		return "", errors.New("not found.")
	}
}

func (b *Box) Delete(key string) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	if _, ok := b.kv[key]; ok {
		delete(b.kv, key)
		return nil
	} else {
		return errors.New("not found.")
	}
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
