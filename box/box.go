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
	RaftNode *raft.Raft
	kv       map[string]string
}

func NewBox() *Box {
	peers := make([]string, 0)
	peers = append(peers, "127.0.0.1:3871")
	peers = append(peers, "127.0.0.1:4871")
	peers = append(peers, "127.0.0.1:5871")
	me := os.Args[1]
	b := &Box{
		mutex:    sync.Mutex{},
		RaftNode: raft.NewRaft(peers, me),
		kv:       make(map[string]string),
	}
	log.Printf("RaftNode:%p\n", &(b.RaftNode))
	log.Printf("Box:%p\n", b)
	gob.Register(raft.RequestVoteArgs{})
	gob.Register(raft.RequestVoteReply{})
	gob.Register(raft.AppendEntriesArgs{})
	gob.Register(raft.AppendEntriesReply{})
	return b
}

func (b *Box) Set(key, value string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.kv[key] = value
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
	log.Printf("Box:%p\n", b)
	if len(os.Args) < 2 {
		panic("usage:./box ip:port")
	}
	s := rpc.NewServer(os.Args[1])
	s.Register(b)
	log.Printf("!%p\n", &(b.RaftNode))
	s.Register(b.RaftNode)
	s.Start()
}
