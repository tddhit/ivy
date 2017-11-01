package raft

import (
	"encoding/gob"
	"github.com/tddhit/ivy/rpc"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	NextIndex int
	Success   bool
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type LogEntry struct {
	LogIndex int
	LogTerm  int
	Command  interface{}
}

type ApplyMsg struct {
	LogIndex int
	Command  interface{}
}

type Raft struct {
	mutex   sync.Mutex
	peers   []*rpc.Client
	server  *rpc.Server
	storage Storage

	id          int
	role        int
	currentTerm int
	votedFor    int
	voteCount   int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	heartbeatCh   chan bool
	voteGrantedCh chan bool
	leaderCh      chan bool
	commitCh      chan bool
	applyCh       chan<- ApplyMsg

	lastLogIndex int
	lastLogTerm  int
	baseLogIndex int
}

func (r *Raft) AppendCommand(command interface{}) (index int, isLeader bool) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.role != LEADER {
		return 0, false
	}
	entry := &LogEntry{
		LogIndex: r.lastLogIndex + 1,
		LogTerm:  r.currentTerm,
		Command:  command,
	}
	r.writeLog(entry)
	go r.broadcastAppendEntries()
	return entry.LogIndex, true
}

func (r *Raft) RequestVote(args RequestVoteArgs) (reply RequestVoteReply) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	reply.VoteGranted = false
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		return
	}
	if args.Term > r.currentTerm {
		r.role = FOLLOWER
		r.currentTerm = args.Term
		r.votedFor = -1
		r.saveMetadata()
	}
	reply.Term = args.Term
	var upToDate = false
	if args.LastLogTerm > r.lastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == r.lastLogTerm && args.LastLogIndex >= r.lastLogIndex {
		upToDate = true
	}
	if (r.votedFor == -1 || r.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		r.role = FOLLOWER
		r.votedFor = args.CandidateId
		r.voteGrantedCh <- true
		r.saveMetadata()
	}
	return
}

func (r *Raft) sendRequestVote(peer *rpc.Client, args RequestVoteArgs) {
	reply := peer.Call("Raft.RequestVote", args)
	select {
	case call := <-reply.Done:
		if call.Error != nil {
			peer.Dial()
		} else {
			r.mutex.Lock()
			requestVoteReply := call.Rsp.Reply[0].(RequestVoteReply)
			if r.currentTerm < requestVoteReply.Term {
				r.role = FOLLOWER
				r.currentTerm = requestVoteReply.Term
				r.votedFor = -1
				r.saveMetadata()
			}
			if requestVoteReply.VoteGranted {
				r.voteCount++
			}
			if r.voteCount > len(r.peers)/2 {
				r.leaderCh <- true
			}
			r.mutex.Unlock()
		}
	}
}

func (r *Raft) broadcastRequestVote() {
	r.mutex.Lock()
	args := RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogIndex: r.lastLogIndex,
		LastLogTerm:  r.lastLogTerm,
	}
	r.mutex.Unlock()
	for k, v := range r.peers {
		if k == r.id {
			continue
		}
		go r.sendRequestVote(v, args)
	}
}

func (r *Raft) AppendEntries(args AppendEntriesArgs) (reply AppendEntriesReply) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.heartbeatCh <- true
	if args.Term < r.currentTerm {
		reply.Success = false
		reply.Term = r.currentTerm
		return
	}
	if args.Term > r.currentTerm {
		r.role = FOLLOWER
		r.currentTerm = args.Term
		r.votedFor = -1
		r.saveMetadata()
	}
	if len(args.Entries) == 0 {
		reply.Term = args.Term
		reply.Success = true
		return
	}
	if r.lastLogIndex == 0 {
		for _, entry := range args.Entries {
			r.put(&entry)
		}
		reply.Term = args.Term
		reply.Success = true
		return
	}
	if args.PrevLogIndex > r.lastLogIndex {
		reply.NextIndex = r.lastLogIndex + 1
		reply.Term = args.Term
		reply.Success = false
		return
	}
	prevLogEntry, err := r.storage.Get(args.PrevLogIndex)
	if err != nil {
		panic(err)
	}
	if args.PrevLogTerm != prevLogEntry.LogTerm {
		for i := args.PrevLogIndex - 1; i >= r.baseLogIndex; i-- {
			entry, err := r.storage.Get(i)
			if err != nil {
				panic(err)
			}
			if entry.LogTerm != prevLogEntry.LogTerm {
				reply.NextIndex = i + 1
				reply.Term = args.Term
				reply.Success = false
				break
			}
		}
		return
	}
	r.storage.DeleteLog(args.PrevLogTerm+1, r.lastLogIndex)
	for _, entry := range args.Entries {
		r.put(&entry)
	}
	reply.Term = args.Term
	reply.Success = true
	if args.LeaderCommit > r.commitIndex {
		if args.LeaderCommit > r.lastLogIndex {
			r.commitIndex = r.lastLogIndex
		} else {
			r.commitIndex = args.LeaderCommit
		}
	}
	return
}

func (r *Raft) sendAppendEntries(id int, peer *rpc.Client, args AppendEntriesArgs) {
	reply := peer.Call("Raft.AppendEntries", args)
	select {
	case call := <-reply.Done:
		if call.Error != nil {
			log.Println(call.Error)
			peer.Dial()
		} else {
			r.mutex.Lock()
			defer r.mutex.Unlock()
			appendEntriesReply := call.Rsp.Reply[0].(AppendEntriesReply)
			if r.currentTerm < appendEntriesReply.Term {
				r.role = FOLLOWER
				r.currentTerm = appendEntriesReply.Term
				r.votedFor = -1
				r.saveMetadata()
				return
			} else {
				if len(args.Entries) == 0 {
				} else {
					if appendEntriesReply.Success {
						r.nextIndex[id] = args.Entries[len(args.Entries)-1].LogIndex + 1
						r.matchIndex[id] = r.nextIndex[id] - 1
					} else {
						r.nextIndex[id] = appendEntriesReply.NextIndex
					}
				}
			}
			commitCount := 1
			N := r.commitIndex
			for i := r.commitIndex + 1; i <= r.lastLogIndex; i++ {
				for j := 0; j < len(r.peers); j++ {
					if r.matchIndex[j] >= i {
						commitCount++
					}
				}
				entry, _ := r.storage.Get(i)
				if commitCount > len(r.peers)/2 && r.currentTerm == entry.LogTerm {
					N = i
				}
			}
			if r.commitIndex != N {
				r.commitIndex = N
				r.commitCh <- true
			}
		}
	}
}

func (r *Raft) broadcastAppendEntries() {
	for i := 0; i < len(r.peers); i++ {
		if i == r.id {
			continue
		}
		args := AppendEntriesArgs{
			Term:         r.currentTerm,
			LeaderId:     r.id,
			LeaderCommit: r.commitIndex,
			PrevLogIndex: r.nextIndex[i] - 1,
		}
		prevLogEntry, _ := r.storage.Get(args.PrevLogIndex)
		args.PrevLogTerm = prevLogEntry.LogTerm
		args.Entries = r.storage.BatchGet(args.PrevLogIndex + 1)
		go r.sendAppendEntries(i, r.peers[i], args)
	}
}

func (r *Raft) saveMetadata() {
	r.storage.Put("currentTerm", strconv.Itoa(r.currentTerm))
	r.storage.Put("votedFor", strconv.Itoa(r.votedFor))
	r.storage.Put("lastApplied", strconv.Itoa(r.lastApplied))
}

func (r *Raft) truncateLog() {
}

func (r *Raft) put(entry *LogEntry) error {
	r.lastLogIndex = entry.LogIndex
	r.lastLogTerm = entry.LogTerm
	r.baseLogIndex = 1
	return r.storage.Put(entry)
}

func (r *Raft) Recover() {
	currentTerm, err := r.storage.Get("currentTerm")
	if err == nil {
		r.currentTerm = strconv.Atoi(currentTerm)
	}
	votedFor, err := r.storage.Get("votedFor")
	if err == nil {
		r.votedFor = strconv.Atoi(votedFor)
	}
	lastApplied, err := r.storage.Get("lastApplied")
	if err == nil {
		r.lastApplied = strconv.Atoi(lastApplied)
		r.commitIndex = r.lastApplied
	}
}

func NewRaft(addrs []string, me string, applyCh chan<- ApplyMsg) *Raft {
	var id int
	peers := make([]*rpc.Client, len(addrs))
	for k, v := range addrs {
		c := rpc.NewClient(v)
		peers[k] = c
		if me == v {
			id = k
		} else {
			err := c.Dial()
			if err != nil {
				log.Println("connect failed:", v)
			}
		}
	}
	raft := &Raft{
		mutex:         sync.Mutex{},
		peers:         peers,
		id:            id,
		role:          FOLLOWER,
		votedFor:      -1,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		heartbeatCh:   make(chan bool, 10),
		voteGrantedCh: make(chan bool, 10),
		leaderCh:      make(chan bool, 10),
		commitCh:      make(chan bool, 10),
		applyCh:       applyCh,
	}
	db, err := NewLeveldb("/tmp/raft.db_" + strconv.Itoa(id))
	if err != nil {
		panic(err)
	}
	raft.storage = db
	raft.put(&LogEntry{LogIndex: 0, LogTerm: 0})
	raft.Recover()
	gob.Register(RequestVoteArgs{})
	gob.Register(RequestVoteReply{})
	gob.Register(AppendEntriesArgs{})
	gob.Register(AppendEntriesReply{})
	rand.Seed(time.Now().UnixNano())
	time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
	go func() {
		for {
			switch raft.role {
			case FOLLOWER:
				select {
				case <-raft.heartbeatCh:
				case <-raft.voteGrantedCh:
				case <-time.After(2000 * time.Millisecond):
					raft.mutex.Lock()
					raft.role = CANDIDATE
					raft.mutex.Unlock()
					log.Println("heartbeat timeout...")
				}
			case CANDIDATE:
				raft.mutex.Lock()
				raft.currentTerm++
				raft.votedFor = raft.id
				raft.voteCount = 1
				raft.saveMetadata()
				raft.mutex.Unlock()
				go raft.broadcastRequestVote()
				select {
				case <-raft.leaderCh:
					raft.mutex.Lock()
					raft.role = LEADER
					for i := 0; i < len(raft.peers); i++ {
						raft.nextIndex[i] = raft.lastLogIndex + 1
						raft.matchIndex[i] = 0
					}
					raft.mutex.Unlock()
				case <-raft.heartbeatCh:
					raft.mutex.Lock()
					raft.role = FOLLOWER
					raft.mutex.Unlock()
				case <-time.After(time.Duration(rand.Intn(151)+150) * time.Millisecond):
					log.Println("vote timeout...")
				}
			case LEADER:
				log.Println("I am leader...")
				raft.broadcastAppendEntries()
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-raft.commitCh:
				if raft.commitIndex > raft.lastApplied {
					for i := raft.lastApplied + 1; i <= raft.commitIndex; i++ {
						entry, _ := raft.storage.Get(i)
						raft.lastApplied++
						raft.saveMetadata()
						applyMsg := ApplyMsg{
							LogIndex: i,
							Command:  entry.Command,
						}
						raft.applyCh <- applyMsg
					}
				}
			}
		}
	}()
	raft.server = rpc.NewServer(me)
	raft.server.Register(raft)
	go raft.server.Start()
	return raft
}
