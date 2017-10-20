package raft

import (
	"github.com/tddhit/ivy/rpc"
	"log"
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
	Command interface{}
}

type Raft struct {
	mutex  sync.Mutex
	peers  []*rpc.Client
	server *rpc.Server

	id          int
	role        int
	currentTerm int
	votedFor    int
	log         []LogEntry
	voteCount   int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	heartbeatCh   chan bool
	voteGrantedCh chan bool
	leaderCh      chan bool
	commitCh      chan bool
	applyCh       chan<- *ApplyMsg
}

func (r *Raft) AppendCommand(command interface{}) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	logEntry := LogEntry{
		LogIndex: r.log[len(r.log)-1].LogIndex + 1,
		LogTerm:  r.currentTerm,
		Command:  command,
	}
	r.log = append(r.log, logEntry)
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
	}
	reply.Term = args.Term
	var (
		lastLogIndex int
		lastLogTerm  int
	)
	if len(r.log) > 0 {
		lastLogIndex = r.log[len(r.log)-1].LogIndex
		lastLogTerm = r.log[len(r.log)-1].LogTerm
	}
	var upToDate = false
	if args.LastLogTerm > lastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		upToDate = true
	}
	if (r.votedFor == -1 || r.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		r.role = FOLLOWER
		r.votedFor = args.CandidateId
		r.voteGrantedCh <- true
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
		LastLogIndex: r.log[len(r.log)-1].LogIndex,
		LastLogTerm:  r.log[len(r.log)-1].LogTerm,
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
	}
	if len(args.Entries) == 0 {
		reply.Term = args.Term
		reply.Success = true
		return
	}
	if len(r.log) == 0 {
		r.log = append(r.log, args.Entries...)
		reply.Term = args.Term
		reply.Success = true
		return
	}
	if args.PrevLogIndex > r.log[len(r.log)-1].LogIndex {
		reply.NextIndex = r.log[len(r.log)-1].LogIndex + 1
		reply.Term = args.Term
		reply.Success = false
		return
	}
	baseLogIndex := r.log[0].LogIndex
	if args.PrevLogIndex > baseLogIndex {
		term := r.log[args.PrevLogIndex-baseLogIndex].LogTerm
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= baseLogIndex; i-- {
				if r.log[i-baseLogIndex].LogTerm != term {
					reply.NextIndex = i + 1
					reply.Term = args.Term
					reply.Success = false
					return
				}
			}
		}
	}
	r.log = append(r.log[:args.PrevLogIndex-baseLogIndex+1], args.Entries...)
	reply.Term = args.Term
	reply.Success = true
	if args.LeaderCommit > r.commitIndex {
		if args.LeaderCommit > r.log[len(r.log)-1].LogIndex {
			r.commitIndex = r.log[len(r.log)-1].LogIndex
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
			peer.Dial()
		} else {
			r.mutex.Lock()
			defer r.mutex.Unlock()
			appendEntriesReply := call.Rsp.Reply[0].(AppendEntriesReply)
			if r.currentTerm < appendEntriesReply.Term {
				r.role = FOLLOWER
				r.currentTerm = appendEntriesReply.Term
				r.votedFor = -1
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
			for i := r.commitIndex + 1; i < r.log[len(r.log)-1].LogIndex; i++ {
				for j := 0; j < len(r.peers); j++ {
					if r.matchIndex[j] >= i {
						commitCount++
					}
				}
				if commitCount > len(r.peers)/2 && r.currentTerm == r.log[i].LogTerm {
					N = i
				}
			}
			if r.currentTerm != N {
				r.commitIndex = N
				r.commitCh <- true
			}
		}
	}
}

func (r *Raft) broadcastAppendEntries() {
	baseLogIndex := r.log[0].LogIndex
	for i := 0; i < len(r.peers); i++ {
		if i == r.id {
			continue
		}
		if r.nextIndex[i] > baseLogIndex {
			args := AppendEntriesArgs{
				Term:         r.currentTerm,
				LeaderId:     r.id,
				LeaderCommit: r.commitIndex,
				PrevLogIndex: r.nextIndex[i] - 1,
			}
			args.PrevLogTerm = r.log[args.PrevLogIndex-baseLogIndex].LogTerm
			args.Entries = make([]LogEntry, len(r.log[args.PrevLogIndex-baseLogIndex+1:]))
			copy(args.Entries, r.log[args.PrevLogIndex-baseLogIndex+1:])
			go r.sendAppendEntries(i, r.peers[i], args)
		}
	}
}

func NewRaft(addrs []string, me string, applyCh chan<- *ApplyMsg) *Raft {
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
		log:           make([]LogEntry, 0),
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		heartbeatCh:   make(chan bool, 10),
		voteGrantedCh: make(chan bool, 10),
		leaderCh:      make(chan bool, 10),
		commitCh:      make(chan bool, 10),
		applyCh:       applyCh,
	}
	raft.log = append(raft.log, LogEntry{LogTerm: 0})
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
				raft.mutex.Unlock()
				go raft.broadcastRequestVote()
				select {
				case <-raft.leaderCh:
					raft.mutex.Lock()
					raft.role = LEADER
					for i := 0; i < len(raft.peers); i++ {
						raft.nextIndex[i] = raft.log[len(raft.log)-1].LogIndex + 1
						raft.matchIndex[i] = 0
					}
					raft.mutex.Unlock()
				case <-raft.heartbeatCh:
					raft.mutex.Lock()
					raft.role = FOLLOWER
					raft.mutex.Unlock()
				case <-time.After(1500 * time.Millisecond):
					log.Println("vote timeout...")
				}
			case LEADER:
				log.Println("I am leader...")
				raft.broadcastAppendEntries()
				time.Sleep(1000 * time.Millisecond)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-raft.commitCh:
				baseLogIndex := raft.log[0].LogIndex
				if raft.commitIndex > raft.lastApplied {
					for i := raft.lastApplied + 1; i <= raft.commitIndex; i++ {
						raft.lastApplied++
						applyMsg := &ApplyMsg{Command: raft.log[i-baseLogIndex]}
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
