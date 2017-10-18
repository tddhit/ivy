package raft

import (
	"github.com/tddhit/ivy/rpc"
	"log"
	"sync"
	"time"
)

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type Raft struct {
	mutex sync.Mutex
	peers []*rpc.Client

	id          int
	role        int
	currentTerm int
	votedFor    int
	voteCount   int

	heartbeatCh   chan bool
	voteGrantedCh chan bool
	leaderCh      chan bool
}

func (r *Raft) RequestVote(args RequestVoteArgs) (reply RequestVoteReply) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	reply.VoteGranted = false
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		return
	}
	r.currentTerm = args.Term
	reply.Term = args.Term
	if r.votedFor == -1 || r.votedFor == args.CandidateId {
		reply.VoteGranted = true
		r.role = FOLLOWER
		r.votedFor = args.CandidateId
		r.voteCount = 0
		r.voteGrantedCh <- true
	}
	return
}

func (r *Raft) sendRequestVote(peer *rpc.Client, args RequestVoteArgs) {
	reply := peer.Call("Raft.RequestVote", args)
	select {
	case call := <-reply.Done:
		if call.Error != nil {
			log.Println("sendRequestVote:", call.Error)
			peer.Dial()
		} else {
			r.mutex.Lock()
			requestVoteReply := call.Rsp.Reply[0].(RequestVoteReply)
			if r.currentTerm < requestVoteReply.Term {
				r.role = FOLLOWER
				r.currentTerm = requestVoteReply.Term
				r.votedFor = -1
				r.voteCount = 0
			}
			if requestVoteReply.VoteGranted {
				r.voteCount += 1
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
		Term:        r.currentTerm,
		CandidateId: r.id,
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
	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return
	}
	r.currentTerm = args.Term
	reply.Term = args.Term
	reply.Success = true
	r.heartbeatCh <- true
	return
}

func (r *Raft) sendAppendEntries(peer *rpc.Client, args AppendEntriesArgs) AppendEntriesReply {
	reply := peer.Call("Raft.AppendEntries", args)
	select {
	case call := <-reply.Done:
		if call.Error != nil {
			log.Println(call.Error)
			return AppendEntriesReply{
				Term:    args.Term,
				Success: false,
			}
		} else {
			return call.Rsp.Reply[0].(AppendEntriesReply)
		}
	}
}

func (r *Raft) broadcastAppendEntries() {
	args := AppendEntriesArgs{
		Term:     r.currentTerm,
		LeaderId: r.id,
	}
	for k, v := range r.peers {
		if k == r.id {
			continue
		}
		r.sendAppendEntries(v, args)
	}
}

func NewRaft(addrs []string, me string) *Raft {
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
		currentTerm:   1,
		votedFor:      -1,
		voteCount:     0,
		heartbeatCh:   make(chan bool, 10),
		voteGrantedCh: make(chan bool, 10),
		leaderCh:      make(chan bool, 10),
	}
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
					raft.currentTerm += 1
					log.Printf("raft:%p\n", &raft)
					log.Printf("votedFor:%p\n", &raft.votedFor)
					raft.votedFor = raft.id
					raft.voteCount = 1
					raft.mutex.Unlock()
					log.Println("heartbeat timeout...")
				}
			case CANDIDATE:
				raft.broadcastRequestVote()
				select {
				case <-raft.leaderCh:
					raft.mutex.Lock()
					raft.role = LEADER
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
	return raft
}
