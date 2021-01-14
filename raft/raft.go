package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"
//state const
const (
	Follower  = 0
	Leader    = 1
	Candidate = 2
)

//time interval const
const (
	HeartbeatPeriod = int64(time.Millisecond * 200) //200ms
	ElectTimeMin    = HeartbeatPeriod * 2           //Electimeout :400ms~800ms
	TickTime        = time.Millisecond * 20         //every 20ms check state
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	wtev int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state    int //1leader 0follower 2candidate
	LeaderID int //who is current leader

	ElectTimeOut int64 //for how long a candidate will wait before it becomes the leader,if elapsed it will restart voting
	LastAlive    int64 //last time recieve AppendEntry or send Vote
	VoteStart    int64 //as candidate,when was the last time start a vote

	//Persistent state
	currentTerm int //current term
	votedFor    int //vote for who in current term,-1 for not voted yet
	log         []Log

	//Volatile state
	commitIndex int //index of hightest log entry known to be commited
	lastApplied int //index of hightest log entry applied to state machine

	//Volatile State on leaders
	nextIndex  []int //fig2 state
	matchindex []int
}

func GetTime() int64 {
	return time.Now().UnixNano() / 1e6 //millesecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == 1)
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) StateSwitch(toState int) {
	rf.mu.Lock()
	switch toState {
	case Follower: //to follower
		rf.state = Follower
		rf.votedFor = -1
		rf.LeaderID = -1
		rf.RandElecTime()
		rf.LastAlive = GetTime()
		rf.mu.Unlock()
		go rf.FollowTimeOut()

	case Leader: //to leader
		rf.state = Leader
		rf.mu.Unlock()
		go rf.Heartbeat()

	case Candidate:
		rf.state = Candidate
		rf.LeaderID = -1
		rf.VoteStart = GetTime()
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.mu.Unlock()
		go rf.CandiTimeOut()
		go rf.StartVote()
	default:
		fmt.Println("server: ", rf.me, " state error")
		rf.mu.Unlock()
	}
}

func (rf *Raft) Raftroutine() {
	//what Raft server do in routine
	for {
		switch rf.state {
		case 0: //follower
			go rf.FollowTimeOut()
		case 1: //leader

		case 2: //candidate
			rf.currentTerm++
			rf.StartVote()
		}
	}

}

func (rf *Raft) StartVote() {
	//as candidate start a vote
	rf.mu.Lock()
	args := new(RequestVoteArgs)
	args.CandidateID = rf.me
	args.Term = rf.currentTerm

	reply := new(RequestVoteReply)
	rf.mu.Unlock()

	go rf.CandiTimeOut()

	rf.mu.Lock()
	agree := 1
	for id := range rf.peers {
		if id != rf.me {
			if rf.sendRequestVote(id, args, reply) {
				if reply.VoteGtanted {
					agree++
				} else {
					if reply.Term > rf.currentTerm {
						rf.mu.Unlock()
						rf.StateSwitch(Follower)
						return
					}
				}
			}
		}
	}

	if agree > len(rf.peers)/2 {
		//becomes leader
		rf.mu.Unlock()
		rf.StateSwitch(Leader)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) Heartbeat() {
	rf.mu.Lock()
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderID = rf.me

	reply := new(AppendEntriesReply)
	rf.mu.Unlock()

	for {
		time.Sleep(time.Duration(HeartbeatPeriod))

		rf.mu.Lock()
		if rf.state == Leader {
			for id := range rf.peers {
				if id != rf.me {
					if rf.sendAppendEntries(id, args, reply) {
						if !reply.Success && reply.Term > rf.currentTerm {
							rf.mu.Unlock()
							rf.StateSwitch(Follower)
							return
						}
					}
				}
			}
		} else {
			rf.mu.Unlock()
			break
		}
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateID int //candidate  id
	Term        int //candidate's current term
	//set as fig2 required
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGtanted bool //does the server vote for candidate?
	Term        int  //current term ,for candidate to update itself
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.VoteGtanted = false
		reply.Term = rf.currentTerm
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.mu.Unlock()
			rf.StateSwitch(Follower) //switch to follower
			rf.mu.Lock()
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			reply.VoteGtanted = true
			rf.votedFor = args.CandidateID
			rf.LastAlive = GetTime()
		}

	}

}

func (rf *Raft) RandElecTime() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.ElectTimeOut = r.Int63()%ElectTimeMin + ElectTimeMin
}

func (rf *Raft) FollowTimeOut() { //go this func
	//time.Sleep(time.Millisecond * time.Duration(rf.ElectTimeOut))
	for {
		time.Sleep(TickTime)
		rf.mu.Lock()
		if rf.state == Follower {
			if GetTime()-rf.LastAlive > rf.ElectTimeOut {
				rf.mu.Unlock()
				rf.StateSwitch(2)
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			break
		}
	}
}

func (rf *Raft) CandiTimeOut() {
	for {
		time.Sleep(TickTime)
		rf.mu.Lock()
		if rf.state == Candidate {
			if GetTime()-rf.VoteStart > rf.ElectTimeOut {
				rf.mu.Unlock()
				rf.StateSwitch(2) //start another vote,and increase term
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			break
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
//heartbeat implements
type AppendEntriesArgs struct {
	Term     int //leader's term
	LeaderID int //so follower can redirect clients

	//whatever not understood&decided yet
	PrevLogIndex int
	PrevLogTerm  int
	entries      []Log
	LeaderCommit int //leader's commitIndex
}

type AppendEntriesReply struct {
	Success bool //true if follower contained entry matching
	Term    int  //current term for leader to update
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		if args.Term > rf.currentTerm {
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
			rf.StateSwitch(Follower)
			rf.mu.Lock()
		}
		if rf.state != Follower {
			rf.mu.Unlock()
			rf.StateSwitch(Follower)
			rf.mu.Lock()
		}
		rf.LeaderID = args.LeaderID
		rf.LastAlive = GetTime()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = 0 //start as a follower
	rf.RandElecTime()
	rf.LastAlive = GetTime()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Log{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
