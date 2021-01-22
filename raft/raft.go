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

var BasicTime int64

//time interval const
const (
	HeartbeatPeriod = time.Millisecond * 200 //200ms
	ElectTimeMin    = HeartbeatPeriod * 2    //Electimeout :400ms~800ms
	TickTime        = time.Millisecond * 50  //every 20ms check state
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

type LogEntry struct {
	Term    int
	Command interface{} //no idea yet
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
	logs        []LogEntry

	//Volatile state
	commitIndex int //index of hightest log entry known to be commited
	lastApplied int //index of hightest log entry applied to state machine

	//Volatile State on leaders
	nextIndex  []int //fig2 state
	matchIndex []int
}

func GetTime() int64 {
	return time.Now().UnixNano()/1e6 - BasicTime //millesecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	term = rf.currentTerm
	isleader = (rf.state == 1)
	rf.Unlock()
	return term, isleader
}

func (rf *Raft) Lock() {
	rf.mu.Lock()
	//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " locked ")
}

func (rf *Raft) Unlock() {
	//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " unlocked ")
	rf.mu.Unlock()
}

func (rf *Raft) StateSwitch(toState int) {
	rf.Lock()
	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " will switch to ", toState)
	switch toState {
	case Follower: //to follower
		rf.state = Follower
		rf.votedFor = -1
		rf.LeaderID = -1
		rf.RandElecTime()
		rf.LastAlive = GetTime()
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " switching to follower setting LastAlive as ", GetTime())
		rf.Unlock()
		go rf.FollowTimeOut()

	case Leader: //to leader
		rf.state = Leader
		rf.Unlock()
		go rf.Heartbeat()

	case Candidate:
		rf.state = Candidate
		rf.LeaderID = -1
		rf.currentTerm++
		rf.votedFor = rf.me
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}

		rf.Unlock()
		go rf.StartVote()
		go rf.CandiTimeOut()
	default:
		fmt.Println("server: ", rf.me, " state error")
		rf.Unlock()
	}
}

func (rf *Raft) StartVote() {
	//as candidate start a vote

	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " started a vote")

	rf.mu.Lock()
	args := new(RequestVoteArgs)
	args.CandidateID = rf.me
	args.Term = rf.currentTerm
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	reply := new(RequestVoteReply)

	selfid := rf.me

	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " set Votestart as", GetTime())

	var agree int32 = 1

	rf.VoteStart = GetTime()
	peerNum := len(rf.peers)
	rf.mu.Unlock()

	for id := 0; id < peerNum; id++ {
		if id != selfid {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " send vote request to ", id)
			go rf.goRequestVote(id, &agree, args, reply)
		}
	}

	time.Sleep(TickTime) //wait for adding agree
	rf.mu.Lock()
	if int(agree) > peerNum/2 && rf.state == Candidate {
		//becomes leader
		rf.mu.Unlock()
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " will be a leader")
		rf.StateSwitch(Leader)
	} else {
		rf.mu.Unlock()
	}

}

func (rf *Raft) goRequestVote(id int, agree *int32, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.sendRequestVote(id, args, reply) {
		if reply.VoteGranted {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " vote  granted ")
			atomic.AddInt32(agree, 1)
		} else {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, "collecting votes but found a higher peer")
				rf.mu.Unlock()
				rf.StateSwitch(Follower)
			} else {
				rf.mu.Unlock()
				fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " vote not granted,not due to term")
			}
		}
	} else {
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, "call Request to ", id, " but failed")
	}
}

func (rf *Raft) Heartbeat() {
	rf.Lock()
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.LeaderCommit = rf.commitIndex

	reply := new(AppendEntriesReply)

	peerNum := len(rf.peers)
	selfid := rf.me
	rf.Unlock()

	for {
		rf.mu.Lock()
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader,heartbeat woke")
		if rf.killed() {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader", " found itself killed,will shut down")
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
		}

		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader,passed kill test")
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " woke and want lock")
		rf.mu.Lock()
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " woke and got lock")
		if rf.state == Leader {
			rf.mu.Unlock()
			for id := 0; id < peerNum; id++ {
				if id != selfid {
					fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " about to heartbeat ", id)
					go rf.goHeartbeat(id, args, reply)
				}
			}
		} else {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " sending heartbeat found itself no longer leader,Heart beat break")
			rf.mu.Unlock()
			break
		}

		time.Sleep(HeartbeatPeriod)
	}
}

func (rf *Raft) goHeartbeat(id int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.sendAppendEntries(id, args, reply) {
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader,heartbeat sent to ", id, " recieved", reply.Success)
		rf.mu.Lock()
		if !reply.Success && reply.Term > rf.currentTerm {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader sending heartbeat,but found higher peer")
			rf.mu.Unlock()
			rf.StateSwitch(Follower)
		} else {
			rf.mu.Unlock()
		}
	} else {
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader sent heartbeat to ", id, "but failed")
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
	VoteGranted bool //does the server vote for candidate?
	Term        int  //current term ,for candidate to update itself
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.Lock()
	if args.Term < rf.currentTerm {
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " got a lower vote request")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.Unlock()
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " got a higher vote request,will be a follower,now state is ", rf.state)
			rf.Unlock()
			rf.StateSwitch(Follower) //switch to follower
			rf.Lock()
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " voted for ", args.CandidateID)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.LastAlive = GetTime()
		}
		rf.Unlock()
	}
}

func (rf *Raft) RandElecTime() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.ElectTimeOut = (r.Int63()/1e6%int64(ElectTimeMin) + int64(ElectTimeMin)) / 1e6
}

func (rf *Raft) FollowTimeOut() { //go this func
	//time.Sleep(time.Millisecond * time.Duration(rf.ElectTimeOut))
	for {
		time.Sleep(TickTime)
		rf.Lock()
		if rf.killed() {
			rf.Unlock()
			return
		}
		if rf.state == Follower {
			if GetTime()-rf.LastAlive > rf.ElectTimeOut {
				fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " FollowTimeOut,will be a candidate")
				rf.Unlock()
				rf.StateSwitch(2)
			} else {
				rf.Unlock()
			}

		} else {
			rf.Unlock()
			return
		}
	}
}

func (rf *Raft) CandiTimeOut() {
	for {
		time.Sleep(TickTime)
		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " candi woke")
		rf.Lock()
		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " candi got the lock")
		if rf.killed() {
			//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " found itself killed")
			rf.Unlock()
			return
		}

		if rf.state == Candidate {
			if GetTime()-rf.VoteStart > rf.ElectTimeOut {
				fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " CandiTimeOut,will start vote agian")

				rf.state = Candidate
				rf.LeaderID = -1
				rf.currentTerm++
				rf.votedFor = rf.me

				rf.Unlock()
				rf.StartVote()
				//rf.StateSwitch(2) //start another vote,and increase term

			} else {
				fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " gettime - votestart  = ", GetTime()-rf.VoteStart, " < Electimeout:", rf.ElectTimeOut)
				rf.Unlock()
			}

		} else {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " found itself no longer a candidate,shut down")
			rf.Unlock()
			return
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
	LeaderCommit int //leader's commitIndex

	Entries []LogEntry
}

type AppendEntriesReply struct {
	Success bool //true if follower contained entry matching
	Term    int  //current term for leader to update
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.Lock()
	if args.Term < rf.currentTerm {
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " got a lower append from ", args.LeaderID)
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		if args.Term > rf.currentTerm {
			reply.Term = rf.currentTerm
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " got a higher append from ", args.LeaderID)
			rf.Unlock()
			rf.StateSwitch(Follower)
			rf.Lock()
		}
		if rf.state != Follower {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " will switch to follwer,recieving append from ", args.LeaderID)
			rf.Unlock()
			rf.StateSwitch(Follower)
			rf.Lock()
		}
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " recieved heartbeat from", args.LeaderID)
		reply.Success = true
		rf.LeaderID = args.LeaderID
		rf.LastAlive = GetTime()
	}
	rf.Unlock()
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if !isLeader {
		return -1, -1, isLeader
	}
	rf.logs = append(rf.logs, LogEntry{term, command})

	//dont know if the leader will respond to client yet
	for id := range rf.peers {
		if id != rf.me {
			go rf.Synchronize(id)
		}
	}

	defer rf.UpdateCommit()
	return index, term, isLeader
}

func (rf *Raft) Synchronize(id int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.logs)-1 >= rf.nextIndex[id] {
		args := new(AppendEntriesArgs)
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.PrevLogIndex = len(rf.logs) - 2
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		args.Entries = rf.logs[rf.nextIndex[id]:]

		reply := new(AppendEntriesReply)

		for rf.state == Leader {
			if rf.sendAppendEntries(id, args, reply) {
				if reply.Success {
					rf.nextIndex[id] += len(args.Entries)
					rf.matchIndex[id] = rf.nextIndex[id] - 1
					break
				} else {
					rf.nextIndex[id]--
					args.PrevLogIndex--
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
					args.Entries = rf.logs[rf.nextIndex[id]:]
				}

			}
		}
	}
}

func (rf *Raft) UpdateCommit() {
	time.Sleep(TickTime)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for newc := len(rf.logs) - 1; newc > rf.commitIndex; newc-- {
		commit := 1
		for id := range rf.peers {
			if id != rf.me {
				if rf.matchIndex[id] >= newc {
					commit++
				}
			}
		}
		if commit > len(rf.peers)/2 && rf.logs[newc].Term == rf.currentTerm {
			//newc will be new commitIndex
			rf.commitIndex = newc
			break
		}
	}
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

	BasicTime = time.Now().UnixNano() / 1e6
	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower //start as a follower
	rf.dead = 0
	rf.LeaderID = -1
	rf.RandElecTime()
	rf.LastAlive = GetTime()
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{}
	rf.commitIndex = 0 //highest log entry commited
	rf.lastApplied = 0 //highest log entry applied
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Println("Make peer:", rf.me, " Electime: ", time.Duration(rf.ElectTimeOut))
	fmt.Println("Basictime: ", BasicTime, "Ticktime: ", TickTime, "Electimemin: ", time.Duration(ElectTimeMin), "HeartBeatPeriod: ", time.Duration(HeartbeatPeriod))
	go rf.FollowTimeOut()

	return rf
}
