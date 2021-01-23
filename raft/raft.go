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
	HeartbeatPeriod = time.Millisecond * 120 //120ms
	ElectTimeMin    = time.Millisecond * 200 //Electimeout :200ms~400ms
	TickTime        = time.Millisecond * 20  //every 20ms check state
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

func (rf *Raft) Apply(fi int, ti int) {
	for i := fi; i <= ti; i++ {
		msg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " sending applymsg index ", msg.CommandIndex, " command ", msg.Command)
		rf.ApplyCh <- msg
	}
}

func (rf *Raft) ApplyRoutine() {
	for !rf.killed() {
		time.Sleep(TickTime)
		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " apply routine want lock ")

		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			from := rf.lastApplied + 1
			to := rf.commitIndex
			rf.lastApplied = to
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " applying from ", from, " to ", to)
			rf.Apply(from, to)
		}
		rf.mu.Unlock()
	}
}

type LogEntry struct {
	Term    int
	Command interface{}
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

	ApplyCh chan ApplyMsg
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
	//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " will switch to ", toState)
	switch toState {
	case Follower: //to follower
		rf.state = Follower
		rf.votedFor = -1
		rf.LeaderID = -1
		rf.RandElecTime()
		rf.LastAlive = GetTime()
		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " switching to follower setting LastAlive as ", GetTime())
		rf.Unlock()
		go rf.FollowTimeOut()

	case Leader: //to leader
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
		rf.Unlock()
		go rf.Heartbeat()

	case Candidate:
		rf.state = Candidate
		rf.LeaderID = -1
		rf.currentTerm++
		rf.votedFor = rf.me

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
	if args.LastLogIndex > -1 {
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	} else {
		args.LastLogTerm = rf.currentTerm
	}

	reply := new(RequestVoteReply)

	selfid := rf.me

	//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " set Votestart as", GetTime())

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

	reply := new(AppendEntriesReply)

	peerNum := len(rf.peers)
	selfid := rf.me
	rf.Unlock()

	for {
		rf.mu.Lock()
		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader,heartbeat woke")
		if rf.killed() {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader", " found itself killed,will shut down")
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Unlock()
		}

		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader,passed kill test")
		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " woke and want lock")
		rf.mu.Lock()
		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " woke and got lock")
		if rf.state == Leader {
			rf.mu.Unlock()
			for id := 0; id < peerNum; id++ {
				if id != selfid {
					//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " heartbeat ", id)
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
	rf.mu.Lock()
	args.LeaderCommit = rf.commitIndex
	rf.mu.Unlock()

	if rf.sendAppendEntries(id, args, reply) {
		//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " as leader,heartbeat sent to ", id, " recieved", reply.Success)
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
		if (rf.currentTerm > args.LastLogTerm) || (rf.currentTerm == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex) {
			//refuse
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " not uptodate,refuse")
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			rf.Unlock()
		} else {
			if args.Term > rf.currentTerm {
				rf.currentTerm = args.Term
				//fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " got a higher vote request,will be a follower,now state is ", rf.state)
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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

func (rf *Raft) ContainLog(index int, term int) bool {
	if index < 0 {
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " index < 1,return true")
		return true
	} else {
		if len(rf.logs)-1 >= index {
			if rf.logs[index].Term == term {
				fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " contained log index: ", index, " term: ", term)
				return true
			}
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " have no log as: ", index, " term: ", term)
			return false
		}
	}
	return false
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
		if len(args.Entries) > 0 {
			//got entries
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " got Entries to append ")
			if rf.ContainLog(args.PrevLogIndex, args.PrevLogTerm) {
				if args.PrevLogIndex > -1 {
					rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
				} else {
					rf.logs = args.Entries
				}
				reply.Success = true
				reply.Term = rf.currentTerm
				fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " new logs appended,now log lenth is ", len(rf.logs))
			} else {
				reply.Success = false
				reply.Term = rf.currentTerm
			}
		} else {
			//heartbeat
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " recieved heartbeat from", args.LeaderID)
			reply.Success = true
			rf.LeaderID = args.LeaderID
			rf.LastAlive = GetTime()
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.logs)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.logs) - 1
			}
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " commitIndex updated to", rf.commitIndex)
		} else {
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, "leaderCommit: ", args.LeaderCommit, "<= commitIndex: ", rf.commitIndex)
		}
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
	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, "------got start command")

	// Your code here (2B).
	rf.Lock()

	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if !isLeader {
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " refused start command")
		rf.Unlock()
		return -1, -1, isLeader
	}
	rf.logs = append(rf.logs, LogEntry{term, command})

	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " will go sync")
	//dont know if the leader will respond to client yet
	for id := range rf.peers {
		if id != rf.me {
			go rf.Synchronize(id)
		}
	}
	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " go sync finished")

	rf.Unlock()
	rf.UpdateCommit()
	return index, term, isLeader
}

func (rf *Raft) Synchronize(id int) {
	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " syncing to ", id)
	rf.Lock()
	if len(rf.logs)-1 >= rf.nextIndex[id] {
		fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " syncing will send append")
		args := new(AppendEntriesArgs)
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.PrevLogIndex = len(rf.logs) - 2
		if args.PrevLogIndex > 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		} else {
			args.PrevLogTerm = -1
		}

		args.LeaderCommit = rf.commitIndex
		args.Entries = rf.logs[rf.nextIndex[id]:]

		reply := new(AppendEntriesReply)

		for rf.state == Leader {
			rf.Unlock()
			if rf.sendAppendEntries(id, args, reply) {
				rf.Lock()
				if reply.Success {
					rf.nextIndex[id] += len(args.Entries)
					rf.matchIndex[id] = rf.nextIndex[id] - 1
					fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " append success,nextIndex to", id, ": ", rf.nextIndex[id], " matchIndex: ", rf.matchIndex[id])
					break
				} else {
					rf.nextIndex[id]--
					args.PrevLogIndex--
					args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
					args.Entries = rf.logs[rf.nextIndex[id]:]
					fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " append to ", id, " failed ,will try nextIndex:", rf.nextIndex[id], " prevIdx: ", args.PrevLogIndex, " prevTerm: ", args.PrevLogTerm)
				}
				rf.Unlock()
			}
			rf.Lock()
		}
	}
	rf.Unlock()
}

func (rf *Raft) UpdateCommit() {
	time.Sleep(TickTime)
	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " updatecommit woke want the lock ")
	rf.Lock()
	defer rf.Unlock()

	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " start update commit")
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
			fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " commitIndex changed to", rf.commitIndex)
			break
		}
	}
	fmt.Println("time:", GetTime(), "   peer", rf.me, " state: ", rf.state, " commit update loop done")
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
	rf.commitIndex = -1 //highest log entry commited
	rf.lastApplied = -1 //highest log entry applied
	rf.ApplyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	fmt.Println("Make peer:", rf.me, " Electime: ", time.Duration(rf.ElectTimeOut))
	fmt.Println("Basictime: ", BasicTime, "Ticktime: ", TickTime, "Electimemin: ", time.Duration(ElectTimeMin), "HeartBeatPeriod: ", time.Duration(HeartbeatPeriod))
	go rf.FollowTimeOut()
	go rf.ApplyRoutine()

	return rf
}
