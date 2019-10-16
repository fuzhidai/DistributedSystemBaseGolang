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
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    string // candidateId that received vote in current term (or null if none)
	log         []Log  // log entries

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// Timeout
	timer  chan int // timeout
	voteCh chan int // vote Channel
	state  int      // 0 means Follower ； 1 means Candidate ； 2 means Leader
}

// peer state
const (
	Follower  = 0 // Follower
	Candidate = 1 // Candidate
	Leader    = 2 // Leader
)

// each entry contains command for state machine, and term when entry was received by leader (first index is 1)
type Log struct {
	LogIndex int
	Term     int
	Command  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
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
	Term         int    // candidate’s term
	CandidateId  string // candidate requesting vote
	LastLogIndex int    // index of candidate’s last log entry
	LastLogTerm  int    // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term         int    // leader’s term
	LeaderId     string // so follower can redirect clients
	PrevLogIndex int    // index of log entry immediately preceding new ones
	PrevLogTerm  int    // term of prevLogIndex entry
	Entries      []Log  // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int    // leader’s commitIndex
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	DPrintf("RequestVote —— peer: %d ; currentTerm: %d ; term: %d", rf.me, rf.currentTerm, args.Term)

	if rf.currentTerm > args.Term {
		// candidate term behind current term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.mu.Lock()
	if rf.currentTerm < args.Term {

		DPrintf("peer %d has vote %s.", rf.me, args.CandidateId)

		// if old leader has something wrong，current peer receive a new term vote，so vote it
		// a first-come-first-served basis
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId

		// prepare reply
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {

		// current rf.currentTerm == args.Term
		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if rf.votedFor == "" || (strings.Compare(rf.votedFor, args.CandidateId) == 0 && rf.lastApplied == args.LastLogIndex) {

			rf.state = Follower
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
		reply.Term = args.Term
	}
	rf.mu.Unlock()
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	DPrintf("peer %d get AppendEntries. | term: %d", rf.me, rf.currentTerm)

	// (b) another server establishes itself as leader
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	DPrintf("peer %d get heartbeat from %s. | term: %d | current: %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	// whatever you are a follower， so first change or keep peer to Follower
	rf.state = Follower
	if args.Term > rf.currentTerm {
		DPrintf("Old Leader peer %d become Follower.", rf.me)

		// old Leader online.
		rf.currentTerm = args.Term // second update current term（when old leader back）
	}
	rf.mu.Unlock()

	// don't need to close vote channel，because it will close when get the next vote or at the begin of the next vote

	// restart election timeout
	rf.timer <- 1

	// set reply.
	reply.Term = rf.currentTerm
	reply.Success = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// start Timer (election timeout)
func (rf *Raft) startTimer() {

	// Timer
	var ticker *time.Ticker
	for {
		if rf.state != Leader {
			// Restart Timer
			ticker = time.NewTicker(time.Duration(RandInt64(200, 400)) * time.Millisecond)
		} else {
			// Leader every 100 ms send a heartBeat
			ticker = time.NewTicker(time.Duration(100) * time.Millisecond)
		}
	Timeout:
		for {
			select {
			case <-ticker.C:

				// while timeout.
				if rf.state != Leader {
					// Not Leader，First to collect vote
					rf.collectVote()
					// Begin to vote for itself
					rf.startVote()
				} else {
					// Leader send heartbeat
					rf.sendHeartbeats()
				}
			case <-rf.timer:
				DPrintf("peer %d restart timer.", rf.me)

				// Stop to restart Timer
				ticker.Stop()
				break Timeout
			}
		}
	}
}

// Start vote.
func (rf *Raft) startVote() {

	rf.state = Candidate              // current peer state is Candidate
	rf.votedFor = strconv.Itoa(rf.me) // vote itself
	rf.currentTerm++                  // increase current term

	DPrintf("Peer %d begin to vote, current term is %d", rf.me, rf.currentTerm)

	for server, _ := range rf.peers {
		if server != rf.me {
			// start a new go routine to send request vote
			go func(server int) {

				args := &RequestVoteArgs{rf.currentTerm, strconv.Itoa(rf.me), 0, 0}
				reply := &RequestVoteReply{}

				DPrintf("Send vote to peer %d.", server)

				// send request vote
				if rf.sendRequestVote(server, args, reply) {

					if !reply.VoteGranted {
						// group has haven Leader，so this peer change to Follower
						rf.state = Follower
						// current server may have out
						rf.currentTerm = reply.Term
					}

					// send message to collect func.
					rf.voteCh <- 1

				} else {
					// mean have something wrong with RPC
				}
			}(server)
		}
	}
}

// Collect vote
func (rf *Raft) collectVote() {
	// first vote to itsel
	voteNum := 1
	// init a new vote channel to clean old vote
	rf.voteCh = make(chan int)

	go func() {
		// get state for vote from vote channel
		for range rf.voteCh {
			// mean vote have finish
			if rf.state == Follower {
				return
			}
			// add one vote
			voteNum++
			// (a) it wins the election
			if voteNum >= ((len(rf.peers) + 1) / 2) {

				DPrintf("peer: %d become Leader, term is %d \n", rf.me, rf.currentTerm)

				// peer have got enough vote，so it become Leader
				rf.state = Leader
				// send heartbeats
				rf.sendHeartbeats()
				rf.timer <- 1
				// finish vote
				return
			}
		}
	}()
}

// send heartbeats
func (rf *Raft) sendHeartbeats() {

	DPrintf("peer %d send heartbeats.", rf.me)

	// send heartbeat
	go func() {
		for server, _ := range rf.peers {
			if server != rf.me {
				go func(server int) {
					args := &AppendEntriesArgs{rf.currentTerm, strconv.Itoa(rf.me), 0, 0, nil, 0}
					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(server, args, reply)
				}(server)
			}
		}
	}()
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Set Timer and Set vote channel
	rf.timer = make(chan int)
	rf.voteCh = make(chan int)

	go func() {
		// randomized election timeouts
		rf.startTimer()
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func RandInt64(min, max int64) int64 {
	if min >= max || min == 0 || max == 0 {
		return max
	}
	return rand.Int63n(max-min) + min
}
