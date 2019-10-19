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
	applyCh   chan ApplyMsg       // applyCh for test

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    string // candidateId that received vote in current term (or null if none)
	log         []Log  // log entries (first index is 1)

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
	Command  interface{}
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

	// the last log entity of peer log
	log := rf.log[len(rf.log)-1]
	APrintf("Peer %d argsTerm %d AND currentTerm %d | argsIndex %d AND logId %d | term %d", rf.me, args.LastLogTerm, log.Term, args.LastLogIndex, log.LogIndex, rf.currentTerm)
	if rf.currentTerm > args.Term || log.Term > args.LastLogTerm || (log.Term == args.LastLogTerm && log.LogIndex > args.LastLogIndex) {
		// candidate term behind current term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// In the same term, peer has voted for other candidate or itself, don't vote this candidate
	// guarantee one term only vote for one candidate.
	if rf.currentTerm == args.Term && strings.Compare(rf.votedFor, args.CandidateId) != 0 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// (1) In the same term , but this peer has vote for this candidate, so keep it
	// (2) Or this is a new term and newer this currentTerm, vote for this candidate
	rf.mu.Lock()
	APrintf("peer %d has vote %s. ", rf.me, args.CandidateId)

	// if old leader has something wrong，current peer receive a new term vote，so vote it
	// a first-come-first-served basis
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId

	// prepare reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.mu.Unlock()
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	APrintf("peer %d get AppendEntries, and args is %v", rf.me, args)
	// CPrintf("Term: %d | Current Leader is %v | peer is %d and args is %v | log %v", args.Term, args.LeaderId, rf.me, args, rf.log)
	//APrintf("Peer %d | log %v | args %v", rf.me ,rf.log, args)

	// (b) another server establishes itself as leader
	// the second is doing consistent work
	if args.Term < rf.currentTerm || len(rf.log) <= args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// the Leader Term higher than current peer
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = true

		rf.mu.Lock()
		rf.state = Follower
		rf.timer <- 1
		rf.mu.Unlock()

		return
	}

	CPrintf("args.Term: %d | rf.Term: %d | peer %d begin to append log with %d.", args.Term, rf.currentTerm, rf.me, args.Entries)

	// if rf.log[prevLogIndex].Term != prevLogTerm return false
	prevLogIndex, prevLogTerm := args.PrevLogIndex, args.PrevLogTerm
	// do consistent work

	// if the term of log entity which logIndex is prevLogIndex is not equal to prevLogTerm
	// return false to Leader
	if rf.log[prevLogIndex].Term != prevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// APrintf("Peer %d Send ApplyMsg LeaderCommit %d AND commitIndex %d.", rf.me, args.LeaderCommit, rf.commitIndex)

	// APrintf("peer %d len %d and args %v", rf.me, len(rf.log), args)

	// delete form the next prevLogIndex to rear
	rf.log = rf.log[:prevLogIndex+1]

	// APrintf("peer %d current log %v", rf.me, rf.log)

	// deal with the committed msg after Leader commit log（update current commitIndex）
	if args.LeaderCommit > rf.commitIndex && args.LeaderCommit < len(rf.log) {

		// send ApplyMsg from the next of commitIndex to LeaderCommit
		for _, l := range rf.log[rf.commitIndex+1 : args.LeaderCommit+1] {

			APrintf("Peer %d Send ApplyMsg Command %d AND Index %d. | log %v", rf.me, l.Command, l.LogIndex, rf.log)

			rf.applyCh <- ApplyMsg{true, l.Command, l.LogIndex}
		}
		// change current commitIndex
		rf.commitIndex = args.LeaderCommit
	}

	// is not heartbeat or consistent message but append log entities
	if args.Entries != nil {

		//if args.LeaderCommit > rf.commitIndex {
		//	rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.log[len(rf.log)-1].LogIndex)))
		//}

		// append log
		rf.log = append(rf.log, args.Entries...)

		CPrintf("peer %d has append log, current log is %v.", rf.me, rf.log)
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

	APrintf("Peer %d begin to vote, current term is %d", rf.me, rf.currentTerm)

	for server, _ := range rf.peers {
		if server != rf.me {
			// start a new go routine to send request vote
			go func(server int) {

				args := &RequestVoteArgs{rf.currentTerm, strconv.Itoa(rf.me), len(rf.log), rf.log[len(rf.log)-1].Term}
				reply := &RequestVoteReply{}

				DPrintf("Send vote to peer %d.", server)

				// send request vote
				if rf.sendRequestVote(server, args, reply) {

					//if !reply.VoteGranted {
					//	// group has haven Leader，so this peer change to Follower
					//	rf.state = Follower
					//	// current server may have out
					//	rf.currentTerm = reply.Term
					//}

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

				APrintf("peer: %d become Leader, term is %d ", rf.me, rf.currentTerm)

				rf.state = Leader   // peer have got enough vote，so it become Leader
				rf.sendHeartbeats() // send heartbeats

				rf.mu.Lock()
				// init nextIndex (initialized to leader last log index + 1)
				rf.nextIndex = make([]int, len(rf.peers))

				var li int
				if len(rf.log) > 0 {
					li = rf.log[len(rf.log)-1].LogIndex + 1
				} else {
					// current log is empty
					li = 1
				}
				for index, _ := range rf.nextIndex {
					rf.nextIndex[index] = li
				}

				// init matchIndex (initialized to 0)
				rf.matchIndex = make([]int, len(rf.peers))
				rf.matchIndex[rf.me] = len(rf.log) - 1 // first log node is empty
				rf.mu.Unlock()

				rf.timer <- 1 // start Timer
				return        // finish vote
			}
		}
	}()
}

// send heartbeats
func (rf *Raft) sendHeartbeats() {

	DPrintf("peer %d send heartbeats.", rf.me)

	// send heartbeat
	rf.sendAppendEntriesToOtherServer(nil, rf.commitIndex, rf.processReplyForAppendEntriesRPC)
}

func (rf *Raft) sendAppendEntriesToOtherServer(entries []Log, leaderCommit int, processReplyFunc func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply)) {
	go func() {
		for server, _ := range rf.peers {
			if server != rf.me {
				go func(server int) {

					// get index of highest log entry known to be replicated on server
					// Note: there can't be matchIndex, because it is initialized 0 for each server

					index := rf.nextIndex[server] - 1

					// APrintf("peer: %d current index: %d | logLen %d", server, index, len(rf.log))

					// index of log entry immediately preceding new ones AND term of prevLogIndex entry
					prevLogIndex, prevLogTerm := rf.log[index].LogIndex, rf.log[index].Term

					args := &AppendEntriesArgs{rf.currentTerm, strconv.Itoa(rf.me), prevLogIndex, prevLogTerm, entries, leaderCommit}
					reply := &AppendEntriesReply{}

					CPrintf("Send consistent message to peer %d | args: %v.", server, args)

					// process reply
					if rf.sendAppendEntries(server, args, reply) && processReplyFunc != nil {
						// start a new go routine to process reply
						go func() { processReplyFunc(server, args, reply) }()
					}
				}(server)
			}
		}
	}()
}

/**
new solution
*/

func (rf *Raft) sendCommand(command interface{}) {

	log := Log{rf.nextIndex[rf.me], rf.currentTerm, command}
	rf.log = append(rf.log, log)

	rf.matchIndex[rf.me] = log.LogIndex
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	CPrintf("Peer %d begin to send consistent message.", rf.me)

	// send AppendEntities RPC to other peer and process reply with processReplyForAppendEntriesRPC Func
	//rf.sendAppendEntriesToOtherServer(nil, rf.commitIndex, rf.processReplyForAppendEntriesRPC)

	rf.checkAndAppendLog(log)
}

func (rf *Raft) checkAndAppendLog(log Log) {

	NPrintf("%v", rf.nextIndex)

	for server, _ := range rf.peers {

		if rf.checkEachFollowerIfNeedToAppendThisLog(server, log.LogIndex) {

			logs := rf.getAllAppendEntriesForServer(server, log.LogIndex)
			NPrintf("peer %d | logs %v", server, logs)

			rf.doAppendAppendEntries(server, logs)
		}
	}
}

// If last log index ≥ nextIndex for a follower
func (rf *Raft) checkEachFollowerIfNeedToAppendThisLog(follower int, logIndex int) bool {
	return rf.nextIndex[follower] <= logIndex
}

func (rf *Raft) getAllAppendEntriesForServer(server int, endLogIndex int) []Log {
	return rf.log[rf.nextIndex[server] : endLogIndex+1]
}

// Actually append entries
func (rf *Raft) doAppendAppendEntries(server int, logs []Log) {

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	args := &AppendEntriesArgs{rf.currentTerm, strconv.Itoa(rf.me), prevLogIndex, prevLogTerm, logs, rf.commitIndex}
	reply := &AppendEntriesReply{}

	APrintf("doAppendAppendEntries for peer %d", server)

	if rf.sendAppendEntries(server, args, reply) {
		rf.processAppendEntriesReply(server, args, reply)
	} else {
		// something wrong with network
	}
}

func (rf *Raft) processAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if reply.Success {

		// If successful: update nextIndex and matchIndex for follower
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
		rf.updateCommitIndex()

	} else {

		if reply.Term == rf.currentTerm {
			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
			rf.nextIndex[server]--
		} else {
			rf.currentTerm = reply.Term
		}

		// Try again
		go func(server int, logs ...Log) { rf.doAppendAppendEntries(server, logs) }(server, args.Entries...)

	}
}

func (rf *Raft) updateCommitIndex() {

	if res, index := rf.checkIfCanUpdateCommitIndex(); res {
		rf.sendApplyMsg(rf.commitIndex, index)
		rf.commitIndex = index
	}
}

func (rf *Raft) checkIfCanUpdateCommitIndex() (bool, int) {
	index := FindValueCountOverHalf(rf.matchIndex)
	return index > 0 && index > rf.commitIndex, index
}

func (rf *Raft) sendApplyMsg(start int, end int) {

	for i := start + 1; i <= end; i++ {
		rf.applyCh <- ApplyMsg{true, rf.log[i].Command, rf.log[i].LogIndex}
		NPrintf("Leader %d has commit command %v", rf.me, rf.log[i].Command)
	}
}

func (rf *Raft) processReplyForAppendEntriesRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// if current peer is not Leader， Not has access to invoke this method
	if rf.state != Leader {
		return
	}

	// process reply after send AppendEntities RPC
	// the reply term is 0 when occur net fail
	if !reply.Success && reply.Term != 0 {

		// CPrintf("peer: %d | server: %d | args: %v | nextIndex: %v",rf.me, server, args, rf.nextIndex)
		APrintf("server %d | args %v | reply %v", server, args, reply)

		// Deal with Network partition AND Old leader reconnected
		if reply.Term > rf.currentTerm {
			// update to Follower
			rf.state = Follower
			rf.currentTerm = reply.Term
			return
		} else {
			return
		}

		// if reply success is false，try to send AppendEntities again after decrease the index of log
		rf.nextIndex[server]--

		// get index of highest log entry known to be replicated on server
		// Note: there can't be matchIndex, because it is initialized 0 for each server
		index := rf.nextIndex[server] - 1

		// index of log entry immediately preceding new ones AND term of prevLogIndex entry
		prevLogIndex, prevLogTerm := rf.log[index].LogIndex, rf.log[index].Term

		// init args and reply
		args := &AppendEntriesArgs{rf.currentTerm, strconv.Itoa(rf.me), prevLogIndex, prevLogTerm, args.Entries, rf.commitIndex}
		reply := &AppendEntriesReply{}

		// start a new go routine to process reply
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

			if rf.sendAppendEntries(server, args, reply) {
				rf.processReplyForAppendEntriesRPC(server, args, reply)
			}
		}(server, args, reply)
	} else {

		// if this is a heartbeat，return directly
		if len(rf.log) == rf.nextIndex[server] {

			// init matchIndex for follower after vote
			rf.matchIndex[server] = args.PrevLogIndex

			return
		}

		CPrintf("Peer %d finish consistent work，begin to send command log. Current nextIndex %v", server, rf.nextIndex)

		// if reply is success, do consistent work
		// copy log entities from nextIndex
		startIndex := rf.nextIndex[server]
		entities := rf.log[startIndex:]

		BPrintf("peer %d startIndex %d.", server, startIndex)

		// get index of highest log entry known to be replicated on server
		index := rf.nextIndex[server] - 1
		// index of log entry immediately preceding new ones AND term of prevLogIndex entry
		prevLogIndex, prevLogTerm := rf.log[index].LogIndex, rf.log[index].Term

		// init args and reply
		args := &AppendEntriesArgs{rf.currentTerm, strconv.Itoa(rf.me), prevLogIndex, prevLogTerm, entities, rf.commitIndex}
		reply := &AppendEntriesReply{}

		// APrintf("Peer %d begin to copy log entities %v", server, args)

		// start a new go routine to begin copy log work
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

			// occur net fail，stop invoke method to avoid error
			if !rf.sendAppendEntries(server, args, reply) {
				return
			}

			rf.mu.Lock()
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.mu.Unlock()

			// reply client finish the work of copy
			if index := FindValueCountOverHalf(rf.matchIndex); index > 0 && index > rf.commitIndex {

				// send ApplyMsg from the next of commitIndex to current commitIndex
				// there must use for loop，because index may more big than original commitIndex（> 1）
				for _, l := range rf.log[rf.commitIndex+1 : index+1] {

					APrintf("Leader Peer %d Send ApplyMsg Command %d AND Index %d | matchIndex %v", rf.me, l.Command, l.LogIndex, rf.matchIndex)

					rf.applyCh <- ApplyMsg{true, l.Command, l.LogIndex}
				}

				// update log state to committed
				rf.commitIndex = index
			}
			APrintf("Current matchIndex %v， current commitIndex %d", rf.matchIndex, rf.commitIndex)

			BPrintf("peer %d | matchIndex %d | nextIndex %d", server, rf.matchIndex[server], rf.nextIndex[server])
		}(server, args, reply)
	}

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

	rf.mu.Lock()
	// Your code here (2B).
	if rf.state == Leader {

		go func() {
			rf.sendCommand(command)
		}()

		index = len(rf.log)
		term = rf.currentTerm
		APrintf("The command is at index %d by %d.", index, rf.me)

	} else {
		isLeader = false
	}
	rf.mu.Unlock()

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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// Set Timer and Set vote channel
	rf.timer = make(chan int)
	rf.voteCh = make(chan int)
	rf.log = []Log{Log{0, 0, nil}} // init first index of log (index 0), so the beginning index of log is 1

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

// version 1.0
func FindValueCountOverHalf(arr []int) int {

	val := -1
	for i, x := range arr {
		count := 0
		for _, y := range arr[i:] {
			if x == y {
				count++
			}
		}
		if count > len(arr)/2 {
			val = x
			break
		}
	}
	return val
}
