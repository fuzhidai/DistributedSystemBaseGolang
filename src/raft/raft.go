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
	"bytes"
	"labgob"
	"math"
	"math/rand"
	"sort"
	"src/github.com/sasha-s/go-deadlock"
	"strconv"
	"strings"
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
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu        deadlock.Mutex      // Lock to protect shared access to this peer's state (test deadlock)
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

	// other
	electionTimerCh  chan int // election timeout
	heartbeatTimerCh chan int // heartbeat timeout

	voteCh     chan int // vote Channel
	loopSwitch bool

	commandCh chan interface{} // send command Channel
	state     int              // 0 means Follower ； 1 means Candidate ； 2 means Leader
}

// peer state
const (
	Follower  = 0 // Follower
	Candidate = 1 // Candidate
	Leader    = 2 // Leader
	PreVote   = 3 // PreVote
)

const ElectionTimeout = 1000 * time.Millisecond
const HeartbeatTimeout = 100 * time.Millisecond

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

	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)

	//rf.mu.Lock()
	_ = encoder.Encode(rf.currentTerm)
	_ = encoder.Encode(rf.votedFor)
	_ = encoder.Encode(rf.log)

	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
	//rf.mu.Unlock()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
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

	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)

	var currentTerm int
	var votedFor string
	var log []Log

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil {

		// decode error

	} else {

		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.mu.Unlock()
	}
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
	Index   int  //  the follower can include the term of the conflicting entry and the first index it stores for that term.
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		// candidate term behind current term
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// the last log entity of peer log
	rf.mu.Lock()
	log := rf.log[len(rf.log)-1]
	rf.mu.Unlock()
	APrintf("Peer %d argsTerm %d AND currentTerm %d | argsIndex %d AND logId %d | term %d", rf.me, args.LastLogTerm, log.Term, args.LastLogIndex, log.LogIndex, rf.currentTerm)
	if log.Term > args.LastLogTerm || (log.Term == args.LastLogTerm && log.LogIndex > args.LastLogIndex) {
		// candidate term behind current term
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.mu.Unlock()

		reply.Term = args.Term
		reply.VoteGranted = false
		return
	}

	// In the same term, peer has voted for other candidate or itself, don't vote this candidate
	// guarantee one term only vote for one candidate.
	rf.mu.Lock()
	if rf.currentTerm == args.Term && strings.Compare(rf.votedFor, args.CandidateId) != 0 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	APrintf("peer %d has vote %s. ", rf.me, args.CandidateId)
	rf.stopHeartbeatTimer()
	rf.resetElectionTimer()

	rf.mu.Lock()
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.loopSwitch = false
	rf.votedFor = args.CandidateId
	rf.mu.Unlock()
	rf.persist()

	// prepare reply
	reply.Term = args.Term
	reply.VoteGranted = true
}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	APrintf("peer %d get AppendEntries, and args is %v", rf.me, args)

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}
	rf.mu.Unlock()

	rf.resetElectionTimer()
	rf.stopHeartbeatTimer()

	rf.mu.Lock()
	rf.currentTerm = args.Term
	rf.persist()
	rf.state = Follower
	rf.loopSwitch = false

	reply.Term = args.Term

	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		reply.Index = len(rf.log) // optimize
		rf.mu.Unlock()
		return
	} else if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Index = rf.findFirstLogIndexForTerm(rf.log[args.PrevLogIndex].Term)
		rf.mu.Unlock()
		return
	} else {
		if args.PrevLogIndex >= 0 {
			rf.log = rf.log[:args.PrevLogIndex+1]
		}

		if args.Entries != nil {
			// append log
			NPrintf("peer %d append logs %v", rf.me, args.Entries)
			rf.log = append(rf.log, args.Entries...)
			// do persist
		}

		rf.persist()

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
		}
		rf.updateLastApplyIndex()

		reply.Index = len(rf.log)
		rf.mu.Unlock()
		reply.Success = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Controller of Timer
func (rf *Raft) startElectionTimer() {
	go rf.electionTimer()
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimerCh <- 1
}

func (rf *Raft) stopElectionTimer() {
	rf.electionTimerCh <- -1
}

func (rf *Raft) initHeartbeatTimer() {
	go rf.heartbeatTimer()
}

func (rf *Raft) startHeartbeatTimer() {
	rf.heartbeatTimerCh <- 1
}

func (rf *Raft) stopHeartbeatTimer() {
	rf.heartbeatTimerCh <- -1
}

func (rf *Raft) electionTimer() {

	rf.electionTimerCh = make(chan int)
	for {
		ticker := time.NewTicker(time.Duration(RandInt64(200, 400)) * time.Millisecond)
	election:
		for {
			select {
			case <-ticker.C:
				NPrintf("peer %d begin to vote.", rf.me)
				go rf.collectVote()
				rf.startVote()
			case command := <-rf.electionTimerCh:

				switch command {
				case 1:
					ticker.Stop()
					break election
				case -1:
					ticker.Stop()
					for c := range rf.electionTimerCh {
						if c == 1 {
							break election
						}
					}
				default:

				}
			}
		}
	}
}

func (rf *Raft) heartbeatTimer() {

	rf.mu.Lock()
	rf.heartbeatTimerCh = make(chan int)
	rf.mu.Unlock()

	for c := range rf.heartbeatTimerCh {
		if c == 1 {
			break
		}
	}

	for {
		ticker := time.NewTicker(HeartbeatTimeout)
	heartbeat:
		for {
			select {
			case <-ticker.C:
				rf.sendHeartbeats()
			case command := <-rf.heartbeatTimerCh:
				switch command {
				case 1:
					break heartbeat
				case -1:
					ticker.Stop()
					for c := range rf.heartbeatTimerCh {
						if c == 1 {
							break heartbeat
						}
					}
				default:

				}
			}
		}
	}
}

// Start vote.
func (rf *Raft) startVote() {

	rf.mu.Lock()
	rf.state = Candidate // current peer state is Candidate
	rf.mu.Unlock()

	rf.votedFor = strconv.Itoa(rf.me) // vote itself
	rf.currentTerm++
	rf.persist()
	//rf.mu.Unlock()// increase current term

	APrintf("Peer %d begin to vote, current term is %d", rf.me, rf.currentTerm)

	for server, _ := range rf.peers {
		if server != rf.me {
			// start a new go routine to send request vote
			go func(server int) {

				rf.mu.Lock()
				//lastLogIndex := len(rf.log) - 1
				//lastLogTerm := rf.log[lastLogIndex].Term

				args := &RequestVoteArgs{rf.currentTerm, strconv.Itoa(rf.me), len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
				reply := &RequestVoteReply{}
				rf.mu.Unlock()

				DPrintf("Send vote to peer %d.", server)

				// send request vote
				if rf.sendRequestVote(server, args, reply) {

					if reply.VoteGranted {
						// send message to collect func.
						rf.voteCh <- 1
					} else {
						rf.mu.Lock()
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.currentTerm = reply.Term
							rf.loopSwitch = false
							rf.persist()
							rf.mu.Unlock()

							rf.stopHeartbeatTimer()
							rf.resetElectionTimer()
						} else {
							rf.mu.Unlock()
						}
					}

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
	// rf.voteCh = make(chan int)

	// get state for vote from vote channel
	for range rf.voteCh {
		// mean vote have finish
		// There must be rf.state != Candidate ! To prevent old vote affect!!!
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// add one vote
		voteNum++
		// (a) it wins the election
		if voteNum >= ((len(rf.peers) + 1) / 2) {

			APrintf("peer: %d become Leader, term is %d ", rf.me, rf.currentTerm)
			//go rf.sendHeartbeats() // send heartbeats right now !
			rf.stopElectionTimer()
			rf.startHeartbeatTimer() // start Timer

			rf.mu.Lock()

			rf.state = Leader // peer have got enough vote，so it become Leader
			rf.loopSwitch = true
			rf.persist()
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

			return // finish vote
		}
	}
}

/**
new solution
Send heartbeat
*/

func (rf *Raft) sendHeartbeats() {
	for server, _ := range rf.peers {
		if server != rf.me {
			go rf.sendHeartbeat(server)
		}
	}
}

func (rf *Raft) sendHeartbeat(server int) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	commitIndex := rf.commitIndex
	rf.mu.Unlock()

	args := &AppendEntriesArgs{currentTerm, strconv.Itoa(rf.me), prevLogIndex, prevLogTerm, nil, commitIndex}
	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, reply) {
		rf.processHeartbeatReply(server, args, reply)
	} else {
		// There is something wrong with network
	}
}

func (rf *Raft) processHeartbeatReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		// Send Heartbeat success.
	} else {
		// Send Heartbeat success, but something wrong with args.
		rf.mu.Lock()
		if rf.currentTerm < reply.Term {
			rf.state = Follower
			rf.currentTerm = reply.Term
			rf.loopSwitch = false

			rf.persist()
			rf.stopHeartbeatTimer()
			rf.resetElectionTimer()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) findFirstLogIndexForTerm(term int) int {

	index := 1
	for i := 1; i < len(rf.log); i++ {
		if rf.log[i].Term == term {
			index = i
			break
		}
	}
	return index
}

/**
new solution
Send Command
version 1.1
*/

func (rf *Raft) startCommandChannel() {

	for command := range rf.commandCh {
		go func(command interface{}) {
			rf.mu.Lock()

			log := Log{rf.nextIndex[rf.me], rf.currentTerm, command}
			rf.log = append(rf.log, log)

			rf.matchIndex[rf.me] = log.LogIndex
			rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1

			rf.persist()
			rf.mu.Unlock()
		}(command)
	}
}

func (rf *Raft) sendCommand(command interface{}) bool {
	rf.commandCh <- command
	return true
}

func (rf *Raft) checkFollowerNeedToAppendThisLog(server int, logIndex int) bool {
	return rf.nextIndex[server] <= logIndex
}

func (rf *Raft) updateCommitIndex() {

	NPrintf("Leader %d current commitIndex %d A, logLen %d ", rf.me, rf.commitIndex, len(rf.log))
	if res, index := rf.checkIfCanUpdateCommitIndex(); res && rf.log[index].Term == rf.currentTerm {
		NPrintf("Leader %d current commitIndex %d B ", rf.me, rf.commitIndex)
		//rf.sendApplyMsg(rf.commitIndex, index)
		rf.commitIndex = index
	}
}

func (rf *Raft) updateLastApplyIndex() {
	for {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{true, rf.log[rf.lastApplied].Command, rf.lastApplied}
			NPrintf("Peer %d apply %d", rf.me, rf.lastApplied)
		} else {
			break
		}
	}
}

func (rf *Raft) checkIfCanUpdateCommitIndex() (bool, int) {
	index := FindValueCountOverHalf(rf.matchIndex)
	return index > 0 && index > rf.commitIndex, index
}

// version 1.0
func FindValueCountOverHalf(arr []int) int {

	var tmp = make([]int, len(arr))
	copy(tmp, arr)
	sort.Ints(tmp)
	return tmp[len(arr)-3]
}

/**
new solution
Send Command
version 1.2
*/
func (rf *Raft) initConsistentLoop() {
	for server := range rf.peers {
		if server != rf.me {

			go func(server int) {

				for {

					time.Sleep(HeartbeatTimeout / 50)

					rf.mu.Lock()
					if rf.loopSwitch && rf.matchIndex[server] < rf.matchIndex[rf.me] {
						rf.mu.Unlock()

						rf.mu.Lock()
						prevLogIndex := rf.nextIndex[server] - 1
						prevLogTerm := rf.log[prevLogIndex].Term
						logs := rf.log[rf.nextIndex[server]:rf.nextIndex[rf.me]]

						args := &AppendEntriesArgs{rf.currentTerm, strconv.Itoa(rf.me), prevLogIndex, prevLogTerm, logs, rf.commitIndex}
						reply := &AppendEntriesReply{}
						rf.mu.Unlock()

						if rf.sendAppendEntries(server, args, reply) {
							if reply.Success {
								NPrintf("Success! server %d return %v", server, reply)
								rf.mu.Lock()
								if index := reply.Index; index >= rf.nextIndex[server] {
									rf.nextIndex[server] = index
									rf.matchIndex[server] = index - 1
									rf.updateCommitIndex()
									rf.updateLastApplyIndex()

									go func() {
										rf.mu.Lock()
										currentTerm := rf.currentTerm
										commitIndex := rf.commitIndex
										rf.mu.Unlock()

										args := &AppendEntriesArgs{currentTerm, strconv.Itoa(rf.me), -1, -1, nil, commitIndex}
										reply := &AppendEntriesReply{}
										rf.sendAppendEntries(server, args, reply)
									}()

								}
								rf.mu.Unlock()

							} else {
								NPrintf("Fail! server %d return %v nextIndex %v", server, reply, rf.nextIndex)

								if rf.currentTerm == reply.Term {
									rf.mu.Lock()
									if reply.Index > 0 && rf.nextIndex[server] > reply.Index {
										rf.nextIndex[server] = reply.Index
									}
									rf.mu.Unlock()
								} else {
									// Send Heartbeat success, but something wrong with args.
									rf.mu.Lock()
									if rf.currentTerm < reply.Term {
										rf.state = Follower
										rf.currentTerm = reply.Term
										rf.loopSwitch = false

										rf.persist()
										rf.stopHeartbeatTimer()
										rf.resetElectionTimer()
									}
									rf.mu.Unlock()
								}
							}

						} else {
							// network fail return directly.
						}
					} else {
						rf.mu.Unlock()
					}
				}
			}(server)
		}
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

	// Your code here (2B).
	//NPrintf("Send Command peer %d state %V. A", rf.me, rf.state)
	rf.mu.Lock()
	if rf.state == Leader {
		//NPrintf("Send Command. B")

		log := rf.log
		for _, l := range log {
			if l.Command == command {
				index = l.LogIndex
				break
			}
		}

		if index == -1 {
			index = rf.nextIndex[rf.me]
			rf.sendCommand(command)
		}

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
	//rf.timer = make(chan int)
	rf.voteCh = make(chan int)
	rf.commandCh = make(chan interface{})
	rf.log = []Log{Log{0, 0, nil}} // init first index of log (index 0), so the beginning index of log is 1

	rf.startElectionTimer()
	rf.initHeartbeatTimer()
	go rf.startCommandChannel()
	rf.initConsistentLoop()

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
