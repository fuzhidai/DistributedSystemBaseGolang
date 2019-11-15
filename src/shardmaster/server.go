package shardmaster

import (
	"log"
	"raft"
	"strconv"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	hr *HashRing

	curRequest    int64   // the identity of current request which is processing.
	requestRecord []int64 // slice of identity which client has got the request of this identity.

	appendCh chan int64  // Notify the Append method when it receives an ApplyMsg of type Append.
	configCh chan Config // Notify the Query method when it receives an ApplyMsg of type Query and pass config to Query method.

	dispatchCh chan string // channel to lock Func so that Func can execute linearly.
	joinCh     chan int    // channel for Join unlock Func.
	leaveCh    chan int    // channel for Leave unlock Func.
	moveCh     chan int    // channel for move unlock Func.
	queryCh    chan int    // channel for query unlock Func.

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Identity  int64
	Operation string // Join | Leave | Move -> Append & Query -> Query
	ConfigNum int
	Config    Config
}

func (sm *ShardMaster) initDispatch() {

	sm.dispatchCh = make(chan string)
	sm.joinCh = make(chan int)
	sm.leaveCh = make(chan int)
	sm.moveCh = make(chan int)
	sm.queryCh = make(chan int)
	go sm.doDispatch()
}

func (sm *ShardMaster) doDispatch() {

	var queue []string
	lock := false

	for request := range sm.dispatchCh {
		switch request {
		case "join":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				sm.joinCh <- 1
			}

		case "leave":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				sm.leaveCh <- 1
			}

		case "move":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				sm.moveCh <- 1
			}

		case "query":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				sm.queryCh <- 1
			}

		case "end":
			if len(queue) > 0 {
				item := queue[0]
				queue = queue[1:]
				lock = true

				switch item {
				case "join":
					sm.joinCh <- 1
				case "leave":
					sm.leaveCh <- 1
				case "move":
					sm.moveCh <- 1
				case "query":
					sm.queryCh <- 1
				}
			} else {
				lock = false
			}
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	sm.dispatchCh <- "join"
	<-sm.joinCh
	defer func() { sm.dispatchCh <- "end" }()

	_, isLeader := sm.rf.GetState()
	reply.WrongLeader = !isLeader

	if sm.findRecordContainIdentity(args.Identity, sm.requestRecord) {
		reply.Err = OK
		return
	}

	if isLeader {

		sm.deleteRequestRecordWithIdentity(args.LastReply)
		config := sm.createConfigForJoinOp(args)
		reply.Err, reply.WrongLeader = sm.consistentAppend(config, args.Identity)
	}
}

func (sm *ShardMaster) createConfigForJoinOp(args *JoinArgs) Config {

	servers := args.Servers

	config := Config{}
	config.Num = len(sm.configs)
	groups := make(map[int][]string)

	lastConfig := sm.configs[len(sm.configs)-1]
	sm.copyMap(lastConfig.Groups, groups)
	sm.copyMap(servers, groups)
	config.Groups = groups

	// Divide the shards as evenly as possible among the full set of groups.
	sm.balance(&config.Shards, sm.getKeySet(groups))

	DPrintf("Join：%v current Group：%v Shards %v", args.Servers, config.Groups, config.Shards)

	return config
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	sm.dispatchCh <- "leave"
	<-sm.leaveCh
	defer func() { sm.dispatchCh <- "end" }()

	_, isLeader := sm.rf.GetState()
	reply.WrongLeader = !isLeader

	if sm.findRecordContainIdentity(args.Identity, sm.requestRecord) {
		reply.Err = OK
		return
	}

	if isLeader {

		sm.deleteRequestRecordWithIdentity(args.LastReply)
		config := sm.createConfigForLeaveOp(args)
		reply.Err, reply.WrongLeader = sm.consistentAppend(config, args.Identity)
	}
}

func (sm *ShardMaster) createConfigForLeaveOp(args *LeaveArgs) Config {

	gids := args.GIDs

	config := Config{}
	config.Num = len(sm.configs)
	groups := make(map[int][]string)

	lastConfig := sm.configs[len(sm.configs)-1]
	sm.copyMap(lastConfig.Groups, groups)

	// Create a new configuration that does not include those groups.
	for _, gid := range gids {
		delete(groups, gid)
		sm.hr.removeNode(strconv.Itoa(gid))
	}
	config.Groups = groups

	// Assigns those groups' shards to the remaining groups.
	sm.balance(&config.Shards, sm.getKeySet(groups))

	DPrintf("Leave：%v current Group：%v Shards %v", args.GIDs, config.Groups, config.Shards)

	return config
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	sm.dispatchCh <- "move"
	<-sm.moveCh
	defer func() { sm.dispatchCh <- "end" }()

	_, isLeader := sm.rf.GetState()
	reply.WrongLeader = !isLeader

	if sm.findRecordContainIdentity(args.Identity, sm.requestRecord) {
		reply.Err = OK
		return
	}

	if isLeader {

		sm.deleteRequestRecordWithIdentity(args.LastReply)

		config := sm.createConfigForMoveOp(args)
		reply.Err, reply.WrongLeader = sm.consistentAppend(config, args.Identity)
	}
}

func (sm *ShardMaster) createConfigForMoveOp(args *MoveArgs) Config {

	config := Config{}
	config.Num = len(sm.configs)
	groups := make(map[int][]string)

	lastConfig := sm.configs[len(sm.configs)-1]
	sm.copyMap(lastConfig.Groups, groups)
	sm.copyArray(&lastConfig.Shards, &config.Shards)

	config.Groups = groups

	config.Shards[args.Shard] = args.GID

	return config
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	sm.dispatchCh <- "query"
	<-sm.queryCh
	defer func() { sm.dispatchCh <- "end" }()

	_, isLeader := sm.rf.GetState()
	reply.WrongLeader = !isLeader

	if sm.findRecordContainIdentity(args.Identity, sm.requestRecord) {
		reply.Err = OK
		reply.Config = sm.queryConfig(args.Num)
		return
	}

	if isLeader {

		sm.deleteRequestRecordWithIdentity(args.LastReply)

		command := Op{args.Identity, "Query", args.Num, Config{}}
		sm.curRequest = args.Identity

		flag := 0
		closeCh := make(chan interface{})

		go func(flag *int) {
			select {
			case config := <-sm.configCh:
				reply.Config = config
				*flag = 1
				return
			case <-closeCh:
				return
			}
		}(&flag)

		oldIndex, oldTerm, _ := sm.rf.Start(command)
		time.Sleep(time.Duration(20) * time.Millisecond)

		for index, term := oldIndex, oldTerm; flag == 0 && oldIndex == index && oldTerm == term; {
			// request to fast will make repeat invoke.
			index, term, _ = sm.rf.Start(command)
			time.Sleep(time.Duration(20) * time.Millisecond)
		}

		// Leader loses its leadership before the request is committed to the log.
		if flag == 0 {
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			close(closeCh) // quit goroutine.
			return
		}

		reply.Err = OK
	}
}

/*
 * This method is mainly responsible for implementing fault tolerance by relying on the underlying
 * Raft algorithm to synchronize server data.
 */
func (sm *ShardMaster) consistentAppend(config Config, identity int64) (err Err, wrongLeader bool) {

	command := Op{identity, "Append", config.Num, config}
	sm.curRequest = identity

	flag := 0
	closeCh := make(chan interface{})

	go func(flag *int) {
		select {
		case <-sm.appendCh:
			*flag = 1
			return
		case <-closeCh:
			return
		}
	}(&flag)

	oldIndex, oldTerm, _ := sm.rf.Start(command)
	time.Sleep(time.Duration(20) * time.Millisecond)

	for index, term := oldIndex, oldTerm; flag == 0 && oldIndex == index && oldTerm == term; {
		// request to fast will make repeat invoke.
		index, term, _ = sm.rf.Start(command)
		time.Sleep(time.Duration(20) * time.Millisecond)
	}

	// Leader loses its leadership before the request is committed to the log.
	if flag == 0 {
		close(closeCh) // quit goroutine.
		return "LoseLeadership", true
	}

	return OK, true
}

/*
	Some util function.
*/
func (sm *ShardMaster) copyMap(src map[int][]string, des map[int][]string) {
	for key, _ := range src {
		des[key] = src[key]
	}
}

func (sm *ShardMaster) copyArray(src *[NShards]int, des *[NShards]int) {
	for index, val := range src {
		des[index] = val
	}
}

func (sm *ShardMaster) getKeySet(m map[int][]string) []int {
	var keySet []int
	for key, _ := range m {
		keySet = append(keySet, key)
	}
	return keySet
}

func (sm *ShardMaster) appendConfig(config Config) {
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) queryConfig(configNum int) Config {
	var config Config
	if configNum == -1 || configNum > len(sm.configs) {
		config = sm.configs[len(sm.configs)-1]
	} else {
		config = sm.configs[configNum]
	}
	return config
}

func (sm *ShardMaster) balance(shards *[NShards]int, groups []int) {

	if len(groups) == 0 {
		for idx, _ := range shards {
			shards[idx] = 0
		}
		return
	}

	mod := NShards % len(groups)
	divide := NShards / len(groups)

	iter := 0

	for _, val := range groups {

		count := divide
		if mod > 0 {
			count++
			mod--
		}

		for count > 0 {
			shards[iter] = val
			iter++
			count--
		}
	}
}

func (sm *ShardMaster) findRecordContainIdentity(identity int64, record []int64) bool {
	for _, value := range record {
		if value == identity {
			return true
		}
	}
	return false
}

func (sm *ShardMaster) deleteRequestRecordWithIdentity(identity int64) bool {
	index := -1

	if len(sm.requestRecord) == 0 {
		return false
	}

	for i, value := range sm.requestRecord {
		if value == identity {
			index = i
			break
		}
	}

	if index == -1 {
		return false
	}

	sm.requestRecord = append(sm.requestRecord[:index], sm.requestRecord[index+1:]...)
	return true
}

func (sm *ShardMaster) initApplyCommand() {
	go sm.doApplyCommand()
}

func (sm *ShardMaster) doApplyCommand() {

	for applyMsg := range sm.applyCh {

		if applyMsg.CommandValid {

			command, _ := applyMsg.Command.(Op)

			if sm.findRecordContainIdentity(command.Identity, sm.requestRecord) {
				continue
			}

			// Cope with duplicate Clerk requests.
			sm.requestRecord = append(sm.requestRecord, command.Identity)

			switch command.Operation {
			case "Query":
				if command.Identity == sm.curRequest {
					config := sm.queryConfig(command.ConfigNum)
					sm.configCh <- config
				}
			case "Append":
				sm.appendConfig(command.Config)
				if command.Identity == sm.curRequest {
					sm.appendCh <- command.Identity
				}

			default:
				// do nothing.
			}
		} else {
			// do nothing
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.hr = New(nil, 10)
	sm.initApplyCommand()
	sm.initDispatch()

	sm.configCh = make(chan Config)
	sm.appendCh = make(chan int64)

	return sm
}
