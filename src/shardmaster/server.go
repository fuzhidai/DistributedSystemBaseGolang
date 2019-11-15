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

const Debug = 1

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

	curRequest    int64
	lastRequest   int64
	requestRecord []int64

	appendCh chan int64
	configCh chan Config

	dispatchCh chan string
	joinCh     chan int
	leaveCh    chan int
	moveCh     chan int
	queryCh    chan int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Identity  int64
	Operation string // Join | Leave | Move | Query
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
		command := Op{args.Identity, "Append", config.Num, config}
		sm.curRequest = args.Identity

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
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			close(closeCh) // quit goroutine.
			return
		}

		reply.Err = OK
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

	//sm.hr.addNodes(sm.getKeySet(servers))
	//for index, _ := range lastConfig.Shards {
	//	config.Shards[index], _ = strconv.Atoi(sm.hr.getNode(strconv.Itoa(index)))
	//}
	//sm.doBalance(&config.Shards)

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

		command := Op{args.Identity, "Append", config.Num, config}
		sm.curRequest = args.Identity

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
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			close(closeCh) // quit goroutine.
			return
		}

		reply.Err = OK
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
	//for index, _ := range lastConfig.Shards {
	//	config.Shards[index], _ = strconv.Atoi(sm.hr.getNode(strconv.Itoa(index)))
	//}

	//sm.doBalance(&config.Shards)

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
		command := Op{args.Identity, "Append", config.Num, config}
		sm.curRequest = args.Identity

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
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			close(closeCh) // quit goroutine.
			return
		}

		reply.Err = OK
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
	util
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

func (sm *ShardMaster) doBalance(shards *[NShards]int) {

	_, maxCount, maxValue, minCount, minValue := ArrayCountValues(*shards)

	for maxCount > minCount+1 {

		count := (maxCount - minCount) / 2
		for idx, val := range shards {
			if count == 0 {
				break
			}
			if val == maxValue[0] {
				shards[idx] = minValue[0]
				count--
			}
		}

		_, maxCount, maxValue, minCount, minValue = ArrayCountValues(*shards)
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

//求数组中出现次数最多的值和次数
func ArrayCountValues(args [NShards]int) (Status bool, MaxCount int, MaxValue []int, MinCount int, MinValue []int) {
	/*【1】没值直接退出*/
	if len(args) == 0 {
		return false, 0, nil, 0, nil
	}

	/*【2】求出每个值对应出现的次数，例:[值:次数,值:次数]*/
	newMap := make(map[int]int)
	for _, value := range args {
		if newMap[value] != 0 {
			newMap[value]++
		} else {
			newMap[value] = 1
		}
	}

	/*【3】求出出现最多的次数*/
	var allCount []int //所有的次数
	var maxCount int   //出现最多的次数
	var minCount int   //出现最少的次数
	for _, value := range newMap {
		allCount = append(allCount, value)
	}
	maxCount = allCount[0]
	minCount = allCount[0]
	for i := 0; i < len(allCount); i++ {
		if maxCount < allCount[i] {
			maxCount = allCount[i]
		} else if minCount > allCount[i] {
			minCount = allCount[i]
		}
	}

	/*【4】求数组中出现次数最多的值，例：[8,9]这个两个值出现的次数一样多*/
	var maxValue []int
	var minValue []int
	for key, value := range newMap {
		if value == maxCount {
			maxValue = append(maxValue, key)
		} else if value == minCount {
			minValue = append(minValue, key)
		}
	}

	return true, maxCount, maxValue, minCount, minValue
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
