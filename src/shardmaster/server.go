package shardmaster

import (
	"raft"
	"strconv"
)
import "labrpc"
import "sync"
import "labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	hr *HashRing

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Identity  int64
	Operation string // Join | Leave | Move | Query
	Config    Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	config := sm.createConfigForJoinOp(args)

	// do implemented as a fault-tolerant service using Raft.
	command := Op{args.Identity, "Join", config}

	sm.configs = append(sm.configs, config)

	reply.Err = OK
	reply.WrongLeader = false
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

	sm.hr.addNodes(sm.getKeySet(servers))
	for index, _ := range lastConfig.Shards {
		config.Shards[index], _ = strconv.Atoi(sm.hr.getNode(strconv.Itoa(index)))
	}

	return config
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	config := sm.createConfigForLeaveOp(args)

	sm.configs = append(sm.configs, config)

	reply.Err = OK
	reply.WrongLeader = false
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
	for index, _ := range lastConfig.Shards {
		config.Shards[index], _ = strconv.Atoi(sm.hr.getNode(strconv.Itoa(index)))
	}

	return config
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	config := sm.createConfigForQueryOp(args)

	sm.configs = append(sm.configs, config)

	reply.Err = OK
	reply.WrongLeader = false
}

func (sm *ShardMaster) createConfigForQueryOp(args *MoveArgs) Config {

	config := Config{}
	config.Num = len(sm.configs)
	groups := make(map[int][]string)

	lastConfig := sm.configs[len(sm.configs)-1]
	sm.copyMap(lastConfig.Groups, groups)
	config.Groups = groups

	config.Shards[args.Shard] = args.GID

	return config
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if args.Num == -1 || args.Num > len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
		reply.Err = OK
	}
}

func (sm *ShardMaster) copyMap(src map[int][]string, des map[int][]string) {
	for key, _ := range src {
		des[key] = src[key]
	}
}

func (sm *ShardMaster) getKeySet(m map[int][]string) []string {
	var keySet []string
	for key, _ := range m {
		keySet = append(keySet, strconv.Itoa(key))
	}
	return keySet
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
	sm.hr = New(nil, 1)

	return sm
}
