package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Identity  int64
	LastReply int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	Key         string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Identity  int64
	LastReply int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	Key         string
}

type TransferShardsArgs struct {
	ConfigNum int
	Shards    []int
	Logs      map[string]string
}

type TransferShardsReply struct {
	Err Err
}

type SynchronizeConfigArgs struct {
	Type      string
	ConfigNum int
	Config    shardmaster.Config
}

type SynchronizeConfigReply struct {
	Success         bool
	NeedSynchronize bool
}
