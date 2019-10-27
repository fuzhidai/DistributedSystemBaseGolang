package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Identity  int64
	Operation string // Get | Put | Append
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	log           map[string]string
	requestRecord []int64
	lastRequest   int64
	valueCh       chan string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// use lock to guarantee invoking linearly.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader

	// Cope with duplicate Clerk requests.
	if kv.findRecordContainIdentity(args.Identity, kv.requestRecord) {
		reply.Err = OK
		reply.Value = kv.getValue(args.Key)
		//kv.lastRequest = args.Identity
		return
	}

	if isLeader {

		command := Op{args.Identity, "Get", args.Key, ""}

		DPrintf("server %d start args %v", kv.me, args)

		kv.rf.Start(command)

		reply.Value = <-kv.valueCh
		DPrintf("server %d get value %v", kv.me, reply.Value)

		// Cope with duplicate Clerk requests.
		kv.requestRecord = append(kv.requestRecord, args.Identity)

		// Free server memory quickly.
		//if args.Identity != kv.lastRequest {
		//	kv.deleteRequestRecordWithIdentity(kv.lastRequest)
		//}
		//kv.lastRequest = args.Identity

		if reply.Err = OK; len(reply.Value) == 0 {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// use lock to guarantee invoking linearly.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader

	// Cope with duplicate Clerk requests.
	if kv.findRecordContainIdentity(args.Identity, kv.requestRecord) {
		reply.Err = OK
		//kv.lastRequest = args.Identity
		return
	}

	if isLeader {

		command := Op{args.Identity, args.Op, args.Key, args.Value}
		DPrintf("server %d start args %v", kv.me, args)

		kv.rf.Start(command)

		<-kv.valueCh // block to wait to finish consistent.

		// Cope with duplicate Clerk requests.
		kv.requestRecord = append(kv.requestRecord, args.Identity)

		// Free server memory quickly.
		//if args.Identity != kv.lastRequest {
		//	kv.deleteRequestRecordWithIdentity(kv.lastRequest)
		//}
		//kv.lastRequest = args.Identity

		DPrintf("server %d submit command %v | isLeader %v", kv.me, command, isLeader)
	}
	reply.Err = OK
}

func (kv *KVServer) initApplyCommand() {
	go kv.applyCommand()
}

func (kv *KVServer) applyCommand() {

	for applyMsg := range kv.applyCh {
		command, _ := applyMsg.Command.(Op)
		//DPrintf("server %d begin apply command %v", kv.me, command)

		switch command.Operation {
		case "Get":
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.valueCh <- kv.getValue(command.Key)
			}
			//DPrintf("server %d over get.", kv.me)

		case "Put":
			kv.putValue(command.Key, command.Value)
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.valueCh <- command.Value
			}
			//DPrintf("server %d over put.", kv.me)

		case "Append":
			kv.appendValue(command.Key, command.Value)
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.valueCh <- command.Value
			}
			//DPrintf("server %d over append.", kv.me)

		default:
			// do nothing.
		}
	}
}

func (kv *KVServer) findRecordContainIdentity(identity int64, record []int64) bool {
	for _, value := range record {
		if value == identity {
			return true
		}
	}
	return false
}

func (kv *KVServer) deleteRequestRecordWithIdentity(identity int64) bool {
	index := -1

	if len(kv.requestRecord) == 0 {
		return false
	}

	for i, value := range kv.requestRecord {
		if value == identity {
			index = i
			break
		}
	}

	if index == -1 {
		return false
	}

	kv.requestRecord = append(kv.requestRecord[:index], kv.requestRecord[index+1:]...)
	return true
}

func (kv *KVServer) getValue(key string) string {
	return kv.log[key]
}

func (kv *KVServer) putValue(key string, value string) bool {
	kv.log[key] = value
	return true
}

func (kv *KVServer) appendValue(key string, value string) {
	curValue := kv.getValue(key)
	if len(curValue) > 0 {
		value = curValue + value
	}
	kv.putValue(key, value)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.log = make(map[string]string)
	kv.valueCh = make(chan string)
	kv.initApplyCommand()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
