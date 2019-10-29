package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"src/github.com/sasha-s/go-deadlock"
	"time"
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
	//	mu      sync.Mutex
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	log           map[string]string
	requestRecord []int64
	lastRequest   int64

	valueCh    chan Value
	valueState bool

	identityCh    chan int64
	identityState bool

	dispatchCh  chan string
	getCh       chan int
	putAppendCh chan int

	lock *int
}

type Value struct {
	identity int64
	value    string
}

func (kv *KVServer) initDispatch() {

	kv.dispatchCh = make(chan string)
	kv.getCh = make(chan int)
	kv.putAppendCh = make(chan int)
	go kv.dispatch()
}

func (kv *KVServer) dispatch() {

	var queue []string
	lock := false

	for request := range kv.dispatchCh {
		switch request {
		case "getStart":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				kv.getCh <- 1
			}

		case "getEnd":
			if len(queue) > 0 {
				item := queue[0]
				queue = queue[1:]
				switch item {
				case "getStart":
					lock = true
					kv.getCh <- 1
				case "putAppendStart":
					lock = true
					kv.putAppendCh <- 1
				}
			} else {
				lock = false
			}

		case "putAppendStart":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				kv.putAppendCh <- 1
			}

		case "putAppendEnd":
			if len(queue) > 0 {
				item := queue[0]
				queue = queue[1:]
				switch item {
				case "getStart":
					lock = true
					kv.getCh <- 1
				case "putAppendStart":
					lock = true
					kv.putAppendCh <- 1
				}
			} else {
				lock = false
			}
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// v1.0
	// use lock to guarantee invoking linearly.
	//kv.mu.Lock()
	//defer kv.mu.Unlock()

	// v1.1
	kv.dispatchCh <- "getStart"
	<-kv.getCh
	defer func() { kv.dispatchCh <- "getEnd" }()

	// v1.2
	//for *kv.lock == 1 {
	//}
	//*kv.lock = 1
	//defer func() { *kv.lock = 0 }()

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader

	// Cope with duplicate Clerk requests.
	if kv.findRecordContainIdentity(args.Identity, kv.requestRecord) {
		reply.Err = OK
		reply.Value = kv.getValue(args.Key)
		DPrintf("return with reply %v", reply)
		kv.lastRequest = args.Identity
		return
	}

	if args.Identity != kv.lastRequest {
		kv.deleteRequestRecordWithIdentity(kv.lastRequest)
	}

	if isLeader {

		command := Op{args.Identity, "Get", args.Key, ""}

		DPrintf("server %d start args %v", kv.me, args)

		flag := 0
		go func(flag *int, args *GetArgs) {
			for value := range kv.valueCh {
				if value.identity == args.Identity {
					reply.Value = value.value
					*flag = 1
					return
				} else if value.identity == -1 {
					return
				}
			}

			DPrintf("value %v args %v", reply.Value, args)
		}(&flag, args)

		kv.valueState = true
		oldIndex, oldTerm, _ := kv.rf.Start(command)
		DPrintf("server %d old index %d old term %d", kv.me, oldIndex, oldTerm)

		for index, term := oldIndex, oldTerm; flag == 0 && oldIndex == index && oldTerm == term; {
			// request to fast will make repeat invoke.
			time.Sleep(time.Duration(20) * time.Millisecond)
			index, term, _ = kv.rf.Start(command)
			DPrintf("server %d current index %d current term %d flag %d", kv.me, index, term, flag)
		}

		// Leader loses its leadership before the request is committed to the log.
		if flag == 0 {
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			kv.valueState = false
			kv.valueCh <- Value{-1, ""}
			DPrintf("return with reply %v", reply)
			return
		}
		DPrintf("server %d get value %v", kv.me, reply.Value)

		if reply.Err = OK; len(reply.Value) == 0 {
			reply.Err = ErrNoKey
		}

		// Free server memory quickly.
		//if args.Identity != kv.lastRequest {
		//	kv.deleteRequestRecordWithIdentity(kv.lastRequest)
		//}
		//kv.lastRequest = args.Identity
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// v1.0
	// use lock to guarantee invoking linearly.
	//kv.mu.Lock()
	//defer kv.mu.Unlock()

	// v1.1
	kv.dispatchCh <- "putAppendStart"
	<-kv.putAppendCh
	defer func() { kv.dispatchCh <- "putAppendEnd" }()

	//v1.2
	//for *kv.lock == 1 {
	//}
	//*kv.lock = 1
	//defer func() { *kv.lock = 0 }()

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader

	// Cope with duplicate Clerk requests.
	if kv.findRecordContainIdentity(args.Identity, kv.requestRecord) {
		reply.Err = OK
		kv.lastRequest = args.Identity
		return
	}

	if args.Identity != kv.lastRequest {
		kv.deleteRequestRecordWithIdentity(kv.lastRequest)
	}

	if isLeader {

		command := Op{args.Identity, args.Op, args.Key, args.Value}
		DPrintf("server %d start args %v", kv.me, args)

		flag := 0
		go func(flag *int, args *PutAppendArgs) {
			for identity := range kv.identityCh {
				DPrintf("identity %d args %v", identity, args)
				if identity == args.Identity {
					*flag = 1
					return
				} else if identity == -1 {
					return
				}
			}
		}(&flag, args)

		kv.identityState = true
		oldIndex, oldTerm, _ := kv.rf.Start(command)

		DPrintf("server %d old index %d old term %d", kv.me, oldIndex, oldTerm)

		for index, term := oldIndex, oldTerm; flag == 0 && oldIndex == index && oldTerm == term; {
			// request to fast will make repeat invoke.
			time.Sleep(time.Duration(20) * time.Millisecond)
			index, term, _ = kv.rf.Start(command)
			DPrintf("server %d current index %d current term %d flag %d", kv.me, index, term, flag)
		}

		// Leader loses its leadership before the request is committed to the log.
		if flag == 0 {
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			kv.identityCh <- -1
			kv.identityState = false
			DPrintf("return with reply %v", reply)
			return
		}
		DPrintf("server %d submit command %v | isLeader %v", kv.me, command, isLeader)
	}
	reply.Err = OK

	// Free server memory quickly.
	//if args.Identity != kv.lastRequest {
	//	kv.deleteRequestRecordWithIdentity(kv.lastRequest)
	//}
	//kv.lastRequest = args.Identity
}

func (kv *KVServer) initApplyCommand() {
	go kv.applyCommand()
}

func (kv *KVServer) applyCommand() {

	for applyMsg := range kv.applyCh {
		command, _ := applyMsg.Command.(Op)
		// Cope with duplicate Clerk requests.
		kv.requestRecord = append(kv.requestRecord, command.Identity)

		switch command.Operation {
		case "Get":
			DPrintf("server %d put command %v.", kv.me, command)
			if _, isLeader := kv.rf.GetState(); isLeader && kv.valueState {
				kv.valueCh <- Value{command.Identity, kv.getValue(command.Key)}
			}
			//DPrintf("server %d over get.", kv.me)

		case "Put":
			DPrintf("server %d put command %v.", kv.me, command)
			kv.putValue(command.Key, command.Value)
			if _, isLeader := kv.rf.GetState(); isLeader && kv.identityState {
				kv.identityCh <- command.Identity
			}
			//DPrintf("server %d over put.", kv.me)

		case "Append":
			DPrintf("server %d append command %v.", kv.me, command)
			kv.appendValue(command.Key, command.Value)
			if _, isLeader := kv.rf.GetState(); isLeader && kv.identityState {
				kv.identityCh <- command.Identity
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
	kv.valueCh = make(chan Value)
	kv.identityCh = make(chan int64)
	kv.initDispatch()

	//lockVal := 0
	//kv.lock = &lockVal // 0 is unlock | 1 is lock.

	kv.initApplyCommand()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
