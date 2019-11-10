package raftkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"src/github.com/sasha-s/go-deadlock"
	"time"
)

const Debug = 0

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

	maxraftstate int             // snapshot if log grows this big
	persister    *raft.Persister // Object to hold this peer's persisted state

	// Your definitions here.
	log           map[string]string
	requestRecord []int64
	lastRequest   int64
	curRequest    int64

	valueCh chan Value

	identityCh chan int64

	dispatchCh  chan string
	getCh       chan int
	putAppendCh chan int

	internalSnapshotCh chan raft.Snapshot // snapshot
	snapshotCh         chan raft.Snapshot // snapshot

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
	go kv.doDispatch()
}

func (kv *KVServer) doDispatch() {

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

	// v1.1
	kv.dispatchCh <- "getStart"
	<-kv.getCh
	defer func() { kv.dispatchCh <- "getEnd" }()

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader

	// Cope with duplicate Clerk requests.
	if kv.findRecordContainIdentity(args.Identity, kv.requestRecord) {
		reply.Err = OK
		reply.Value = kv.getValue(args.Key)
		return
	}

	if isLeader {

		// delete unused record.
		kv.deleteRequestRecordWithIdentity(args.LastReply)

		command := Op{args.Identity, "Get", args.Key, ""}
		kv.curRequest = args.Identity

		flag := 0
		closeCh := make(chan interface{})

		go func(flag *int) {
			select {
			case value := <-kv.valueCh:
				reply.Value = value.value
				*flag = 1
				return
			case <-closeCh:
				return
			}
		}(&flag)

		oldIndex, oldTerm, _ := kv.rf.Start(command)
		time.Sleep(time.Duration(20) * time.Millisecond)

		for index, term := oldIndex, oldTerm; flag == 0 && oldIndex == index && oldTerm == term; {
			// request to fast will make repeat invoke.
			index, term, _ = kv.rf.Start(command)
			time.Sleep(time.Duration(20) * time.Millisecond)
		}

		// Leader loses its leadership before the request is committed to the log.
		if flag == 0 {
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			close(closeCh) // quit goroutine.
			return
		}

		if reply.Err = OK; len(reply.Value) == 0 {
			reply.Err = ErrNoKey
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// v1.1
	kv.dispatchCh <- "putAppendStart"
	<-kv.putAppendCh
	defer func() { kv.dispatchCh <- "putAppendEnd" }()

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader

	// Cope with duplicate Clerk requests.
	if kv.findRecordContainIdentity(args.Identity, kv.requestRecord) {
		reply.Err = OK
		return
	}

	if isLeader {

		// delete unused record.
		kv.deleteRequestRecordWithIdentity(args.LastReply)

		command := Op{args.Identity, args.Op, args.Key, args.Value}
		kv.curRequest = args.Identity

		flag := 0
		closeCh := make(chan interface{})

		go func(flag *int) {

			select {
			case <-kv.identityCh:
				*flag = 1
				return
			case <-closeCh:
				return
			}
		}(&flag)

		oldIndex, oldTerm, _ := kv.rf.Start(command)
		time.Sleep(time.Duration(20) * time.Millisecond)

		for index, term := oldIndex, oldTerm; flag == 0 && oldIndex == index && oldTerm == term; {
			// request to fast will make repeat invoke.
			index, term, _ = kv.rf.Start(command)
			time.Sleep(time.Duration(20) * time.Millisecond)
		}

		// Leader loses its leadership before the request is committed to the log.
		if flag == 0 {
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			close(closeCh) // quit goroutine.
			return
		}
	}
	reply.Err = OK
}

func (kv *KVServer) initApplyCommand() {
	go kv.doApplyCommand()
}

func (kv *KVServer) doApplyCommand() {

	for applyMsg := range kv.applyCh {

		if applyMsg.CommandValid {

			command, _ := applyMsg.Command.(Op)

			if kv.findRecordContainIdentity(command.Identity, kv.requestRecord) {
				continue
			}

			// Cope with duplicate Clerk requests.
			kv.requestRecord = append(kv.requestRecord, command.Identity)

			switch command.Operation {
			case "Get":
				if command.Identity == kv.curRequest {
					kv.valueCh <- Value{command.Identity, kv.getValue(command.Key)}
				}

			case "Put":
				kv.putValue(command.Key, command.Value)
				if command.Identity == kv.curRequest {
					kv.identityCh <- command.Identity
				}

			case "Append":
				kv.appendValue(command.Key, command.Value)
				if command.Identity == kv.curRequest {
					kv.identityCh <- command.Identity
				}

			default:
				// do nothing.
			}

			// should do after apply command.
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate*9/10 {
				kv.internalSnapshotCh <- applyMsg.Snapshot // snapshot
			}
		} else {
			// do snapshot
			kv.log = applyMsg.Snapshot.StateMachineState
		}
	}
}

func (kv *KVServer) doDeleteUnusedDuplicateRequests() {
	if kv.lastRequest != kv.curRequest {
		kv.deleteRequestRecordWithIdentity(kv.lastRequest)
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

/**
Snapshot
*/
func (kv *KVServer) initSnapshot(persister *raft.Persister) {
	if kv.maxraftstate != -1 {
		kv.internalSnapshotCh = make(chan raft.Snapshot)
		kv.snapshotCh = make(chan raft.Snapshot)
		go kv.doSnapshot(persister)
	}
}

func (kv *KVServer) doSnapshot(persister *raft.Persister) {

	for snapshot := range kv.internalSnapshotCh {
		//if persister.RaftStateSize() >= kv.maxraftstate {
		snapshot.StateMachineState = kv.log
		snapshot.RequestRecord = kv.requestRecord
		go func() { kv.snapshotCh <- snapshot }()
		//}
	}
}

func (kv *KVServer) restoreFromSnapshot(snapshotBytes []byte) {

	buf := bytes.NewBuffer(snapshotBytes)
	decoder := labgob.NewDecoder(buf)
	var snapshot raft.Snapshot
	_ = decoder.Decode(&snapshot)
	if snapshot.StateMachineState == nil {
		kv.log = make(map[string]string)
	} else {
		kv.log = snapshot.StateMachineState
		kv.requestRecord = snapshot.RequestRecord
	}
}

func (kv *KVServer) doSerializeLog(logData map[string]string) []byte {

	//v1.1 GOB-encode
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(&logData); err != nil {
		log.Panic(err)
	}
	fmt.Printf("序列化后：%x\n")
	snapshot := buffer.Bytes()

	return snapshot
}

func (kv *KVServer) doDeserializeLog(snapshot []byte) map[string]string {

	//v1.1 GOB-decode
	var buffer bytes.Buffer
	byteEn := buffer.Bytes()
	decoder := gob.NewDecoder(bytes.NewReader(byteEn))
	var logData map[string]string
	if err := decoder.Decode(&logData); err != nil {
		log.Panic(err)
	}
	return logData
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

	//kv.maxraftstate = 1 // test snapshot.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.log = make(map[string]string)
	kv.valueCh = make(chan Value)
	kv.identityCh = make(chan int64)
	kv.initDispatch()
	kv.initSnapshot(persister)

	//lockVal := 0
	//kv.lock = &lockVal // 0 is unlock | 1 is lock.

	kv.initApplyCommand()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.rf.SetAndInitSnapshotCh(kv.snapshotCh) // snapshot
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	return kv
}
