package shardkv

// import "shardmaster"
import (
	"bytes"
	"labrpc"
	"log"
	"shardmaster"
	"time"
)
import "raft"
import "sync"
import "labgob"

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
	Operation string // Get | Put | Append | Config
	Key       string
	Value     string
	Config    shardmaster.Config
	Shards    []int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int             // snapshot if log grows this big
	persister    *raft.Persister // Object to hold this peer's persisted state

	// Your definitions here.
	log map[string]string

	curRequest    int64   // the identity of current request which is processing.
	requestRecord []int64 // slice of identity which client has got the request of this identity.

	applyGetCh          chan string // Notify the Append method when it receives an ApplyMsg of type Get.
	applyPutAppendCh    chan int64  // Notify the Append method when it receives an ApplyMsg of type Append and Put.
	applyConfigCh       chan map[int][]int
	applyConfigAcceptCh chan int64
	applyTransferCh     chan int64

	dispatchCh       chan string // channel to lock Func so that Func can execute linearly.
	getCh            chan int    // channel for Get unlock Func.
	putAppendCh      chan int    // channel for PutAppend unlock Func.
	transferShardsCh chan int
	synchronizeCh    chan int

	internalSnapshotCh chan raft.Snapshot // snapshot
	snapshotCh         chan raft.Snapshot // snapshot

	config           shardmaster.Config
	shardMasterClerk *shardmaster.Clerk

	doSynchronize bool
}

func (kv *ShardKV) initDispatch() {

	kv.dispatchCh = make(chan string)
	kv.getCh = make(chan int)
	kv.putAppendCh = make(chan int)
	kv.synchronizeCh = make(chan int)
	kv.transferShardsCh = make(chan int)
	go kv.doDispatch()
}

func (kv *ShardKV) doDispatch() {

	var queue []string
	lock := false

	for request := range kv.dispatchCh {
		switch request {
		case "get":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				kv.getCh <- 1
			}

		case "putAppend":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				kv.putAppendCh <- 1
			}

		case "synchronize":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				kv.synchronizeCh <- 1
			}

		case "transferShards":
			if lock {
				queue = append(queue, request)
			} else {
				lock = true
				kv.transferShardsCh <- 1
			}

		case "end":
			if len(queue) > 0 {
				item := queue[0]
				queue = queue[1:]
				lock = true

				switch item {
				case "get":
					kv.getCh <- 1
				case "putAppend":
					kv.putAppendCh <- 1
				case "synchronize":
					kv.synchronizeCh <- 1
				case "transferShards":
					kv.transferShardsCh <- 1
				}
			} else {
				lock = false
			}
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	DPrintf("Gid %v Server %v Get A.", kv.gid, kv.me)

	kv.dispatchCh <- "get"
	<-kv.getCh
	defer func() { kv.dispatchCh <- "end" }()

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader

	if isLeader {
		DPrintf("Gid %v Server %v Get B. Shards %v", kv.gid, kv.me, kv.config.Shards)

		if ok := kv.shouldResponsibleForKey(args.Key); !ok {
			reply.Key = args.Key
			reply.Err = ErrWrongGroup
			return
		}

		// Cope with duplicate Clerk requests.
		if kv.findRecordContainIdentity(args.Identity, kv.requestRecord) {
			reply.Value = kv.getValue(args.Key)
			reply.Err = OK
			return
		}

		// delete unused record.
		kv.deleteRequestRecordWithIdentity(args.LastReply)

		command := Op{args.Identity, "Get", args.Key, "", shardmaster.Config{}, nil}
		kv.curRequest = args.Identity

		flag := 0
		closeCh := make(chan interface{})

		go func(flag *int) {
			select {
			case value := <-kv.applyGetCh:
				reply.Value = value
				reply.Err = OK
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
			if gid := kv.config.Shards[key2shard(args.Key)]; gid != kv.gid {
				close(closeCh) // quit goroutine.
				reply.Key = args.Key
				reply.Err = ErrWrongGroup
				return
			}

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

		if reply.Err == OK && len(reply.Value) == 0 {
			reply.Err = ErrNoKey
		}
	}

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	DPrintf("Gid %v Server %v Put Append. A", kv.gid, kv.me)

	kv.dispatchCh <- "putAppend"
	<-kv.putAppendCh
	defer func() { kv.dispatchCh <- "end" }()

	DPrintf("Gid %v Server %v Put Append. B", kv.gid, kv.me)

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader

	if isLeader {

		DPrintf("Gid %v Server %v Put Append. C", kv.gid, kv.me)

		if ok := kv.shouldResponsibleForKey(args.Key); !ok {
			reply.Key = args.Key
			reply.Err = ErrWrongGroup
			return
		}

		// Cope with duplicate Clerk requests.
		if kv.findRecordContainIdentity(args.Identity, kv.requestRecord) {
			reply.Err = OK
			return
		}

		// delete unused record.
		kv.deleteRequestRecordWithIdentity(args.LastReply)

		command := Op{args.Identity, args.Op, args.Key, args.Value, shardmaster.Config{}, nil}
		kv.curRequest = args.Identity

		flag := 0
		closeCh := make(chan interface{})

		go func(flag *int) {
			select {
			case <-kv.applyPutAppendCh:
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
			if gid := kv.config.Shards[key2shard(args.Key)]; gid != kv.gid {
				close(closeCh) // quit goroutine.
				reply.Key = args.Key
				reply.Err = ErrWrongGroup
				return
			}

			index, term, _ = kv.rf.Start(command)
			time.Sleep(time.Duration(20) * time.Millisecond)
		}

		// Leader loses its leadership before the request is committed to the log.
		if flag == 0 {
			reply.Err = "LoseLeadership"
			reply.WrongLeader = true
			close(closeCh) // quit goroutine.
			DPrintf("Put Append. D")
			return
		}

		reply.Err = OK
	}
}

func (kv *ShardKV) TransferShards(args *TransferShardsArgs, reply *TransferShardsReply) {

	DPrintf("Gid %v Server %v Get transfer A shards old %v new %v", kv.gid, kv.me, kv.config.Shards, args.Shards)

	_, isLeader := kv.rf.GetState()
	reply.WrongLeader = !isLeader
	reply.Err = OK

	if isLeader {

		DPrintf("Gid %v Server %v Get transfer B shards old %v new %v", kv.gid, kv.me, kv.config.Shards, args.Shards)

		DPrintf("Gid %v Server %v Tran Lock", kv.gid, kv.me)
		kv.dispatchCh <- "transferShards"
		<-kv.transferShardsCh
		defer func() {
			kv.dispatchCh <- "end"
			DPrintf("Gid %v Server %v Tran UnLock", kv.gid, kv.me)
		}()

		DPrintf("Gid %v Server %v Get transfer C shards old %v new %v", kv.gid, kv.me, kv.config.Shards, args.Shards)

		for key, val := range args.Logs {

			DPrintf("TRAN key %v BEG.", key)

			identity := nrand()
			command := Op{identity, "Transfer", key, val, shardmaster.Config{}, nil}
			kv.curRequest = identity

			flag := 0
			closeCh := make(chan interface{})

			go func(flag *int) {
				DPrintf("open normal %v", identity)
				select {
				case <-kv.applyTransferCh:
					*flag = 1
					DPrintf("close normal.%v", identity)
					return
				case <-closeCh:
					DPrintf("close normal.%v", identity)
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

			if flag == 0 {
				close(closeCh) // quit goroutine.
				break
			}

			DPrintf("TRAN key %v END.", key)
		}

		DPrintf("TRAN key OVER.")

		go func(shards []int) {
			var transferShards []int
			for _, shard := range shards {
				transferShards = append(transferShards, shard)
			}
			kv.updateConfigToAcceptSomeRequest(transferShards)
		}(args.Shards)
	}
}

/*
 * Log.
 */
func (kv *ShardKV) initLog() {
	kv.log = make(map[string]string)
}

/*
 * Apply.
 */
func (kv *ShardKV) initApplyCommand() {
	kv.applyGetCh = make(chan string)
	kv.applyPutAppendCh = make(chan int64)
	kv.applyConfigCh = make(chan map[int][]int)
	kv.applyConfigAcceptCh = make(chan int64)
	kv.applyTransferCh = make(chan int64)
	go kv.doApplyCommand()
}

func (kv *ShardKV) doApplyCommand() {

	for applyMsg := range kv.applyCh {

		if applyMsg.CommandValid {

			command, _ := applyMsg.Command.(Op)

			DPrintf("Gid %v Server %v deal command %v +A+", kv.gid, kv.me, command)

			// 1. Judge according to Identity, skip if the current request has been processed.
			// 2. All group members agree on a reconfiguration occurs after client Put/Append/Get requests.
			//    So the Put won't take effect and client must re-try at the new owner.
			if gid := kv.config.Shards[key2shard(command.Key)]; kv.findRecordContainIdentity(command.Identity, kv.requestRecord) || (command.Operation != "Transfer" && command.Operation != "Config" && command.Operation != "ConfigAccept" && gid != kv.gid) {
				continue
			}

			DPrintf("Gid %v Server %v deal command %v A", kv.gid, kv.me, command)
			DPrintf("Gid %v Server %v current shards %v A", kv.gid, kv.me, kv.config.Shards)

			// Cope with duplicate Clerk requests.
			kv.requestRecord = append(kv.requestRecord, command.Identity)

			switch command.Operation {
			case "Get":
				if command.Identity == kv.curRequest {
					kv.applyGetCh <- kv.getValue(command.Key)
				}

			case "Put":
				kv.putValue(command.Key, command.Value)
				if command.Identity == kv.curRequest {
					kv.applyPutAppendCh <- command.Identity
				}

			case "Append":
				kv.appendValue(command.Key, command.Value)
				if command.Identity == kv.curRequest {
					kv.applyPutAppendCh <- command.Identity
				}

			case "Transfer":
				kv.putValue(command.Key, command.Value)
				if command.Identity == kv.curRequest {
					DPrintf("Send to Channel.")
					kv.applyTransferCh <- command.Identity
				}

			case "Config":
				transferShards := kv.shieldInvalidShards(command.Config)
				if command.Identity == kv.curRequest {
					kv.applyConfigCh <- transferShards
				}

				DPrintf("Gid %v Server %v current shards %v B", kv.gid, kv.me, kv.config.Shards)

			case "ConfigAccept":
				for _, shard := range command.Shards {
					kv.config.Shards[shard] = kv.gid
				}
				if command.Identity == kv.curRequest {
					kv.applyConfigAcceptCh <- command.Identity
				}

			default:
				// do nothing.
			}

			DPrintf("Gid %v Server %v deal command %v B", kv.gid, kv.me, command)

			// should do after apply command.
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate*9/10 {
				kv.internalSnapshotCh <- applyMsg.Snapshot // snapshot
			}

			DPrintf("Gid %v Server %v deal command %v C", kv.gid, kv.me, command)

		} else {
			// do snapshot
			kv.log = applyMsg.Snapshot.StateMachineState
		}
	}
}

/*
 * Snapshot.
 */
func (kv *ShardKV) initSnapshot(persister *raft.Persister) {
	if kv.maxraftstate != -1 {
		kv.internalSnapshotCh = make(chan raft.Snapshot)
		kv.snapshotCh = make(chan raft.Snapshot)
		go kv.doSnapshot(persister)
	}
}

func (kv *ShardKV) doSnapshot(persister *raft.Persister) {

	for snapshot := range kv.internalSnapshotCh {
		snapshot.StateMachineState = kv.log
		snapshot.RequestRecord = kv.requestRecord
		go func() { kv.snapshotCh <- snapshot }()
	}
}

func (kv *ShardKV) restoreFromSnapshot(snapshotBytes []byte) {

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

	DPrintf("Gid %v server %v Restore %v", kv.gid, kv.me, kv.log)
}

/*
 * transfer shards.
 */
func (kv *ShardKV) initFetchConfiguration() {
	go kv.doFetchConfiguration()
}

func (kv *ShardKV) doFetchConfiguration() {
	for {
		if kv.config.Shards[0] == 0 {
			config := kv.shardMasterClerk.Query(-1)
			DPrintf("Gid %v Server %v init config %v", kv.gid, kv.me, config)
			kv.config = config
		}

		// Only Leader do fetch.
		if _, isLeader := kv.rf.GetState(); isLeader {

			config := kv.shardMasterClerk.Query(-1)
			DPrintf("Gid %v Server %v CURRENT %v NEW %v", kv.gid, kv.me, kv.config.Shards, config)

			old := kv.config
			if needUpdateShards := kv.needUpdateShards(config); needUpdateShards {

				DPrintf("Gid %v Server %v Config Lock", kv.gid, kv.me)
				kv.dispatchCh <- "synchronize"
				<-kv.synchronizeCh

				DPrintf("Gid %v Server %v Need to transfer shards old %v new %v", kv.gid, kv.me, old, config.Shards)

				identity := nrand()
				// do consistent.
				command := Op{identity, "Config", "", "", config, nil}
				kv.curRequest = identity

				flag := 0
				closeCh := make(chan interface{})

				go func(flag *int) {
					select {
					case transferShards := <-kv.applyConfigCh:
						kv.transferShards(transferShards, command.Config)
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

				if flag == 0 {
					close(closeCh) // quit goroutine.
				}
				// kv.transferShards(shards, config)

				kv.dispatchCh <- "end"
				DPrintf("Gid %v Server %v Config Lock", kv.gid, kv.me)
			}

		}
		// periodically poll the shardmaster to learn about new configurations.
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) transferShards(shards map[int][]int, config shardmaster.Config) { // gid -> shards[]

	transferLogs := make(map[int]map[string]string) // gid -> log[key->val]
	for gid, shardSlice := range shards {
		for _, shard := range shardSlice {
			if len(transferLogs[gid]) == 0 {
				transferLogs[gid] = make(map[string]string)
			}
			kv.findValueForShard(transferLogs[gid], shard)
		}
	}

	DPrintf("transfer logs %v shards %v", transferLogs, shards)

	kv.doTransferShards(transferLogs, shards, config)
}

/*
 * find key-value from log by shard.
 */
func (kv *ShardKV) findValueForShard(shardMap map[string]string, shard int) {
	for key, val := range kv.log {
		if key2shard(key) == shard {
			shardMap[key] = val
		}
	}
}

func (kv *ShardKV) doTransferShards(transferLogs map[int]map[string]string, shards map[int][]int, config shardmaster.Config) {
	for gid, logs := range transferLogs {
		servers := kv.getServersByGid(gid, config)
		kv.transferShardsToOneGroup(gid, logs, shards[gid], servers, config.Num)
	}
}

func (kv *ShardKV) getServersByGid(gid int, config shardmaster.Config) []string {
	return config.Groups[gid]
}

func (kv *ShardKV) transferShardsToOneGroup(gid int, logs map[string]string, shards []int, servers []string, configNum int) {

	for {
		for idx, server := range servers {
			// concurrent.
			srv := kv.make_end(server)
			args := TransferShardsArgs{configNum, shards, logs}
			var reply TransferShardsReply

			DPrintf("Send to %v transfer RPC %v", idx, args)

			ok := srv.Call("ShardKV.TransferShards", &args, &reply)
			if ok && reply.Err == OK && reply.WrongLeader == false {
				return
			}
		}
	}
}

func (kv *ShardKV) needUpdateShards(config shardmaster.Config) bool {
	needUpdateShards := false
	newShards := config.Shards
	for shard, gid := range kv.config.Shards {
		if gid == kv.gid && newShards[shard] != gid {
			needUpdateShards = true
			break
		}
	}

	DPrintf("Should update %v config %v current %v", needUpdateShards, config.Shards, kv.config.Shards)
	return needUpdateShards
}

func (kv *ShardKV) shieldInvalidShards(config shardmaster.Config) map[int][]int {

	shards := make(map[int][]int) // gid -> shards[]
	newShards := config.Shards

	for shard, gid := range kv.config.Shards {
		if gid == kv.gid && newShards[shard] != gid {
			newGid := newShards[shard]
			// shield shards which has not responsible by current server.
			kv.config.Shards[shard] = newGid
			shards[newGid] = append(shards[newGid], shard)
		}
	}

	return shards
}

/*
 * At first reject client requests if the receiving group isn't responsible for the client's key's shard.
 */
func (kv *ShardKV) updateConfigToRejectSomeRequest(config shardmaster.Config) (bool, map[int][]int) {

	shouldTransferShards := false
	shards := make(map[int][]int) // gid -> shards[]

	newShards := config.Shards
	for shard, gid := range kv.config.Shards {
		if gid == kv.gid && newShards[shard] != gid {
			newGid := newShards[shard]
			// shield shards which has not responsible by current server.
			kv.mu.Lock()
			kv.config.Shards[shard] = newGid
			kv.mu.Unlock()

			shards[newGid] = append(shards[newGid], shard)
			shouldTransferShards = true
		}
	}

	DPrintf("GID %v Server %v Need to transfer shards new %v", kv.gid, kv.me, config.Shards)

	return shouldTransferShards, shards
}

/*
 * Accept request for new shard after accept shards from old server.
 * If a replica group gains a shard, it needs to wait for the previous owner to send
 * over the old shard data before accepting requests for that shard.
 */
func (kv *ShardKV) updateConfigToAcceptSomeRequest(shards []int) {

	DPrintf("Config Accept.")

	kv.dispatchCh <- "synchronize"
	<-kv.synchronizeCh

	identity := nrand()
	// do consistent.
	DPrintf("Shards %v", shards)
	command := Op{identity, "ConfigAccept", "", "", shardmaster.Config{}, shards}
	kv.curRequest = identity

	flag := 0
	closeCh := make(chan interface{})

	go func(flag *int) {
		select {
		case <-kv.applyConfigAcceptCh:
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

	if flag == 0 {
		close(closeCh) // quit goroutine.
	}
	// kv.transferShards(shards, config)

	kv.dispatchCh <- "end"
}

func (kv *ShardKV) shouldResponsibleForKey(key string) bool {
	shard := key2shard(key)
	kv.mu.Lock()
	gid := kv.config.Shards[shard]
	kv.mu.Unlock()
	return gid == kv.gid
}

func (kv *ShardKV) findRecordContainIdentity(identity int64, record []int64) bool {
	for _, value := range record {
		if value == identity {
			return true
		}
	}
	return false
}

func (kv *ShardKV) deleteRequestRecordWithIdentity(identity int64) bool {
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

func (kv *ShardKV) getValue(key string) string {
	return kv.log[key]
}

func (kv *ShardKV) putValue(key string, value string) bool {
	kv.log[key] = value
	DPrintf("current log %v", value)
	return true
}

func (kv *ShardKV) appendValue(key string, value string) {
	curValue := kv.getValue(key)
	if len(curValue) > 0 {
		value = curValue + value
	}
	kv.putValue(key, value)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.initLog()
	kv.initDispatch()
	kv.initApplyCommand()
	kv.initSnapshot(persister)
	kv.initFetchConfiguration()
	kv.persister = persister

	// Use something like this to talk to the shardmaster:
	kv.shardMasterClerk = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.rf.SetAndInitSnapshotCh(kv.snapshotCh) // snapshot
	kv.restoreFromSnapshot(persister.ReadSnapshot())

	return kv
}
