package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastReply int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// uniquely identify client operations to ensure that the key/value service executes each one just once.
	identity := nrand()
	for {
		time.Sleep(time.Duration(5) * time.Millisecond)
		for server := range ck.servers {

			// You will have to modify this function.
			args := &GetArgs{key, identity, ck.lastReply}
			reply := &GetReply{}

			ok := ck.sendGet(server, args, reply)

			// keeps trying forever in the face of all other errors.
			for !ok && reply.Err == OK {
				DPrintf("something wrong with args %v", args)
				ok = ck.sendGet(server, args, reply)
			}

			// send PRC to Leader successfully. Return fetch value.
			if !reply.WrongLeader && reply.Err == OK {
				DPrintf("Get args %v return", args)
				ck.lastReply = identity
				switch reply.Err {
				case OK:
					return reply.Value
				case ErrNoKey:
					return ""
				}
			}
		}
	}
}

func (ck *Clerk) sendGet(server int, args *GetArgs, reply *GetReply) bool {
	ok := ck.servers[server].Call("KVServer.Get", args, reply)
	return ok
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// uniquely identify client operations to ensure that the key/value service executes each one just once.
	identity := nrand()
	for {
		time.Sleep(time.Duration(5) * time.Millisecond)
		for server := range ck.servers {
			args := &PutAppendArgs{key, value, op, identity, ck.lastReply}
			reply := &PutAppendReply{}

			ok := ck.sendPutAppend(server, args, reply)

			// keeps trying forever in the face of all other errors.
			for !ok && reply.Err == OK {
				DPrintf("something wrong with args %v", args)
				ok = ck.sendPutAppend(server, args, reply)
			}

			// send PRC to Leader successfully. Return fetch value.
			if !reply.WrongLeader && reply.Err == OK {
				DPrintf("server %d PutAppend args %v return", server, args)
				ck.lastReply = identity
				return
			}
		}
	}
}

func (ck *Clerk) sendPutAppend(server int, args *PutAppendArgs, reply *PutAppendReply) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend", args, reply)
	return ok
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
