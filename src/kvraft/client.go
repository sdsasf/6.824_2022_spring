package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId  int64
	clerkId   int64
	requestNo int64
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
	ck.leaderId = 0
	ck.clerkId = nrand()
	ck.requestNo = 1
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		RequestNo: ck.requestNo,
	}
	reply := GetReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case OK, ErrNoKey:
				ck.requestNo++
				DPrintf("clerk %d finish Get %v at key %v, requestNo: %d\n", ck.clerkId, reply.Value, key, ck.requestNo-1)
				return reply.Value
			case ErrTimeout, ErrFailed, ErrWrongLeader:
			}
		}
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		time.Sleep(15 * time.Millisecond)
		DPrintf("clerk %d retry Get to new leader %d, requestNo: %d\n", ck.clerkId, ck.leaderId, ck.requestNo)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		RequestNo: ck.requestNo,
	}
	reply := PutAppendReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK, ErrDupCommand:
				ck.requestNo++
				DPrintf("clerk %d finish %v %v at key %v, requestNo: %d\n", ck.clerkId, op, value, key, ck.requestNo-1)
				return
			case ErrTimeout, ErrFailed, ErrWrongLeader:
			}
		}
		ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
		time.Sleep(15 * time.Millisecond)
		DPrintf("clerk %d retry PutAppend to new leader %d, requestNo: %d\n", ck.clerkId, ck.leaderId, ck.requestNo)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
