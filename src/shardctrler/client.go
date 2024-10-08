package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
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
	// Your code here.
	ck.clerkId = nrand()
	ck.requestNo = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.RequestNo = ck.requestNo
	args.Num = num
	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			// DPrintf("clerk %d send Query to server %d, reply.Err = %v, wrongLeader = %v", ck.clerkId, i, reply.Err, reply.WrongLeader)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("clerk %d send Query to server %d success", ck.clerkId, i)
				ck.requestNo++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.RequestNo = ck.requestNo
	args.Servers = servers

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("clerk %d send Join to server %d success", ck.clerkId, i)
				ck.requestNo++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.RequestNo = ck.requestNo
	args.GIDs = gids

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("clerk %d send Leave to server %d success", ck.clerkId, i)
				ck.requestNo++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.ClerkId = ck.clerkId
	args.RequestNo = ck.requestNo
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for i, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				DPrintf("clerk %d send Move to server %d success", ck.clerkId, i)
				ck.requestNo++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
