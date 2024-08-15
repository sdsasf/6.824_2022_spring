package shardctrler

import (
	"6.824/raft"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const SERVER_TIMEOUT = 200
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	commitIdxCh map[int]chan result
	finishMap   map[int64]int64
	configs     []Config // indexed by config num
	dead        int32
}

type Op struct {
	// Your data here.
	OpKind    int
	Num       int              // for Query
	Servers   map[int][]string // for join
	GIDs      []int            // for Leave
	Shard     int              // for Move
	GID       int
	ClerkId   int64
	RequestNo int64
}

// applyMsgReceiver return result struct through commitIdxCh to server threads
type result struct {
	err       Err
	clerkId   int64
	requestNo int64
}

const (
	QUERY = iota
	JOIN
	LEAVE
	MOVE
)

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if args.RequestNo <= sc.finishMap[args.ClerkId] {
		// dup command, needn't apply
		DPrintf("server %d receive dup Join", sc.me)
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	command := Op{
		OpKind:    JOIN,
		Num:       0,
		Servers:   args.Servers,
		GIDs:      nil,
		Shard:     0,
		GID:       0,
		ClerkId:   args.ClerkId,
		RequestNo: args.RequestNo,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	// channel maybe already existed
	ch, ok := sc.commitIdxCh[index]
	if !ok {
		ch = make(chan result, 1)
		sc.commitIdxCh[index] = ch
	}
	sc.mu.Unlock()

	for {
		select {
		case res := <-ch:
			sc.mu.Lock()
			delete(sc.commitIdxCh, index)
			sc.mu.Unlock()
			if res.clerkId == args.ClerkId && res.requestNo == args.RequestNo {
				reply.Err = res.err
				DPrintf("server %d execute Join success, reply.Err = %v", sc.me, reply.Err)
			} else {
				reply.Err = ErrFailed
			}
			return
		case <-time.After(SERVER_TIMEOUT * time.Millisecond):
			sc.mu.Lock()
			delete(sc.commitIdxCh, index)
			sc.mu.Unlock()
			reply.Err = ErrTimeout
			return
		}
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.RequestNo <= sc.finishMap[args.ClerkId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	command := Op{
		OpKind:    LEAVE,
		Num:       0,
		Servers:   nil,
		GIDs:      args.GIDs,
		Shard:     0,
		GID:       0,
		ClerkId:   args.ClerkId,
		RequestNo: args.RequestNo,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	// channel maybe already existed
	ch, ok := sc.commitIdxCh[index]
	if !ok {
		ch = make(chan result, 1)
		sc.commitIdxCh[index] = ch
	}
	sc.mu.Unlock()

	for {
		select {
		case res := <-ch:
			sc.mu.Lock()
			delete(sc.commitIdxCh, index)
			sc.mu.Unlock()
			if res.clerkId == args.ClerkId && res.requestNo == args.RequestNo {
				reply.Err = res.err
				DPrintf("server %d execute Leave success, reply.Err = %v", sc.me, reply.Err)
			} else {
				reply.Err = ErrFailed
			}
			return
		case <-time.After(SERVER_TIMEOUT * time.Millisecond):
			sc.mu.Lock()
			delete(sc.commitIdxCh, index)
			sc.mu.Unlock()
			reply.Err = ErrTimeout
			return
		}
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if args.RequestNo <= sc.finishMap[args.ClerkId] {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	command := Op{
		OpKind:    MOVE,
		Num:       0,
		Servers:   nil,
		GIDs:      nil,
		Shard:     args.Shard,
		GID:       args.GID,
		ClerkId:   args.ClerkId,
		RequestNo: args.RequestNo,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	// channel maybe already existed
	ch, ok := sc.commitIdxCh[index]
	if !ok {
		ch = make(chan result, 1)
		sc.commitIdxCh[index] = ch
	}
	sc.mu.Unlock()

	for {
		select {
		case res := <-ch:
			sc.mu.Lock()
			delete(sc.commitIdxCh, index)
			sc.mu.Unlock()
			if res.clerkId == args.ClerkId && res.requestNo == args.RequestNo {
				reply.Err = res.err
				DPrintf("server %d execute Move success", sc.me)
			} else {
				reply.Err = ErrFailed
			}
			return
		case <-time.After(SERVER_TIMEOUT * time.Millisecond):
			sc.mu.Lock()
			delete(sc.commitIdxCh, index)
			sc.mu.Unlock()
			reply.Err = ErrTimeout
			return
		}
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	// if config could return directly
	if args.Num > 0 && args.Num <= len(sc.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sc.configs[args.Num]
		DPrintf("Query return config %d", args.Num)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	// else input raft
	command := Op{
		OpKind:    QUERY,
		Num:       args.Num,
		Servers:   nil,
		GIDs:      nil,
		Shard:     0,
		GID:       0,
		ClerkId:   args.ClerkId,
		RequestNo: args.RequestNo,
	}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	// channel maybe already existed
	ch, ok := sc.commitIdxCh[index]
	if !ok {
		ch = make(chan result, 1)
		sc.commitIdxCh[index] = ch
	}
	sc.mu.Unlock()

	reply.WrongLeader = false
	for {
		select {
		case res := <-ch:
			sc.mu.Lock()
			if res.clerkId == args.ClerkId && res.requestNo == args.RequestNo {
				reply.Err = res.err
			} else {
				reply.Err = ErrFailed
			}
			if res.err == OK {
				if args.Num == -1 || args.Num >= len(sc.configs) {
					reply.Config = sc.configs[len(sc.configs)-1]
					DPrintf("Query -1, return config %d, have %d group", len(sc.configs)-1, len(reply.Config.Groups))
				} else {
					reply.Config = sc.configs[args.Num]
					DPrintf("Query return config %d", args.Num)
				}
			}
			delete(sc.commitIdxCh, index)
			sc.mu.Unlock()
			return
		case <-time.After(SERVER_TIMEOUT * time.Millisecond):
			sc.mu.Lock()
			delete(sc.commitIdxCh, index)
			sc.mu.Unlock()
			reply.Err = ErrTimeout
			return
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyMsgReceiver() {
	for {
		msg := <-sc.applyCh
		// apply msg to shardCtrler
		op := msg.Command.(Op)
		res := result{
			clerkId:   op.ClerkId,
			requestNo: op.RequestNo,
		}
		sc.mu.Lock()
		switch op.OpKind {
		case JOIN:
			if sc.finishMap[op.ClerkId] >= op.RequestNo {
				// request is repeated
				res.err = ErrFailed
			} else {
				sc.doJoin(op)
				sc.finishMap[op.ClerkId] = op.RequestNo
				res.err = OK
			}
		case LEAVE:
			if sc.finishMap[op.ClerkId] >= op.RequestNo {
				// request is repeated
				res.err = ErrFailed
			} else {
				sc.doLeave(op)
				sc.finishMap[op.ClerkId] = op.RequestNo
				res.err = OK
			}
		case QUERY:
			if sc.finishMap[op.ClerkId] < op.RequestNo {
				sc.finishMap[op.ClerkId] = op.RequestNo
			}
			if (op.Num > 0 && op.Num < len(sc.configs)) || sc.finishMap[op.ClerkId] == op.RequestNo {
				res.err = OK
			} else {
				res.err = ErrFailed
			}
		case MOVE:
			if sc.finishMap[op.ClerkId] >= op.RequestNo {
				// request is repeated
				res.err = ErrFailed
			} else {
				sc.doMove(op)
				sc.finishMap[op.ClerkId] = op.RequestNo
				res.err = OK
			}
		}
		// reply through commitIdxCh
		ch, ok := sc.commitIdxCh[msg.CommandIndex]
		sc.mu.Unlock()
		if ok {
			select {
			case ch <- res:
			default:
			}
		}
	}
}

func (sc *ShardCtrler) doMove(op Op) {
	newConfig := copyConfig(sc.configs[len(sc.configs)-1])
	newConfig.Num++
	if op.Shard < NShards {
		newConfig.Shards[op.Shard] = op.GID
		sc.configs = append(sc.configs, newConfig)
	}
}

func (sc *ShardCtrler) doJoin(op Op) {
	newConfig := copyConfig(sc.configs[len(sc.configs)-1])
	newConfig.Num++
	// add new servers to the new config
	for gid, servers := range op.Servers {
		newConfig.Groups[gid] = servers
		DPrintf("server %d config %d join new server, gid:%d, have %d group", sc.me, newConfig.Num, gid, len(newConfig.Groups))
	}
	// make gidShardMap
	// gid -> shards
	gidShardMap := sc.makeGidShardMap(newConfig)
	for {
		source, target := getMaxShardGid(gidShardMap), getMinShardGid(gidShardMap)
		if source != 0 && len(gidShardMap[source])-len(gidShardMap[target]) <= 1 {
			break
		}
		gidShardMap[target] = append(gidShardMap[target], gidShardMap[source][0])
		newConfig.Shards[gidShardMap[source][0]] = target
		gidShardMap[source] = gidShardMap[source][1:]
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) doLeave(op Op) {
	newConfig := copyConfig(sc.configs[len(sc.configs)-1])
	newConfig.Num++
	// make gidShardMap
	// gid -> shards
	gidShardMap := sc.makeGidShardMap(newConfig)
	DPrintf("gidShardMap %v", gidShardMap)
	orphanShards := make([]int, 0)
	for _, gid := range op.GIDs {
		orphanShards = append(orphanShards, gidShardMap[gid]...)
		DPrintf("server %d leave server, gid:%d", sc.me, gid)
		delete(newConfig.Groups, gid)
		delete(gidShardMap, gid)
	}
	// default value should be 0 in array
	var newShards [NShards]int
	if len(gidShardMap) != 0 {
		for _, shard := range orphanShards {
			target := getMinShardGid(gidShardMap)
			gidShardMap[target] = append(gidShardMap[target], shard)
		}
		// make new shard array from gidShardMap
		for gid, shards := range gidShardMap {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	DPrintf("after move gidShardMap %v", gidShardMap)
	DPrintf("newShard %v", newShards)
	newConfig.Shards = newShards
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) makeGidShardMap(config Config) map[int][]int {
	// make gidShardMap
	// gid -> shards
	gidShardMap := make(map[int][]int)
	for gid := range config.Groups {
		gidShardMap[gid] = make([]int, 0)
	}
	for shard, gid := range config.Shards {
		gidShardMap[gid] = append(gidShardMap[gid], shard)
	}
	return gidShardMap
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.commitIdxCh = make(map[int]chan result)
	sc.finishMap = make(map[int64]int64)

	go sc.applyMsgReceiver()
	return sc
}
