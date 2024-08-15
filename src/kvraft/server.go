package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const RPC_TIMEOUT = 500

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Command   string
	ClerkId   int64
	RequestNo int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commitIdxCh map[int]chan Op
	persister   *raft.Persister
	// persistent state
	finishMap map[int64]int64
	dataBase  map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.finishMap[args.ClerkId] >= args.RequestNo {
		// command has been applied, return directly
		value, ok := kv.dataBase[args.Key]
		if !ok {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = value
		}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Op{
		Key:       args.Key,
		Value:     "",
		Command:   "Get",
		ClerkId:   args.ClerkId,
		RequestNo: args.RequestNo,
	}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// channel maybe already existed
	ch, ok := kv.commitIdxCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.commitIdxCh[index] = ch
	}
	kv.mu.Unlock()

	for {
		select {
		case op := <-ch:
			kv.mu.Lock()
			delete(kv.commitIdxCh, index)
			kv.mu.Unlock()
			if op.ClerkId == args.ClerkId && op.RequestNo == args.RequestNo {
				if len(op.Value) == 0 {
					reply.Err = ErrNoKey
				} else {
					reply.Err = OK
				}
				reply.Value = op.Value
			} else {
				reply.Err = ErrFailed
			}
			return
		case <-time.After(RPC_TIMEOUT * time.Millisecond):
			kv.mu.Lock()
			delete(kv.commitIdxCh, index)
			kv.mu.Unlock()
			reply.Err = ErrTimeout
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	// check request number first to reduce some replicated command
	if kv.finishMap[args.ClerkId] >= args.RequestNo {
		// command has been applied, return directly
		reply.Err = ErrDupCommand
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Op{
		Key:       args.Key,
		Value:     args.Value,
		Command:   args.Op,
		ClerkId:   args.ClerkId,
		RequestNo: args.RequestNo,
	}
	DPrintf("S%d start command %v\n", kv.me, command.Command)
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	// channel maybe already existed
	ch, ok := kv.commitIdxCh[index]
	if !ok {
		ch = make(chan Op, 1)
		kv.commitIdxCh[index] = ch
	}
	kv.mu.Unlock()

	for {
		select {
		case op := <-ch:
			kv.mu.Lock()
			delete(kv.commitIdxCh, index)
			kv.mu.Unlock()
			if op.ClerkId == args.ClerkId && op.RequestNo == args.RequestNo {
				reply.Err = OK
			} else {
				reply.Err = ErrFailed
			}
			return
		case <-time.After(RPC_TIMEOUT * time.Millisecond):
			kv.mu.Lock()
			DPrintf("Server %d timeout", kv.me)
			delete(kv.commitIdxCh, index)
			kv.mu.Unlock()
			reply.Err = ErrTimeout
			return
		}
	}
}

func (kv *KVServer) applyMsgReceiver() {
	for !kv.killed() {
		msg := <-kv.applyCh
		index := msg.CommandIndex
		if msg.CommandValid {
			op := msg.Command.(Op)
			DPrintf("S%d receive applyMsg %v at index %d\n", kv.me, op.Command, index)
			kv.mu.Lock()
			if op.Command == "Get" {
				if kv.finishMap[op.ClerkId] < op.RequestNo {
					kv.finishMap[op.ClerkId] = op.RequestNo
				}
				ch, ok := kv.commitIdxCh[index]
				if ok {
					// if key not exist, should return empty string
					op.Value = kv.dataBase[op.Key]
					select {
					case ch <- op:
					default:
					}
				}
			} else {
				key := op.Key
				value := op.Value
				// check request number again is necessary
				// because lock is not always hold
				if kv.finishMap[op.ClerkId] < op.RequestNo {
					// must update request number before apply to database
					// so that all peer could update its own request number together
					kv.finishMap[op.ClerkId] = op.RequestNo
					if op.Command == "Put" {
						kv.dataBase[key] = value
						DPrintf("S%d put %v at key %v, value is %v\n", kv.me, value, key, kv.dataBase[key])
					} else {
						kv.dataBase[key] = kv.dataBase[key] + value
						DPrintf("S%d append %v at key %v, value is %v\n", kv.me, value, key, kv.dataBase[key])
					}
					_, ok := kv.commitIdxCh[index]
					if ok {
						select {
						case kv.commitIdxCh[index] <- op:
						default:
						}
					}
				} else {
					// replicated command but is in raft peer's log
					op.RequestNo = 0
					_, ok := kv.commitIdxCh[index]
					if ok {
						select {
						case kv.commitIdxCh[index] <- op:
						default:
						}
					}
				}
			}
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				snapshot := kv.makeSnapshot()
				kv.mu.Unlock()
				go func(i int) {
					kv.rf.Snapshot(i, snapshot)
				}(index)
				continue
			}
			kv.mu.Unlock()
		} else {
			// receive snapshot message
			kv.mu.Lock()
			snapshot := msg.Snapshot
			kv.installSnapshot(snapshot)
			kv.mu.Unlock()
		}
	}
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.dataBase)
	e.Encode(kv.finishMap)
	return w.Bytes()
}

func (kv *KVServer) installSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	w := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(w)
	var dataBase map[string]string
	var finishMap map[int64]int64
	if d.Decode(&dataBase) != nil ||
		d.Decode(&finishMap) != nil {
		panic("Decode failed")
	} else {
		kv.dataBase = dataBase
		kv.finishMap = finishMap
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.commitIdxCh = make(map[int]chan Op)
	kv.finishMap = make(map[int64]int64)
	kv.dataBase = make(map[string]string)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.installSnapshot(kv.persister.ReadSnapshot())

	// You may need initialization code here.
	go kv.applyMsgReceiver()

	return kv
}
