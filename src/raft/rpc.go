package raft

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dVote, "S%d receive requestVote", rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d reject requestVote in lower term (%d > %d)", rf.me, rf.currentTerm, args.Term)
		return
	}
	if args.Term == rf.currentTerm && (rf.state == LEADER || rf.state == CANDIDATE) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d is not follower in T%d, reject requestVote", rf.me, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		if rf.state == LEADER {
			rf.stopHeartbeatCh <- struct{}{}
		}
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		Debug(dVote, "S%d receive higher term requestVote in T%d, become follower", rf.me, args.Term)
	}
	if (rf.votedFor != -1 && rf.votedFor != args.CandidateId) ||
		(rf.lastLogTerm() > args.LastLogTerm ||
			(rf.lastLogTerm() == args.LastLogTerm && rf.lastLogIndex() > args.LastLogIndex)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d reject to vote for S%d in T%d", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		rf.resetTimer(rf.electionTimer, true, 0)
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		Debug(dVote, "S%d vote for S%d in T%d", rf.me, args.CandidateId, rf.currentTerm)
	}
	rf.persist()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// for accelerated log backtracking optimization
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check term
	if args.Term < rf.currentTerm {
		// outdated leader heartbeat, return high term directly
		Debug(dLog, "S%d reject outdated appendEntries from S%d, (%d > %d)",
			rf.me, args.LeaderId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// reset election timeout
	rf.resetTimer(rf.electionTimer, true, 0)
	Debug(dLog, "S%d receive heartbeat from S%d", rf.me, args.LeaderId)

	needPersist := false
	if args.Term > rf.currentTerm {
		// outdated leader
		if rf.state == LEADER {
			// should stop heartbeat
			rf.stopHeartbeatCh <- struct{}{}
		}
		Debug(dLog, "S%d receive higher term heartbeat (%d > %d) from S%d",
			rf.me, args.Term, rf.currentTerm, args.LeaderId)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		needPersist = true
	}
	rf.state = FOLLOWER

	reply.Term = args.Term
	var prevLogIndex int
	exist := true
	if args.PrevLogIndex == rf.snapshotData.lastIncludedIndex {
		prevLogIndex = -1
	} else {
		prevLogIndex, exist = rf.convertIndex(args.PrevLogIndex)
	}
	// check prevLogIndex and prevLogTerm
	if !exist {
		// if prevLogIndex not in log
		Debug(dLog, "S%d don't have prevLogIndex %d in it's log, reject heartbeat", rf.me, args.PrevLogIndex)
		reply.Success = false
		reply.ConflictTerm = 0
		reply.ConflictIndex = rf.lastLogIndex() + 1
		if needPersist {
			rf.persist()
		}
		return
	}
	// if prevLogIndex in log or have no log in leader
	if prevLogIndex != -1 && rf.log[prevLogIndex].Term != args.PrevLogTerm {
		// if term conflict at the same index
		reply.ConflictTerm = rf.log[prevLogIndex].Term
		i := prevLogIndex
		for i > 0 && rf.log[i].Term == reply.ConflictTerm {
			i--
		}
		//reply.ConflictIndex = rf.log[i+1].Index
		reply.ConflictIndex = rf.log[i].Index
		Debug(dLog, "S%d log at prevLogIndex %d term unmatched (%d != %d), ConflictIndex %d",
			rf.me, args.PrevLogIndex, rf.log[prevLogIndex].Term, args.PrevLogTerm, reply.ConflictIndex)
		reply.Success = false
	} else {
		// prevLogIndex and prevLogTerm matching
		reply.Success = true
		// append new logs
		if len(args.Entries) > 0 {
			for i := 0; i < len(args.Entries); i++ {
				if i+1+prevLogIndex < len(rf.log) {
					if rf.log[i+1+prevLogIndex].Term == args.Entries[i].Term {
						continue
					} else {
						rf.log = rf.log[:i+1+prevLogIndex]
					}
				}
				rf.log = append(rf.log, args.Entries[i:]...)
				needPersist = true
				break
			}
			// don't truncate log directly !!!
			// rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			Debug(dLog, "S%d grant appendEntry from S%d in T%d, append %d log at %d",
				rf.me, args.LeaderId, rf.currentTerm, len(args.Entries), args.PrevLogIndex+1)
		}

		// update commit index
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit <= rf.lastLogIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.lastLogIndex()
			}
			Debug(dCommit, "S%d update commitIndex to %d in T%d", rf.me, rf.commitIndex, rf.currentTerm)
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Signal()
			}
		}
	}

	if needPersist {
		rf.persist()
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	needPersist := false
	rf.resetTimer(rf.electionTimer, true, 0)
	rf.state = FOLLOWER
	if args.Term > rf.currentTerm {
		// don't forget stop heartbeat !!!
		if rf.state == LEADER {
			rf.stopHeartbeatCh <- struct{}{}
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
		needPersist = true
	}
	reply.Term = rf.currentTerm
	if args.LastIncludedIndex <= rf.snapshotData.lastIncludedIndex || args.LastIncludedIndex <= rf.commitIndex {
		// outdated snapshot
		if needPersist {
			rf.persist()
		}
		return
	}
	// update state in Raft
	i, _ := rf.convertIndex(args.LastIncludedIndex)
	rf.snapshotData.lastIncludedIndex = args.LastIncludedIndex
	rf.snapshotData.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshotData.currentSnapshot = args.Data
	// trim log before index (include index)
	newLog := make([]Entry, 0)
	if i+1 < len(rf.log) {
		newLog = append(newLog, rf.log[i+1:]...)
	}
	rf.log = newLog
	if args.LastIncludedIndex > rf.commitIndex {
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = args.LastIncludedIndex
	}
	raftState := rf.persistRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, args.Data)
	Debug(dSnap, "S%d accept snapshot from S%d", rf.me, args.LeaderId)

	rf.snapshotApplyMsg = ApplyMsg{
		CommandValid:  false,
		Command:       nil,
		CommandIndex:  0,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCond.Signal()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
