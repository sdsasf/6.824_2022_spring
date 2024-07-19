package raft

import "fmt"

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

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term == rf.currentTerm && (rf.state == LEADER || rf.state == CANDIDATE) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		if rf.state == LEADER {
			rf.stopHeartbeatCh <- struct{}{}
		}
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if (rf.votedFor != -1 && rf.votedFor != args.CandidateId) ||
		(rf.log[len(rf.log)-1].Term > args.LastLogTerm ||
			(rf.log[len(rf.log)-1].Term == args.LastLogTerm && len(rf.log)-1 > args.LastLogIndex)) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		rf.resetTimer(rf.electionTimer, true, 0)
		if ENABLE_VOTE_LOG {
			fmt.Printf("node %d vote for node %d in term %d\n", rf.me, args.CandidateId, rf.currentTerm)
		}
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check term
	if args.Term < rf.currentTerm {
		// outdated leader heartbeat, return high term directly
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// reset election timeout
	rf.resetTimer(rf.electionTimer, true, 0)
	if ENABLE_HEARTBEAT_LOG {
		fmt.Printf("node %d receive heartbeat from node %d in term %d\n", rf.me, args.LeaderId, rf.currentTerm)
	}

	if args.Term > rf.currentTerm {
		// outdated leader
		if rf.state == LEADER {
			// should stop heartbeat
			rf.stopHeartbeatCh <- struct{}{}
		}
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER

		if ENABLE_STATE_LOG {
			fmt.Printf("node %d become follower in term %d\n", rf.me, rf.currentTerm)
		}

		// check prevLogIndex and prevLog
		if (len(rf.log)-1 < args.PrevLogIndex) || (rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		// prevLogIndex and prevLog is right
		reply.Term = rf.currentTerm
		reply.Success = true

		// append new logs
		if len(args.Entries) > 0 {
			rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
			if ENABLE_APPEND_LOG {
				fmt.Printf("node %d append %d log at %d\n", rf.me, len(args.Entries), args.PrevLogIndex+1)
			}
		}

		// update commit index
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < len(rf.log)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = len(rf.log) - 1
			}
			if ENABLE_APPEND_LOG {
				fmt.Printf("node %d commit log at %d\n", rf.me, rf.commitIndex)
			}
			rf.applyCond.Signal()
		}
		rf.persist()
	} else {
		// heartbeat term is equal to current term
		if rf.state == CANDIDATE {
			rf.state = FOLLOWER
		} else {
			// normal heartbeat, rf.state must FOLLOWER
			// check prevLogIndex and prevLog
			if (len(rf.log)-1 < args.PrevLogIndex) ||
				(args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
				reply.Term = rf.currentTerm
				reply.Success = false
				return
			}
			reply.Term = rf.currentTerm
			reply.Success = true

			// append new logs
			if len(args.Entries) > 0 {
				rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
				if ENABLE_APPEND_LOG {
					fmt.Printf("node %d append %d log at %d\n", rf.me, len(args.Entries), args.PrevLogIndex+1)
				}
			}

			// update commit index
			if args.LeaderCommit > rf.commitIndex {
				if args.LeaderCommit < len(rf.log)-1 {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.log) - 1
				}
				if ENABLE_APPEND_LOG {
					fmt.Printf("node %d commit log at %d\n", rf.me, rf.commitIndex)
				}
				rf.applyCond.Signal()
			}
		}
		rf.persist()
	}
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
