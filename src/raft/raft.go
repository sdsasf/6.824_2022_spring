package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"crypto/rand"
	"math/big"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	HEARTBEAT_TIMEOUT = 60

	ENABLE_HEARTBEAT_LOG = false
	ENABLE_TIMER_LOG     = false
	ENABLE_VOTE_LOG      = false
	ENABLE_APPEND_LOG    = false
	ENABLE_STATE_LOG     = false
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Command interface{}
}

type peerState int

const (
	FOLLOWER peerState = iota
	CANDIDATE
	LEADER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCond *sync.Cond
	applyCh   chan ApplyMsg
	killCh    chan struct{}

	stopElectionCh chan struct{}
	electionTimer  *time.Timer
	// channel that stop heartbeat
	stopHeartbeatCh chan struct{}
	heartbeatTimer  *time.Timer

	// state on all servers
	// persistent state
	currentTerm int
	votedFor    int
	log         []Entry
	// volatile state
	state       peerState
	commitIndex int
	lastApplied int
	votes       map[int]bool

	// leader state
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.votedFor)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var log []Entry
	var votedFor int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&votedFor) != nil {
		panic("read persistent state error")
	} else {
		rf.currentTerm = currentTerm
		rf.log = log
		rf.votedFor = votedFor
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		return -1, -1, false
	}
	entry := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	Debug(dClient, "S%d append command %d at index %d in T%d",
		rf.me, entry.Command, len(rf.log)-1, rf.currentTerm)
	go rf.sendHeartbeat()
	rf.persist()
	index := len(rf.log) - 1
	term := rf.currentTerm
	isLeader := (rf.state == LEADER)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// for rf.killed() == false {

	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().
	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.electionTimer.C:
			// election timeout
			rf.electLeader()
		}
	}
}

func (rf *Raft) electLeader() {
	rf.mu.Lock()
	rf.resetTimer(rf.electionTimer, true, 0)
	if rf.state == LEADER {
		rf.mu.Unlock()
		return
	}
	rf.becomeCandidate()
	Debug(dVote, "S%d become candidate in T%d", rf.me, rf.currentTerm)
	rf.persist()
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	currentTerm := rf.currentTerm
	rf.votes = make(map[int]bool)
	rf.votes[rf.me] = true
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm < reply.Term {
				Debug(dVote,
					"S%d Term is lower than S%d, Candidate change to Follower (%d < %d)",
					rf.me, i, args.Term, rf.currentTerm)
				rf.state = FOLLOWER
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.persist()
				rf.resetTimer(rf.electionTimer, true, 0)
				Debug(dTimer, "S%d reset election timer", rf.me)
				return
			}
			if rf.state != CANDIDATE || args.Term != rf.currentTerm {
				// if this requestVote message is delay while next requestVote is begun
				return
			}
			if reply.VoteGranted {
				Debug(dVote, "S%d receive granted vote from S%d in T%d", rf.me, i, rf.currentTerm)
				_, exist := rf.votes[i]
				oldLen := len(rf.votes)
				if !exist {
					rf.votes[i] = true
				}
				if (oldLen <= len(rf.peers)/2) && (len(rf.votes) > len(rf.peers)/2) {
					rf.becomeLeader()
					Debug(dLeader,
						"S%d receive %d votes in T%d, become leader", rf.me, len(rf.votes), rf.currentTerm)
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = LEADER
	rf.nextIndex = make([]int, len(rf.peers))
	nextIndex := len(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIndex
	}

	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = nextIndex - 1

	go rf.heartbeatTicker()
}

func (rf *Raft) becomeCandidate() {
	rf.state = CANDIDATE

	rf.votedFor = rf.me
	rf.currentTerm++
}

func (rf *Raft) heartbeatTicker() {
	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.stopHeartbeatCh:
			return
		case <-rf.heartbeatTimer.C:
			rf.resetTimer(rf.heartbeatTimer, false, HEARTBEAT_TIMEOUT)
			go rf.sendHeartbeat()
			// term := rf.currentTerm
			// Debug(dLeader, "S%d send heartbeat in T%d", rf.me, term)
		}
	}
}

// send AppendEntries message to all nodes
func (rf *Raft) sendHeartbeat() {
	// must read old currentTerm here
	// otherwise it will be modified by handleAppendEntriesReply
	// and send RPC with new higher term rather than original currentTerm !!!!
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			entries := make([]Entry, 0)
			if rf.nextIndex[i] < len(rf.log) {
				entries = append(entries, rf.log[rf.nextIndex[i]:]...)
			}
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(i, &args, &reply)
			// Debug(dLeader, "S%d send heartbeat to S%d in T%d", rf.me, i, rf.currentTerm)
			if !ok {
				return
			}
			rf.handleAppendEntriesReply(i, &args, &reply)
		}(i)
	}
}

func (rf *Raft) retryHeartbeat(i int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
		Entries:      rf.log[rf.nextIndex[i]:],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{Term: 0}
	ok := rf.sendAppendEntries(i, &args, &reply)
	Debug(dLeader, "S%d retry heartbeat to S%d in T%d", rf.me, i, rf.currentTerm)
	if !ok {
		return
	}
	rf.handleAppendEntriesReply(i, &args, &reply)
}

func (rf *Raft) handleAppendEntriesReply(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term == 0 || rf.state != LEADER {
		return
	}
	if reply.Term > args.Term {
		rf.currentTerm = reply.Term
		rf.persist()
		if rf.state == LEADER {
			rf.resetTimer(rf.electionTimer, true, 0)
			rf.state = FOLLOWER
			// stop heartbeat in this node
			rf.stopHeartbeatCh <- struct{}{}
			rf.votedFor = -1
			rf.persist()
			Debug(dLeader, "S%d receive higher term requestVoteReply (%d > %d), become follower",
				rf.me, reply.Term, args.Term)
		}
		return
	}

	if reply.Success {
		// rf.nextIndex[i] = rf.nextIndex[i] + len(args.Entries)
		rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[i] = rf.nextIndex[i] - 1
		Debug(dLog, "S%d receive granted appendEntryReply from S%d in T%d, nextIndex is %d",
			rf.me, i, rf.currentTerm, rf.nextIndex[i])
		// only commit entries in this term, see paper 5.4.2
		if rf.updateCommitIndex() {
			// for efficiency
			go rf.sendHeartbeat()
		}
	} else {
		if reply.ConflictTerm == 0 {
			rf.nextIndex[i] = reply.ConflictIndex
		} else {
			j := args.PrevLogIndex
			for j > 0 && rf.log[j].Term == args.PrevLogTerm {
				j--
			}
			if j == 0 {
				rf.nextIndex[i] = reply.ConflictIndex
			} else {
				rf.nextIndex[i] = j + 1
			}
		}
		Debug(dLog, "S%d receive reject appendEntryReply from S%d in T%d, nextIndex is %d",
			rf.me, i, rf.currentTerm, rf.nextIndex[i])
		go rf.retryHeartbeat(i)
	}
}

func (rf *Raft) updateCommitIndex() bool {
	i := rf.commitIndex + 1
	for ; i < len(rf.log); i++ {
		if rf.log[i].Term < rf.currentTerm {
			continue
		} else {
			count := 1
			for j, matchedIndex := range rf.matchIndex {
				if j == rf.me {
					continue
				}
				if matchedIndex >= i {
					count++
				}
			}
			// log is replicated by more than half peers
			if count <= len(rf.peers)/2 {
				break
			}
		}
	}
	i--
	if i > rf.commitIndex && rf.log[i].Term == rf.currentTerm {
		rf.commitIndex = i
		Debug(dCommit, "S%d commit log at index %d in T%d", rf.me, rf.commitIndex, rf.currentTerm)
		rf.applyCond.Signal()
		return true
	}
	return false
}

func (rf *Raft) resetTimer(timer *time.Timer, needRandom bool, duration time.Duration) {
	if !timer.Stop() {
		select {
		//try to drain from the channel
		case <-timer.C:
		default:
		}
	}
	if needRandom {
		randomTimeout := randomTimeout()
		timer.Reset(randomTimeout * time.Millisecond)
		Debug(dTimer, "S%d reset election timeout %d", rf.me, randomTimeout)
	} else {
		timer.Reset(duration * time.Millisecond)
	}
}

// a background goroutine that apply command to state
// wait for applyCond condition variable
func (rf *Raft) applyCommand() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.killed() == false {
		if rf.lastApplied < rf.commitIndex {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msg := ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[i].Command,
					CommandIndex:  i,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				rf.applyCh <- msg
			}
			rf.lastApplied = rf.commitIndex
		} else {
			rf.applyCond.Wait()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.mu = sync.Mutex{}

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.killCh = make(chan struct{})
	rf.stopHeartbeatCh = make(chan struct{})
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = []Entry{{Term: 0, Command: 0}}
	rf.currentTerm = 1
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(randomTimeout())
	rf.heartbeatTimer = time.NewTimer(HEARTBEAT_TIMEOUT)
	rf.resetTimer(rf.electionTimer, true, 0)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommand()
	return rf
}

func randomTimeout() time.Duration {
	n, _ := rand.Int(rand.Reader, big.NewInt(130))
	// rand.Seed(time.Now().UnixNano())
	// election timeout 350-480
	return time.Duration(n.Int64()) + 350
}
