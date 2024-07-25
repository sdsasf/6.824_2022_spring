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
)

type peerState int

const (
	FOLLOWER peerState = iota
	CANDIDATE
	LEADER
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
	Index   int
}

type RaftSnapshot struct {
	lastIncludedIndex int
	lastIncludedTerm  int
	currentSnapshot   []byte
}

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
	state            peerState
	commitIndex      int
	lastApplied      int
	votes            map[int]bool
	snapshotData     RaftSnapshot
	snapshotApplyMsg ApplyMsg

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
	raftState := rf.persistRaftState()
	rf.persister.SaveRaftState(raftState)
}

func (rf *Raft) persistRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotData.lastIncludedIndex)
	e.Encode(rf.snapshotData.lastIncludedTerm)
	return w.Bytes()
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
	var votedFor int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("read persistent state error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotData.lastIncludedIndex = lastIncludedIndex
		rf.snapshotData.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if index > rf.commitIndex || index <= rf.snapshotData.lastIncludedIndex {
		Debug(dSnap, "S%d reject snapshot at index %d", rf.me, index)
		return
	}
	// update state in Raft
	// entry must in rf.log
	i, _ := rf.convertIndex(index)
	rf.snapshotData.lastIncludedIndex = index
	rf.snapshotData.lastIncludedTerm = rf.log[i].Term
	rf.snapshotData.currentSnapshot = snapshot
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	// trim log before index (include index)
	newLog := make([]Entry, 0)
	if i+1 < len(rf.log) {
		newLog = append(newLog, rf.log[i+1:]...)
	}

	rf.log = newLog
	raftState := rf.persistRaftState()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
	Debug(dSnap, "S%d Snapshot to index %d, have %d log now", rf.me, index, len(rf.log))
}

func (rf *Raft) leaderInstallSnapshot(i int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {
		return
	}

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotData.lastIncludedIndex,
		LastIncludedTerm:  rf.snapshotData.lastIncludedTerm,
		Data:              rf.snapshotData.currentSnapshot,
	}
	reply := InstallSnapshotReply{}
	Debug(dSnap, "S%d send snapshot to S%d, nextIndex is %d lastIncludedIndex is %d",
		rf.me, i, rf.nextIndex[i], rf.snapshotData.lastIncludedIndex)
	ok := rf.sendInstallSnapshot(i, &args, &reply)
	if !ok {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.stopHeartbeatCh <- struct{}{}
		rf.votedFor = -1
		rf.persist()
		return
	}
	// Install snapshot successfully
	rf.nextIndex[i] = rf.snapshotData.lastIncludedIndex + 1
	rf.matchIndex[i] = rf.snapshotData.lastIncludedIndex
	go rf.retryHeartbeat(i)
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
		Index:   rf.lastLogIndex() + 1,
	}
	rf.log = append(rf.log, entry)
	Debug(dClient, "S%d append command %d at index %d in T%d",
		rf.me, entry.Command, entry.Index, rf.currentTerm)
	rf.persist()
	// for efficiency
	// go rf.sendHeartbeat()
	index := entry.Index
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
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
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
				rf.resetTimer(rf.electionTimer, true, 0)
				Debug(dTimer, "S%d reset election timer", rf.me)
				rf.persist()
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
	rf.matchIndex = make([]int, len(rf.peers))
	nextIndex := rf.lastLogIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = rf.snapshotData.lastIncludedIndex
	}
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
			term := rf.currentTerm
			Debug(dLeader, "S%d send heartbeat in T%d", rf.me, term)
		}
	}
}

// send AppendEntries message to all nodes
func (rf *Raft) sendHeartbeat() {
	// must read old currentTerm here
	// otherwise it will be modified by handleAppendEntriesReply
	// and send RPC with new higher term rather than original currentTerm !!!!

	//rf.mu.Lock()
	//currentTerm := rf.currentTerm
	//rf.mu.Unlock()
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
			if rf.nextIndex[i] <= rf.snapshotData.lastIncludedIndex {
				rf.mu.Unlock()
				go rf.leaderInstallSnapshot(i)
				return
			}
			entries := make([]Entry, 0)
			var prevLogIndex int
			var prevLogTerm int
			// fetch prevLogTerm
			// maybe previous log is in snapshot
			if rf.nextIndex[i]-1 == rf.snapshotData.lastIncludedIndex {
				prevLogIndex = -1
				prevLogTerm = rf.snapshotData.lastIncludedTerm
			} else {
				// previous log must in rf.log or it's index is 0
				prevLogIndex, _ = rf.convertIndex(rf.nextIndex[i] - 1)
				if prevLogIndex == -1 {
					// have no entries in log
					prevLogTerm = 0
				} else {
					prevLogTerm = rf.log[prevLogIndex].Term
				}
			}
			if rf.nextIndex[i] < rf.lastLogIndex()+1 {
				// use append deep copy, assign value directly is shallow copy
				entries = append(entries, rf.log[prevLogIndex+1:]...)
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  prevLogTerm,
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
	if rf.nextIndex[i] <= rf.snapshotData.lastIncludedIndex {
		rf.mu.Unlock()
		go rf.leaderInstallSnapshot(i)
		return
	}
	var prevLogTerm int
	var prevLogIndex int
	if rf.nextIndex[i]-1 == rf.snapshotData.lastIncludedIndex {
		prevLogIndex = -1
		prevLogTerm = rf.snapshotData.lastIncludedTerm
	} else {
		// previous log must in rf.log or it's index is 0
		prevLogIndex, _ = rf.convertIndex(rf.nextIndex[i] - 1)
		if prevLogIndex == -1 {
			// have no entries in log
			prevLogTerm = 0
		} else {
			prevLogTerm = rf.log[prevLogIndex].Term
		}
	}
	entries := make([]Entry, 0)
	entries = append(entries, rf.log[prevLogIndex+1:]...)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[i] - 1,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
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
		if rf.state == LEADER {
			rf.resetTimer(rf.electionTimer, true, 0)
			rf.state = FOLLOWER
			// stop heartbeat in this node
			rf.stopHeartbeatCh <- struct{}{}
			rf.votedFor = -1
			Debug(dLeader, "S%d receive higher term heartbeatReply (%d > %d), become follower",
				rf.me, reply.Term, args.Term)
		}
		rf.persist()
		return
	}

	if reply.Success {
		// rf.nextIndex[i] = rf.nextIndex[i] + len(args.Entries)
		rf.nextIndex[i] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[i] = rf.nextIndex[i] - 1
		Debug(dLog, "S%d receive granted appendEntryReply from S%d in T%d, nextIndex is %d",
			rf.me, i, rf.currentTerm, rf.nextIndex[i])
		// only commit entries in this term, see paper 5.4.2
		if len(args.Entries) > 0 {
			if rf.updateCommitIndex() {
				// make leader fast commit logs, 2D testSpeed need
				go rf.sendHeartbeat()
			}
		}
	} else {
		if reply.ConflictTerm == 0 {
			rf.nextIndex[i] = reply.ConflictIndex
		} else {
			j, _ := rf.convertIndex(args.PrevLogIndex)
			for j >= 0 && rf.log[j].Term == args.PrevLogTerm {
				j--
			}
			if j < 0 {
				rf.nextIndex[i] = reply.ConflictIndex
			} else {
				rf.nextIndex[i] = rf.log[j].Index + 1
			}
		}
		Debug(dLog, "S%d receive reject appendEntryReply from S%d in T%d, nextIndex is %d",
			rf.me, i, rf.currentTerm, rf.nextIndex[i])
		go rf.retryHeartbeat(i)
	}
}

func (rf *Raft) updateCommitIndex() bool {
	i := rf.commitIndex + 1
	for {
		x, exist := rf.convertIndex(i)
		if !exist {
			break
		}
		if rf.log[x].Term < rf.currentTerm {
			i++
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
			if count > len(rf.peers)/2 {
				i++
			} else {
				break
			}
		}
	}

	i--
	x, _ := rf.convertIndex(i)
	if i > rf.commitIndex && rf.log[x].Term == rf.currentTerm {
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
		if len(rf.snapshotApplyMsg.Snapshot) > 0 {
			msg := ApplyMsg{
				CommandValid:  false,
				Snapshot:      rf.snapshotApplyMsg.Snapshot,
				SnapshotValid: true,
				SnapshotIndex: rf.snapshotApplyMsg.SnapshotIndex,
				SnapshotTerm:  rf.snapshotApplyMsg.SnapshotTerm,
			}
			rf.snapshotApplyMsg = ApplyMsg{}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else if rf.lastApplied < rf.commitIndex {
			massages := make([]ApplyMsg, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				x, _ := rf.convertIndex(i)
				msg := ApplyMsg{
					CommandValid:  true,
					Command:       rf.log[x].Command,
					CommandIndex:  i,
					SnapshotValid: false,
					Snapshot:      nil,
					SnapshotTerm:  0,
					SnapshotIndex: 0,
				}
				massages = append(massages, msg)
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
			for _, msg := range massages {
				//Debug(dCommit, "S%d apply %v", rf.me, msg)
				rf.applyCh <- msg
			}
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) convertIndex(index int) (int, bool) {
	if index == 0 {
		return -1, true
	}
	if index == rf.snapshotData.lastIncludedIndex {
		return -1, true
	}
	if len(rf.log) == 0 {
		return -1, false
	}
	l, r := 0, len(rf.log)-1
	for l < r {
		mid := l + (r-l)/2
		if rf.log[mid].Index >= index {
			r = mid
		} else {
			l = mid + 1
		}
	}
	return l, rf.log[l].Index == index
}

func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.snapshotData.lastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshotData.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
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
	rf.log = []Entry{}
	rf.currentTerm = 1
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.snapshotData.lastIncludedIndex = 0
	rf.snapshotData.lastIncludedTerm = 0
	rf.snapshotApplyMsg = ApplyMsg{}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshotData.currentSnapshot = rf.persister.ReadSnapshot()

	rf.electionTimer = time.NewTimer(randomTimeout())
	rf.heartbeatTimer = time.NewTimer(HEARTBEAT_TIMEOUT)
	rf.resetTimer(rf.electionTimer, true, 0)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommand()
	return rf
}

func randomTimeout() time.Duration {
	n, _ := rand.Int(rand.Reader, big.NewInt(100))
	// rand.Seed(time.Now().UnixNano())
	// election timeout 400-500
	return time.Duration(n.Int64()) + 400
}
