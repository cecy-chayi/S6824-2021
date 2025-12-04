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
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
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

type PrevAppendEntriesArgs struct {
	session_id     int32
	term           int32
	prev_log_term  int32
	prev_log_index int32
	first_entry    *LogEntry
	last_entry     *LogEntry
	leader_commit  int32
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
	current_term  int32
	vote_for      int32
	log           []LogEntry
	role          Role
	commit_index  int32
	last_applied  int32
	applyCh       chan ApplyMsg
	log_entry_len int32

	// for leader
	next_index   []int32
	match_index  []int32
	expect_index []int32
	prev_args    []PrevAppendEntriesArgs

	// follower
	is_received_heartbeat int32
	leader_id             int32
	// 用于停止当前 leader election
	stop_election_chan            chan struct{}
	stop_heartbeat_chan           chan struct{}
	stop_commit_index_update_chan chan struct{}
	stop_snapshot_chan            chan struct{}

	// snapshot
	need_snapshot       []bool
	last_included_index int32
	last_included_term  int32
	snapshot            []byte
}

type Role = int32

const (
	RoleFollower Role = iota
	RoleCandidate
	RoleLeader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Lock()
	defer rf.Unlock()
	term = int(rf.current_term)
	isleader = rf.isLeader()
	return term, isleader
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

	// currentTerm
	e.Encode(rf.current_term)
	// votedFpr
	e.Encode(rf.vote_for)
	// log
	e.Encode(rf.log)
	e.Encode(rf.log_entry_len)
	// snapshot
	e.Encode(rf.last_included_index)
	e.Encode(rf.last_included_term)
	e.Encode(rf.snapshot)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.resetVoteFor()
		rf.current_term = 0
		// raft 的 index 是 1-index
		rf.log = make([]LogEntry, 1)
		rf.log_entry_len = 0
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
	var term int32
	var vote_for int32
	var log []LogEntry
	var log_entry_len int32
	var last_included_index int32
	var last_included_term int32
	var snapshot []byte
	if e := d.Decode(&term); e != nil {
		fmt.Printf("[%d] readPersist term error: %v\n", rf.me, e)
	} else if e := d.Decode(&vote_for); e != nil {
		fmt.Printf("[%d] readPersist vote_for error: %v\n", rf.me, e)
	} else if e := d.Decode(&log); e != nil {
		fmt.Printf("[%d] readPersist log error: %v\n", rf.me, e)
	} else if e := d.Decode(&log_entry_len); e != nil {
		fmt.Printf("[%d] readPersist log_entry_len error: %v\n", rf.me, e)
	} else if e := d.Decode(&last_included_index); e != nil {
		fmt.Printf("[%d] readPersist last_included_index error: %v\n", rf.me, e)
	} else if e := d.Decode(&last_included_term); e != nil {
		fmt.Printf("[%d] readPersist last_included_term error: %v\n", rf.me, e)
	} else if e := d.Decode(&snapshot); e != nil {
		fmt.Printf("[%d] readPersist snapshot error: %v\n", rf.me, e)
	} else {
		rf.current_term = term
		rf.vote_for = vote_for
		rf.log = log
		rf.log_entry_len = log_entry_len
		rf.last_included_index = last_included_index
		rf.last_included_term = last_included_term
		rf.last_applied = last_included_index
		rf.commit_index = last_included_index
		rf.snapshot = snapshot
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.setIsReceivedHeartbeat()
	var command int
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&command); err != nil {
		fmt.Printf("CondInstallSnapshot: decode command error: %v\n", err)
	} else {
		fmt.Printf("[%d] CondInstallSnapshot: lastIncludedTerm %d, lastIncludedIndex %d, snapshot %v\n", rf.me, lastIncludedTerm, lastIncludedIndex, command)
		rf.Lock()
		defer rf.Unlock()
		rf.discard_old_log(int32(lastIncludedIndex), int32(lastIncludedTerm))
		rf.snapshot = snapshot
		rf.last_included_index = int32(lastIncludedIndex)
		rf.last_included_term = int32(lastIncludedTerm)
		if rf.getCommitIndex() < int32(lastIncludedIndex) {
			rf.setCommitIndex(int32(lastIncludedIndex))
		}
		if rf.getLastApplied() < int32(lastIncludedIndex) {
			rf.SetLastApplied(int32(lastIncludedIndex))
		}
		if rf.current_term < int32(lastIncludedTerm) {
			rf.current_term = int32(lastIncludedTerm)
		}
		rf.persist()
	}
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.Lock()
	defer rf.Unlock()
	fmt.Printf("[%d] Snapshot: index %d, term %d, snapshot %v\n", rf.me, index, rf.current_term, snapshot)
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var command int
	if d.Decode(&command) != nil {
		fmt.Printf("Snapshot: decode command error: %v\n", d)
	} else {
		// 利用 snapshot 重新构建 log
		rf.discard_old_log(int32(index), rf.current_term)
		// index 是最新的 last_applied，此处需要更新
		rf.snapshot = snapshot
		rf.last_included_index = int32(index)
		rf.last_included_term = int32(rf.current_term)
		if rf.getCommitIndex() < int32(index) {
			rf.setCommitIndex(int32(index))
		}
		fmt.Printf("[%d] after snapshot: log_len: %d, log: %v\n", rf.me, rf.log_entry_len, rf.log)
		rf.persist()

	}
}

func (rf *Raft) discard_old_log(last_included_index int32, last_included_term int32) {
	for i := rf.getLogLen(); i >= 1; i-- {
		if rf.log[i].INDEX == int32(last_included_index) &&
			rf.log[i].TERM == int32(last_included_term) {
			new_log := make([]LogEntry, 1)
			new_log = append(new_log, rf.log[i+1:]...)
			rf.log = new_log
			rf.log_entry_len -= i
			return
		}
	}
	// 这种情况在 follower 没有完成任何 log entry 的复制时，会出现
	rf.log = make([]LogEntry, 1)
	rf.log_entry_len = 0
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	TERM           int32
	CANDIDATE_ID   int32
	LAST_LOG_INDEX int32
	LAST_LOG_TERM  int32
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	TERM         int32
	VOTE_GRANTED bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	fmt.Printf("[%d] RequestVote: %v\n", rf.me, args)
	rf.Lock()
	defer rf.Unlock()
	term := rf.current_term
	reply.TERM = max(term, args.TERM)
	reply.VOTE_GRANTED = false
	if args.TERM < term {
		return
	}
	if args.TERM > term {
		// 先更新 role 到 follower 再更新 term，确保 同一个 term 不出现两个 leader 的情况
		fmt.Printf("[%d] become follower because of received higher term: %v\n", rf.me, args)
		if !rf.isFollower() {
			rf.toFollower()
		}
		rf.current_term = args.TERM
		rf.resetVoteFor()
		rf.persist()
	}
	vote_for := rf.vote_for
	if vote_for == -1 || vote_for == args.CANDIDATE_ID {
		last_log_entry := rf.getLastLogEntry()
		if args.LAST_LOG_TERM > last_log_entry.TERM ||
			(args.LAST_LOG_TERM == last_log_entry.TERM && args.LAST_LOG_INDEX >= last_log_entry.INDEX) {
			// 当认可 candidate 的 vote 时，才设置心跳
			// 否则会出现某个 server 一直拒绝 request vote 但是无法当上 leader 的情况
			if rf.isFollower() {
				// 收到 candidate 发来的 rpc，保持 follower状态
				rf.setIsReceivedHeartbeat()
			}
			rf.vote_for = args.CANDIDATE_ID
			rf.persist()
			reply.VOTE_GRANTED = true
		}
	}
	fmt.Printf("[%d] RequestVote reply: %v\n", rf.me, reply)
}

func (rf *Raft) toFollower() {
	fmt.Printf("[%d] toFollower, now is %v\n", rf.me, int2Role(rf.getRole()))
	if rf.isLeader() {
		// 无阻塞发送 stop signal
		select {
		case rf.stop_heartbeat_chan <- struct{}{}:
			// fmt.Printf("[%d] stop heartbeat signal sent successfully\n", rf.me)
		default:
		}
		select {
		case rf.stop_commit_index_update_chan <- struct{}{}:
			// fmt.Printf("[%d] stop commit index update signal sent successfully\n", rf.me)
		default:
		}
		select {
		case rf.stop_snapshot_chan <- struct{}{}:
			// fmt.Printf("[%d] stop snapshot signal sent successfully\n", rf.me)
		default:
		}
	}
	if rf.isCandidate() {
		// 无阻塞发送 stop signal
		select {
		case rf.stop_election_chan <- struct{}{}:
			// fmt.Printf("[%d] stop election signal sent successfully\n", rf.me)
		default:
		}
	}
	rf.setIsReceivedHeartbeat()
	rf.setRole(RoleFollower)
}

type LogEntry struct {
	TERM    int32
	INDEX   int32
	COMMAND interface{}
}

var session_id int32 = 0

type AppendEntriesArgs struct {
	SESSION_ID     int32
	TERM           int32
	LEADER_ID      int32
	PREV_LOG_INDEX int32
	PREV_LOG_TERM  int32
	ENTRIES        []LogEntry
	LEADER_COMMIT  int32
}

func (a AppendEntriesArgs) String() string {
	entries_string := ""
	if len(a.ENTRIES) == 0 {
		entries_string = "[]"
	} else {
		entries_string = fmt.Sprintf("%v", a.ENTRIES[len(a.ENTRIES)-1:])
	}
	return fmt.Sprintf("{session_id:%d term:%d leader_id:%d prev_log_index:%d prev_log_term:%d last_entries:%v leader_commit:%d}", a.SESSION_ID, a.TERM, a.LEADER_ID, a.PREV_LOG_INDEX, a.PREV_LOG_TERM, entries_string, a.LEADER_COMMIT)
}

type AppendEntriesReply struct {
	SESSION_ID int32
	TERM       int32
	SEVER_ID   int32
	SUCCESS    bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("[%s] [%d] AppendEntries: %v\n", time.Now().Format("15:04:05.000"), rf.me, args)
	rf.setIsReceivedHeartbeat()
	rf.Lock()
	defer rf.Unlock()
	term := rf.current_term
	reply.SESSION_ID = args.SESSION_ID
	reply.TERM = max(term, args.TERM)
	reply.SEVER_ID = int32(rf.me)
	reply.SUCCESS = false
	if args.TERM < term {
		fmt.Printf("[%d] reject AppendEntries because of lower term, reply: %v\n", rf.me, reply)
		return
	}
	if !rf.isFollower() {
		fmt.Printf("[%d] become follower because of received higher term: %v\n", rf.me, args)
		rf.toFollower()
		rf.resetVoteFor()
	}
	rf.current_term = args.TERM
	if !rf.consitencyCheck(args) {
		fmt.Printf("[%d] reject AppendEntries because of inconsitency, reply: %v\n", rf.me, reply)
		return
	}
	reply.SUCCESS = true
	rf.leader_id = args.LEADER_ID
	rf.setCommitIndex(args.LEADER_COMMIT)
	rf.appendLogEntry(args.PREV_LOG_INDEX, args.ENTRIES...)
	rf.persist()
	fmt.Printf("[%d] AppendEntries reply: %v\n", rf.me, reply)
}

type InstallSnapshotArgs struct {
	TERM                int32
	LEADER_ID           int32
	LAST_INCLUDED_INDEX int32
	LAST_INCLUDED_TERM  int32
	DATA                []byte
}

type InstallSnapshotReply struct {
	SERVER_ID int32
	TERM      int32
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.Lock()
	fmt.Printf("[%d] InstallSnapshot: %v\n", rf.me, args)
	reply.TERM = max(rf.current_term, args.TERM)
	reply.SERVER_ID = int32(rf.me)
	if rf.current_term > args.TERM {
		rf.Unlock()
		return
	}
	if rf.current_term < args.TERM {
		if !rf.isFollower() {
			rf.toFollower()
		}
		rf.current_term = args.TERM
		rf.resetVoteFor()
		rf.persist()
	}
	if rf.getLastApplied() > args.LAST_INCLUDED_INDEX {
		rf.Unlock()
		return
	}
	rf.leader_id = args.LEADER_ID
	rf.Unlock()
	apply_snap := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.DATA,
		SnapshotTerm:  int(args.LAST_INCLUDED_TERM),
		SnapshotIndex: int(args.LAST_INCLUDED_INDEX),
	}
	rf.applyCh <- apply_snap
}

func (rf *Raft) consitencyCheck(args *AppendEntriesArgs) bool {
	prev_log_entry, ok := rf.getLogEntry(args.PREV_LOG_INDEX)
	if ok && (args.PREV_LOG_TERM != prev_log_entry.TERM || args.PREV_LOG_INDEX != prev_log_entry.INDEX) {
		return false
	}
	if !ok && (rf.last_included_index != args.PREV_LOG_INDEX || rf.last_included_term != args.PREV_LOG_TERM) {
		return false
	}
	return true
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
	if !ok {
		// fmt.Printf("[%d] sendRequestVote to %d failed\n", rf.me, server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("[%d] sendAppendEntries to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	rf.Lock()
	defer rf.Unlock()
	term, isLeader := rf.current_term, rf.isLeader()
	last_log_entry_index := 0
	if rf.getLogLen() == 0 {
		last_log_entry_index = int(rf.last_included_index)
	} else {
		last_log_entry_index = int(rf.getLastLogEntry().INDEX)
	}
	if !isLeader {
		return last_log_entry_index + 1, int(term), isLeader
	}
	// fmt.Printf("[%d] Start: %v\n", rf.me, command)

	index := int32(last_log_entry_index + 1)
	rf.appendLogEntry(int32(last_log_entry_index), LogEntry{
		TERM:    int32(term),
		INDEX:   index,
		COMMAND: command,
	})
	rf.persist()
	fmt.Printf("[%d] Start: %v, index: %d\n", rf.me, command, index)
	return int(index), int(term), isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleep_time := rand.Intn(150) + 150
		time.Sleep(time.Duration(sleep_time) * time.Millisecond)
		if rf.isCandidate() {
			select {
			case rf.stop_election_chan <- struct{}{}:
			default:
			}
		}
		if rf.isLeader() {
			continue
		}

		if rf.isReceivedHeartbeat() {
			rf.resetIsReceivedHeartbeat()
		} else {
			fmt.Printf("[%d] ticker: no heartbeat received, start election\n", rf.me)
			rf.setRole(RoleCandidate)
			go rf.leaderElection()
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
	rf.applyCh = applyCh
	rf.commit_index = 0
	rf.last_applied = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.setRole(RoleFollower)
	rf.resetIsReceivedHeartbeat()
	rf.stop_election_chan = make(chan struct{}, 1)
	rf.stop_heartbeat_chan = make(chan struct{}, 1)
	rf.stop_commit_index_update_chan = make(chan struct{}, 1)
	rf.stop_snapshot_chan = make(chan struct{}, 1)
	rf.next_index = make([]int32, len(rf.peers))
	rf.match_index = make([]int32, len(rf.peers))
	rf.expect_index = make([]int32, len(rf.peers))
	rf.need_snapshot = make([]bool, len(rf.peers))
	rf.prev_args = make([]PrevAppendEntriesArgs, len(rf.peers))
	for i := range rf.peers {
		rf.next_index[i] = 1
		rf.match_index[i] = 0
		rf.need_snapshot[i] = false
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.StatePrint()
	go rf.applyLog()

	return rf
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.Lock()
		prev_log_entry := rf.getLastLogEntry()
		rf.Unlock()
		if rf.getCommitIndex() == rf.getLastApplied() ||
			prev_log_entry.INDEX == rf.getLastApplied() {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if rf.getCommitIndex() > rf.getLastApplied() &&
			prev_log_entry.INDEX > rf.getLastApplied() {
			rf.Lock()
			apply_log, ok := rf.getLogEntry(int32(rf.last_applied + 1))
			if !ok {
				rf.Unlock()
				fmt.Printf("panic [%d] apply log: log entry not found, index: %d\n", rf.me, rf.last_applied+1)
				continue
			}
			rf.Unlock()
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      apply_log.COMMAND,
				CommandIndex: int(apply_log.INDEX),
			}
			fmt.Printf("[%d] apply log: %v\n", rf.me, apply_log)
			rf.AddLastApplied(1)
		}
	}
}

func (rf *Raft) StatePrint() {
	for !rf.killed() {
		rf.Lock()
		fmt_string := ""
		if rf.isLeader() {
			fmt_string += fmt.Sprintf("[%d] role: %v, term: %d, leader_id: %d, commit_index: %d, vote_for: %d, last_applied: %d, next_index: %v, match_index: %v, last_included_index: %d, last_included_term: %d, log: %v\n",
				rf.me, int2Role(rf.getRole()), rf.current_term, rf.leader_id, rf.commit_index, rf.vote_for, rf.getLastApplied(), rf.next_index, rf.match_index, rf.last_included_index, rf.last_included_term, rf.log)
		} else {
			fmt_string += fmt.Sprintf("[%d] role: %v, term: %d, leader_id: %d, commit_index: %d, vote_for: %d, last_applied: %d, last_included_index: %d, last_included_term: %d, log: %v\n",
				rf.me, int2Role(rf.getRole()), rf.current_term, rf.leader_id, rf.commit_index, rf.vote_for, rf.getLastApplied(), rf.last_included_index, rf.last_included_term, rf.log)
		}
		fmt.Printf("%s", fmt_string)
		rf.Unlock()
		time.Sleep(150 * time.Millisecond)
	}
}

func int2Role(role int32) string {
	switch role {
	case int32(RoleFollower):
		return "RoleFollower"
	case int32(RoleCandidate):
		return "RoleCandidate"
	case int32(RoleLeader):
		return "RoleLeader"
	default:
		return "RoleFollower"
	}
}

func (rf *Raft) getRole() Role {
	return atomic.LoadInt32(&rf.role)
}

func (rf *Raft) setRole(role Role) {
	atomic.StoreInt32(&rf.role, role)
}

func (rf *Raft) isLeader() bool {
	role := rf.getRole()
	return role == RoleLeader
}

func (rf *Raft) isFollower() bool {
	role := rf.getRole()
	return role == RoleFollower
}

func (rf *Raft) isCandidate() bool {
	role := rf.getRole()
	return role == RoleCandidate
}

func (rf *Raft) resetIsReceivedHeartbeat() {
	atomic.StoreInt32(&rf.is_received_heartbeat, 0)
}

func (rf *Raft) setIsReceivedHeartbeat() {
	atomic.StoreInt32(&rf.is_received_heartbeat, 1)
}

func (rf *Raft) isReceivedHeartbeat() bool {
	z := atomic.LoadInt32(&rf.is_received_heartbeat)
	return z == 1
}

func (rf *Raft) leaderElection() {
	rf.Lock()
	// 清空 stop_election_chan
	select {
	case <-rf.stop_election_chan:
	default:
	}
	rf.current_term += 1
	term := rf.current_term
	rf.vote_for = int32(rf.me)
	rf.persist()
	fmt.Printf("[%d] start leader election, term: %d\n", rf.me, term)
	last_log_entry := rf.getLastLogEntry()
	args := &RequestVoteArgs{
		TERM:           rf.current_term,
		CANDIDATE_ID:   int32(rf.me),
		LAST_LOG_TERM:  last_log_entry.TERM,
		LAST_LOG_INDEX: last_log_entry.INDEX,
	}
	reply_chan := make(chan RequestVoteReply)
	all_servers := len(rf.peers)
	majority := (all_servers + 1) / 2

	rf.Unlock()
	var wg sync.WaitGroup
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(peer, args, reply); ok {
				reply_chan <- *reply
			}
		}(peer)
	}

	go func() {
		wg.Wait()
		close(reply_chan)
	}()

	go rf.handle_request_vote(term, majority, reply_chan)
}

func (rf *Raft) handle_request_vote(term int32, majority int, request_chan chan RequestVoteReply) {
	defer fmt.Printf("[%d] stop leader election, term: %d\n", rf.me, term)
	vote_counts := 1
	for reply := range request_chan {
		rf.Lock()
		fmt.Printf("[%d] received vote reply: %v\n", rf.me, reply)
		// 过期消息，丢弃
		if reply.TERM < rf.current_term {
			rf.Unlock()
			continue
		}
		if reply.VOTE_GRANTED {
			vote_counts += 1
		} else if reply.TERM > rf.current_term {
			rf.setRole(RoleFollower)
			rf.setIsReceivedHeartbeat()
			rf.current_term = reply.TERM
			rf.resetVoteFor()
			rf.persist()
			rf.Unlock()
			return
		}
		rf.Unlock()

		if vote_counts >= majority {
			break
		}
	}
	if vote_counts >= majority {
		fmt.Printf("server %d now is leader, term: %d\n", rf.me, term)
		rf.toLeader()
	}
}

func (rf *Raft) toLeader() {
	rf.setRole(RoleLeader)
	rf.Lock()
	rf.leader_id = int32(rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.match_index[peer] = 0
		rf.need_snapshot[peer] = false
		rf.next_index[peer] = rf.getLastLogEntry().INDEX + 1
	}
	rf.Unlock()

	go rf.sendHeartbeats()
	go rf.updateCommitIndex()
	go rf.sendSnapshotIfNeeded()
}

func (rf *Raft) updateCommitIndex() {
	fmt.Printf("[%d] start updateCommitIndex\n", rf.me)
	defer fmt.Printf("[%d] stop updateCommitIndex\n", rf.me)
	for !rf.killed() {
		time.Sleep(30 * time.Millisecond)
		select {
		case <-rf.stop_commit_index_update_chan:
			return
		default:
			rf.Lock()
			num_peers := len(rf.match_index) - 1
			match_index := make([]int32, 0, num_peers)
			for peer, index := range rf.match_index {
				if peer == rf.me {
					continue
				}
				match_index = append(match_index, index)
			}
			sort.Slice(match_index, func(i, j int) bool {
				return match_index[i] < match_index[j]
			})
			follower_match_index := int32(match_index[len(match_index)/2])
			if log_entry, ok := rf.getLogEntry(follower_match_index); ok &&
				follower_match_index > rf.getCommitIndex() &&
				log_entry.TERM == rf.current_term {
				fmt.Printf("[%d] update commit index to %d\n", rf.me, follower_match_index)
				rf.setCommitIndex(follower_match_index)
			}
			rf.Unlock()
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	// 清空 stop_heartbeat_chan
	select {
	case <-rf.stop_heartbeat_chan:
	default:
	}
	fmt.Printf("[%d] start sendHeartbeats\n", rf.me)
	defer fmt.Printf("[%d] stop sendHeartbeats\n", rf.me)
	for !rf.killed() {
		rf.Lock()
		term := rf.current_term
		rf.Unlock()
		select {
		case <-rf.stop_heartbeat_chan:
			return
		default:
			rf.Lock()
			// 如果当前 term 已经过期，那么就直接返回
			if term < rf.current_term {
				rf.Unlock()
				return
			}
			all_servers := len(rf.peers)
			args := make([]*AppendEntriesArgs, all_servers)
			// 如果需要发送快照，那么就先不发送心跳
			for peer := 0; peer < all_servers; peer++ {
				prev_log_entry, ok := rf.getLogEntry(rf.next_index[peer] - 1)
				if !ok || peer == rf.me || rf.need_snapshot[peer] {
					rf.need_snapshot[peer] = true
					args[peer] = &AppendEntriesArgs{
						TERM: 0,
					}
					continue
				}
				prev_local_index := rf.getLogEntryLocalIndex(prev_log_entry.INDEX)
				entriesCopy := make([]LogEntry, 0)
				for _, entry := range rf.log[prev_local_index+1:] {
					entriesCopy = append(entriesCopy, LogEntry{
						TERM:  entry.TERM,
						INDEX: entry.INDEX,
						// entry.COMMAND 若进行深拷贝，需要更加复杂的设计
						COMMAND: entry.COMMAND,
					})
				}
				args[peer] = &AppendEntriesArgs{
					TERM:           term,
					LEADER_ID:      int32(rf.me),
					PREV_LOG_INDEX: prev_log_entry.INDEX,
					PREV_LOG_TERM:  prev_log_entry.TERM,
					ENTRIES:        entriesCopy,
					LEADER_COMMIT:  rf.getCommitIndex(),
				}
				// 为了防止同样的请求被响应多次，我们需要记录上一次发送的请求
				// 如果上一次发送的请求和当前请求相同，那么就直接使用上一次的 session_id
				current_args := PrevAppendEntriesArgs{
					term:           term,
					prev_log_index: prev_log_entry.INDEX,
					prev_log_term:  prev_log_entry.TERM,
					leader_commit:  rf.getCommitIndex(),
				}
				if len(entriesCopy) > 0 {
					current_args.first_entry = &entriesCopy[0]
					current_args.last_entry = &entriesCopy[len(entriesCopy)-1]
				}
				if rf.is_prev_args_equal(peer, current_args) {
					args[peer].SESSION_ID = rf.prev_args[peer].session_id
				} else {
					args[peer].SESSION_ID = atomic.AddInt32(&session_id, 1)
					rf.prev_args[peer] = current_args
					rf.prev_args[peer].session_id = args[peer].SESSION_ID
				}
			}
			var wg sync.WaitGroup
			reply_ch := make(chan AppendEntriesReply, all_servers-1)
			close_reply_ch := func() {
				wg.Wait()
				close(reply_ch)
			}
			for peer := 0; peer < all_servers; peer++ {
				// 过滤自己以及需要发送快照的 server
				if peer == rf.me || rf.need_snapshot[peer] {
					continue
				}
				wg.Add(1)
				go func(peer int) {
					defer wg.Done()
					reply := &AppendEntriesReply{}
					if ok := rf.sendAppendEntries(peer, args[peer], reply); ok {
						reply_ch <- *reply
					}
				}(peer)
			}
			rf.Unlock()

			go rf.handle_heartbeats(args, reply_ch)
			go close_reply_ch()
		}
		// 60ms 后发送下一批心跳
		time.Sleep(60 * time.Millisecond)
	}
}

func (rf *Raft) handle_heartbeats(args []*AppendEntriesArgs, heartbeat_chan chan AppendEntriesReply) {
	for reply := range heartbeat_chan {
		server := reply.SEVER_ID
		// 处理消息乱序，当接收到较低的 session_id 时，忽略该消息
		rf.Lock()
		if rf.expect_index[server] > reply.SESSION_ID {
			rf.Unlock()
			continue
		} else {
			rf.expect_index[server] = reply.SESSION_ID + 1
			rf.Unlock()
		}
		rf.Lock()
		fmt.Printf("[%d] received %d heartbeat reply: %v\n", rf.me, server, reply)
		if reply.TERM > rf.current_term {
			rf.toFollower()
			rf.current_term = reply.TERM
			rf.resetVoteFor()
			rf.persist()
			rf.Unlock()
			continue
		}
		rf.Unlock()
		if reply.SUCCESS {
			if len(args[server].ENTRIES) > 0 {
				rf.Lock()
				rf.match_index[server] = args[server].ENTRIES[len(args[server].ENTRIES)-1].INDEX
				rf.next_index[server] = rf.match_index[server] + 1
				rf.Unlock()
			}
		} else {
			rf.Lock()
			var prev_log_entry *LogEntry = nil
			for rf.next_index[server] > 1 {
				log_entry, ok := rf.getLogEntry(rf.next_index[server] - 1)
				if !ok {
					if rf.next_index[server] > rf.last_included_index+1 {
						// 回退到 snapshot 的日志，如果还是匹配不成功那么需要重新推送 snapshot
						rf.next_index[server] = rf.last_included_index + 1
					}
					break
				} else if prev_log_entry == nil || log_entry.TERM == prev_log_entry.TERM {
					rf.next_index[server] -= 1
					prev_log_entry = &log_entry
				} else {
					break
				}

			}
			rf.Unlock()
		}
	}

}

func (rf *Raft) sendSnapshotIfNeeded() {
	select {
	case <-rf.stop_snapshot_chan:
	default:
	}
	fmt.Printf("[%d] start sendSnapshotIfNeeded\n", rf.me)
	defer fmt.Printf("[%d] stop sendSnapshotIfNeeded\n", rf.me)
	for !rf.killed() {
		rf.Lock()
		term := rf.current_term
		rf.Unlock()
		select {
		case <-rf.stop_snapshot_chan:
			return
		default:
			rf.Lock()
			all_server := len(rf.peers)
			reply_ch := make(chan InstallSnapshotReply, all_server-1)
			var wg sync.WaitGroup
			args := make([]*InstallSnapshotArgs, all_server)
			for peer := 0; peer < all_server; peer++ {
				if peer == rf.me {
					continue
				}
				if rf.need_snapshot[peer] {
					fmt.Printf("[%d] %d need snapshot\n", rf.me, peer)
					args[peer] = &InstallSnapshotArgs{
						TERM:                term,
						LEADER_ID:           int32(rf.me),
						LAST_INCLUDED_INDEX: rf.last_included_index,
						LAST_INCLUDED_TERM:  rf.last_included_term,
						DATA:                rf.snapshot,
					}
					wg.Add(1)
					go func(peer int) {
						defer wg.Done()
						reply := &InstallSnapshotReply{}
						rf.sendInstallSnapshot(peer, args[peer], reply)
						reply_ch <- *reply
					}(peer)
				}
			}
			rf.Unlock()

			go rf.handle_snapshot(args, reply_ch)

			go func() {
				wg.Wait()
				close(reply_ch)
			}()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) handle_snapshot(args []*InstallSnapshotArgs, reply_chan chan InstallSnapshotReply) {
	for reply := range reply_chan {
		fmt.Printf("[%d] received %d snapshot reply: %v\n", rf.me, reply.SERVER_ID, reply)
		rf.Lock()
		// 过期的回复，直接忽略
		if reply.TERM < rf.current_term {
			rf.Unlock()
			return
		}
		if reply.TERM > rf.current_term {
			rf.toFollower()
			rf.current_term = reply.TERM
			rf.resetVoteFor()
			rf.persist()
			rf.Unlock()
			return
		}
		server := reply.SERVER_ID
		rf.match_index[server] = args[server].LAST_INCLUDED_INDEX
		rf.next_index[server] = args[server].LAST_INCLUDED_INDEX + 1
		rf.need_snapshot[server] = false
		fmt.Printf("[%d] %d snapshot install success\n", rf.me, server)
		rf.Unlock()
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	fmt.Printf("[%d] sendInstallSnapshot to %d\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		fmt.Printf("[%d] sendInstallSnapshot to %d failed\n", rf.me, server)
	}
	return ok
}

func (rf *Raft) getLastLogEntry() LogEntry {
	if rf.getLogLen() == 0 {
		return LogEntry{
			INDEX: rf.last_included_index,
			TERM:  rf.last_included_term,
		}
	}
	if int(rf.getLogLen()) >= len(rf.log) {
		panic(fmt.Sprintf("[%d] log_len: %d, log_entry: %v\n", rf.me, rf.getLogLen(), rf.log))
	}
	return rf.log[int(rf.getLogLen())]
}

func (rf *Raft) getLogEntry(index int32) (LogEntry, bool) {
	if index == 0 {
		return LogEntry{}, true
	}
	len := int(rf.getLogLen())
	for i := 1; i <= len; i++ {
		if rf.log[i].INDEX == index {
			return rf.log[i], true
		}
	}
	if rf.last_included_index == index {
		return LogEntry{
			INDEX: rf.last_included_index,
			TERM:  rf.last_included_term,
		}, true
	}
	// 未找到对应 index 的 log entry，返回空 log entry
	return LogEntry{}, false
}

// local index 指使用 index 唯一标识的 log entry 存储在本地的 log 中对应的下标
func (rf *Raft) getLogEntryLocalIndex(index int32) int {
	len := int(rf.getLogLen())
	for i := 1; i <= len; i++ {
		if rf.log[i].INDEX == index {
			return i
		}
	}
	// 未找到对应 index 的 log entry，返回 0
	return 0
}

func (rf *Raft) getLogLen() int32 {
	return rf.log_entry_len
}

func (rf *Raft) appendLogEntry(prev_log_index int32, entries ...LogEntry) {
	log_len := int(rf.getLogLen())
	for i := 0; i <= log_len; i++ {
		if rf.log[i].INDEX == prev_log_index {
			rf.log = append(rf.log[:i+1], entries...)
			rf.log_entry_len = int32(i + len(entries))
			return
		}
	}
	if prev_log_index == rf.last_included_index {
		rf.log = append(rf.log[:1], entries...)
		rf.log_entry_len = int32(len(entries))
		return
	}
}

func (rf *Raft) resetVoteFor() {
	rf.vote_for = -1
}

func (rf *Raft) getCommitIndex() int32 {
	return atomic.LoadInt32(&rf.commit_index)
}

func (rf *Raft) setCommitIndex(commit_index int32) {
	atomic.StoreInt32(&rf.commit_index, commit_index)
}

func (rf *Raft) getLastApplied() int32 {
	return atomic.LoadInt32(&rf.last_applied)
}

func (rf *Raft) AddLastApplied(x int32) {
	atomic.AddInt32(&rf.last_applied, x)
}

func (rf *Raft) SetLastApplied(last_applied int32) {
	atomic.StoreInt32(&rf.last_applied, last_applied)
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) is_prev_args_equal(server int, current_args PrevAppendEntriesArgs) bool {
	prev := rf.prev_args[server]
	if prev.term != current_args.term ||
		prev.prev_log_term != current_args.prev_log_term ||
		prev.prev_log_index != current_args.prev_log_index ||
		prev.leader_commit != current_args.leader_commit {
		return false
	}
	// 比较first_entry字段
	if !entriesEqual(prev.first_entry, current_args.first_entry) {
		return false
	}

	// 比较last_entry字段
	if !entriesEqual(prev.last_entry, current_args.last_entry) {
		return false
	}
	return true
}

// 辅助函数：比较两个条目是否相等（包括nil检查）
func entriesEqual(a, b *LogEntry) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.INDEX == b.INDEX && a.TERM == b.TERM
}

func (rf *Raft) Lock() {
	// _, file, line, ok := runtime.Caller(1)
	// if ok {
	// 	fmt.Printf("[%d] Lock at %s:%d\n", rf.me, file, line)
	// }
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	// _, file, line, ok := runtime.Caller(1)
	// if ok {
	// 	fmt.Printf("[%d] Unlock at %s:%d\n", rf.me, file, line)
	// }
	rf.mu.Unlock()
}
