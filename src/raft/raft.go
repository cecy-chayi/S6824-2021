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
	next_index  []int32
	match_index []int32

	// follower
	is_received_heartbeat int32
	leader_id             int32
	// 用于停止当前 leader election
	stop_election_chan            chan struct{}
	stop_heartbeat_chan           chan struct{}
	stop_commit_index_update_chan chan struct{}
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
	term = int(rf.getTerm())
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
	e.Encode(rf.getTerm())
	// votedFpr
	e.Encode(rf.getVoteFor())
	// log
	e.Encode(rf.log)
	e.Encode(rf.log_entry_len)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.setVoteFor(-1)
		rf.setTerm(0)
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
	if d.Decode(&term) != nil ||
		d.Decode(&vote_for) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&log_entry_len) != nil {
		fmt.Printf("readPersist error: %v\n", d)
	} else {
		rf.setTerm(term)
		rf.setVoteFor(vote_for)
		rf.log = log
		rf.log_entry_len = log_entry_len
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
	term := rf.getTerm()
	reply.TERM = max(term, args.TERM)
	reply.VOTE_GRANTED = false
	if args.TERM < term {
		return
	}
	if args.TERM > term {
		// 先更新 role 到 follower 再更新 term，确保 同一个 term 不出现两个 leader 的情况
		fmt.Printf("[%d] become follower because of received higher term: %v\n", rf.me, args)
		rf.toFollower()
		rf.setTerm(args.TERM)
		rf.setVoteFor(-1)
		rf.Lock()
		rf.persist()
		rf.Unlock()
	}
	// 这个锁是必要的，因为可能会同时收到多个 candidate 的 rpc，需要利用锁确保以下操作的原子性
	rf.Lock()
	defer rf.Unlock()
	vote_for := rf.getVoteFor()
	if vote_for == -1 || vote_for == args.CANDIDATE_ID {
		last_log_entry := rf.getLastLogEntry()
		if args.LAST_LOG_TERM > last_log_entry.TERM ||
			(args.LAST_LOG_TERM == last_log_entry.TERM && args.LAST_LOG_INDEX >= last_log_entry.INDEX) {
			rf.setVoteFor(args.CANDIDATE_ID)
			rf.persist()
			reply.VOTE_GRANTED = true
			// 当认可 candidate 的 vote 时，才设置心跳
			// 否则会出现某个 server 一直拒绝 request vote 但是无法当上 leader 的情况
			if rf.isFollower() {
				// 收到 candidate 发来的 rpc，保持 follower状态
				rf.setIsReceivedHeartbeat()
			}
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
			fmt.Printf("[%d] stop heartbeat signal sent successfully\n", rf.me)
		default:
		}
		select {
		case rf.stop_commit_index_update_chan <- struct{}{}:
			fmt.Printf("[%d] stop commit index update signal sent successfully\n", rf.me)
		default:
		}
	}
	if rf.isCandidate() {
		// 无阻塞发送 stop signal
		select {
		case rf.stop_election_chan <- struct{}{}:
			fmt.Printf("[%d] stop election signal sent successfully\n", rf.me)
		default:
		}
	}
	rf.setRole(RoleFollower)
}

type LogEntry struct {
	TERM    int32
	INDEX   int32
	COMMAND interface{}
}

type AppendEntriesArgs struct {
	TERM           int32
	LEADER_ID      int32
	PREV_LOG_INDEX int32
	PREV_LOG_TERM  int32
	ENTRIES        []LogEntry
	LEADER_COMMIT  int32
}

type AppendEntriesReply struct {
	TERM     int32
	SEVER_ID int32
	SUCCESS  bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("[%s] [%d] AppendEntries: %v\n", time.Now().Format("15:04:05.000"), rf.me, args)
	rf.setIsReceivedHeartbeat()
	rf.Lock()
	defer rf.Unlock()
	term := rf.getTerm()
	reply.TERM = max(term, args.TERM)
	reply.SEVER_ID = int32(rf.me)
	reply.SUCCESS = false
	if args.TERM < term || !rf.consitencyCheck(args) {
		fmt.Printf("[%d] reject AppendEntries because of inconsitency: %v\n", rf.me, args)
		return
	}
	reply.SUCCESS = true
	if !rf.isFollower() {
		fmt.Printf("[%d] become follower because of received higher term: %v\n", rf.me, args)
		rf.toFollower()
	}
	rf.setTerm(args.TERM)
	rf.leader_id = args.LEADER_ID
	rf.setCommitIndex(min(args.LEADER_COMMIT, int32(rf.getLogLen())))
	rf.appendLogEntry(args.PREV_LOG_INDEX, args.ENTRIES...)
	rf.persist()
	fmt.Printf("[%d] AppendEntries reply: %v\n", rf.me, reply)
}

func (rf *Raft) consitencyCheck(args *AppendEntriesArgs) bool {
	// follower 掉线，新 leader 不清楚该 follower 情况会出现
	// prev_log_index 大于 follower 日志长度的情况
	if args.PREV_LOG_INDEX > rf.getLogLen() {
		return false
	}
	prev_log_entry := rf.getLogEntry(args.PREV_LOG_INDEX)
	if args.PREV_LOG_TERM != prev_log_entry.TERM || args.PREV_LOG_INDEX != prev_log_entry.INDEX {
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
		fmt.Printf("[%d] sendRequestVote to %d failed\n", rf.me, server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		fmt.Printf("[%d] sendAppendEntries to %d failed\n", rf.me, server)
	}
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
	term, isLeader := rf.GetState()
	last_log_entry_index := int(rf.getLastLogEntry().INDEX)
	if !isLeader {
		return last_log_entry_index + 1, term, isLeader
	}
	fmt.Printf("[%d] Start: %v\n", rf.me, command)

	index := int32(last_log_entry_index + 1)
	rf.appendLogEntry(int32(last_log_entry_index), LogEntry{
		TERM:    int32(term),
		INDEX:   index,
		COMMAND: command,
	})
	rf.persist()
	fmt.Printf("[%d] Start: %v, index: %d\n", rf.me, command, index)
	return int(index), term, isLeader
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
	rf.next_index = make([]int32, len(rf.peers))
	rf.match_index = make([]int32, len(rf.peers))
	for i := range rf.peers {
		rf.next_index[i] = 1
		rf.match_index[i] = 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.StatePrint()
	go rf.applyLog()

	return rf
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		if rf.getCommitIndex() > rf.getLastApplied() {
			rf.Lock()
			apply_log := rf.getLogEntry(int32(rf.last_applied + 1))
			fmt.Printf("[%d] apply log: %v\n", rf.me, apply_log)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      apply_log.COMMAND,
				CommandIndex: int(apply_log.INDEX),
			}
			rf.AddLastApplied(1)
			rf.Unlock()
		}
	}
}

func (rf *Raft) StatePrint() {
	for !rf.killed() {
		rf.Lock()
		if rf.isLeader() {
			fmt.Printf("[%d] role: %v, term: %d, leader_id: %d, commit_index: %d, vote_for: %d, next_index: %v, match_index: %v, log: %v\n",
				rf.me, int2Role(rf.getRole()), rf.getTerm(), rf.leader_id, rf.commit_index, rf.getVoteFor(), rf.next_index, rf.match_index, rf.log)
		} else {
			fmt.Printf("[%d] role: %v, term: %d, leader_id: %d, commit_index: %d, vote_for: %d, log: %v\n",
				rf.me, int2Role(rf.getRole()), rf.getTerm(), rf.leader_id, rf.commit_index, rf.getVoteFor(), rf.log)
		}
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
	fmt.Printf("[%d] start leader election\n", rf.me)
	defer fmt.Printf("[%d] stop leader election\n", rf.me)
	rf.Lock()
	// 清空 stop_election_chan
	select {
	case <-rf.stop_election_chan:
	default:
	}
	term := rf.AddTerm(1)
	rf.setVoteFor(int32(rf.me))
	rf.persist()
	last_log_entry := rf.getLastLogEntry()
	args := &RequestVoteArgs{
		TERM:           term,
		CANDIDATE_ID:   int32(rf.me),
		LAST_LOG_TERM:  last_log_entry.TERM,
		LAST_LOG_INDEX: last_log_entry.INDEX,
	}
	reply_chan := make(chan RequestVoteReply)
	all_servers := len(rf.peers)
	majority := (all_servers + 1) / 2
	vote_counts := 1

	rf.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(peer, args, reply)
			reply_chan <- *reply
		}(peer)
	}

	for !rf.killed() && vote_counts < majority {
		select {
		case <-rf.stop_election_chan:
			return
		case reply := <-reply_chan:
			if reply.VOTE_GRANTED {
				vote_counts += 1
			} else if reply.TERM > term {
				rf.Lock()
				rf.setRole(RoleFollower)
				rf.setIsReceivedHeartbeat()
				rf.setTerm(reply.TERM)
				rf.persist()
				rf.Unlock()
				return
			}
		}
	}
	fmt.Printf("server %d now is leader\n", rf.me)
	rf.toLeader()
}

func (rf *Raft) toLeader() {
	rf.setRole(RoleLeader)
	rf.Lock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.match_index[peer] = 0
		rf.next_index[peer] = rf.getLastLogEntry().INDEX + 1
	}
	rf.Unlock()

	go rf.sendHeartbeats()
	go rf.updateCommitIndex()
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
			if follower_match_index > rf.getCommitIndex() &&
				rf.getLogEntry(follower_match_index).TERM == rf.getTerm() {
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
		select {
		case <-rf.stop_heartbeat_chan:
			return
		default:
			rf.Lock()
			all_servers := len(rf.peers)
			args := make([]*AppendEntriesArgs, all_servers)
			for peer := 0; peer < all_servers; peer++ {
				prev_log_entry := rf.getLogEntry(rf.next_index[peer] - 1)
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
					TERM:           rf.getTerm(),
					LEADER_ID:      int32(rf.me),
					PREV_LOG_INDEX: prev_log_entry.INDEX,
					PREV_LOG_TERM:  prev_log_entry.TERM,
					ENTRIES:        entriesCopy,
					LEADER_COMMIT:  rf.getCommitIndex(),
				}
			}
			rf.Unlock()

			var wg sync.WaitGroup
			reply_ch := make(chan AppendEntriesReply, all_servers-1)
			close_reply_ch := func() {
				wg.Wait()
				close(reply_ch)
			}

			// 50ms 后停止接收 reply
			stop_sign := int32(0)
			go func() {
				time.Sleep(50 * time.Millisecond)
				atomic.StoreInt32(&stop_sign, 1)
			}()
			for peer := 0; peer < all_servers; peer++ {
				if peer == rf.me {
					continue
				}
				wg.Add(1)
				go func(peer int) {
					defer wg.Done()
					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(peer, args[peer], reply)
					reply_ch <- *reply
				}(peer)
			}

			reply_counts := 0
			for atomic.LoadInt32(&stop_sign) == 0 && reply_counts < all_servers-1 {
				time.Sleep(10 * time.Millisecond)
				select {
				case reply := <-reply_ch:
					reply_counts += 1
					if reply.TERM > rf.getTerm() {
						// 先更新 role 到 follower 再更新 term，确保 同一个 term 不出现两个 leader 的情况
						// persist 需要 Lock
						rf.Lock()
						rf.toFollower()
						rf.setTerm(reply.TERM)
						rf.persist()
						rf.Unlock()
						go close_reply_ch()
						return
					}
					server := reply.SEVER_ID
					if reply.SUCCESS {
						if len(args[server].ENTRIES) > 0 {
							rf.Lock()
							rf.match_index[server] = args[server].ENTRIES[len(args[server].ENTRIES)-1].INDEX
							rf.next_index[server] = rf.match_index[server] + 1
							rf.Unlock()
						}
					} else {
						rf.Lock()
						for rf.next_index[server] > 1 && rf.getLogEntry(rf.next_index[server]-1).TERM != reply.TERM {
							rf.next_index[server] -= 1
						}
						rf.Unlock()
					}
				case <-rf.stop_heartbeat_chan:
					go close_reply_ch()
					return
				default:
				}
			}
			go close_reply_ch()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) setTerm(term int32) {
	atomic.StoreInt32(&rf.current_term, term)
}

func (rf *Raft) getTerm() int32 {
	return atomic.LoadInt32(&rf.current_term)
}

func (rf *Raft) getLastLogEntry() LogEntry {
	return rf.log[int(rf.getLogLen())]
}

func (rf *Raft) getLogEntry(index int32) LogEntry {
	len := int(rf.getLogLen())
	for i := 1; i <= len; i++ {
		if rf.log[i].INDEX == index {
			return rf.log[i]
		}
	}
	// 未找到对应 index 的 log entry，返回空 log entry
	return LogEntry{}
}

// local index 指使用 index 唯一标识的 log entry 存储在本地的 log 中对应的下标
func (rf *Raft) getLogEntryLocalIndex(index int32) int {
	len := int(rf.getLogLen())
	for i := 1; i <= len; i++ {
		if rf.log[i].INDEX == index {
			return i
		}
	}
	// 未找到对应 index 的 log entry，返回 -1
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
}

func (rf *Raft) AddTerm(v int32) int32 {
	return atomic.AddInt32(&rf.current_term, v)
}

func (rf *Raft) getVoteFor() int32 {
	return atomic.LoadInt32(&rf.vote_for)
}

func (rf *Raft) setVoteFor(vote_for int32) {
	atomic.StoreInt32(&rf.vote_for, vote_for)
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
