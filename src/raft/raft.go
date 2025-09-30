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

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	term int32
	role Role

	// follower
	vote_for              int32
	last_log_index        int32
	last_log_term         int32
	is_received_heartbeat int32
	commit_index          int32
	leader_id             int32
	// 用于停止当前 leader election
	stop_election_chan  chan struct{}
	stop_heartbeat_chan chan struct{}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isFollower() {
		// 收到 candidate 发来的 rpc，保持 follower状态
		rf.setIsReceivedHeartbeat()
	}
	term := rf.getTerm()
	reply.TERM = max(term, args.TERM)
	reply.VOTE_GRANTED = false
	if args.TERM > term {
		rf.setTerm(args.TERM)
		rf.vote_for = args.CANDIDATE_ID
		rf.setRole(RoleFollower)
		reply.VOTE_GRANTED = true
	} else if args.TERM == term && (rf.vote_for == -1 || rf.vote_for == args.CANDIDATE_ID) {
		if args.LAST_LOG_TERM > rf.last_log_term || (args.LAST_LOG_TERM == rf.last_log_term && args.LAST_LOG_INDEX >= rf.last_log_index) {
			reply.VOTE_GRANTED = true
		}
	}
	fmt.Printf("[%d] RequestVote reply: %v\n", rf.me, reply)
}

type LogEntry struct {
	TERM  int32
	INDEX int32
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
	TERM    int32
	SUCCESS bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.setIsReceivedHeartbeat()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.getTerm()
	reply.TERM = max(term, args.TERM)
	reply.SUCCESS = false
	if args.TERM > term {
		rf.setTerm(args.TERM)
		rf.setRole(RoleFollower)
		rf.leader_id = args.LEADER_ID
		reply.SUCCESS = true
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

	// Your initialization code here (2A, 2B, 2C).
	rf.setRole(RoleFollower)
	rf.vote_for = -1
	rf.setTerm(0)
	rf.last_log_index = 0
	rf.last_log_term = 0
	rf.resetIsReceivedHeartbeat()
	rf.stop_election_chan = make(chan struct{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
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
	rf.mu.Lock()
	// 清空 stop_election_chan
	select {
	case <-rf.stop_election_chan:
	default:
	}
	term := rf.AddTerm(1)
	rf.vote_for = int32(rf.me)
	args := &RequestVoteArgs{
		TERM:           term,
		CANDIDATE_ID:   int32(rf.me),
		LAST_LOG_TERM:  rf.last_log_term,
		LAST_LOG_INDEX: rf.last_log_index,
	}
	reply_chan := make(chan RequestVoteReply)
	all_servers := len(rf.peers)
	majority := (all_servers + 1) / 2
	vote_counts := 1

	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		reply := &RequestVoteReply{}
		go func(peer int) {
			rf.sendRequestVote(peer, args, reply)
			reply_chan <- *reply
		}(peer)
	}

	for vote_counts < majority {
		select {
		case <-rf.stop_election_chan:
			return
		case reply := <-reply_chan:
			if reply.VOTE_GRANTED {
				vote_counts += 1
			} else if reply.TERM > term {
				rf.setRole(RoleFollower)
				rf.setIsReceivedHeartbeat()
				rf.setTerm(reply.TERM)
				return
			}
		}
	}
	rf.setRole(RoleLeader)
	fmt.Printf("server %d now is leader\n", rf.me)
	go rf.sendHeartbeats()
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		select {
		case <-rf.stop_election_chan:
			return
		default:
			rf.mu.Lock()
			all_servers := len(rf.peers)
			args := &AppendEntriesArgs{
				TERM:           rf.getTerm(),
				LEADER_ID:      int32(rf.me),
				PREV_LOG_INDEX: rf.last_log_index,
				PREV_LOG_TERM:  rf.last_log_term,
				ENTRIES:        []LogEntry{},
				LEADER_COMMIT:  rf.commit_index,
			}
			rf.mu.Unlock()

			replyCh := make(chan AppendEntriesReply)
			for peer := 0; peer < all_servers; peer++ {
				if peer == rf.me {
					continue
				}
				reply := &AppendEntriesReply{}
				go func(peer int) {
					rf.sendAppendEntries(peer, args, reply)
					replyCh <- *reply
				}(peer)
			}

			reply_counts := 0
			for reply_counts < all_servers-1 {
				select {
				case reply := <-replyCh:
					reply_counts += 1
					if reply.TERM > rf.getTerm() {
						rf.setRole(RoleFollower)
						rf.setIsReceivedHeartbeat()
						rf.setTerm(reply.TERM)
						return
					}
				case <-rf.stop_heartbeat_chan:
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) setTerm(term int32) {
	atomic.StoreInt32(&rf.term, term)
}

func (rf *Raft) getTerm() int32 {
	return atomic.LoadInt32(&rf.term)
}

func (rf *Raft) AddTerm(v int32) int32 {
	return atomic.AddInt32(&rf.term, v)
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}
