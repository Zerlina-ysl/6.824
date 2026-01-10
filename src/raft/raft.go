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
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Status int

const (
	Follower Status = iota + 1
	Candidate
	Leader

	OLD_TERM_MSG      = "old term"
	INCONSISTENCE_MSG = "log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm"

	NOT_UP_TO_DATE_MSG = "log not up to date"
)

var ()

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
	term                 int32
	status               Status
	voteFor              int // vote for who,ensure each server will vote for at most one candidate in a given term
	resetElectionTimerCh chan bool

	commitIndex int32   // index of highest log entry known to be committed，成功同步至follower后更新
	logEntries  []Entry // 下标为idx，logEntries[i]为第i个log entry
	lastApplied int     // index of highest log entry applied to state machine

	nextIndex  []int // index of the next log entry to send to that server
	matchIndex []int // index of highest log entry known to be replicated on server
}

type Entry struct {
	Term    int32
	Command interface{}
	Index   int // identify position in log
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return int(rf.term), rf.status == Leader
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
	Term         int32 // candidate's term
	CandidateId  int
	LastLogIndex int   // index of candidate's last log entry
	LastLogTerm  int32 // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32 // currentTerm, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
	Msg         string
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dInfo, "S%v recv requestVote,req:%v", rf.me, marshal(args))

	reply.Term = rf.term

	// 请求方term过时
	if args.Term < rf.term {
		reply.VoteGranted = false
		reply.Msg = OLD_TERM_MSG
		PrettyDebug(dInfo, "S%v->%v reject vote because term %d-%d", rf.me, args.CandidateId, args.Term, rf.term)
		return
	}

	// 给定term下至多只能投一次
	if args.Term > rf.term {
		rf.voteFor = -1
	}

	// 已投票
	if rf.voteFor != -1 {
		reply.VoteGranted = false
		reply.Msg = fmt.Sprintf("voted for %d", rf.voteFor)
		PrettyDebug(dInfo, "S%v->%v reject vote because term %d votefor %d,resp:%v", rf.me, args.CandidateId, rf.term, rf.voteFor, marshal(reply))
		return
	}

	// candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if args.LastLogTerm == rf.logEntries[len(rf.logEntries)-1].Term { // Term相同
		if args.LastLogIndex < len(rf.logEntries)-1 {
			reply.VoteGranted = false
			reply.Msg = NOT_UP_TO_DATE_MSG
			PrettyDebug(dInfo, "S%v->%v reject vote, current.lastLogIdx:%d, req.lastLogIdx:%d",
				rf.me, args.CandidateId, len(rf.logEntries)-1, args.LastLogIndex)
			return
		}
	} else { // Term不同
		if args.LastLogTerm < rf.logEntries[len(rf.logEntries)-1].Term {
			reply.VoteGranted = false
			reply.Msg = NOT_UP_TO_DATE_MSG
			PrettyDebug(dInfo, "S%v->%v reject vote,currentTerm:%d,reqTerm:%d,reply:%v", rf.me, args.CandidateId, rf.term, args.Term, marshal(reply))
			return
		}

	}

	rf.resetElectionTimer()
	rf.voteFor = args.CandidateId
	rf.status = Follower
	reply.VoteGranted = true
	rf.term = args.Term
	PrettyDebug(dInfo, "S%v vote for %d,reply:%v", rf.me, args.CandidateId, marshal(reply))
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
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

type AppendEntriesArgs struct {
	Term         int32 // 领导者任期
	LeaderId     int
	PrevLogIndex int     // leader希望follower追加日志的索引的前一个位置 index of log entry immediately preceding new ones
	PrevLogTerm  int32   // leader希望follower追加日志时的前一个位置的任期
	Entries      []Entry // 并非leader的全量Entry，而是需同步给follower的Entry
	LeaderCommit int     // leader的commit索引
}

type AppendEntriesReply struct {
	Term       int32
	Success    bool
	Msg        string
	CurrentIdx int // 渐进回退较慢，当idx冲突时，follower直接返回冲突的索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(args.Entries) > 0 {
		PrettyDebug(dInfo, "S%v->%v recv AppendEntries,len:%v, lastCmd:%v", rf.me, args.LeaderId, len(args.Entries), args.Entries[0].Command)
	}
	reply.Term = rf.term
	if args.Term < rf.term {
		PrettyDebug(dInfo, "S%v->%v reject AppendEntries because req.term[%d] less than rf.term[%d]", rf.me, args.Term, rf.term)
		reply.Success = false
		reply.Msg = OLD_TERM_MSG
		return
	}

	rf.resetElectionTimer()

	if args.PrevLogIndex == -1 {
		reply.Success = true
		rf.commitIndex = int32(min(args.LeaderCommit, len(rf.logEntries)-1))

		PrettyDebug(dInfo, "S%v AppendEntries just for heatBeat, commitIdx:%v", rf.me, rf.commitIndex)
		return
	}
	// 	2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 要保证追加时索引和任期一致，否则leader需要decrease，直至找到match point
	PrettyDebug(dInfo, "S%v args.preLogIdx:%d, args.prevLogItem:%d", rf.me, args.PrevLogIndex, args.PrevLogTerm)
	PrettyDebug(dInfo, "S%v len(log):%v, lastCmd:%v", rf.me, len(rf.logEntries), rf.logEntries[len(rf.logEntries)-1].Command)

	if args.PrevLogIndex >= len(rf.logEntries) ||
		rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm { // inconsistency
		PrettyDebug(dInfo, "S%v->%v reject AppendEntries for inconsistency prevLogIdx",
			rf.me, args.LeaderId)
		reply.Success = false
		reply.Msg = INCONSISTENCE_MSG
		// 找到冲突的索引
		if args.PrevLogIndex >= len(rf.logEntries) {
			reply.CurrentIdx = len(rf.logEntries)
		} else {
			reply.CurrentIdx = args.PrevLogIndex
		}
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	rf.logEntries = rf.logEntries[:args.PrevLogIndex+1]

	// 4. Append any new entries not already in the log
	rf.logEntries = append(rf.logEntries, args.Entries...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rf.commitIndex = int32(min(args.LeaderCommit, len(rf.logEntries)-1))

	rf.voteFor = -1
	rf.status = Follower
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	rf.term = args.Term
	reply.Term = rf.term
	reply.Success = true
	PrettyDebug(dInfo, "S%v commitIdx:%v, current log:%v", rf.me, rf.commitIndex, marshal(rf.logEntries[1:]))
	PrettyDebug(dInfo, "S%v->%v admit leader state in term[%d]", rf.me, args.LeaderId, rf.term)

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
// if it's ever committed.
//the second return value is the current term.
//the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	// send and receive new log entries via AppendEntries RPCs
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader {
		return -1, -1, false
	}

	// 更新command
	entry := Entry{
		Command: command,
		Index:   len(rf.logEntries),
		Term:    rf.term,
	}
	rf.logEntries = append(rf.logEntries, entry)
	// 只追加日志，不负责同步
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logEntries) - 1
	}
	PrettyDebug(dInfo, "S%v receive new log, cmd:%v in term[%d]", rf.me, command, rf.term)

	return len(rf.logEntries) - 1, int(rf.term), true
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		switch rf.getCurrentStatus() {
		case Follower:
			PrettyDebug(dTimer, "S%v is follower", rf.me)
			// 超时发起选举 150-300ms
			randomElectionTimeout := time.Duration(rand.Intn(300)+150) * time.Millisecond
			timer := time.NewTimer(randomElectionTimeout)
			select {
			case <-timer.C:
				if rf.killed() {
					timer.Stop()
					return
				}
				rf.mu.Lock()
				PrettyDebug(dInfo, "S%v no heartbeat longtime, change to candidate", rf.me)
				rf.status = Candidate
				rf.voteFor = -1
				rf.mu.Unlock()
			case <-rf.resetElectionTimerCh:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}

			}
		case Candidate:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			voteFor := rf.voteFor
			PrettyDebug(dInfo, "S%v is candidate, voteFor:%v", rf.me, voteFor)
			rf.mu.Unlock()
			// 在一个 Term 内最多只能投一票
			if voteFor == -1 {
				rf.startElection()
			}
			// 发起选举后等待一个选举周期
			time.Sleep(time.Duration(rand.Intn(300)+150) * time.Millisecond)
			if rf.getCurrentStatus() == Candidate {
				PrettyDebug(dInfo, "S%v start election timeout", rf.me)
				rf.voteFor = -1
			}
		case Leader:
			PrettyDebug(dTimer, "S%v is Leader", rf.me)

			for rf.getCurrentStatus() == Leader && !rf.killed() {
				time.Sleep(100 * time.Millisecond)
			}

		}

	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.term = rf.term + 1
	rf.voteFor = rf.me // 投自己一票
	PrettyDebug(dTimer, "S%v entering election with term=%d", rf.me, rf.term)
	rf.mu.Unlock()

	var receiveVotes = int32(1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			if server == rf.me {
				return
			}
			req := &RequestVoteArgs{
				Term:         rf.getCurrentTerm(),
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logEntries) - 1,
				LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
			}
			reply := &RequestVoteReply{}
			PrettyDebug(dTimer, "S%v->%d sendRequestVote", rf.me, server)

			for j := 0; j < 3; j++ {
				if rf.getCurrentStatus() != Candidate {
					PrettyDebug(dInfo, "S%v is not candidate, exit", rf.me)
					return
				}
				succ := rf.sendRequestVote(server,
					req,
					reply)

				rf.mu.Lock()
				if !succ {
					PrettyDebug(dWarn, "S%v->%d sendRequestVote failed, retry %d times", rf.me, server, j)
					rf.mu.Unlock()
					continue
				}
				if reply.VoteGranted {
					// 收到投票
					atomic.AddInt32(&receiveVotes, 1)
					PrettyDebug(dInfo, "S%v current term:%d,receive vote count:%d,latest vote from %d", rf.me, rf.term, atomic.LoadInt32(&receiveVotes), server)
					if int(atomic.LoadInt32(&receiveVotes)) > len(rf.peers)/2 && rf.status == Candidate {
						rf.status = Leader
						rf.voteFor = -1
						PrettyDebug(dInfo, "S%v change to leader!term:%d;count:%d", rf.me, rf.term, int(atomic.LoadInt32(&receiveVotes)))
						// 释放锁后再启动心跳，避免在持有锁时启动goroutine
						rf.mu.Unlock()
						// 刚成为leader时启动心跳，需要对齐日志
						go rf.startReplicators()
						return
					}
					rf.mu.Unlock()
					return
				} else {
					// term过时，退出选举，更新term
					rf.status = Follower
					rf.term = reply.Term
					rf.voteFor = -1
					PrettyDebug(dInfo, "S%v->%v requestVote fail for %v", rf.me, server, reply.Msg)
					rf.mu.Unlock()
					return
				}
			}

		}(i)
	}
}

func (rf *Raft) getCurrentTerm() int32 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term
}

func (rf *Raft) getCurrentStatus() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.status
}

func (rf *Raft) resetElectionTimer() {
	// 非阻塞发送
	select {
	case rf.resetElectionTimerCh <- true:

	default:
	}

}

func (rf *Raft) commitMsg(ch chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		var msgs []ApplyMsg
		if rf.lastApplied < int(rf.commitIndex) {
			lastApplied := rf.lastApplied
			for i := lastApplied + 1; i <= int(rf.commitIndex); i++ {
				msg := ApplyMsg{
					Command:      rf.logEntries[i].Command,
					CommandIndex: i,
					CommandValid: true,
				}
				rf.lastApplied = i
				PrettyDebug(dInfo, "S%v %v commit to state machine succ",
					rf.me, rf.logEntries[i].Command)
				msgs = append(msgs, msg)
			}
		}
		rf.mu.Unlock()
		for _, msg := range msgs {
			ch <- msg // 不要在持有锁的时候向ch发消息
		}
		time.Sleep(30 * time.Millisecond)
	}

}

/**
replicator goroutine 自动同步新日志
*/
func (rf *Raft) startReplicators() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.startReplicator(i)
	}

}

/**
心跳&日志同步
*/
func (rf *Raft) startReplicator(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status != Leader {
			rf.mu.Unlock()
			return
		}
		prevLogIdx := rf.nextIndex[server] - 1
		if prevLogIdx >= len(rf.logEntries) || prevLogIdx < 0 {
			PrettyDebug(dWarn, "S%v->%v prevLogIdx:%v out of range:%v", rf.me, server, prevLogIdx, len(rf.logEntries))
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		prevLogItem := rf.logEntries[prevLogIdx]
		entries := rf.logEntries[prevLogIdx+1:]
		args := &AppendEntriesArgs{
			Term:         rf.term,
			LeaderId:     rf.me,
			LeaderCommit: int(rf.commitIndex),
			PrevLogIndex: prevLogIdx,
			PrevLogTerm:  prevLogItem.Term,
			Entries:      entries,
		}
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		if len(args.Entries) == 0 {
			PrettyDebug(dInfo, "S%v->%v just for heartbeat", rf.me, server)
		} else {
			PrettyDebug(dInfo, "S%v->%v sync log, prevLogIdx:%v log:%v", rf.me, server, args.PrevLogIndex, marshal(args.Entries))
		}
		success := rf.sendAppendEntries(server, args, reply)

		rf.mu.Lock()
		if success && reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = prevLogIdx + len(args.Entries) + 1
				rf.matchIndex[server] = prevLogIdx + len(args.Entries)
				PrettyDebug(dInfo, "S%v->%v sync log success, nextIndex:%v,matchIndex:%v",
					rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
				rf.dealCommitIndex()
			}
		} else if success {
			if reply.Msg == OLD_TERM_MSG {
				rf.status = Follower
				rf.term = reply.Term
				rf.voteFor = -1
				PrettyDebug(dWarn, "S%v->%v  change to follower, because %v-%v",
					rf.me, server, args.Term, reply.Term)
				rf.mu.Unlock()
				continue
			}
			if rf.nextIndex[server] > 1 {
				rf.nextIndex[server] = reply.CurrentIdx
				PrettyDebug(dWarn, "S%v->%v  sync log failed, retry, nextIndex:%v", rf.me, server, rf.nextIndex[server])
			}
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)

	}

}

func (rf *Raft) dealCommitIndex() {
	if rf.status != Leader {
		return
	}
	for i := int(rf.commitIndex) + 1; i < len(rf.logEntries); i++ {
		if rf.logEntries[i].Term != rf.term {
			// Leader 不能提交旧 term 的日志
			continue
		}
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if j != rf.me && rf.matchIndex[j] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = int32(i)
		} else {
			// commit 必须连续，一旦不满足就 break
			break
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
	rf.status = Follower
	rf.voteFor = -1
	rf.resetElectionTimerCh = make(chan bool, 10)
	rf.logEntries = []Entry{ // dummy log,保证prevLOgIdx初始为1
		Entry{
			Command: nil,
			Index:   0,
			Term:    0,
		},
	}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range peers {
		rf.nextIndex[i] = len(rf.logEntries)
		rf.matchIndex[i] = 0
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commitMsg(applyCh)

	return rf
}

func marshal(v interface{}) string {
	bytes, _ := json.Marshal(v)
	return string(bytes)
}
