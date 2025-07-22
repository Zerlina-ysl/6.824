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

	OLD_TERM_MSG = "old term"
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
	Term        int32 // candidate's term
	CandidateId int
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
	PrettyDebug(dInfo, "S%v %v recv requestvote,req:", rf.me, time.Now().Format("15:04:05.000"), marshal(args))

	reply.Term = rf.term

	// 请求方term过时
	if args.Term < rf.term {
		PrettyDebug(dInfo, "S%v %v RequestVote  reject vote %d becauseof term %d-%d", rf.me, time.Now().Format("15:04:05.000"), args.CandidateId, args.Term, rf.term)
		reply.VoteGranted = false
		reply.Msg = OLD_TERM_MSG
		PrettyDebug(dInfo, "S%v %v recv requestvote,resp:", rf.me, time.Now().Format("15:04:05.000"), marshal(reply))

		return
	}

	// 给定term下至多只能投一次
	if args.Term > rf.term {
		rf.voteFor = -1
	}

	// 已投票
	if rf.voteFor != -1 {
		PrettyDebug(dInfo, "S%v %v reject for %d because term %d votefor %d", rf.me, time.Now().Format("15:04:05.000"), args.CandidateId, rf.term, rf.voteFor)
		reply.VoteGranted = false
		reply.Msg = fmt.Sprintf("voted for %d", rf.voteFor)
		PrettyDebug(dInfo, "S%v %v recv requestvote,resp:", rf.me, time.Now().Format("15:04:05.000"), marshal(reply))

		return
	}

	rf.voteFor = args.CandidateId
	rf.status = Follower
	reply.VoteGranted = true
	rf.term = args.Term
	PrettyDebug(dInfo, "S%v %v vote for %d,reply:%v", rf.me, time.Now().Format("15:04:05.000"), args.CandidateId, marshal(reply))
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
	Term     int32 // 领导者任期
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
	Msg     string
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dInfo, "S%v %v recv AppendEntries from %d,args:%v", rf.me, time.Now().Format("15:04:05.000"), args.LeaderId, marshal(args))
	reply.Term = rf.term
	if args.Term < rf.term {
		PrettyDebug(dInfo, "S%v %v AppendEntries reject because req.term[%d] less than rf.term[%d]", rf.me, time.Now().Format("15:04:05.000"), args.Term, rf.term)
		reply.Success = false
		reply.Msg = OLD_TERM_MSG
		return
	}
	rf.voteFor = -1
	rf.status = Follower
	rf.resetElectionTimer()
	rf.term = args.Term
	reply.Term = rf.term
	reply.Success = true
	PrettyDebug(dInfo, "S%v %v AppendEntries  admit [%d]'s leader state in term[%d]", rf.me, time.Now().Format("15:04:05.000"), args.LeaderId, rf.term)

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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		switch rf.getCurrentStatus() {
		case Follower:
			PrettyDebug(dTimer, "S%v %v is follower", rf.me, time.Now().Format("15:04:05.000"))
			// 超时发起选举 150-300ms
			randomElectionTimeout := time.Duration(rand.Intn(100)+150) * time.Millisecond
			//timeout, _ := context.WithTimeout(context.Background(), randomElectionTimeout)
			timer := time.NewTimer(randomElectionTimeout)
			select {
			case <-timer.C:
				if rf.killed() {
					timer.Stop()
					return
				}
				rf.mu.Lock()
				PrettyDebug(dInfo, "S%v %v no heatbeat longtime, change to candidate", rf.me, time.Now().Format("15:04:05.000"))
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
				PrettyDebug(dInfo, "S%v %v receive heatbeat ", rf.me, time.Now().Format("15:04:05.000"))

			}
		case Candidate:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			voteFor := rf.voteFor
			PrettyDebug(dInfo, "S%v %v is candidate, voteFor %v ", rf.me, time.Now().Format("15:04:05.000"), voteFor)
			rf.mu.Unlock()
			// 在一个 Term 内最多只能投一票
			if voteFor == -1 || voteFor == rf.me {
				rf.startElection()
			}
			// 发起选举后等待一个选举周期
			time.Sleep(time.Duration(rand.Intn(100)+150) * time.Millisecond)

		case Leader:
			PrettyDebug(dTimer, "S%v %v is Leader", rf.me, time.Now().Format("15:04:05.000"))

			for rf.getCurrentStatus() == Leader && !rf.killed() {
				// 发送心跳
				rf.sendHeartbeat()
				// no more than ten times per second.
				time.Sleep(200 * time.Millisecond)
			}

		}

	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.term = rf.term + 1
	currentTerm := rf.term
	rf.voteFor = rf.me // 投自己一票
	PrettyDebug(dTimer, "S%v %ventering election with term=%d", rf.me, time.Now().Format("15:04:05.000"), rf.term)
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
				Term:       rf.getCurrentTerm(),
				CandidateId: rf.me,
			}
			reply := &RequestVoteReply{}
			PrettyDebug(dTimer, "S%v %v sendRequestVote to %d", rf.me, time.Now().Format("15:04:05.000"), server)

			succ := rf.sendRequestVote(server,
				req,
				reply)

			if !succ {
				PrettyDebug(dWarn, "S%v %v sendRequestVote to [%d] failed", rf.me, time.Now().Format("15:04:05.000"), server)
				return
			}
			
			rf.mu.Lock()
			
			// 检查状态是否仍然有效
			if rf.status != Candidate || rf.term != currentTerm {
				rf.mu.Unlock()
				return
			}
			
			if reply.VoteGranted {
				// 收到投票
				atomic.AddInt32(&receiveVotes, 1)
				PrettyDebug(dInfo, "S%v %v current term:%d,receive vote count:%d,latest vote from %d", rf.me, time.Now().Format("15:04:05.000"), rf.term, atomic.LoadInt32(&receiveVotes), server)

				if int(atomic.LoadInt32(&receiveVotes)) > len(rf.peers)/2 {
					rf.status = Leader
					rf.voteFor = -1
					PrettyDebug(dInfo, "S%v %v change to leader!term:%d;count:%d", rf.me, time.Now().Format("15:04:05.000"), rf.term, int(atomic.LoadInt32(&receiveVotes)))
					// 释放锁后再启动心跳，避免在持有锁时启动goroutine
					rf.mu.Unlock()
					go rf.sendHeartbeat()
					return
				}
			} else if reply.Msg == OLD_TERM_MSG {
				// term过时，退出选举，更新term
				rf.status = Follower
				rf.term = reply.Term
				rf.voteFor = -1
				PrettyDebug(dInfo, "S%v %v received higher term=%d in vote response, stepping down", rf.me, time.Now().Format("15:04:05.000"), reply.Term)
				rf.mu.Unlock()
				return 
			}
			rf.mu.Unlock()

		}(i)
	}
}

func (rf *Raft) sendHeartbeat() {
	if rf.getCurrentStatus() != Leader {
		PrettyDebug(dWarn, "S%v %v not leader", rf.me, time.Now().Format("15:04:05.000"))
		return
	}
	currentTerm := rf.getCurrentTerm()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &AppendEntriesReply{}
			PrettyDebug(dInfo, "S%v %v send heartbeat to [%d]", rf.me, time.Now().Format("15:04:05.000"), server)
			
			succ := rf.sendAppendEntries(server, &AppendEntriesArgs{
				Term:     currentTerm,
				LeaderId: rf.me,
			}, reply)

			if !succ {
				PrettyDebug(dWarn, "S%v %v send heartbeat to [%d] failed", rf.me, time.Now().Format("15:04:05.000"), server)
				return
			}
			
			if !reply.Success {
				rf.mu.Lock()
				// 只有在当前term没有变化时才更新状态，避免过时的响应影响当前状态
				if rf.term == currentTerm && rf.status == Leader {
					PrettyDebug(dWarn, "S%v %v change to follower", rf.me, time.Now().Format("15:04:05.000"))
					rf.status = Follower
					rf.term = reply.Term
					rf.voteFor = -1
				}
				rf.mu.Unlock()
			} else {
				PrettyDebug(dWarn, "S%v %v send heartbeat to [%d] success", rf.me, time.Now().Format("15:04:05.000"), server)
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

// func (rf *Raft) initLog() {
// 	logPath, _ := os.Getwd()
// 	// 按照pid输出
// 	logName := fmt.Sprintf("%s/tmp/%d.%d", logPath, rf.me, os.Getpid())
// 	writer, _ := rotatelogs.New(logName + "%Y%m%d")

// 	fileFormatter := &prefixed.TextFormatter{
// 		FullTimestamp:   true,
// 		TimestampFormat: "2006-01-02.15:04:05.000.000000",
// 		ForceFormatting: true,
// 		ForceColors:     true,
// 		DisableColors:   true,
// 	}

// 	logger := logrus.New()
// 	logger.SetFormatter(fileFormatter)
// 	logger.SetLevel(logrus.DebugLevel)
// 	logger.AddHook(lfshook.NewHook(lfshook.WriterMap{
// 		logrus.InfoLevel:  writer,
// 		logrus.DebugLevel: writer,
// 		logrus.ErrorLevel: writer,
// 		logrus.FatalLevel: writer,
// 	}, fileFormatter))
// 	logger.SetOutput(io.Discard)
// 	rf.logger = logger

// 	rf.logger.Infof("[%d]init log ....", rf.me)
// }

func (rf *Raft) resetElectionTimer() {
	// 非阻塞发送
	select {
	case rf.resetElectionTimerCh<-true:
	
	default:
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
	rf.resetElectionTimerCh = make(chan bool, 1)


	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func marshal(v interface{}) string {
	bytes, _ := json.Marshal(v)
	return string(bytes)
}
