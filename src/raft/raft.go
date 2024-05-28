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
	"bytes"

	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
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

type LogEntry struct {
	Term int
	Command  interface{}
}

const (
	Follower = iota
	Candidate
	Leader
)

const (
    SendHeartBeatsTime = time.Duration(100) * time.Millisecond
    CheckCommitTime = time.Duration(100) * time.Millisecond
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

	currentTerm int
	votedFor    int
	log         []LogEntry

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int

	state      int
	heartTime time.Time 
	voteCount int
	muVote    sync.Mutex 
	applyCh     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    var term int
    var isleader bool
    // Your code here (3A).

    term = rf.currentTerm
    isleader = rf.state == Leader

    return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    raftstate := w.Bytes()
    rf.persister.Save(raftstate, nil)
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
    var log []LogEntry

    if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
        DPrintf("server %v 读取持久化数据失败\n", rf.me)
    } else {
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.log = log
    }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
    isLeader := false 

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

    isLeader = rf.state == Leader

	if !isLeader {
		return index, term, isLeader 
	}

    LogEntry := LogEntry{
        Term: rf.currentTerm,
        Command: command,
    }

	rf.log = append(rf.log, LogEntry)

    DPrintf("leader %v 开始向log中添加新的命令: %+v\n", rf.me, LogEntry)

    rf.persist()

	return len(rf.log) - 1, rf.currentTerm, true
}



type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int     // leader’s term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader’s commitIndex
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
    XTerm   int  
    XIndex  int  
    XLen    int
}

func (rf *Raft) sendAppendEntries(serverTo int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverTo].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
    defer rf.mu.Unlock()
    
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
        DPrintf("server %v 收到了旧的leader% v 的心跳函数, args=%+v, 更新的term: %v\n", rf.me, args.LeaderId, args, reply.Term)
		return
	}

	rf.heartTime = time.Now()

	if args.Term > rf.currentTerm { 
		rf.currentTerm = args.Term 
		rf.votedFor = -1           
		rf.state = Follower
	}

	if len(args.Entries) == 0 {
		DPrintf("server %v 接收到 leader %v 的心跳: %+v\n", rf.me, args.LeaderId, args)
	} else {
		DPrintf("server %v 收到 leader %v 的AppendEntries: %+v\n", rf.me, args.LeaderId, args)
	}

	reply.Term = rf.currentTerm

    isLogOk := true

	if args.PrevLogIndex >= len(rf.log) {
        reply.XTerm = -1
        reply.XLen = len(rf.log)
        isLogOk = false
        DPrintf("server %v 在PrevLogIndex: %v 处没有log, log长度为 %v",rf.me, args.PrevLogIndex, len(rf.log))
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        reply.XTerm = rf.log[args.PrevLogIndex].Term
        i := args.PrevLogIndex
        for rf.log[i].Term == reply.XTerm {
            i -= 1
        }
        reply.XIndex = i + 1 
        isLogOk = false
		DPrintf("server %v 的log在PrevLogIndex: %v 位置Term不匹配, args.Term=%v, 实际的term=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm)    
    }

    if !isLogOk {
        reply.Success = false
        rf.persist()
        return
    }

	if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 {
		rf.log = rf.log[:args.PrevLogIndex+1]
	}

	rf.log = append(rf.log, args.Entries...)
	if len(args.Entries) != 0 {
		DPrintf("server %v 成功进行append\n", rf.me)
	}

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	}
    
    rf.persist()
}

func (rf *Raft) handleAppendEntries(serverTo int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
    defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		rf.matchIndex[serverTo] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

        N := len(rf.log) - 1

        for ; N > rf.commitIndex; N-- {
			count := 1 
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				break
			}
		}

        rf.commitIndex = N

		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("server %v 旧的leader收到了来自 server % v 的心跳函数中更新的term: %v, 转化为Follower\n", rf.me, serverTo, reply.Term)

		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.heartTime = time.Now()
        rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.state == Leader {
		if reply.XTerm == -1 {
			DPrintf("leader %v 收到 server %v 的回退请求, 原因是log过短, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, serverTo, rf.nextIndex[serverTo], serverTo, reply.XLen)
			rf.nextIndex[serverTo] = reply.XLen
			return
		}

		i := rf.nextIndex[serverTo] - 1
		for i > 0 && rf.log[i].Term > reply.XTerm {
			i -= 1
		}
		if rf.log[i].Term == reply.XTerm {
			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的最后一个XTerm索引为%v, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, reply.XTerm, reply.XIndex, i, serverTo, rf.nextIndex[serverTo], serverTo, i+1)
			rf.nextIndex[serverTo] = i + 1
		} else {
			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的XTerm不存在, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, reply.XTerm, reply.XIndex, serverTo, rf.nextIndex[serverTo], serverTo, reply.XIndex)
			rf.nextIndex[serverTo] = reply.XIndex
		}
		return
	}
}

func (rf *Raft) SendHeartBeats() { // 有新的日志就发送新的日志，没有就发送心跳
	DPrintf("leader %v 开始发送心跳\n", rf.me)

	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
            if len(rf.log)-1 >= rf.nextIndex[i] { // 说明有新的日志要copy
				args.Entries = rf.log[rf.nextIndex[i]:]
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries, args = %+v \n", rf.me, i, args)
			} else { // 没有就发送心跳
				args.Entries = make([]LogEntry, 0)
				DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v \n", rf.me, i, args)
			}
			go rf.handleAppendEntries(i, args)
		}

		rf.mu.Unlock()

		time.Sleep(SendHeartBeatsTime)
	}
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// code to send a RequestVote RPC to a server.
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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		DPrintf("server %v 拒绝向 server %v投票: 旧的term: %v, args = %+v\n", rf.me, args.CandidateId, args.Term, args)
		return
	}


	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term 
		rf.state = Follower
        rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {// 选举限制
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.state = Follower
			rf.heartTime = time.Now()

            rf.persist()

			rf.mu.Unlock()
			reply.VoteGranted = true
			DPrintf("server %v 同意向 server %v投票, args = %+v\n", rf.me, args.CandidateId, args)
			return
		}
	} else {
		DPrintf("server %v 拒绝向 server %v投票: 已投票, args = %+v\n", rf.me, args.CandidateId, args)
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false
}

func (rf *Raft) GetVoteAnswer(server int, args *RequestVoteArgs) bool {
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if sendArgs.Term != rf.currentTerm {
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower

        rf.persist()
	}
	return reply.VoteGranted
}

func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	rf.muVote.Lock()
	if rf.voteCount > len(rf.peers)/2 {
		rf.muVote.Unlock()
		return
	}

	rf.voteCount += 1
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.state == Follower {
			rf.mu.Unlock()
			rf.muVote.Unlock()
			return
		}
		rf.state = Leader
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()

		go rf.SendHeartBeats()
	}

	rf.muVote.Unlock()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.currentTerm += 1       
	rf.state = Candidate      
	rf.votedFor = rf.me       
	rf.voteCount = 1          
	rf.heartTime = time.Now() 

    rf.persist()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, args)
	}
}

func (rf *Raft) ticker() {
    heartTimeOut := time.Duration(1500+rand.Intn(500)) * time.Millisecond
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.heartTime) > heartTimeOut {
			// 超时, 进行选举
			go rf.startElection()
		}
		rf.mu.Unlock()
        
		// pause for a random amount of time between 50 and 350
		// milliseconds.
        ms := 50 + (rand.Int63() % 300)
        time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) CheckCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			ApplyCommand := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- *ApplyCommand
			DPrintf("server %v 准备将命令 %v(索引为 %v ) 应用到状态机\n", rf.me, ApplyCommand.Command, ApplyCommand.CommandIndex)
		}
		rf.mu.Unlock()
		time.Sleep(CheckCommitTime)
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
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})// 为了方便计算，第一个元素不用, 从1开始
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.heartTime = time.Now()
	rf.state = Follower
	rf.applyCh = applyCh
    rf.persister = persister

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = len(rf.log) 
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.CheckCommit()

	return rf
}
