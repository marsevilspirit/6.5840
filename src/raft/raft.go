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

	//"math"
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
    SendHeartBeatsTime = 100
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
    permu     sync.Mutex
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
    lastIncludedIndex int // 用于快照
    lastIncludedTerm int // 用于快照

    snapshot []byte // 快照

	nextIndex  []int
	matchIndex []int

	commitIndex int
	lastApplied int

	state      int
    voteTimer  *time.Timer
    heartTimer *time.Timer
    rd         *rand.Rand
	applyCh     chan ApplyMsg

    condApply *sync.Cond

}

func GetRandomElectTimeOut(rd *rand.Rand) int {
    return int(rd.Float64() * 150) + 450
}

func (rf *Raft) ResetVoteTimer() {
    rdTimeOut := GetRandomElectTimeOut(rf.rd)
    rf.voteTimer.Reset(time.Duration(rdTimeOut) * time.Millisecond)
}

func (rf *Raft) ResetHeartTimer(timeStamp int) {
    rf.heartTimer.Reset(time.Duration(timeStamp) * time.Millisecond)
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
    rf.permu.Lock()//不加会出现竞争
    defer rf.permu.Unlock()

    w := new(bytes.Buffer)
    e := labgob.NewEncoder(w)
    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
    e.Encode(rf.log)
    
    e.Encode(rf.lastIncludedIndex)
    e.Encode(rf.lastIncludedTerm)

    raftstate := w.Bytes()
    rf.persister.Save(raftstate, rf.snapshot)
    //DPrintf("server %v 持久化数据成功, currentTerm=%v, votedFor=%v, log=%v, lastIncludedIndex=%v, lastIncludedTerm=%v\n", rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.lastIncludedIndex, rf.lastIncludedTerm)
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
    var lastIncludedIndex int
    var lastIncludedTerm int

    if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil{
        DPrintf("server %v 读取持久化数据失败\n", rf.me)
    } else {
        rf.currentTerm = currentTerm
        rf.votedFor = votedFor
        rf.log = log
        rf.lastIncludedIndex = lastIncludedIndex
        rf.lastIncludedTerm = lastIncludedTerm

        rf.commitIndex = rf.lastIncludedIndex
        rf.lastApplied = rf.lastIncludedIndex
        DPrintf("server %v 读取持久化数据成功, currentTerm=%v, votedFor=%v, log=%v, lastIncludedIndex=%v, lastIncludedTerm=%v\n", rf.me, currentTerm, votedFor, log, lastIncludedIndex, lastIncludedTerm)
    }
}

func (rf *Raft) readSnapshot(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
    
    rf.snapshot = data
    DPrintf("server %v 读取快照成功\n", rf.me)
}

func (rf *Raft) leaveLogIndex(allLogIndex int) int {
    return allLogIndex - rf.lastIncludedIndex
}

func (rf *Raft) allLogIndex(leaveLogIndex int) int {
    return leaveLogIndex + rf.lastIncludedIndex
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if index <= rf.lastIncludedIndex || index > rf.commitIndex {
        DPrintf("server %v 拒绝了快照的请求，index=%v, commitIndex=%v, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
        return
    }

    DPrintf("server %v 同意了快照的请求, index=%v, commitIndex=%v, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)

    rf.snapshot = snapshot

    rf.lastIncludedTerm = rf.log[rf.leaveLogIndex(index)].Term
    rf.log = rf.log[rf.leaveLogIndex(index):]
    rf.lastIncludedIndex = index

    if rf.lastApplied < index {
        rf.lastApplied = index
    }

    rf.persist() //-------------------------------------------------持久化储存点
}

// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
    Term                int
    LeaderId            int
    LastIncludedIndex   int
    LastIncludedTerm    int
    Data                []byte
    LastIncludedCommand interface{}
}

// InstallSnapshot RPC reply structure.
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

    rf.ResetVoteTimer()

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
    }

    rf.state = Follower

    haveSameEntry := false
    sameIndex := 0
    for ; sameIndex < len(rf.log); sameIndex++ {
        if rf.allLogIndex(sameIndex) == args.LastIncludedIndex && rf.log[sameIndex].Term == args.LastIncludedTerm {
            haveSameEntry = true
            break
        }
    }

    if haveSameEntry {
        DPrintf("sever %v 存在sameEntry", rf.me)
        rf.log = rf.log[sameIndex:]
    } else {
        rf.log = make([]LogEntry, 0)
        rf.log = append(rf.log, LogEntry{Term: args.LastIncludedTerm, Command: args.LastIncludedCommand})
    }

    msg := &ApplyMsg{
        SnapshotValid: true,
        Snapshot: args.Data,
        SnapshotIndex: args.LastIncludedIndex,
        SnapshotTerm: args.LastIncludedTerm,
    }



    if args.LastIncludedIndex <= rf.lastIncludedIndex {
        return
    }

    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm

    if rf.commitIndex < args.LastIncludedIndex {
        rf.commitIndex = args.LastIncludedIndex
    }

    if rf.lastApplied < args.LastIncludedIndex {
        rf.lastApplied = args.LastIncludedIndex
    }

    rf.snapshot = args.Data

    rf.applyCh <- *msg

    reply.Term = rf.currentTerm

    rf.persist() //-------------------------------------------------持久化储存点

    DPrintf("server %v 安装快照成功, lastIncludedIndex=%v, lastIncludedTerm=%v, commitIndex=%v, lastApplied=%v\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) sendInstallSnapshot(serverTo int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
    ok := rf.peers[serverTo].Call("Raft.InstallSnapshot", args, reply)
    return ok
}

func (rf *Raft) handleInstallSnapshot(serverTo int) {

    rf.mu.Lock()

    if rf.state != Leader {
        rf.mu.Unlock()
        return 
    }

    args := &InstallSnapshotArgs{
        Term:                   rf.currentTerm,
        LeaderId:               rf.me,
        LastIncludedIndex:      rf.lastIncludedIndex,
        LastIncludedTerm:       rf.lastIncludedTerm,
        Data:                   rf.snapshot,
        LastIncludedCommand:    rf.log[0].Command,
    }

    rf.mu.Unlock()

    reply := &InstallSnapshotReply{}
    ok := rf.sendInstallSnapshot(serverTo, args, reply)
    if !ok {
        return
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()

    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.votedFor = -1
        rf.state = Follower
        rf.ResetVoteTimer()
        rf.persist() //-------------------------------------------------持久化储存点
        return
    }

    rf.nextIndex[serverTo] = rf.allLogIndex(1)
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
        DPrintf("server %v 不是leader, 无法开始新的命令\n", rf.me)
		return -1, -1, false
	}

    LogEntry := LogEntry{
        Term: rf.currentTerm,
        Command: command,
    }

	rf.log = append(rf.log, LogEntry)

    DPrintf("leader %v 开始向log中添加新的命令: %+v\n", rf.me, LogEntry)

    rf.persist()

    defer rf.ResetHeartTimer(15)

	return rf.allLogIndex(len(rf.log) - 1), rf.currentTerm, true
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
	// 新leader发送的第一个消息
	rf.mu.Lock()
	// DPrintf("server %v AppendEntries 获取锁mu", rf.me)
	defer func() {
		// DPrintf("server %v AppendEntries 释放锁mu", rf.me)
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {

		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("server %v 收到了旧的leader% v 的心跳函数, args=%+v, 更新的term: %v\n", rf.me, args.LeaderId, args, reply.Term)
		return
	}

	rf.ResetVoteTimer()

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term // 更新iterm
		rf.votedFor = -1           // 易错点: 更新投票记录为未投票
		rf.state = Follower
		rf.persist()
	}

	if len(args.Entries) == 0 {
		// 心跳函数
		DPrintf("server %v 接收到 leader %v 的心跳, 自身lastIncludedIndex=%v, PrevLogIndex=%v, len(Entries) = %v\n", rf.me, args.LeaderId, rf.lastIncludedIndex, args.PrevLogIndex, len(args.Entries))
	} else {
		DPrintf("server %v 收到 leader %v 的AppendEntries, 自身lastIncludedIndex=%v, PrevLogIndex=%v, len(Entries)= %+v \n", rf.me, args.LeaderId, rf.lastIncludedIndex, args.PrevLogIndex, len(args.Entries))
	}

	isConflict := false

	if args.PrevLogIndex < rf.lastIncludedIndex {
		// 过时的RPC, 其 PrevLogIndex 甚至在lastIncludedIndex之前
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	} else if args.PrevLogIndex >= rf.allLogIndex(len(rf.log)) {
		// PrevLogIndex位置不存在日志项
		reply.XTerm = -1
		reply.XLen = rf.allLogIndex(len(rf.log)) // Log长度, 包括了已经snapShot的部分
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置不存在日志项, Log长度为%v\n", rf.me, args.PrevLogIndex, reply.XLen)
	} else if rf.log[rf.leaveLogIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// PrevLogIndex位置的日志项存在, 但term不匹配
		reply.XTerm = rf.log[rf.leaveLogIndex(args.PrevLogIndex)].Term
		i := args.PrevLogIndex
		for i > rf.commitIndex && rf.log[rf.leaveLogIndex(i)].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		reply.XLen = rf.allLogIndex(len(rf.log)) // Log长度, 包括了已经snapShot的部分
		isConflict = true
		DPrintf("server %v 的log在PrevLogIndex: %v 位置Term不匹配, args.Term=%v, 实际的term=%v\n", rf.me, args.PrevLogIndex, args.PrevLogTerm, reply.XTerm)
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	for idx, log := range args.Entries {
		ridx := rf.leaveLogIndex(args.PrevLogIndex) + 1 + idx
		if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
			// 某位置发生了冲突, 覆盖这个位置开始的所有内容
			rf.log = rf.log[:ridx]
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		} else if ridx == len(rf.log) {
			// 没有发生冲突但长度更长了, 直接拼接
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}
	if len(args.Entries) != 0 {
		DPrintf("server %v 成功进行apeend, lastApplied=%v, len(log)=%v\n", rf.me, rf.lastApplied, len(rf.log))
	}

	// 4. Append any new entries not already in the log
	// 补充append的业务
	rf.persist()

	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.allLogIndex(len(rf.log)-1) {
			rf.commitIndex = rf.allLogIndex(len(rf.log) - 1)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		DPrintf("server %v 唤醒检查commit的协程, commitIndex=%v, len(log)=%v\n", rf.me, rf.commitIndex, len(rf.log))
		rf.condApply.Signal() // 唤醒检查commit的协程
	}
}
func (rf *Raft) handleAppendEntries(serverTo int, args *AppendEntriesArgs) {
	// 目前的设计, 重试自动发生在下一次心跳函数, 所以这里不需要死循环

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	// DPrintf("server %v handleAppendEntries 获取锁mu", rf.me)
	defer func() {
		// DPrintf("server %v handleAppendEntries 释放锁mu", rf.me)
		rf.mu.Unlock()
	}()

	if rf.state != Leader || args.Term != rf.currentTerm {
		// 函数调用间隙值变了, 已经不是发起这个调用时的term了
		// 要先判断term是否改变, 否则后续的更改matchIndex等是不安全的
		return
	}

	if reply.Success {
		// server回复成功
		newMatchIdx := args.PrevLogIndex + len(args.Entries)
		if newMatchIdx > rf.matchIndex[serverTo] {
			// 有可能在此期间让follower安装了快照, 导致 rf.matchIndex[serverTo] 本来就更大
			rf.matchIndex[serverTo] = newMatchIdx
		}

		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		// 需要判断是否可以commit
		N := rf.allLogIndex(len(rf.log) - 1)

		DPrintf("leader %v 确定N以决定新的commitIndex, lastIncludedIndex=%v, commitIndex=%v", rf.me, rf.lastIncludedIndex, rf.commitIndex)

		for N > rf.commitIndex {
			count := 1 // 1表示包括了leader自己
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[rf.leaveLogIndex(N)].Term == rf.currentTerm {
					// TODO: N有没有可能自减到snapShot之前的索引导致log出现负数索引越界?
					// 解答: 需要确保调用SnapShot时检查索引是否超过commitIndex
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				// +1 表示包括自身
				// 如果至少一半的follower回复了成功, 更新commitIndex
				break
			}
			N -= 1
		}

		rf.commitIndex = N
		rf.condApply.Signal() // 唤醒检查commit的协程

		return
	}

	if reply.Term > rf.currentTerm {
		// 回复了更新的term, 表示自己已经不是leader了
		DPrintf("server %v 旧的leader收到了来自 server % v 的心跳函数中更新的term: %v, 转化为Follower\n", rf.me, serverTo, reply.Term)

		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.ResetVoteTimer()
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.state == Leader {
		// term仍然相同, 且自己还是leader, 表名对应的follower在prevLogIndex位置没有与prevLogTerm匹配的项
		// 快速回退的处理
		if reply.XTerm == -1 {
			// PrevLogIndex这个位置在Follower中不存在
			DPrintf("leader %v 收到 server %v 的回退请求, 原因是log过短, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, serverTo, rf.nextIndex[serverTo], serverTo, reply.XLen)
			if rf.lastIncludedIndex >= reply.XLen {
				// 由于snapshot被截断
				// 下一次心跳添加InstallSnapshot的处理
				rf.nextIndex[serverTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[serverTo] = reply.XLen
			}
			return
		}

		// 防止数组越界
		// if rf.nextIndex[serverTo] < 1 || rf.nextIndex[serverTo] >= len(rf.log) {
		// 	rf.nextIndex[serverTo] = 1
		// }
		i := rf.nextIndex[serverTo] - 1
		if i < rf.lastIncludedIndex {
			i = rf.lastIncludedIndex
		}
		for i > rf.lastIncludedIndex && rf.log[rf.leaveLogIndex(i)].Term > reply.XTerm {
			i -= 1
		}

		if i == rf.lastIncludedIndex && rf.log[rf.leaveLogIndex(i)].Term > reply.XTerm {
			// 要找的位置已经由于snapshot被截断
			// 下一次心跳添加InstallSnapshot的处理
			rf.nextIndex[serverTo] = rf.lastIncludedIndex
		} else if rf.log[rf.leaveLogIndex(i)].Term == reply.XTerm {
			// 之前PrevLogIndex发生冲突位置时, Follower的Term自己也有

			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的最后一个XTerm索引为%v, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, reply.XTerm, reply.XIndex, i, serverTo, rf.nextIndex[serverTo], serverTo, i+1)
			rf.nextIndex[serverTo] = i + 1 // i + 1是确保没有被截断的
		} else {
			// 之前PrevLogIndex发生冲突位置时, Follower的Term自己没有
			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的XTerm不存在, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, reply.XTerm, reply.XIndex, serverTo, rf.nextIndex[serverTo], serverTo, reply.XIndex)
			if reply.XIndex <= rf.lastIncludedIndex {
				// XIndex位置也被截断了
				// 添加InstallSnapshot的处理
				rf.nextIndex[serverTo] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[serverTo] = reply.XIndex
			}
		}
		return
	}
}

func (rf *Raft) SendHeartBeats() { // 有新的日志就发送新的日志，没有就发送心跳
	DPrintf("leader %v 开始发送心跳\n", rf.me)

	for !rf.killed() {

        <- rf.heartTimer.C

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
				LeaderCommit: rf.commitIndex,
			}
            
            sendInstallSnapshot := false

            if args.PrevLogIndex < rf.lastIncludedIndex {
                sendInstallSnapshot = true
                DPrintf("server %v 太过落后以至于被截断, 准备发送InstallSnapshot RPC", i)
            } else if rf.allLogIndex(len(rf.log)-1) > args.PrevLogIndex {
                args.Entries = rf.log[rf.leaveLogIndex(rf.nextIndex[i]):]
                DPrintf("leader %v 开始向 server %v 广播新的AppendEntries, args = %+v \n", rf.me, i, args)
            } else {
                args.Entries = make([]LogEntry, 0)
                DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v \n", rf.me, i, args)
            }

            if sendInstallSnapshot {
                go rf.handleInstallSnapshot(i)
            } else {
                args.PrevLogTerm = rf.log[rf.leaveLogIndex(rf.nextIndex[i]-1)].Term
			    go rf.handleAppendEntries(i, args)
            }

		}

		rf.mu.Unlock()

        rf.ResetHeartTimer(SendHeartBeatsTime)
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
        rf.persist() //-------------------------------------------------持久化储存点
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.allLogIndex(len(rf.log)-1)) {// 选举限制
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.state = Follower
            rf.ResetVoteTimer()

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

	if sendArgs.Term != rf.currentTerm {
	    rf.mu.Unlock()
		return false
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	rf.mu.Unlock()
    rf.persist() //-------------------------------------------------持久化储存点

	return reply.VoteGranted
}

func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs, muVote *sync.Mutex, voteCount *int) {
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	muVote.Lock()
	if *voteCount > len(rf.peers)/2 {
		muVote.Unlock()
		return
	}

	*voteCount += 1
	if *voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.state == Follower {
			rf.mu.Unlock()
			muVote.Unlock()
			return
		}
		rf.state = Leader
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.allLogIndex(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludedIndex
		}
		rf.mu.Unlock()

		go rf.SendHeartBeats()
	}

	muVote.Unlock()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.currentTerm += 1       
	rf.state = Candidate      
	rf.votedFor = rf.me       
    voteCount := 1          
    var muVote sync.Mutex

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.allLogIndex(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, args, &muVote, &voteCount)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

        <-rf.voteTimer.C

		rf.mu.Lock()
		if rf.state != Leader {
			// 超时, 进行选举
			go rf.startElection()
		}
        rf.ResetVoteTimer()
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
        //ms := 50 + (rand.Int63() % 300)
        //time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) CheckCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}
		msgBuf := make([]*ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		tmpApplied := rf.lastApplied
		for rf.commitIndex > tmpApplied {
			tmpApplied += 1
			if tmpApplied <= rf.lastIncludedIndex {
				continue
			}
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.leaveLogIndex(tmpApplied)].Command,
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[rf.leaveLogIndex(tmpApplied)].Term,
			}

			msgBuf = append(msgBuf, msg)
		}
		rf.mu.Unlock()

		for _, msg := range msgBuf {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			DPrintf("server %v 准备commit, log = %v:%v, lastIncludedIndex=%v", rf.me, msg.CommandIndex, msg.SnapshotTerm, rf.lastIncludedIndex)

			rf.mu.Unlock()

			rf.applyCh <- *msg

			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
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
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})// 为了方便计算，第一个元素不用, 从1开始
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
    rf.rd = rand.New(rand.NewSource(int64(rf.me)))
    rf.voteTimer = time.NewTimer(0)
    rf.heartTimer = time.NewTimer(0)
	rf.state = Follower
	rf.applyCh = applyCh
    rf.condApply = sync.NewCond(&rf.mu)
    rf.ResetVoteTimer()

	// initialize from state persisted before a crash
    rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

    
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.allLogIndex(len(rf.log))
	}
    

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.CheckCommit()

	return rf
}
