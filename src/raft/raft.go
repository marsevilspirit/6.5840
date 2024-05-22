package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.

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
    "math/rand"
    "sync"
    "sync/atomic"
    "time"

    //	"6.5840/labgob"
    "6.5840/labrpc"
    "fmt"
)

const (
    Reset  = "\033[0m"
    Red    = "\033[31m"
    Green  = "\033[32m"
    Yellow = "\033[33m"
    Blue   = "\033[34m"
    Purple = "\033[35m"
    Cyan   = "\033[36m"
    White  = "\033[37m"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int

    // For 3D:
    SnapshotValid bool
    Snapshot      []byte
    SnapshotTerm  int
    SnapshotIndex int
}

type LogEntry struct {
    Term int
    Command interface{}
}

const (
    Follower = iota
    Candidate
    Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]
    dead      int32               // set by Kill()

    // Your data here (3A, 3B, 3C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

    // Persistent state on all servers
    currentTerm int
    votedFor int
    log []LogEntry

    // Volatile state on all servers
    commitIndex int
    lastApplied int

    // Volatile state on leaders
    nextIndex []int
    matchIndex []int

    state int
    heartTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

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
    // Your code here (3C).
    // Example:
    // w := new(bytes.Buffer)
    // e := labgob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // raftstate := w.Bytes()
    // rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
    // Your code here (3C).
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    // Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
    // Your data here (3A, 3B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
    // Your data here (3A).
    Term int
    VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // Your code here (3A, 3B).
    rf.mu.Lock()
    defer rf.mu.Unlock()

    rf.resetElectionTimeout()

    if args.Term < rf.currentTerm {
        reply.VoteGranted = false
        reply.Term = rf.currentTerm
        return
    }

    if args.Term > rf.currentTerm {
        rf.votedFor = -1
    } 

    var lastLogIndex int
    var lastLogTerm int
    if len(rf.log) == 0 {
        lastLogIndex = -1
        lastLogTerm = 0
    } else {
        lastLogIndex = len(rf.log) - 1
        lastLogTerm = rf.log[lastLogIndex].Term
    }

    if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
        if args.Term > rf.currentTerm || (args.LastLogIndex >= lastLogIndex && args.LastLogTerm >= lastLogTerm) {
            rf.currentTerm = args.Term
            reply.Term = rf.currentTerm
            rf.votedFor = args.CandidateId
            rf.state = Follower
            rf.heartTime = time.Now()

            reply.VoteGranted = true
            fmt.Printf("server %v 同意向 server %v投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
            return
        }
    } else {
        fmt.Printf("server %v 拒绝向 server %v投票: 已投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
    }

    rf.votedFor = args.CandidateId
    reply.VoteGranted = true
    reply.Term = rf.currentTerm

    return
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
    if len(rf.log) == 0 {
        args.LastLogTerm = 0
    } else {
        args.LastLogTerm = rf.log[len(rf.log) - 1].Term
    }

    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    if !ok {
        fmt.Println(Red, "Server", rf.me, "sendRequestVote to", server, "failed", "term", rf.currentTerm, Reset)
        return false
    }
    return true
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
    Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Success = false
        reply.Term = rf.currentTerm
        return
    }

    rf.heartTime = time.Now()

    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        fmt.Println(Blue, "Server", rf.me, "becomes follower", "term", rf.currentTerm, Reset)
        rf.state = Follower
        rf.votedFor = -1
    }

    if rf.state == Leader {
        rf.state = Follower
        fmt.Println(Blue, "Server", rf.me, "becomes follower", "term", rf.currentTerm, Reset)
    }

    if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
        reply.Success = false
        return
    }

    if args.Entries == nil {
        fmt.Println(Yellow, "Server", rf.me, "heartbeat from", args.LeaderId, "term", rf.currentTerm, Reset)
    }

    if args.Entries != nil && (args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
        // 校验PrevLogIndex和PrevLogTerm不合法
        // 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)

        reply.Term = rf.currentTerm
        rf.mu.Unlock()
        reply.Success = false
        return
    }

    reply.Term = rf.currentTerm
    reply.Success = true

    if args.LeaderCommit > rf.commitIndex {
        if args.LeaderCommit < len(rf.log) {
            rf.commitIndex = args.LeaderCommit
        } else {
            rf.commitIndex = len(rf.log) - 1
        }
    }
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

    // Your code here (3B).


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

func (rf *Raft) ticker() {
    heartTimeOut := time.Duration(350+rand.Intn(350)) * time.Millisecond

    for rf.killed() == false {

        // Your code here (3A)
        // Check if a leader election should be started.
        rf.mu.Lock()
        state := rf.state
        rf.mu.Unlock()

        if state != Leader && time.Since(rf.heartTime) > heartTimeOut {
            fmt.Println(Blue, "Server", rf.me, "starts election", "term", rf.currentTerm, Reset)
            rf.startElection()
        }

        // pause for a random amount of time between 50 and 350
        // milliseconds.
        ms := 50 + (rand.Int63() % 300)
        time.Sleep(time.Duration(ms) * time.Millisecond)
    }
}

func (rf *Raft) startElection() {
    rf.state = Candidate
    rf.currentTerm++
    rf.votedFor = rf.me
    rf.resetElectionTimeout()

    voteCount := 1
    var wg sync.WaitGroup
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        wg.Add(1)

        go func(server int) {
            defer wg.Done()
            rf.mu.Lock()
            var lastLogIndex int
            var lastLogTerm int
            if len(rf.log) > 0 {
                lastLogIndex = len(rf.log) - 1
                lastLogTerm = rf.log[lastLogIndex].Term
            } else {
                lastLogIndex = -1
                lastLogTerm = 0
            }
            args := &RequestVoteArgs{
                Term:         rf.currentTerm,
                CandidateId:  rf.me,
                LastLogIndex: lastLogIndex,
                LastLogTerm:  lastLogTerm,
            }
            rf.mu.Unlock()

            reply := &RequestVoteReply{}
            ok := rf.sendRequestVote(server, args, reply)
            if ok {
                rf.mu.Lock()
                defer rf.mu.Unlock()


                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.state = Follower
                    rf.votedFor = -1
                    rf.resetElectionTimeout()
                } else if reply.VoteGranted {
                    voteCount++
                    if voteCount > len(rf.peers)/2 && rf.state == Candidate {
                        rf.state = Leader
                        fmt.Println(Green, "Server", rf.me, "becomes leader", "term", rf.currentTerm, Reset)
                        rf.resetElectionTimeout()
                    }
                }
            }
        }(i)
    }

    wg.Wait()

    if rf.state != Leader {
        fmt.Println(Red, "Server", rf.me, "election failed", "term", rf.currentTerm, Reset)
    }
}

func (rf *Raft) resetElectionTimeout() {
    rf.heartTime = time.Now()
}

func (rf *Raft) mainLoop() {
    for rf.killed() == false {
        rf.mu.Lock()
        state := rf.state
        rf.mu.Unlock()

        if state == Leader {
            go rf.broadcastHeartbeat()
        }

        time.Sleep(100 * time.Millisecond)
    }
}

func (rf *Raft) broadcastHeartbeat() {
    rf.mu.Lock()

    var prevLogIndex int
    var prevLogTerm int
    if len(rf.log) == 0 {
        prevLogIndex = -1
        prevLogTerm = 0
    } else {
        prevLogIndex = len(rf.log) - 1
        prevLogTerm = rf.log[prevLogIndex].Term
    }

    args := &AppendEntriesArgs{
        Term:         rf.currentTerm,
        LeaderId:     rf.me,
        PrevLogIndex: prevLogIndex,
        PrevLogTerm:  prevLogTerm,
        Entries:      nil,
        LeaderCommit: rf.commitIndex,
    }
    rf.mu.Unlock()

    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }

        go func(server int) {
            reply := &AppendEntriesReply{}
            ok := rf.sendAppendEntries(server, args, reply)
            if ok {
                rf.mu.Lock()
                defer rf.mu.Unlock()

                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.state = Follower
                    rf.votedFor = -1
                    rf.resetElectionTimeout()
                }
            }
        }(i)
    }
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    if !ok {
        fmt.Println(Red, "Server", rf.me, "sendAppendEntries to", server, "failed", "term", rf.currentTerm, "state", rf.state, Reset)
        return false
    }
    return true
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

    // Your initialization code here (3A, 3B, 3C).
    rf.currentTerm = 0
    rf.votedFor = -1
    rf.log = make([]LogEntry, 0)

    rf.commitIndex = 0
    rf.lastApplied = 0

    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))

    rf.state = Follower
    rf.heartTime = time.Now()

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    // start ticker goroutine to start elections
    go rf.ticker()

    go rf.mainLoop()

    return rf
}
