package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "log"
import "time"
import "sync/atomic"

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func ServerLog(format string, a ...interface{}) {
	DPrintf("server "+format, a...)
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
    dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

    waitCommitCh map[int]*chan Result
    historyMap map[int64]*Result

	configs []Config // indexed by config num
}


type OType string

const (
	OPJoin    OType = "Join"
	OPLeave    OType = "Leave"
	OPMove OType = "Move"
    OPQuery OType = "Query"
)

type Op struct {
	// Your data here.
    OpType     OType
    Servers    map[int][]string
    GIDs       []int
    Shard      int
    GID        int
    Num        int
    Seq        uint64
    Identifier int64
}

type Result struct {
    LastSeq uint64
    Config  Config
    Err Err
    ResTerm int
}

func (sc *ShardCtrler) ConfigExecute(op *Op) (res Result) {
    res.Err = OK
    res.LastSeq = op.Seq
    res.Config = sc.configs[len(sc.configs)-1]

    switch op.OpType {
    case OPJoin:
        for gid, servers := range op.Servers {
            res.Config.Groups[gid] = servers
        }
    case OPLeave:
        for _, gid := range op.GIDs {
            delete(res.Config.Groups, gid)
        }
    case OPMove:
        res.Config.Shards[op.Shard] = op.GID
    case OPQuery:
        if op.Num == -1 || op.Num >= len(sc.configs) {
            res.Config = sc.configs[len(sc.configs)-1]
        } else {
            res.Config = sc.configs[op.Num]
        }
    }

    return res
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
    op := &Op{
        OpType: OPJoin,
        Servers: args.Servers,
        Seq: args.seq,
        Identifier: args.Identifier,
    }

    result := sc.handleOp(op)

    if result.Err == OK {
        reply.WrongLeader = false
    } else {
        reply.WrongLeader = true
    }

    reply.Err = result.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
    op := &Op{
        OpType: OPLeave,
        GIDs: args.GIDs,
        Seq: args.Seq,
        Identifier: args.Identifier,
    }

    result := sc.handleOp(op)

    if result.Err == OK {
        reply.WrongLeader = false
    } else {
        reply.WrongLeader = true
    }

    reply.Err = result.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
    op := &Op{
        OpType: OPMove,
        Shard: args.Shard,
        GID: args.GID,
        Seq: args.Seq,
        Identifier: args.Identifier,
    }

    result := sc.handleOp(op)

    if result.Err == OK {
        reply.WrongLeader = false
    } else {
        reply.WrongLeader = true
    }

    reply.Err = result.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
    op := &Op{
        OpType: OPQuery,
        Num: args.Num,
        Seq: args.Seq,
        Identifier: args.Identifier,
    }

    result := sc.handleOp(op)

    DPrintf("result.Err = %v", result.Err)

    if result.Err == OK {
        reply.WrongLeader = false
        reply.Config = result.Config
    } else {
        reply.WrongLeader = true
    }

    reply.Err = result.Err
}

func (sc *ShardCtrler) handleOp(op *Op) (res Result) {
	// 先判断是否有历史记录
	sc.mu.Lock()
	if hisMap, exist := sc.historyMap[op.Identifier]; exist && hisMap.LastSeq == op.Seq {
		sc.mu.Unlock()
		ServerLog("leader %v HandleOp: identifier %v Seq %v 的请求从历史记录返回\n", sc.me, op.Identifier, op.OpType)
		return *hisMap
	}
	sc.mu.Unlock()

    startIndex, startTerm, isLeader := sc.rf.Start(op)
    if !isLeader {
        return Result{Err: ErrWrongLeader}
    }

    sc.mu.Lock()

    // 直接覆盖之前记录的chan
    newCh := make(chan Result)
    sc.waitCommitCh[startIndex] = &newCh

    ServerLog("leader %v HandleOp: identifier %v Seq %v 的请求: %s 新建管道: %p\n", sc.me, op.Identifier, op.Seq, op.OpType, &newCh)

    sc.mu.Unlock() // Start函数耗时较长, 先解锁

    defer func() {
        sc.mu.Lock()
        delete(sc.waitCommitCh, startIndex)
        close(newCh)
        sc.mu.Unlock()
    }()

    select {
    case <-time.After(2000 * time.Millisecond):
        return Result{Err: ErrHandleOpTimeOut}
    case msg, success := <-newCh:
        if success && msg.ResTerm == startTerm {
            res = msg
            return res
        } else if !success {
            ServerLog("leader %v HandleOp: identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息 或 更新的RPC覆盖, args.OpType=%v", sc.me, op.Identifier, op.Seq, op.OpType)
            return Result{Err: ErrChanClose}
        } else {
            return Result{Err: ErrWrongLeader}
        }
    }
}

func (sc *ShardCtrler) ApplyHandler() {
	for !sc.killed() {
		log := <-sc.applyCh
        DPrintf("log = %v", log)
		if log.CommandValid {
			op := log.Command.(Op)
			sc.mu.Lock()

			// 需要判断这个log是否需要被再次应用
			var res Result

			needApply := false
			if hisMap, exist := sc.historyMap[op.Identifier]; exist {
				if hisMap.LastSeq == op.Seq {
					// 历史记录存在且Seq相同, 直接套用历史记录
					res = *hisMap
				} else if hisMap.LastSeq < op.Seq {
					// 否则新建
					needApply = true
				}
			} else {
				// 历史记录不存在
				needApply = true
			}

			if needApply {
				// 执行log
				res = sc.ConfigExecute(&op)
				res.ResTerm = log.SnapshotTerm

				// 更新历史记录
				sc.historyMap[op.Identifier] = &res
			}

			// Leader还需要额外通知handler处理clerk回复
			ch, exist := sc.waitCommitCh[log.CommandIndex]
            sc.mu.Unlock()
			if exist {
				// 发送消息
				func() {
					defer func() {
						if recover() != nil {
							// 如果这里有 panic，是因为通道关闭
							ServerLog("leader %v ApplyHandler: 发现 identifier %v Seq %v 的管道不存在, 应该是超时被关闭了", sc.me, op.Identifier, op.Seq)
						}
					}()
					res.ResTerm = log.SnapshotTerm

					*ch <- res
				}()
			}
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

    sc.historyMap = make(map[int64]*Result)
    sc.waitCommitCh = make(map[int]*chan Result)

    go sc.ApplyHandler()

	return sc
}
