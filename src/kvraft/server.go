package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
    "time"
)

const Debug = false
const TimeoutInterval = 500 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Operation string // "Put", "Append" 或 "Get"
    Key       string
    Value     string
    Task_Id   uint32
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    kv map[string]string
    requestIDs sync.Map
    resultChans map[int]chan PutAppendReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    _, isLeader := kv.rf.GetState()
    if !isLeader {
        reply.Err = ErrWrongLeader
        return
    }

    reply.Value = kv.kv[args.Key]
    if reply.Value == "" {
        reply.Err = ErrNoKey
    } else {
        reply.Err = OK
    }
    
    DPrintf("Get Key(%s): Value(%s)", args.Key, reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    kv.mu.Lock()
    if args.Mode == Mode_Report {
        kv.requestIDs.Delete(args.Task_Id)
        reply.Err = OK
        kv.mu.Unlock()
        return
    } 

    _, ok := kv.requestIDs.Load(args.Task_Id)
    if ok {
        reply.Err = ErrSameCommand
        kv.mu.Unlock()
        return
    }

    op := Op{
        Operation: "Put",
        Key:       args.Key,
        Value:     args.Value,
        Task_Id:   args.Task_Id,
    }

    index, _, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    } 

    // 创建一个通道并将其存储在 resultChans 中
    ch := make(chan PutAppendReply, 1)
    kv.resultChans[index] = ch
    kv.requestIDs.Store(args.Task_Id, kv.kv[args.Key])

    kv.mu.Unlock()

    select {
    case res := <-ch:
        if res.Err == OK {
            reply.Err = OK
        } else {
            reply.Err = res.Err
        }
    case <-time.After(TimeoutInterval):
        reply.Err = ErrTimeout
        DPrintf("server %d Put 超时", kv.me)
    }

    kv.mu.Lock()
    delete(kv.resultChans, index)
    kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    kv.mu.Lock()

    if args.Mode == Mode_Report {
        kv.requestIDs.Delete(args.Task_Id)
        reply.Err = OK
        kv.mu.Unlock()
        return
    }

    _, ok := kv.requestIDs.Load(args.Task_Id)
    if ok {
         reply.Err = ErrSameCommand
         kv.mu.Unlock()
         return
    }

    op := Op{
        Operation: "Append",
        Key:       args.Key,
        Value:     args.Value,
        Task_Id:   args.Task_Id,
    }

    index, _, isLeader := kv.rf.Start(op)
    if !isLeader {
        reply.Err = ErrWrongLeader
        kv.mu.Unlock()
        return
    } 

    // 创建一个通道并将其存储在 resultChans 中
    ch := make(chan PutAppendReply, 1)
    kv.resultChans[index] = ch
    kv.requestIDs.Store(args.Task_Id, kv.kv[args.Key])

    kv.mu.Unlock()


    select {
    case res := <-ch:
        if res.Err == OK {
            reply.Err = OK
        } else {
            reply.Err = res.Err
        }
    case <-time.After(TimeoutInterval):
        reply.Err = ErrTimeout
        DPrintf("server %d Append 超时", kv.me)
    }

    kv.mu.Lock()
    delete(kv.resultChans, index)
    kv.mu.Unlock()

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

    kv.kv = make(map[string]string)
    kv.requestIDs = sync.Map{}
    kv.resultChans = make(map[int]chan PutAppendReply)

    go kv.applier()

    /*
    go func() {
        for {
            DPrintf("server %d map %v", kv.me, kv.kv)
            time.Sleep(1 * time.Second)
        }
    }()
    */

	return kv
}

func (kv *KVServer) applier() {
    for {
        select {
        case msg := <-kv.applyCh:
            if msg.CommandValid {
                kv.mu.Lock()
                op := msg.Command.(Op)
                
                if op.Operation == "Put" {
                    DPrintf("server %d Put Key(%s): Value(%s)", kv.me, op.Key, op.Value)
                    kv.kv[op.Key] = op.Value
                } else if op.Operation == "Append" {
                    DPrintf("server %d Append Key(%s): Value(%s)", kv.me, op.Key, op.Value)
                    old := kv.kv[op.Key]
                    if old == "" {
                        kv.kv[op.Key] = op.Value
                    } else {
                        kv.kv[op.Key] = old + op.Value
                    }
                }

                // 检查是否有等待该命令结果的通道
                if ch, ok := kv.resultChans[msg.CommandIndex]; ok {
                    ch <- PutAppendReply{Err: OK}
                    delete(kv.resultChans, msg.CommandIndex)
                }

                kv.mu.Unlock()
            }
        }
    }
}
