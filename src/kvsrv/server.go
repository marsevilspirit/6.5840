package kvsrv

import (
//	"fmt"
	"log"
	"sync"
//	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
    kv map[string]string
    requestIDs sync.Map
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    reply.Value = kv.kv[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

    if args.Mode == Mode_Report {
        kv.requestIDs.Delete(args.Task_Id)
        return
    } 

    v, ok := kv.requestIDs.Load(args.Task_Id)
    if ok {
        reply.Value = v.(string)
        return
    }

    kv.mu.Lock()
    old := kv.kv[args.Key]
    if old != args.Value {
        kv.kv[args.Key] = args.Value
    }
    kv.mu.Unlock()

    reply.Value = old

    kv.requestIDs.Store(args.Task_Id, old)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    if args.Mode == Mode_Report {
        kv.requestIDs.Delete(args.Task_Id)
        return
    }

    v, ok := kv.requestIDs.Load(args.Task_Id)
    if ok {
         reply.Value = v.(string)
         return
    }

    kv.mu.Lock()
    old := kv.kv[args.Key]
    if old == "" {
        kv.kv[args.Key] = args.Value
    } else {
        kv.kv[args.Key] = old + args.Value
    }
    kv.mu.Unlock()

    reply.Value = old

    kv.requestIDs.Store(args.Task_Id, old)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
    kv.kv = make(map[string]string, 20)

	// You may need initialization code here.

    /*
    go func() {
        for {
            fmt.Println("finish_tasks:", kv.finish_tasks)
            time.Sleep(time.Second)
        }
    }()
    */

	return kv
}
