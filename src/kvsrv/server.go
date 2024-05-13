package kvsrv

import (
	"fmt"
	"log"
	"sync"
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
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    if val, ok := kv.kv[args.Key]; ok {
        reply.Value = val
    } else {
        reply.Value = ""
    }
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    if val, ok := kv.kv[args.Key]; ok {
        reply.Value = val
    } else {
        reply.Value = ""
    }
    
    kv.kv[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()

    if val, ok := kv.kv[args.Key]; ok {
        reply.Value = val
        kv.kv[args.Key] = val + args.Value
    } else {
        reply.Value = ""
        kv.kv[args.Key] = args.Value
    }
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
    kv.kv = make(map[string]string)

	// You may need initialization code here.
    fmt.Println("lab 2 start")

	return kv
}
