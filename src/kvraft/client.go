package kvraft

import (
	"crypto/rand"
	"math/big"
	//"time"

	"6.5840/labrpc"
	"github.com/google/uuid"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    leaderId int
    curTerm  int
    curIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    ck.leaderId = -1
    ck.curTerm  = 1
    ck.curIndex = 1

    /*
    go func() {
        for {
            DPrintf("leaderId: %d", ck.leaderId)
            time.Sleep(10 * time.Millisecond)
        }
    }()
    */


	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
    getArgs := GetArgs{
        Key:    key,
        Term:   ck.curTerm,
    }

    var serverTo int
    
    for i := 0; ; i = (int(nrand()) % len(ck.servers)) {
        getReply := GetReply{}

        if ck.leaderId != -1 {
            serverTo = ck.leaderId
        } else {
            serverTo = i
        }
        
        ok := ck.servers[serverTo].Call("KVServer.Get", &getArgs, &getReply)

        if ok && getReply.Err == OK {
            ck.leaderId = serverTo
            return getReply.Value
        }

        if ok && getReply.Err == ErrNoKey {
            ck.leaderId = serverTo
            return ""
        }

        if (ok && getReply.Err == ErrWrongLeader) || !ok {
            ck.leaderId = -1
            continue
        }

        if ok && getReply.Err == ErrTerm {
            ck.curTerm = getReply.Term
            getArgs.Term = ck.curTerm
            continue
        }
    }
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    id := uuid.New()
    put_append_args := PutAppendArgs {
        Key:            key,
        Value:          value,
        Task_Id:        id.ID(),
        Mode:           Mode_Modify,
        Term:           ck.curTerm,
        Index:          ck.curIndex,
    }
        
    var serverTo int

    for i := 0; ; i = (i + 1) % len(ck.servers) {
        put_append_reply := PutAppendReply{}

        if ck.leaderId != -1 {
            serverTo = ck.leaderId
        } else {
            serverTo = i
        }

        DPrintf("向服务器发送 op: %-6v key(%v):value(%v) 任务编号：%v", op, key, value, put_append_args.Task_Id)

        ok := ck.servers[serverTo].Call("KVServer."+op, &put_append_args, &put_append_reply)

        if ok {
            DPrintf("向服务器发送 op: %-6v key(%v):value(%v) 任务编号：%v 成功 --执行结果:%s", op, key, value, put_append_args.Task_Id, put_append_reply.Err)
        } 

        if ok && (put_append_reply.Err == OK || put_append_reply.Err == SameCommand) {
            if put_append_reply.Err == OK {
                ck.leaderId = serverTo
            }
            break
        }

        if ok && put_append_reply.Err == ErrWrongLeader {
            continue
        }

        if !ok {
            ck.leaderId = -1
            continue
        }

        if ok && put_append_reply.Err == ErrTerm {
            ck.curTerm = put_append_reply.Term
            continue
        }
    }

	req := PutAppendArgs{
		Task_Id: id.ID(),
		Mode:      Mode_Report,
	}

    for i := 0; ; i = (i + 1) % len(ck.servers) {
	    rsp := PutAppendReply{}

        if ck.leaderId != -1 {
            serverTo = ck.leaderId
        } else {
            serverTo = i
        }

        DPrintf("消除重复的RPC请求, serverTo: %d", serverTo)

        ok := ck.servers[serverTo].Call("KVServer."+op, &req, &rsp)

        if ok && rsp.Err == OK {
            ck.leaderId = serverTo
            break
        }

        if (ok && rsp.Err == ErrWrongLeader) || !ok {
            ck.leaderId = -1
            continue
        }

        if ok && rsp.Err == ErrTerm {
            ck.curTerm = rsp.Term
            continue
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
