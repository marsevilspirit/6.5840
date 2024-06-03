package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
	"github.com/google/uuid"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

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
        Key: key,
    }
    
    for i := 0; ; i = (i + 1) % len(ck.servers) {
        getReply := GetReply{}

        ok := ck.servers[i].Call("KVServer.Get", &getArgs, &getReply)
        if ok && getReply.Err == OK {
            return getReply.Value
        }

        if ok && getReply.Err == ErrNoKey {
            return ""
        }

        if (ok && getReply.Err == ErrWrongLeader) || !ok {
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
    put_append_args := PutAppendArgs{
        Key:            key,
        Value:          value,
        Task_Id:        id.ID(),
        Mode:           Mode_Modify,
    }

    for i := 0; ; i = (i + 1) % len(ck.servers) {
        put_append_reply := PutAppendReply{}

        ok := ck.servers[i].Call("KVServer."+op, &put_append_args, &put_append_reply)

        if ok && put_append_reply.Err == OK {
            break
        }

        if (ok && (put_append_reply.Err == ErrWrongLeader || put_append_reply.Err == ErrSameCommand)) || !ok {
             continue
        }

      	if put_append_reply.Err == OK {
			break
		}
    }

	req := PutAppendArgs{
		Task_Id: id.ID(),
		Mode:      Mode_Report,
	}

    for i := 0; ; i = (i + 1) % len(ck.servers) {
	    rsp := PutAppendReply{}

        ok := ck.servers[i].Call("KVServer."+op, &req, &rsp)
        if ok && rsp.Err == OK {
            break
        }

        if (ok && rsp.Err == ErrWrongLeader) || !ok {
            continue
        }

        if rsp.Err == OK {
            break
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
