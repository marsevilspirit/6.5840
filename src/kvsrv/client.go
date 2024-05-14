package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

//import "fmt"
import "github.com/google/uuid"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
// You will have to modify this function.

    getArgs := GetArgs{
        Key: key,
    }

    getReply := GetReply{}
    
	for !ck.server.Call("KVServer.Get", &getArgs, &getReply) {
	}

	return getReply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
    id := uuid.New()
    put_append_args := PutAppendArgs{
        Key:            key,
        Value:          value,
        Task_Id:        id.ID(),
        Mode:           Mode_Modify,
    }

    put_append_reply := PutAppendReply{}

	for !ck.server.Call("KVServer."+op, &put_append_args, &put_append_reply) {
	}

	ret := put_append_reply.Value

	req := PutAppendArgs{
		Task_Id: id.ID(),
		Mode:      Mode_Report,
	}
	rsp := PutAppendReply{}
	for !ck.server.Call("KVServer."+op, &req, &rsp) {
	}
	return ret
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
