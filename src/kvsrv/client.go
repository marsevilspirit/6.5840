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

    getArgs := GetArgs{}
    getArgs.Key = key
    //getArgs.Task_Id = uuid.New().String()

    getReply := GetReply{}
    
    for {
        ok := ck.server.Call("KVServer.Get", &getArgs, &getReply)
        if !ok {
            //fmt.Println("Get failed(retry)")
            continue
        } 
        break
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
    put_append_args := PutAppendArgs{}
    put_append_args.Key = key
    put_append_args.Value = value
    put_append_args.Task_Id = uuid.New().String()

    put_append_reply := PutAppendReply{}



    if op == "Put" {
        for {
            ok := ck.server.Call("KVServer.Put", &put_append_args, &put_append_reply)
            if !ok {
                //fmt.Println("Put failed(retry)")
                continue
            }
            return put_append_reply.Value
        }
    }

    if op == "Append" {
        for {
            ok := ck.server.Call("KVServer.Append", &put_append_args, &put_append_reply)
            if !ok {
                //fmt.Println("Append failed(retry)")
                continue
            }
            return put_append_reply.Value
        }
    }

    return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
