package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

const (
	RpcRetryInterval = time.Millisecond * 50
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
 	seq        uint64
	identifier int64
	leaderid   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) GetSeq() (SendSeq uint64) {
	SendSeq = ck.seq
	ck.seq += 1
	return
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.identifier = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	args := &QueryArgs{
        Num: num,
        Seq: ck.GetSeq(),
        Identifier: ck.identifier,
    }

	for {
        DPrintf("Query %v", args)
        reply := &QueryReply{}

        ok := ck.servers[ck.leaderid].Call("ShardCtrler.Query", args, reply)
        if !ok || reply.WrongLeader {
            ck.leaderid = (ck.leaderid + 1) % len(ck.servers)
            time.Sleep(RpcRetryInterval)
            continue
        }

        return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	args := &JoinArgs{
        Servers: servers,
        seq: ck.GetSeq(),
        Identifier: ck.identifier,
    }

    DPrintf("Join %v", args)

	for {
        reply := &JoinReply{}

        ok := ck.servers[ck.leaderid].Call("ShardCtrler.Join", args, reply)
        if !ok || reply.WrongLeader {
            ck.leaderid = (ck.leaderid + 1) % len(ck.servers)
            time.Sleep(RpcRetryInterval)
            continue
        }

        return
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	args := &LeaveArgs{
        GIDs: gids,
        Seq: ck.GetSeq(),
        Identifier: ck.identifier,
    }

    DPrintf("Leave %v", args)

	for {
        reply := &LeaveReply{}

        ok := ck.servers[ck.leaderid].Call("ShardCtrler.Leave", args, reply)
        if !ok || reply.WrongLeader {
            ck.leaderid = (ck.leaderid + 1) % len(ck.servers)
            time.Sleep(RpcRetryInterval)
            continue
        }
        
        return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
// Your code here.
	args := &MoveArgs{
        Shard: shard,
        GID: gid,
        Seq: ck.GetSeq(),
        Identifier: ck.identifier,
    }

    DPrintf("Move %v", args)

	for {
        reply := &MoveReply{}

        ok := ck.servers[ck.leaderid].Call("ShardCtrler.Move", args, reply)
        if !ok || reply.WrongLeader {
            ck.leaderid = (ck.leaderid + 1) % len(ck.servers)
            time.Sleep(RpcRetryInterval)
            continue
        }
        
        return
	}
}
