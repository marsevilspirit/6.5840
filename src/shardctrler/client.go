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
	leaderId   int
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
	args := &QueryArgs{
        Num: num, 
        Seq: ck.GetSeq(), 
        Identifier: ck.identifier,
    }

	for {
		// try each known server.
		reply := &QueryReply{}

		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, reply)

		if ok && !reply.WrongLeader && reply.Err == "" {
			return reply.Config
		}

		ck.leaderId += 1
		ck.leaderId %= len(ck.servers)
		time.Sleep(RpcRetryInterval)
		continue
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
        Servers: servers, 
        Seq: ck.GetSeq(), 
        Identifier: ck.identifier,
    }

	for {
		// try each known server.
        reply := &JoinReply{}
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, reply)

		if ok && !reply.WrongLeader && reply.Err == "" {
			return
		}

		ck.leaderId += 1
		ck.leaderId %= len(ck.servers)
		time.Sleep(RpcRetryInterval)
		continue
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
        GIDs: gids, 
        Seq: ck.GetSeq(), 
        Identifier: ck.identifier,
    }

	for {
		// try each known server.
        reply := &LeaveReply{}

		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, reply)

		if ok && !reply.WrongLeader && reply.Err == "" {
			return
		}

		ck.leaderId += 1
		ck.leaderId %= len(ck.servers)
		time.Sleep(RpcRetryInterval)
		continue
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
        Shard: shard, 
        GID: gid, 
        Seq: ck.GetSeq(), 
        Identifier: ck.identifier,
    }

	for {
		// try each known server.
        reply := &MoveReply{}

		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, reply)

		if ok && !reply.WrongLeader && reply.Err == "" {
			return
		}

		ck.leaderId += 1
		ck.leaderId %= len(ck.servers)
		time.Sleep(RpcRetryInterval)
		continue
	}
}
