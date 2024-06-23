package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "log"
import "time"
import "sync/atomic"
import "sort"
import "math"

var Debug = false
var Special = false

const HandleOpTimeOut = time.Millisecond * 2000 // 超时为2s

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func SDPrintf(format string, a ...interface{}) (n int, err error) {
	if Special {
		log.Printf("kv-"+format, a...)
	}
	return
}

func ControlerLog(format string, a ...interface{}) {
	DPrintf("ctl "+format, a...)
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

func (sc *ShardCtrler) CheckAppendConfig(newConfig Config) {
	if newConfig.Num > sc.configs[len(sc.configs)-1].Num {
		sc.configs = append(sc.configs, newConfig)
	}
}

func deep_copy_map(oldConfig *Config, newConfig *Config) {
	oldConfig.check_nil_map()
	newConfig.check_nil_map()

	for gid, group := range oldConfig.Groups {
		newConfig.Groups[gid] = group
	}

	for gid, group := range oldConfig.CachedGid {
		newConfig.CachedGid[gid] = group
	}
}

func get_max_map_len_count(newConfig *Config) (map_shard_len map[int]int, max_map_gid_len int, max_map_gid_count int) {
	map_shard_len = make(map[int]int)

	max_map_gid_len = 0   // gid包含最多shard的数量
	max_map_gid_count = 0 // 含最多shard的数量的group的数量

	for _, gid := range newConfig.Shards {
		map_shard_len[gid]++
		if map_shard_len[gid] > max_map_gid_len {
			max_map_gid_len = map_shard_len[gid]
		}
	}

	for _, gid := range newConfig.Shards {
		if map_shard_len[gid] == max_map_gid_len {
			max_map_gid_count += 1
		}
	}
	return
}

func get_min_arr(newConfig *Config) (map_shard_min []int) {

	map_shard_min = make([]int, 0)

	map_shard_len := make(map[int]int)

	min_map_gid_len := math.MaxInt // gid包含最少shard的数量

	for _, gid := range newConfig.Shards {
		if _, exist := newConfig.Groups[gid]; exist {
			map_shard_len[gid]++
		}
	}

	// Groups中没有映射到的gid置为0
	for gid := range newConfig.Groups {
		if _, exist := map_shard_len[gid]; !exist {
			map_shard_len[gid] = 0
		}
	}

	for _, count := range map_shard_len {
		min_map_gid_len = int(math.Min(float64(count), float64(min_map_gid_len)))
	}

	for gid, count := range map_shard_len {
		if count == min_map_gid_len {
			map_shard_min = append(map_shard_min, gid)
		}
	}
	// 必须确保有序
	sort.Ints(map_shard_min)

	return
}

func CreateNewConfig(me int, configs []Config, newGroups map[int][]string) Config {
	lastConfig := configs[len(configs)-1]
	newConfig := Config{
		Num:       lastConfig.Num + 1,
		Shards:    lastConfig.Shards,
		Groups:    make(map[int][]string),
		CachedGid: make(map[int][]string),
	}

	// Combine old and new groups
	deep_copy_map(&lastConfig, &newConfig)

	// 要确保加入cache的gid在所有节点上相同, 所以要求有序
	total_new_gids := make([]int, 0)
	for gid := range newGroups {
		total_new_gids = append(total_new_gids, gid)
	}
	sort.Ints(total_new_gids)

	new_gids := make([]int, 0)

	for _, gid := range total_new_gids {
		if len(newConfig.Groups) < NShards {
			// group的数量不能超过NShards
			newConfig.Groups[gid] = newGroups[gid]
			new_gids = append(new_gids, gid)
		} else {
			newConfig.add_cache(gid, newGroups[gid])
		}
	}

	// first config shard or previous config is empty
	if len(lastConfig.Groups) == 0 {
		for shard := 0; shard < NShards; shard++ {
			idx := shard % len(new_gids)
			newConfig.Shards[shard] = new_gids[idx]
		}
	} else {
		// Reallocate shards
		map_shard_len, max_map_gid_len, max_map_gid_count := get_max_map_len_count(&newConfig)

		for _, new_gid := range new_gids {
			map_shard_len[new_gid] = 0
			idx := 0
			for max_map_gid_len > map_shard_len[new_gid]+1 {
				old_gid := newConfig.Shards[idx]

				if map_shard_len[old_gid] == max_map_gid_len {
					// old_gid标识的这个group分配了最多的shard, 将当前的shard重新分配给新的Group
					newConfig.Shards[idx] = new_gid
					max_map_gid_count -= map_shard_len[old_gid]
					map_shard_len[old_gid]--
					map_shard_len[new_gid]++
					if max_map_gid_count == 0 {
						// 原来映射最多的组都已经被剥夺了一个映射, 需要重新统计
						map_shard_len, max_map_gid_len, max_map_gid_count = get_max_map_len_count(&newConfig)
					}
				}
				idx++
				idx %= NShards
			}
		}
	}
	SDPrintf("%v", me)

	SDPrintf("%v-CreateNewConfig-lastConfig.Groups= %v, newGroups=%+v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, newGroups, len(lastConfig.Groups))
	SDPrintf("%v-CreateNewConfig-lastConfig.CachedGid= %v, len(lastConfig.CachedGid)=%v", me, lastConfig.CachedGid, len(lastConfig.CachedGid))
	SDPrintf("%v-CreateNewConfig-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-CreateNewConfig-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-CreateNewConfig-newConfig.CachedGid= %v, len(newConfig.CachedGid)=%v", me, newConfig.CachedGid, len(newConfig.CachedGid))
	SDPrintf("%v-CreateNewConfig-newConfig.Shards= %v", me, newConfig.Shards)

	// to_delte_gids := make([]int, 0)

	for gid := range newConfig.Groups {
		missing := gid
		found := false
		for _, maped_gid := range newConfig.Shards {
			if maped_gid == gid {
				found = true
			}
		}
		if !found {
			// to_delte_gids = append(to_delte_gids, missing)
			SDPrintf("%v-CreateNewConfig, missing gid= %v", me, missing)
		}
	}

	// for _, missing_gid := range to_delte_gids {
	// 	delete(newConfig.Groups, missing_gid)
	// }

	return newConfig
}

func RemoveGidServers(me int, configs []Config, gids []int) Config {
	if len(configs) == 0 {
		panic("len(configs) == 0")
	}

	lastConfig := configs[len(configs)-1]
	newConfig := Config{
		Num:       lastConfig.Num + 1,
		Shards:    lastConfig.Shards,
		Groups:    make(map[int][]string),
		CachedGid: make(map[int][]string),
	}

	// 深复制Group
	deep_copy_map(&lastConfig, &newConfig)

	// 先移除缓冲区的gid
	newConfig.remove_cache(gids)

	// 再从Groups移除gid, 同时将shard对应的映射置为0
	newConfig.remove_group(gids)

	// 如果缓冲区有剩余的gid, 移动到Groups, 注意要注意顺序
	newConfig.move_cache()

	// 要确保有序
	min_gid_arr := get_min_arr(&newConfig)

	if len(newConfig.Groups) > 0 {
		for shard, map_gid := range newConfig.Shards {
			if map_gid != 0 {
				continue
			}
			newConfig.Shards[shard] = min_gid_arr[0]
			min_gid_arr = min_gid_arr[1:]

			if len(min_gid_arr) == 0 {
				min_gid_arr = get_min_arr(&newConfig)
			}
		}
	} else {
		newConfig.Shards = [NShards]int{}
	}

	SDPrintf("%v", me)
	SDPrintf("%v-RemoveGidServers-lastConfig.Groups= %v, gids= %v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, gids, len(lastConfig.Groups))
	SDPrintf("%v-RemoveGidServers-lastConfig.CachedGid= %v, len(lastConfig.CachedGid)=%v", me, lastConfig.CachedGid, len(lastConfig.CachedGid))
	SDPrintf("%v-RemoveGidServers-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-RemoveGidServers-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-RemoveGidServers-newConfig.CachedGid= %v, len(newConfig.CachedGid)=%v", me, newConfig.CachedGid, len(newConfig.CachedGid))

	SDPrintf("%v-RemoveGidServers-newConfig.Shards= %v", me, newConfig.Shards)

	for gid := range newConfig.Groups {
		missing := gid
		found := false
		for _, maped_gid := range newConfig.Shards {
			if maped_gid == gid {
				found = true
			}
		}
		if !found {
			SDPrintf("%v-RemoveGidServers, missing gid= %v", me, missing)

			SDPrintf("error")
		}
	}
	return newConfig
}

func MoveShard2Gid(me int, configs []Config, n_shard int, n_gid int) Config {
	if len(configs) == 0 {
		panic("len(configs) == 0")
	}

	lastConfig := configs[len(configs)-1]
	newConfig := Config{
		Num:       lastConfig.Num + 1,
		Shards:    [NShards]int{},
		Groups:    make(map[int][]string),
		CachedGid: make(map[int][]string),
	}

	// 深复制Group
	deep_copy_map(&lastConfig, &newConfig)

	for shard, gid := range lastConfig.Shards {
		if shard != n_shard {
			newConfig.Shards[shard] = gid
		} else {
			newConfig.Shards[n_shard] = n_gid
		}
	}

	SDPrintf("%v", me)
	SDPrintf("%v-MoveShard2Gid-lastConfig.Groups= %v, n_shard= %v,n_gid= %v, len(lastConfig.Groups)=%v", me, lastConfig.Groups, n_shard, n_gid, len(lastConfig.Groups))
	SDPrintf("%v-MoveShard2Gid-lastConfig.CachedGid= %v, len(lastConfig.CachedGid)=%v", me, lastConfig.CachedGid, len(lastConfig.CachedGid))
	SDPrintf("%v-MoveShard2Gid-lastConfig.Shards= %v", me, lastConfig.Shards)
	SDPrintf("%v-MoveShard2Gid-lastConfig.Shards= %v", me, lastConfig.Shards)

	SDPrintf("%v-MoveShard2Gid-newConfig.Groups= %v, len(newConfig.Groups)=%v", me, newConfig.Groups, len(newConfig.Groups))
	SDPrintf("%v-MoveShard2Gid-newConfig.CachedGid= %v, len(newConfig.CachedGid)=%v", me, newConfig.CachedGid, len(newConfig.CachedGid))
	SDPrintf("%v-MoveShard2Gid-newConfig.Shards= %v", me, newConfig.Shards)

	for gid := range newConfig.Groups {
		missing := gid
		found := false
		for _, maped_gid := range newConfig.Shards {
			if maped_gid == gid {
				found = true
			}
		}
		if !found {
			SDPrintf("%v-MoveShard2Gid, missing gid= %v", missing)

			SDPrintf("error")
		}
	}
	return newConfig
}

func QueryConfig(me int, configs []Config, num int) Config {
	if len(configs) == 0 {
		return Config{Num: 0}
	}

	lastConfig := configs[len(configs)-1]
	if num == -1 || num >= lastConfig.Num {
		return lastConfig
	}
	return configs[num]
}

func (sc *ShardCtrler) ConfigExecute(op *Op) (res Result) {
	// 调用时要求持有锁
	res.LastSeq = op.Seq
	switch op.OpType {
	case OPJoin:
		newConfig := CreateNewConfig(sc.me, sc.configs, op.Servers)
		sc.CheckAppendConfig(newConfig)
		res.Err = ""
	case OPLeave:
		newConfig := RemoveGidServers(sc.me, sc.configs, op.GIDs)
		sc.CheckAppendConfig(newConfig)
		res.Err = ""
	case OPMove:
		newConfig := MoveShard2Gid(sc.me, sc.configs, op.Shard, op.GID)
		sc.CheckAppendConfig(newConfig)
		res.Err = ""
	case OPQuery:
		rConfig := QueryConfig(sc.me, sc.configs, op.Num)
		res.Config = rConfig
		res.Err = ""
	}
	return
}

func (sc *ShardCtrler) HandleOp(opArgs *Op) (res Result) {
	// 先判断是否有历史记录
	sc.mu.Lock()
	if hisMap, exist := sc.historyMap[opArgs.Identifier]; exist && hisMap.LastSeq == opArgs.Seq {
		sc.mu.Unlock()
		// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 从历史记录返回\n", sc.me, opArgs.Identifier, opArgs.OpType, opArgs.Servers, opArgs.GIDs, opArgs.Shard, opArgs.GID, opArgs.Num)
		return *hisMap
	}
	sc.mu.Unlock() // Start函数耗时较长, 先解锁

	// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 准备调用Start\n", sc.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Servers, opArgs.GIDs, opArgs.Shard, opArgs.GID, opArgs.Num)

	startIndex, startTerm, isLeader := sc.rf.Start(*opArgs)
	if !isLeader {
		ControlerLog("%v HandleOp: Start发现不是Leader", sc.me)
		return Result{Err: ErrNotLeader}
	}

	sc.mu.Lock()

	// 直接覆盖之前记录的chan
	newCh := make(chan Result)
	sc.waitCommitCh[startIndex] = &newCh
	// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 新建管道: %p\n", sc.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType, opArgs.Servers, opArgs.GIDs, opArgs.Shard, opArgs.GID, opArgs.Num, &newCh)
	sc.mu.Unlock() // Start函数耗时较长, 先解锁

	defer func() {
		sc.mu.Lock()
		delete(sc.waitCommitCh, startIndex)
		close(newCh)
		sc.mu.Unlock()
	}()

	// 等待消息到达或超时
	select {
	case <-time.After(HandleOpTimeOut):
		res.Err = ErrHandleOpTimeOut
		ControlerLog("%v HandleOp: identifier %v Seq %v: 超时", sc.me, opArgs.Identifier, opArgs.Seq)
		return
	case msg, success := <-newCh:
		if success && msg.ResTerm == startTerm {
			res = msg
			// ControlerLog("%v HandleOp: identifier %v Seq %v, HandleOp %s 成功\n", sc.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType)
			return
		} else if !success {
			// 通道已经关闭, 有另一个协程收到了消息 或 通道被更新的RPC覆盖
			// TODO: 是否需要判断消息到达时自己已经不是leader了?
			ControlerLog("%v HandleOp: identifier %v Seq %v: 通道已经关闭, 有另一个协程收到了消息 或 更新的RPC覆盖, args.OpType=%v", sc.me, opArgs.Identifier, opArgs.Seq, opArgs.OpType)
			res.Err = ErrChanClose
			return
		} else {
			// term与一开始不匹配, 说明这个Leader可能过期了
			ControlerLog("%v HandleOp: identifier %v Seq %v: term与一开始不匹配, 说明这个Leader可能过期了, res.ResTerm=%v, startTerm=%+v", sc.me, opArgs.Identifier, opArgs.Seq, res.ResTerm, startTerm)
			res.Err = ErrLeaderOutDated
			return
		}
	}
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	opArgs := &Op{OpType: OPJoin, Seq: args.Seq, Identifier: args.Identifier, Servers: args.Servers}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrNotLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	opArgs := &Op{OpType: OPLeave, Seq: args.Seq, Identifier: args.Identifier, GIDs: args.GIDs}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrNotLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	opArgs := &Op{OpType: OPMove, Seq: args.Seq, Identifier: args.Identifier, Shard: args.Shard, GID: args.GID}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrNotLeader {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	opArgs := &Op{OpType: OPQuery, Seq: args.Seq, Identifier: args.Identifier, Num: args.Num}
	res := sc.HandleOp(opArgs)
	reply.Err = res.Err
	if res.Err == ErrNotLeader {
		reply.WrongLeader = true
	}
	reply.Config = res.Config
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

func (sc *ShardCtrler) ApplyHandler() {
	for !sc.killed() {
		log := <-sc.applyCh
		if log.CommandValid {
			op := log.Command.(Op)
			sc.mu.Lock()

			// ControlerLog("%v ApplyHandler 收到log: identifier %v Seq %v, type %v", sc.me, op.Identifier, op.Seq, op.OpType)

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
				if op.OpType != OPQuery {
					ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s, Shard=%v) 操作前的config[%v].Shards=%+v\n", sc.me, op.Identifier, op.Seq, op.OpType, op.Shard, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
					// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 操作前的config[%v].Shards=%+v\n", sc.me, op.Identifier, op.Seq, op.OpType, op.Servers, op.GIDs, op.Shard, op.GID, op.Num, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
				}
				res = sc.ConfigExecute(&op)
				if op.OpType != OPQuery {
					ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s, Shard=%v) 操作后的config[%v].Shards=%+v\n", sc.me, op.Identifier, op.Seq, op.OpType, op.Shard, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
					// ControlerLog("%v HandleOp: identifier %v Seq %v 请求: %s,Servers=%+v Gids=%+v, Shard=%v, Gid=%v, Num=%v) 结束后的config[%v].Shards=%+v\n", sc.me, op.Identifier, op.Seq, op.OpType, op.Servers, op.GIDs, op.Shard, op.GID, op.Num, len(sc.configs)-1, sc.configs[len(sc.configs)-1].Shards)
				}

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
							ControlerLog("leader %v ApplyHandler: 发现 identifier %v Seq %v 的管道不存在, 应该是超时被关闭了", sc.me, op.Identifier, op.Seq)
						}
					}()
					res.ResTerm = log.SnapshotTerm

					*ch <- res
				}()
			}
		}
	}
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
