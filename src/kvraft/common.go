package kvraft

type Mode int64

const (
    Mode_Modify Mode = 1
    Mode_Report Mode = 2
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
    SameCommand    = "SameCommand"
    ErrTimeout     = "ErrTimeout"
    ErrTerm        = "ErrTerm"
    ErrIndex       = "ErrIndex"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    Task_Id uint32
    Mode    Mode
    Term    int
    Index   int
}

type PutAppendReply struct {
	Err Err
    Term int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
    Term int
}

type GetReply struct {
	Err   Err
	Value string
    Term  int
}
