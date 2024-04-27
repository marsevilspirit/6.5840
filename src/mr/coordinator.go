package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

import "fmt"
import "sync"


type Coordinator struct {
    // Your definitions here.
    filenames []string 
    nReduce int
    workerIDs []int
    tasks map[int]string
    mutex sync.Mutex

    map_tasks_done bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkersCall(args *WorkerArgs, reply *WorkerReply) error {
    //为新的worker分配workerID
    if args.WorkerID == 0 {

        c.mutex.Lock() //防止workerIDs被多个worker同时修改

        if len(c.workerIDs) == 0 {
            c.workerIDs = append(c.workerIDs, 1)
            reply.WorkerID = 1
        } else {
            i := 1
            for i < len(c.workerIDs) + 1 {
                if c.workerIDs[i - 1] != i {
                    var temp []int
                    temp = append(temp, c.workerIDs[:i]...)
                    temp = append(temp, i)
                    temp = append(temp, c.workerIDs[i:]...)
                    c.workerIDs = temp
                    reply.WorkerID = i
                    break
                }
                i++
            }

            if i == len(c.workerIDs) + 1 {
                c.workerIDs = append(c.workerIDs, i)
                reply.WorkerID = i
            }
        }

        c.mutex.Unlock()

        fmt.Println(c.workerIDs)
    }

    //为worker分配任务
    c.mutex.Lock()

    if c.map_tasks_done == false {
        filename := c.filenames[len(c.filenames) - 1]
        c.filenames = c.filenames[:len(c.filenames) - 1]

        c.tasks[args.WorkerID] = filename
    }

    c.mutex.Unlock()

    return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
    reply.Y = args.X + 1
    return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
    rpc.Register(c)
    rpc.HandleHTTP()
    //l, e := net.Listen("tcp", ":1234")
    sockname := coordinatorSock()
    os.Remove(sockname)
    l, e := net.Listen("unix", sockname)
    if e != nil {
        log.Fatal("listen error:", e)
    }
    go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
    ret := false 

    // Your code here.


    return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    c := Coordinator{}


    // Your code here.

    c.filenames = files
    c.nReduce = nReduce
    c.map_tasks_done = false

    c.server()
    return &c
}
