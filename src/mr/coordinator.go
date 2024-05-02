package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	//"time"
)

type Coordinator struct {
    // Your definitions here.
    filenames           []string 
    nReduce             int
    reduce_tasks        []int
    workerIDs           []int
    doing_tasks         map[int]string
    mutex               sync.Mutex
    check_mutex         sync.Mutex

    map_tasks_done      bool
    reduce_tasks_done   bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkersCall(args *WorkerArgs, reply *WorkerReply) error {
    //为新的worker分配workerID
    if args.WorkerID == 0 {
        reply.NReduce = c.nReduce

        c.mutex.Lock() //防止workerIDs被多个worker同时修改

        if len(c.workerIDs) == 0 {
            c.workerIDs = append(c.workerIDs, 1)
            reply.WorkerID = 1
        } else {
            i := 1
            for i < len(c.workerIDs) + 1 {
                if c.workerIDs[i - 1] != i && i != 0{
                    var temp []int
                    temp = append(temp, c.workerIDs[:i - 1]...)
                    temp = append(temp, i)
                    temp = append(temp, c.workerIDs[i - 1:]...)
                    c.workerIDs = temp
                    reply.WorkerID = i
                    break
                } else if c.workerIDs[i - 1] != i && i == 0 {
                    c.workerIDs = append([]int{i}, c.workerIDs...)
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
    } else {
        c.mutex.Lock()

        have := 0
        for i := 0; i < len(c.workerIDs); i++ {
            if c.workerIDs[i] == args.WorkerID {
                have = 1
                break
            }
        }

        if have == 0 {
            //fmt.Println("workerID:", args.WorkerID, "not exist")
            reply.WorkerID = 0

            c.mutex.Unlock()
            return nil
        }

        if args.Ok == false {
            //fmt.Println("workerID:", args.WorkerID, "task:", args.Task, "failed")
            if args.Task[0] == 'r' {
                c.reduce_tasks = append(c.reduce_tasks, int(args.Task[6] - '0'))
                c.nReduce++
            } else {
                c.filenames = append(c.filenames, args.Task)
            }

            delete(c.doing_tasks, args.WorkerID)

            c.mutex.Unlock()
            return nil
        }

        //fmt.Println("done task:", args.WorkerID, c.doing_tasks[args.WorkerID])
        delete(c.doing_tasks, args.WorkerID)

        c.mutex.Unlock()
    }

    //为worker分配任务
    c.mutex.Lock()

    reply.Task = "sleep"

    if c.map_tasks_done == false {
        if len(c.filenames) > 0 {
            filename := c.filenames[len(c.filenames) - 1]
            c.filenames = c.filenames[:len(c.filenames) - 1]

            if args.WorkerID == 0 {
                c.doing_tasks[reply.WorkerID] = filename
                go c.assignTaskTimeout(reply.WorkerID, filename)
            } else {
                c.doing_tasks[args.WorkerID] = filename
                go c.assignTaskTimeout(args.WorkerID, filename)
            }

            reply.Filename = filename
            reply.Task = "map"

        } else {
            if len(c.doing_tasks) == 0 && len(c.doing_tasks) == 0 {
                c.map_tasks_done = true
            }
        }
    }

    if c.reduce_tasks_done == false && c.map_tasks_done == true {
        if c.nReduce > 0 {
            reply.XReduce = c.reduce_tasks[c.nReduce - 1] + 1

            reduce_task := fmt.Sprintf("reduce%v",c.reduce_tasks[c.nReduce - 1])
            if args.WorkerID == 0 {
                c.doing_tasks[reply.WorkerID] = reduce_task
            } else {
                c.doing_tasks[args.WorkerID] = reduce_task
            }

            c.reduce_tasks = c.reduce_tasks[:len(c.reduce_tasks) - 1]
            c.nReduce--
            reply.Task = "reduce"

            if args.WorkerID == 0 {
                go c.assignTaskTimeout(reply.WorkerID, reduce_task)
            } else {
                go c.assignTaskTimeout(args.WorkerID, reduce_task)
            }


        } else if len(c.doing_tasks) == 0 {
            c.reduce_tasks_done = true
            reply.Task = "done"
        }

        c.mutex.Unlock()
        return nil
    }

    if c.reduce_tasks_done == true {
        reply.Task = "done"
    }

    c.mutex.Unlock()

    return nil
}

func (c *Coordinator) assignTaskTimeout(workerID int, task string) error {
    timeoutDuration := 10 * time.Second
    <-time.After(timeoutDuration)

    if c.doing_tasks[workerID] == task {
        c.check_mutex.Lock()

        //删除超时workerID
        for i := 0; i < len(c.workerIDs); i++ {
            if c.workerIDs[i] == workerID {
                c.workerIDs = append(c.workerIDs[:i], c.workerIDs[i+1:]...)
                break
            }
        }

        //fmt.Println("workerID:", workerID, "task:", task, "timeout")
        //fmt.Println("workerIDs:", c.workerIDs)

        if task[0] == 'r' {
            c.reduce_tasks = append(c.reduce_tasks, int(task[6] - '0'))
            c.nReduce++
        } else {
            c.filenames = append(c.filenames, task)
        }

        delete(c.doing_tasks, workerID)

        c.check_mutex.Unlock()
    }

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

    //fmt.Println("map_tasks_done:", c.map_tasks_done)
    //fmt.Println("reduce_tasks_done:", c.reduce_tasks_done)
    //fmt.Println("doing_tasks:", c.doing_tasks)

    if c.map_tasks_done == true && c.reduce_tasks_done == true && len(c.doing_tasks) == 0 {
        ret = true
    }

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

    for i := 0; i < nReduce; i++ {
        c.reduce_tasks = append(c.reduce_tasks, i)
    }

    c.map_tasks_done = false
    c.reduce_tasks_done = false
    c.doing_tasks = make(map[int]string)

    /*
    go func() {
        for {
            fmt.Println("doing_tasks:", c.doing_tasks)
            fmt.Println("c.workerIDs:", c.workerIDs)
            time.Sleep(1 * time.Second)
        }
    }()
    */

    /*
    go func() {
        for {
            if len(c.doing_tasks) == 0 {
                fmt.Println("c.map_tasks_done:", c.map_tasks_done, "c.reduce_tasks_done:", c.reduce_tasks_done)
                fmt.Println("c.workerIDs:", c.workerIDs)
            }
            time.Sleep(1 * time.Second)
        }
    }()
    */

    c.server()
    return &c
}
