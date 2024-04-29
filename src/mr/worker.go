package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
    Key   string
    Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
reducef func(string, []string) string) {

    // Your worker implementation here.
    args := WorkerArgs{};
    args.Ok = false;

    reply := WorkerReply{};

    // uncomment to send the Example RPC to the coordinator.
    //CallExample()//---------------------------------------------------------------------------------------------------

    for {
        ok := CallCoordinator(mapf, reducef, &args, &reply)
        if ok {
            args.WorkerID = reply.WorkerID
            args.Task = reply.Task
            args.Ok = true
        } else {
            os.Exit(1)
        }

        time.Sleep(time.Second)//我擦，tmd没这行indexer test还过不去？
    }
}

func CallCoordinator(mapf func(string, string) []KeyValue, reducef func(string, []string) string, args *WorkerArgs, reply *WorkerReply)  bool {

    ok := call("Coordinator.WorkersCall", &args, &reply)

    if reply.Task == "sleep" {
        //fmt.Printf("worker %v sleep\n", reply.WorkerID)
        return ok
    }

    if reply.Task == "map" {
        //fmt.Printf("worker %v map %v\n", reply.WorkerID, reply.Filename)

        file, err := os.Open(reply.Filename)
        if err != nil {
            log.Fatalf("cannot open %v", reply.Filename)
        }
        content, err := io.ReadAll(file)
        if err != nil {
            log.Fatalf("cannot read %v", reply.Filename)
        }
        defer file.Close()

        kva := mapf(reply.Filename, string(content))


        for _, kv := range kva {
            reduceTask := ihash(kv.Key) % reply.NReduce

            oname := fmt.Sprintf("mr-%v-%v", reduceTask, reply.WorkerID)
            ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)

            enc := json.NewEncoder(ofile)
            enc.Encode(&kv)

            ofile.Close()
        }
    }

    if reply.Task == "reduce" {
        //fmt.Printf("worker %v reduce %v\n", reply.WorkerID, reply.XReduce)

        kva := []KeyValue{}

        dir := "."

        matchname := fmt.Sprintf("mr-%v-*", reply.XReduce - 1)

        // 读取目录中的文件列表
        files, err := os.ReadDir(dir)
        if err != nil {
            fmt.Println("读取目录失败:", err)
        }

        // 遍历文件列表，查找符合条件的文件
        for _, file := range files {
            // 使用通配符匹配文件名
            matched, err := filepath.Match(matchname, file.Name())
            if err != nil {
                fmt.Println("匹配文件名失败:", err)
            }
            // 如果文件名匹配成功，则输出文件名
            if matched {
                filereader,err := os.Open(file.Name())
                if err != nil {
                    fmt.Println("打开文件失败:", err)
                }

                dec := json.NewDecoder(filereader)
                for {
                    var kv KeyValue
                    if err := dec.Decode(&kv); err != nil {
                        break
                    }
                    kva = append(kva, kv)
                }

                os.Remove(file.Name())
            }
        } 

      	sort.Sort(ByKey(kva))

        oname := fmt.Sprintf("mr-out-%v", reply.XReduce - 1)
        ofile, _ := os.Create(oname)

        i := 0
        for i < len(kva) {
            j := i + 1
            for j < len(kva) && kva[j].Key == kva[i].Key {
                j++
            }
            values := []string{}
            for k := i; k < j; k++ {
                values = append(values, kva[k].Value)
            }
            output := reducef(kva[i].Key, values)

            fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

            i = j
        }

        ofile.Close()
    }

    if reply.Task == "done" {
        os.Exit(0)
    }

    return ok
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

    // declare an argument structure.
    args := ExampleArgs{}

    // fill in the argument(s).
    args.X = 99

    // declare a reply structure.
    reply := ExampleReply{}

    // send the RPC request, wait for the reply.
    // the "Coordinator.Example" tells the
    // receiving server that we'd like to call
    // the Example() method of struct Coordinator.
    ok := call("Coordinator.Example", &args, &reply)
    if ok {
        // reply.Y should be 100.
        fmt.Printf("reply.Y %v\n", reply.Y)
    } else {
        fmt.Printf("call failed!\n")
    }
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
    // c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
    sockname := coordinatorSock()
    c, err := rpc.DialHTTP("unix", sockname)
    if err != nil {
        log.Fatal("dialing:", err)
    }
    defer c.Close()

    err = c.Call(rpcname, args, reply)
    if err == nil {
        return true
    }

    fmt.Println(err)
    return false
}
