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
)

type Coordinator struct {
	// Your definitions here.
	mapFiles            []string
	mapTaskStates       []string
	reduceTaskStates    []string
	mapTaskTimestamp    []int64
	reduceTaskTimestamp []int64
	nMapTasksDone       int
	nReduceTasksDone    int
	lock                sync.Mutex
}

const workerTimeout = 20

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {

	for !c.Done() {
		c.lock.Lock()
		if !c.hasAllMapTasksDone() {
			for i, filename := range c.mapFiles {
				if c.mapTaskStates[i] == "Pending" {
					c.mapTaskStates[i] = "Running"
					c.mapTaskTimestamp[i] = time.Now().Unix()
					reply.TaskID = i
					reply.NReduce = len(c.reduceTaskStates)
					reply.TaskType = 1
					reply.Filename = filename
					c.lock.Unlock()
					return nil
				}
			}
		} else {
			for j, _ := range c.reduceTaskStates {
				if c.reduceTaskStates[j] == "Pending" {
					c.reduceTaskStates[j] = "Running"
					c.reduceTaskTimestamp[j] = time.Now().Unix()
					reply.TaskID = j
					reply.NReduce = len(c.reduceTaskStates)
					reply.TaskType = 2
					reply.Filename = fmt.Sprintf("mr-*-%d", j)
					c.lock.Unlock()
					return nil
				}
			}
		}
		c.lock.Unlock()

		time.Sleep(100 * time.Millisecond)
	}

	return &RPCError{"No rest task"}
}

func (c *Coordinator) NotifyMapDone(args *NotifyMapDoneArgs, reply *NotifyMapDoneReply) error {
	c.lock.Lock()
	if c.mapTaskStates[args.TaskID] == "Running" {
		c.mapTaskStates[args.TaskID] = "Done"
		c.nMapTasksDone++
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) NotifyReduceDone(args *NotifyReduceDoneArgs, reply *NotifyReduceDoneReply) error {
	c.lock.Lock()
	if c.reduceTaskStates[args.TaskID] == "Running" {
		c.reduceTaskStates[args.TaskID] = "Done"
		c.nReduceTasksDone++
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) hasAllMapTasksDone() bool {
	return c.nMapTasksDone == len(c.mapFiles)
}

// Sweep thread
func (c *Coordinator) sweep() {
	go func() {
		for {
			c.lock.Lock()
			// scan all tasks, determine which of them are dead
			for i := range c.mapTaskStates {
				if c.mapTaskStates[i] == "Running" {
					deltaSecond := time.Now().Unix() - c.mapTaskTimestamp[i]
					if deltaSecond >= workerTimeout {
						fmt.Printf("sweep map tesk %d\n", i)
						c.mapTaskStates[i] = "Pending"
					}
				}
			}
			for i := range c.reduceTaskStates {
				if c.reduceTaskStates[i] == "Running" {
					deltaSecond := time.Now().Unix() - c.reduceTaskTimestamp[i]
					if deltaSecond >= workerTimeout {
						fmt.Printf("sweep reduce tesk %d\n", i)
						c.reduceTaskStates[i] = "Pending"
					}
				}
			}
			c.lock.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()
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
	c.lock.Lock()
	ret = c.nReduceTasksDone == len(c.reduceTaskStates)
	c.lock.Unlock()

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
	c.mapFiles = files
	c.mapTaskStates = make([]string, len(files))
	for i, _ := range c.mapTaskStates {
		c.mapTaskStates[i] = "Pending"
	}
	c.reduceTaskStates = make([]string, nReduce)
	for i, _ := range c.reduceTaskStates {
		c.reduceTaskStates[i] = "Pending"
	}
	c.mapTaskTimestamp = make([]int64, len(files))
	c.reduceTaskTimestamp = make([]int64, nReduce)
	c.nMapTasksDone = 0
	c.nReduceTasksDone = 0

	c.sweep()
	c.server()
	return &c
}
