package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RPCError struct {
	errMsg string
}

func (e *RPCError) Error() string {
	return e.errMsg
}

type AssignTaskArgs struct {
}

type AssignTaskReply struct {
	TaskID   int
	NReduce  int
	TaskType int
	Filename string
}

type NotifyMapDoneArgs struct {
	TaskID int
}

type NotifyMapDoneReply struct {
}

type NotifyReduceDoneArgs struct {
	TaskID int
}

type NotifyReduceDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
