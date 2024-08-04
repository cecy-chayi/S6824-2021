package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type TaskType int

const (
	MAP TaskType = iota
	REDUCE
	QUIT
)

type GetTaskArg struct {
}

type GetTaskReply struct {
	Task          TaskType
	TaskId        int
	Filenames	  []string
	NReduce       int
	OK			  bool
}

type GetTaskErr struct {
	TaskType    TaskType
	TaskId      int
	ErrorReason string
}

type FinishTaskArg struct {
	Task        TaskType
	TaskId      int
	RetFilenames []string
}

type FinishTaskReply struct {
	OK bool
}

func (er GetTaskErr) Error() string {
	res := er.ErrorReason
	if er.TaskType == MAP {
		res = "MAP: " + strconv.Itoa(er.TaskId) + " " + res
	} else if er.TaskType == REDUCE {
		res = "REDUCE: " + strconv.Itoa(er.TaskId) + " " + res
	}
	return res
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
