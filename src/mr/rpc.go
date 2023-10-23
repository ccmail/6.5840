package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	TaskInit = iota
	TaskAssign
	TaskComplete
	Intermediate = "intermediate"
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type AskTaskReq struct {
	// 0代表map, 1代表reduce
	taskCategory int
	workerId     int
}

type AskTaskResp struct {
	// 0代表map, 1代表reduce
	TaskCategory int
	FileName     string
	MpTask       *MapTask
	RedTask      *ReduceTask
}

// MapTask map任务通过文件划分
type MapTask struct {
	TaskId     int
	FileName   string
	TaskStatus int
	timer      *time.Timer
}

// ReduceTask 分发reduce任务, 每个任务处理
type ReduceTask struct {
	TaskId                 int
	TaskStatus             int
	TargetIntermediateFile []string
	timer                  *time.Timer
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
