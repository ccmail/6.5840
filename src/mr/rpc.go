package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"context"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	TaskInit = iota
	TaskAssign
	TaskComplete
	PhaseMap    = "Task_phase_map"
	PhaseReduce = "Task_phase_reduce"

	IntermediaFileNamePrefix = "mr-mid"
)

type DoneReq struct {
}

type DoneResp struct {
	Done bool
}

type UpdateReq struct {
	WorkerId, TaskId, TaskStatus int
	Phase                        string
}

type UpdateResp struct {
}

// Add your RPC definitions here.
type AskTaskReq struct {
	// 0代表map, 1代表reduce
	TaskCategory int
	WorkerId     int
}

type AskTaskResp struct {
	// 0代表map, 1代表reduce
	TaskCategory  int
	FileName      string
	Task          *Task
	AllTaskDone   bool
	Ctx           context.Context
	CtxCancelFunc context.CancelFunc
	//MpTask       *MapTask
	//RedTask      *ReduceTask
}

type Task struct {
	File              string
	Phase             string
	Status, ReduceNum int
	TaskId, WorkerId  int
	timer             *time.Timer
	FilesName         []string
}

/*
type ParentTask struct {
	TaskId, WorkerId int
	TaskStatus       int
	Timer            *time.Timer
	Ctx              context.Context
	ctxFunc          context.CancelFunc
}

// MapTask map任务通过文件划分
type MapTask struct {
	FileName string
	NReduce  int
	ParentTask
}

// ReduceTask 分发reduce任务, 每个任务处理
type ReduceTask struct {
	TargetIntermediateFile []string
	ParentTask
}*/

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
