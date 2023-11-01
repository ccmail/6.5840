package mr

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	Tasks           []*Task
	WgMap, WgReduce sync.WaitGroup
	RwMutex         sync.RWMutex
	AllDone         bool
	Ctx             context.Context
	CtxFunc         context.CancelFunc
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.RwMutex.Lock()
	defer c.RwMutex.Unlock()
	return c.AllDone
}

func (c *Coordinator) RegisterUpdateTaskStatus(req *UpdateReq, resp *UpdateResp) error {
	c.RwMutex.Lock()
	defer c.RwMutex.Unlock()

	task := c.Tasks[req.TaskId]
	if task == nil {
		return nil
	}
	// 只有处于分发状态的才更改
	if task.Status == TaskAssign {
		task.Status = req.TaskStatus
		if req.TaskStatus == TaskComplete {
			if req.Phase == PhaseMap {
				c.WgMap.Done()
			} else {
				c.WgReduce.Done()
			}
		}
	}
	return nil
}

// RegisterAskTask 等待被worker远程调用
func (c *Coordinator) RegisterAskTask(req *AskTaskReq, resp *AskTaskResp) error {
	c.RwMutex.Lock()
	defer c.RwMutex.Unlock()
	if c.AllDone {
		resp.AllTaskDone = true
		return nil
	}

	for _, task := range c.Tasks {
		if task.Status != TaskInit {
			continue
		}

		task.Status, task.WorkerId = TaskAssign, req.WorkerId
		resp.Task = task
		resp.Ctx, resp.CtxCancelFunc = context.WithTimeout(context.Background(), 10*time.Second)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 这里首先传入了所有的文件, 最多有nReduce个worker去进行reduce操作
	// 根据c的状态判断是否完成所有任务, 所有任务的完成标准是, 所有的worker结束工作
	c := Coordinator{}
	c.Tasks = make([]*Task, 0, len(files))

	//此时是等待map完成
	c.WgMap.Add(len(files))
	go func() {
		c.RwMutex.Lock()
		defer c.RwMutex.Unlock()
		for i, fileName := range files {
			i, fileName := i, fileName
			c.Tasks = append(c.Tasks, &Task{
				File:      fileName,
				Phase:     PhaseMap,
				Status:    TaskInit,
				ReduceNum: nReduce,
				TaskId:    i,
			})
		}
	}()

	c.WgReduce.Add(nReduce)
	go func() {
		// 所有map完成后才可以发放reduce任务
		c.WgMap.Wait()

		c.RwMutex.Lock()
		defer c.RwMutex.Unlock()

		c.Tasks = make([]*Task, 0, nReduce)
		for i := 0; i < nReduce; i++ {
			intermediatesNames := make([]string, len(files))
			for j := 0; j < len(files); j++ {
				// 拼接中间文件名字, intermediate-fileID-reduceID
				intermediatesNames = append(intermediatesNames, fmt.Sprintf("%s-%d-%d", Intermediate, j, i))
			}
			c.Tasks = append(c.Tasks, &Task{
				TaskId:    i,
				Phase:     PhaseReduce,
				Status:    TaskInit,
				ReduceNum: nReduce,
				FilesName: intermediatesNames,
			})
		}

	}()
	// 所有完成的信号
	go func() {
		c.WgReduce.Wait()
		c.RwMutex.Lock()
		defer c.RwMutex.Unlock()
		c.AllDone = true
	}()

	c.server()
	return &c
}
