package mr

import (
	"fmt"
	"io"
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
	MapTasks        []*MapTask
	ReduceTasks     []*ReduceTask
	MapDone         bool
	MapWg, ReduceWg sync.WaitGroup
	RwMutex         sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 等待被worker远程调用
func (c *Coordinator) AskTask(req *AskTaskReq, resp *AskTaskResp) error {
	c.RwMutex.Lock()
	defer c.RwMutex.Unlock()

	// 这里请求任务, 首先判断map任务是否完成
	if !c.MapDone {
		for _, task := range c.MapTasks {
			task := task
			if task.TaskStatus != TaskInit {
				continue
			}
			task.TaskId = req.workerId
			task.TaskStatus = TaskAssign
			resp.MpTask = task

			// 启动超时定时器
			if task.timer != nil {
				task.timer.Stop()
				task.timer = nil
			}
			task.timer = time.AfterFunc(10*time.Second, func() {
				c.RwMutex.Lock()
				defer c.RwMutex.Unlock()
				// 超时丢弃任务
				if task.TaskStatus != TaskComplete {
					task.TaskStatus = TaskInit
				}
			})
			c.MapWg.Add(1)
		}
	} else {
		// 分发reduce
		for _, task := range c.ReduceTasks {
			task := task
			if task.TaskStatus != TaskInit {
				continue
			}
			task.TaskStatus = TaskAssign
			task.TaskId = req.workerId
			resp.RedTask = task
			// 启动超时定时器
			if task.timer != nil {
				task.timer.Stop()
				task.timer = nil
			}
			task.timer = time.AfterFunc(10*time.Second, func() {
				c.RwMutex.Lock()
				defer c.RwMutex.Unlock()
				// 超时丢弃任务
				if task.TaskStatus != TaskComplete {
					task.TaskStatus = TaskInit
				}
			})
		}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() (ret bool) {

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 这里首先传入了所有的文件, 最多有nReduce个worker去进行reduce操作
	// 根据c的状态判断是否完成所有任务, 所有任务的完成标准是, 所有的worker结束工作
	c := Coordinator{}

	c.MapTasks = make([]*MapTask, 0, len(files))
	c.ReduceTasks = make([]*ReduceTask, 0, nReduce)
	// 将所有文件设置为任务, 添加到task数组中, 等待后续分发
	for i, fileName := range files {
		i, fileName := i, fileName
		c.MapTasks = append(c.MapTasks, &MapTask{i, fileName, TaskInit})
	}
	// 添加reduce任务
	for i := 0; i < nReduce; i++ {
		tasks := make([]string, len(c.MapTasks))
		for j := 0; j < len(c.MapTasks); j++ {
			// 拼接中间文件名字, intermediate-fileID-reduceID
			tasks = append(tasks, fmt.Sprintf("%s-%d-%d", Intermediate, j, i))
		}
		c.ReduceTasks = append(c.ReduceTasks, &ReduceTask{
			TaskId:                 i,
			TaskStatus:             TaskInit,
			TargetIntermediateFile: tasks,
		})
	}
	//应该是通过rpc协议, 向worker发送要读取的文件名
	//首先是读取任务
	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("打开文件%v出现了错误", fileName)
		}
		// 这里要考虑怎么对任务进行分块, 将其分配给worker
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("读取文件%v的内容时出现了错误", fileName)
		}
		file.Close()
		//	这里需要将map任务分配给worker. 怎么分配给worker?

	}
	// Your code here.

	c.server()
	return &c
}
