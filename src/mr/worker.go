package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CallIsDone() bool {
	time.Sleep(500 * time.Millisecond)
	resp := DoneResp{}
	ok := call("Coordinator.IsDone", &DoneReq{}, &resp)
	if !ok {
		fmt.Printf("查看所有任务是否完成时失败...")
	}
	return resp.Done
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	idx := 0
	for !CallIsDone() {
		//log.Printf("开始执行第%d次worker任务\n", idx)
		idx++
		taskResp := CallAskTask()
		if taskResp == nil || taskResp.Task == nil {
			//if taskResp.AllTaskDone {
			//	log.Printf("所有的任务都做完了, break掉\n")
			//	break
			//}
			//if taskResp == nil {
			//	log.Println("taskResp是空的")
			//} else {
			//	log.Println("taskResp.Task是空的")
			//}
			//log.Printf("因为call调用得到空值所以提前结束\n")
			continue
		}
		ta := taskResp.Task
		//go func(task *Task) {
		// 启动协程会导致worker启动多个协程执行map, 每个协程执行时会同时操作一个文件
		switch ta.Phase {
		case PhaseMap:
			HandleMapTask(ta, mapf)
		case PhaseReduce:
			HandleReduceTask(ta, reducef)
		}
		//log.Println("实际上已经执行完任务, 开始尝试状态更新...")
		CallUpdateTaskChan(ta.TaskId, TaskComplete, ta.Phase)
		//log.Printf("更新了%v人物的状态", task.Phase)
		//}(taskResp.Task)
	}
}

func CallUpdateTaskChan(taskId, status int, phase string) {

	req := UpdateReq{
		Phase:      phase,
		TaskId:     taskId,
		TaskStatus: status,
		WorkerId:   os.Getpid(),
	}
	ok := call("Coordinator.RegisterUpdateTaskStatus", &req, &UpdateResp{})
	if !ok {
		fmt.Printf("更新任务状态时调用rpc失败")
	}
}

func CallAskTask() *AskTaskResp {
	// 取出task
	req := AskTaskReq{}
	req.WorkerId = os.Getpid()
	resp := AskTaskResp{}
	ok := call("Coordinator.RegisterAskTask", &req, &resp)
	if !ok {
		fmt.Printf("请求任务时调用rpc失败...")
	}
	return &resp
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// 远程调用函数
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatalf("dialing:%v \n", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("call function error: %v", err)
	return false
}

func HandleMapTask(task *Task, mapf func(string, string) []KeyValue) {
	fileName := task.File
	//首先是读取任务
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("打开文件%v出现了错误", fileName)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("读取文件%v的内容时出现了错误", fileName)
	}
	defer file.Close()

	KvRes := mapf(fileName, string(content))
	sort.Slice(KvRes, func(i, j int) bool {
		return KvRes[i].Key < KvRes[j].Key
	})

	nReduce := task.ReduceNum
	encoderRes := make([]*json.Encoder, 0, nReduce)
	// 分区
	for i := 0; i < nReduce; i++ {
		//创建中间文件
		currName := fmt.Sprintf("%s-%d-%d", IntermediaFileNamePrefix, task.TaskId, i)
		currFile, err := os.Create(currName)
		if err != nil {
			log.Fatalf("创建文件map中间结果, %s时失败, %v", currName, err)
		}
		// 向文件中写入encode数据的文件句柄 的容器
		encoderRes = append(encoderRes, json.NewEncoder(currFile))
	}
	for _, kv := range KvRes {
		// 根据hash值向中间文件中写入结果
		hIdx := ihash(kv.Key) % nReduce
		if err := encoderRes[hIdx].Encode(&kv); err != nil {
			log.Fatalf("对json编码后的文件哈希的时侯失败, %v", err)
		}
	}
}

func HandleReduceTask(task *Task, reducef func(string, []string) string) {
	files := task.FilesName
	nFiles := len(files)
	intermediate := make([]KeyValue, 0, nFiles)
	//log.Printf("当前worker%d处理reduce, 这里打印下所有中间文件名,%v\n", task.WorkerId, files)
	//log.Println(files)
	for _, currName := range files {
		currFile, err := os.Open(currName)
		if err != nil {
			log.Println("因为中间文件打开会失败, 这里打印下所有中间文件名")
			log.Println(files)
			log.Fatalf("中间文件{%s}打开失败, 可能是不存在", currName)
		}
		dec := json.NewDecoder(currFile)
		kv := KeyValue{}
		for {
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("对中间文件%s解码时出现了错误: %e", currName, err)
			}
			kv := kv
			intermediate = append(intermediate, kv)
		}
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, err := os.Create(oname)
	defer ofile.Close()

	if err != nil {
		log.Fatalf("创建文件%s时出现问题: %e", oname, err)
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}
