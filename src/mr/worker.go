package mr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		taskResp := CallAskTask()
		if taskResp == nil || taskResp.Task == nil {
			continue
		}
		if taskResp.AllTaskDone {
			break
		}
		go func(task *Task, ctx context.Context, cancelFunc context.CancelFunc) {
			defer cancelFunc()
			select {
			case <-ctx.Done():
				//	任务超时
				CallUpdateTaskChan(task.TaskId, TaskInit, task.Phase)
			default:
				switch task.Phase {
				case PhaseMap:
					HandleMapTask(task, mapf)
				case PhaseReduce:
					HandleReduceTask(task, reducef)
				}
				CallUpdateTaskChan(task.TaskId, TaskComplete, PhaseMap)
			}
		}(taskResp.Task, taskResp.Ctx, taskResp.CtxCancelFunc)
	}
}

func CallUpdateTaskChan(taskId, status int, phase string) {

	req := UpdateReq{
		Phase:      phase,
		TaskId:     taskId,
		TaskStatus: status,
		WorkerId:   os.Getpid(),
	}
	ok := call("Coordinate.RegisterUpdateTaskStatus", &req, &UpdateResp{})
	if !ok {
		fmt.Printf("更新任务状态时调用rpc失败")
	}
}

func CallAskTask() *AskTaskResp {
	// 取出task
	req := AskTaskReq{}
	req.WorkerId = os.Getpid()
	resp := AskTaskResp{}
	ok := call("Coordinate.RegisterAskTask", &req, &resp)
	if !ok {
		fmt.Printf("调用rpc失败")
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
		currName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		currFile, err := os.Create(currName)
		if err != nil {
			log.Fatalf("创建文件map中间结果, %s时失败, %v", currName, err)
		}
		// 向文件中写入encode数据的文件句柄 的容器
		encoderRes = append(encoderRes, json.NewEncoder(currFile))
		currFile.Close()
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
	for _, currName := range files {
		currFile, err := os.Open(currName)
		if err != nil {
			log.Fatalf("中间文件%s打开失败, 可能是不存在", currName)
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
		intermediate = append(intermediate)
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

	for i := 0; i < nFiles; i++ {
		j := i + 1
		for j < nFiles && intermediate[j].Key == intermediate[i].Key {
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
