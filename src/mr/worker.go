package mr

import (
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"io/ioutil"
	"os"
	"path/filepath"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//

var (
	MapTaskFinish    = 0
	ReduceTaskFinish = 0
)

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 获取任务
	task := AskTask()

	// 判断任务类型并执行
	switch task.taskType {
	case MapTask:
		logrus.Info("---DoMapTask---", task.ID)
		DoMapTask(task, mapf)
		MarkTaskFinish(task)
	case ReduceTask:
		logrus.Info("---DoReduceTask---", task.ID)
		DoReduceTask(task, reducef)
		MarkTaskFinish(task)
	default:
		print()
	}
}

func MarkTaskFinish(task *Task) {

	ok := call("Coordinator.FinishTask", task, nil)
	if ok {
		// reply.Y should be 100.
		logrus.Info("call MarkTaskFinish %v", task.ID)
	} else {
		logrus.Warn("call MarkTaskFinish failed!")
	}
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	reduceFileName := fmt.Sprintf("mr-*-%d", task.ID)
	// 遍历当前目录下所有命名匹配reduceFileName的文件名
	// 以reduce任务为粒度，合并统计
	matchFiles, _ := filepath.Glob(reduceFileName)
	for _, matchFile := range matchFiles {
		file, err := os.Open(matchFile)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", file)
		}
		file.Close()
		var kva []KeyValue
		err = json.Unmarshal(content, &kva)
		if err != nil {
			logrus.Error("unmarshal error", kva)
			continue
		}
		intermediate = append(intermediate, kva...)
	}
	oname := fmt.Sprintf("mr-out-%d", task.ID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
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

	ofile.Close()

}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {

	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		for _, key := range kva {
			tmpFileName := fmt.Sprintf("mr-%d-%d", task.ID, ihash(key.Key)%task.reduceNum)
			ofile, _ := os.Create(tmpFileName)
			defer ofile.Close()
			enc := json.NewEncoder(ofile)
			err := enc.Encode(&kva)
			if err != nil {
				logrus.Errorf("encode error,err:%v,task.Id:%v,kv:%v", err, task.ID, kva)
				continue
			}
		}

	}
}

func AskTask() *Task {
	task := Task{}
	ok := call("Coordinator.AssignTask", nil, &task)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", task.ID)
	} else {
		fmt.Printf("call AssignTask failed!\n")
	}
	return &task
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
