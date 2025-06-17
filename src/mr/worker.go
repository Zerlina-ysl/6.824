package mr

import (
	"encoding/json"
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

var fileFormatter *prefixed.TextFormatter // 文件输出格式

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

	initLog()

	for {
		// 获取任务
		task := AskTask()
		logrus.Infof("ask task ,task.ID:%v,task.Type:%v,task.file:%v,reduceNum:%v,StartTime:%v",
			task.ID, task.TaskType, task.File, task.ReduceNum, task.StartTime)
		if task.Status == Finish {
			logrus.Errorf("task has already finish,taskId:%v", task.ID)
			continue
		}
		// 判断任务类型并执行
		switch task.TaskType {
		case MapTask:
			logrus.Info("---DoMapTask---", task.ID)
			err := DoMapTask(task, mapf)
			if err != nil {
				continue
			}
			MarkTaskFinish(task)
		case ReduceTask:
			logrus.Info("---DoReduceTask---", task.ID)
			err := DoReduceTask(task, reducef)
			if err != nil {
				continue
			}
			MarkTaskFinish(task)
		case ExitTask:
			logrus.Info("---ExitTask---")
			return
		default:
			logrus.Info("---UnknownTask---", task.TaskType)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func initLog() {
	logPath, _ := os.Getwd()
	// 按照pid输出
	logName := fmt.Sprintf("%s/worker.%d.%s.", logPath, os.Getpid(), time.Now().Format("20060102-150405"))
	writer, _ := rotatelogs.New(logName + "%Y%m%d")

	fileFormatter = &prefixed.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02.15:04:05.000000",
		ForceFormatting: true,
		ForceColors:     true,
		DisableColors:   true,
	}

	logrus.SetFormatter(fileFormatter)
	logrus.SetLevel(logrus.DebugLevel)
	lfHook := lfshook.NewHook(lfshook.WriterMap{
		logrus.InfoLevel:  writer,
		logrus.DebugLevel: writer,
		logrus.ErrorLevel: writer,
		logrus.FatalLevel: writer,
	}, fileFormatter)

	logrus.AddHook(lfHook)
	logrus.SetOutput(io.Discard)

	logrus.Info("init log ....")
}

func MarkTaskFinish(task *Task) {

	ok := call("Coordinator.FinishTask", &task, &task)
	if ok {
		// reply.Y should be 100.
		logrus.Info("call FinishTask ", task.ID)
	} else {
		logrus.Warn("call FinishTask failed!")
	}
}

func DoReduceTask(task *Task, reducef func(string, []string) string) error {
	intermediate := []KeyValue{}
	reduceFileName := fmt.Sprintf("mr-*-%d", task.ID%task.ReduceNum)
	// 遍历当前目录下所有命名匹配reduceFileName的文件名
	// 以reduce任务为粒度，合并统计
	matchFiles, _ := filepath.Glob(reduceFileName)
	for _, matchFile := range matchFiles {

		file, err := os.Open(matchFile)
		if err != nil {
			logrus.Fatalf("cannot open %v,err:%v", file, err)
			return err
		}

		defer file.Close()
		decoder := json.NewDecoder(file)
		var kva []KeyValue
		for decoder.Decode(&kva) == nil {
			intermediate = append(intermediate, kva...)
		}
	}
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.ID%task.ReduceNum)
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
	return nil
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) error {
	filename := task.File
	file, err := os.Open(filename)
	if err != nil {
		logrus.Fatalf("cannot open %v,err:%v", filename, err)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logrus.Fatalf("cannot read %v,err:%v", filename, err)
		return err
	}
	file.Close()
	kva := mapf(filename, string(content))
	fileNameKvMap := make(map[string][]KeyValue)

	for _, key := range kva {
		tmpFileName := fmt.Sprintf("mr-%d-%d", task.ID, ihash(key.Key)%task.ReduceNum)
		fileNameKvMap[tmpFileName] = append(fileNameKvMap[tmpFileName], key)
	}
	for fileName, writeKv := range fileNameKvMap {
		logrus.Infof("DoMapTask,taskId:%v,fileName:%v,writeKv:%v", task.ID, fileName, writeKv)
		ofile, err := os.Create(fileName)
		if err != nil {
			logrus.Errorf("create error,err:%v,tmpFileName:%v", err, fileName)
			return err
		}
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		err = enc.Encode(writeKv)
		if err != nil {
			logrus.Errorf("encode error,err:%v,task.Id:%v,kv:%v", err, task.ID, writeKv)
			ofile.Close()
			return err
		}
		ofile.Close()
	}
	return nil
}

func AskTask() *Task {
	task := &Task{}
	ok := call("Coordinator.AssignTask", &task, &task)
	if ok {
		logrus.Info("call Coordinator.AssignTask")
	} else {
		logrus.Info("call AssignTask failed!")
	}
	return task
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
