package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	MAP_FILE_PREFIX    = "mr-map-"
	REDUCE_FILE_PREFIX = "mr-out-"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	//  send an RPC to the coordinator asking for a task

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	askFlag := true
	for askFlag {
		task := CallAssignTask()
		fmt.Printf("[INFO]get task from coornator,taskID=%d,taskType=%v,len(task.file)=%d\n", task.TaskID, task.TaskType, len(task.Input))
		switch task.TaskType {

		case MapTask:
			fmt.Println("-------------map----------------")
			DoMapTask(mapf, &task)
			MarkDone(&task)

		case ReduceTask:
			fmt.Println("-------------reduce----------------")
			DoReduceTask(reducef, &task)
			MarkDone(&task)

		case WaitingType:
			fmt.Println("-----------waitingType,sleep----------------")
			time.Sleep(200 * time.Millisecond)

		case ExitType:
			time.Sleep(200 * time.Millisecond)
			askFlag = false
			fmt.Println("-----------all task done------------------")
		}

	}

	time.Sleep(time.Second * 1)

}

func DoReduceTask(reducef func(string, []string) string, t *Task) {
	// 对reduce任务要处理的中间键排序
	fmt.Printf("Coordinator:[INFO] do reduce task,len(file)=%d", len(t.Input))
	intermediate := sortKV(t.Input)
	// 创建临时文件
	dir, _ := os.Getwd()
	tmpFile, _ := ioutil.TempFile(dir, "mr-temp-*")
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
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tmpFile.Close()
	// 写入成功后对临时文件重命名
	fn := REDUCE_FILE_PREFIX + strconv.Itoa(t.TaskID)
	os.Rename(tmpFile.Name(), fn)
}

func sortKV(input []string) []KeyValue {
	var kva []KeyValue
	for _, filename := range input {
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func DoMapTask(mapf func(string, string) []KeyValue, t *Task) {
	// 处理map任务
	intermediate := []KeyValue{}
	filename := t.Input[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("[FATAL] cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[FATAL]  cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// 在这里排序没有意义，因为根据key重新写入文件中
	//sort.Sort(ByKey(intermediate))

	// 将map输出写入文件 并指定运行的reduce任务
	rn := t.ReduceNum
	HashedKV := make([][]KeyValue, rn)
	for _, kv := range intermediate {
		// 根据key值进行hash分配
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {
		// 需要创建中间文件备份map输出，防止crash后所有数据丢失
		// mr-map-任务id-处理kv的reduce任务id
		oname := MAP_FILE_PREFIX + strconv.Itoa(t.TaskID) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("[FATAL]  intermediate Encode error,encode kv %v", kv)
			}
		}
		ofile.Close()
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

func CallAssignTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("[INFO] Coordinator.AssignTask task.id=%v,len(input)=%d\n", reply.TaskID, len(reply.Input))
	} else {
		fmt.Printf("[ERROR] Coordinator.AssignTask call failed!\n")
	}
	return reply
}

func MarkDone(t *Task) Task {
	args := t
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("Worker:[INFO] Coordinator.MarkFinished task.id=%v\n", t.TaskID)
	} else {
		fmt.Printf("Worker:[ERROR] Coordinator.MarkFinished call failed!\n")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
