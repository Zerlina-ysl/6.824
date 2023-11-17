package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var (
	AssignLock       sync.Mutex // 全局锁，在coordinator分配任务时加锁
	MarkFinishedLock sync.Mutex // 全局锁，在coordinator修改任务状态时加锁
)

// TaskMetaInfo 所有任务在coordinator维护状态
type TaskMetaInfo struct {
	status    Status    // 任务状态
	StartTime time.Time // 任务的开始时间
	TaskAdr   *Task     // 任务
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // taskId--> Task
}

// judgeStatus 判定任务的工作状态。正在工作返回false
func (holder TaskMetaHolder) judgeStatus(taskID int) bool {
	taskInfo, has := holder.MetaMap[taskID]
	if !has || taskInfo.status != Waiting {
		return false
	}
	taskInfo.status = Running
	taskInfo.StartTime = time.Now()
	return true
}

func (holder TaskMetaHolder) isTaskDone() bool {
	var (
		mapRunningNum    int = 0
		mapDoneNum       int = 0
		reduceRunningNum     = 0
		reduceDoneNum        = 0
	)
	for _, t := range holder.MetaMap {
		if t.TaskAdr.TaskType == MapTask {
			if t.status == Done {
				mapDoneNum++
			} else if t.status == Running {
				mapRunningNum++
			}
		} else if t.TaskAdr.TaskType == ReduceTask {
			if t.status == Done {
				reduceDoneNum++
			} else if t.status == Running {
				reduceRunningNum++
			}
		}
	}
	if mapRunningNum == 0 && mapDoneNum > 0 && reduceRunningNum == 0 && reduceDoneNum == 0 {
		// map阶段运行完毕
		return true
	} else if reduceDoneNum > 0 && reduceRunningNum == 0 {
		// reduce阶段运行完毕
		return true
	}
	return false
}

// acceptMeta 接受Task元信息并存储
func (holder TaskMetaHolder) acceptMeta(t *TaskMetaInfo) {
	holder.MetaMap[t.TaskAdr.TaskID] = t
}

type Coordinator struct {
	Status Status `json:"status"` // 整个mapreduce任务的状态，任务完成后，所有工作线程也可以退出
	//mu             sync.Mutex
	MapChannel     chan *Task
	ReduceChannel  chan *Task
	Phase          TaskPhase
	TaskMetaHolder TaskMetaHolder // 根据taskID存储task的元数据
	ReduceNum      int            // reduce任务数量
	TaskID         int            // 用于生成自增id
	// TODO 在reduce阶段应该存在汇总过程
	files []string // map接收到输入文件数组后，会将map运行的中间值写入对应文件中
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *Task) error {
	fmt.Printf("[INFO]worker call coordinator for task")
	// 整个过程加锁保证线程安全
	AssignLock.Lock()
	defer AssignLock.Unlock()
	switch c.Phase {
	case MapPhase:
		if len(c.MapChannel) > 0 {
			reply = <-c.MapChannel
			// TODO 如果已分配的任务正在running 如何处理？
			if !c.TaskMetaHolder.judgeStatus(reply.TaskID) {
				fmt.Printf("[WARN] map-taskID[%d] is running", reply.TaskID)
			}
		} else {
			// map任务分发完毕，等待任务执行
			reply.TaskType = WaitingType
			if c.TaskMetaHolder.isTaskDone() {
				c.toNextPhase()
			}
		}
	case ReducePhase:
		if len(c.ReduceChannel) > 0 {
			reply = <-c.ReduceChannel
			if !c.TaskMetaHolder.judgeStatus(reply.TaskID) {
				fmt.Printf("[WARN] reduce-taskID[%d] is running", reply.TaskID)
			}
		} else {
			reply.TaskType = WaitingType
			if c.TaskMetaHolder.isTaskDone() {
				c.toNextPhase()
			}
		}
	case AllDone:
		reply.TaskType = ExitType
	default:
		panic("error phase")
	}

	return nil
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	fmt.Printf("[INFO]worker call coordinator for Mark finish")
	MarkFinishedLock.Lock()
	defer MarkFinishedLock.Unlock()
	if args.TaskType == MapTask || args.TaskType == ReduceTask {
		meta, ok := c.TaskMetaHolder.MetaMap[args.TaskID]
		if ok && meta.status == Running {
			meta.status = Done
		} else {
			fmt.Printf("[WARN]Map task Id[%d] is finished,already ! ! !\n", args.TaskID)
		}
	} else {
		panic("MarkFinished error,unknown taskType")
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	fmt.Printf("[INFO]coordinator server alreay start...")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// mrcoordinator.go will exit
//
func (c *Coordinator) Done() bool {
	//ret := false
	if c.Phase == AllDone {
		return true
	}
	return false
}

// toNextPhase mapreduce的阶段切换
func (c *Coordinator) toNextPhase() {
	if c.Phase == MapPhase {
		c.makeReduceTasks()
		c.Phase = ReducePhase
	} else if c.Phase == ReducePhase {
		c.Phase = AllDone
	}
}

func (c *Coordinator) generateTaskID() int {
	// 采取自增实现
	id := c.TaskID
	c.TaskID++
	return id
}

// makeMapTasks 将对应的任务推入channel,并存储taskid到task的映射
func (c *Coordinator) makeMapTasks(files []string) {
	for _, value := range files {
		task := Task{
			TaskType:  MapTask,
			TaskID:    c.generateTaskID(),
			Input:     []string{value},
			ReduceNum: c.ReduceNum,
		}
		taskMetaInfo := TaskMetaInfo{
			status:  Waiting,
			TaskAdr: &task,
		}
		c.TaskMetaHolder.acceptMeta(&taskMetaInfo)
		c.MapChannel <- &task
	}
	fmt.Printf("[INFO]push map task to channel,len(chan)=%d\n", len(c.MapChannel))
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.generateTaskID()
		task := Task{
			TaskType: ReduceTask,
			TaskID:   id,
			Input:    selectReduceFile(id),
		}
		taskMetaInfo := TaskMetaInfo{
			status:  Waiting,
			TaskAdr: &task,
		}
		c.TaskMetaHolder.acceptMeta(&taskMetaInfo)
		c.ReduceChannel <- &task
	}
}

func selectReduceFile(id int) []string {
	// 读取当前目录下所有文件
	reduceFiles := []string{}
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, file := range files {
		// 根据文件名称匹配
		if strings.HasPrefix(file.Name(), MAP_FILE_PREFIX) &&
			strings.HasSuffix(file.Name(), strconv.Itoa(id)) {
			reduceFiles = append(reduceFiles, file.Name())
		}
	}
	return reduceFiles
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceNum:     nReduce,
		files:         files,
		TaskID:        0,
		Phase:         MapPhase,
		MapChannel:    make(chan *Task, len(files)),
		ReduceChannel: make(chan *Task, nReduce),
		TaskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}
	fmt.Printf("[INFO]make map tasks by files,len(files)=%d\n", len(files))
	c.makeMapTasks(files)
	// Your code here.

	c.server()

	return &c
}
