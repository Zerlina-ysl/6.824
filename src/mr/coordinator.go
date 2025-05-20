package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var TaskID int

type TashPhase int

const (
	Map TashPhase = iota
	Reduce
	Done
)

type TaskStatus int

const (
	Prepare TaskStatus = iota
	Running
	Finish
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type Task struct {
	ID        int
	file      string
	startTime time.Time
	status    TaskStatus
	taskType  TaskType
	reduceNum int
}

var (
	MapFinishTaskNum    = 0
	ReduceFinishTaskNum = 0
	calNumLock          sync.Mutex
)

type Coordinator struct {
	ReduceNum      int
	files          []string
	phase          TashPhase
	taskInfo       map[int]*Task
	mapTaskChan    chan *Task
	reduceTaskChan chan *Task
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
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) genTaskInfo() {
	for _, file := range c.files {
		task := &Task{
			file:     file,
			status:   Prepare,
			ID:       TaskID,
			taskType: MapTask,
		}
		c.taskInfo[TaskID] = task
		// 串行初始化，无需额外考虑并发问题
		TaskID++
		c.mapTaskChan <- task
	}
}

func (c *Coordinator) AssignTask(req *Task, reply *Task) error {
	var taskChan chan *Task
	if c.phase == Map {
		taskChan = c.mapTaskChan
	} else if c.phase == Reduce {
		taskChan = c.reduceTaskChan
	}
	select {
	case task := <-taskChan:
		reply = task
	default:
		time.Sleep(time.Second)
	}
	return nil
}

func (c *Coordinator) FinishTask(req *Task, reply *Task) error {
	req.status = Finish
	switch req.taskType {
	case MapTask:
		calNumLock.Lock()
		defer calNumLock.Unlock()
		MapFinishTaskNum++
	case ReduceTask:
		calNumLock.Lock()
		defer calNumLock.Unlock()
		ReduceTaskFinish++

	}
	if len(c.files) == MapFinishTaskNum {
		c.toNextPhase()
	} else if c.ReduceNum == ReduceTaskFinish {
		c.toNextPhase()
	}
}

func (c *Coordinator) toNextPhase() {

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		ReduceNum:      nReduce,
		files:          files,
		phase:          Map,
		mapTaskChan:    make(chan *Task, len(files)), // 使用slice可以吗？
		reduceTaskChan: make(chan *Task, nReduce),
		taskInfo:       make(map[int]*Task),
	}
	c.genTaskInfo()

	c.server()
	return &c
}
