package mr

import (
	"fmt"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
	"io"
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
	Map TashPhase = iota + 1
	Reduce
	Done
)

type TaskStatus int

const (
	Prepare TaskStatus = iota + 1
	Running
	Finish
)

type TaskType int

const (
	MapTask TaskType = iota + 1
	ReduceTask
	ExitTask
)

type Task struct {
	ID        int
	File      string
	startTime time.Time
	status    TaskStatus
	TaskType  TaskType
	ReduceNum int
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
	doneTaskChan   chan *Task
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
	return c.phase == Done
}

func (c *Coordinator) genTaskInfo() {
	for _, file := range c.files {
		task := &Task{
			File:      file,
			status:    Prepare,
			ID:        TaskID,
			TaskType:  MapTask,
			ReduceNum: c.ReduceNum,
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
	} else if c.phase == Done {
		taskChan = c.doneTaskChan
	}
	select {
	case task := <-taskChan:
		*reply = *task

		logrus.Infof("assign task ,task.ID:%v,task.Type:%v,task.File:%v,",
			reply.ID, reply.TaskType, reply.File)
	default:
		time.Sleep(300 * time.Microsecond)
	}
	return nil
}

func (c *Coordinator) FinishTask(req *Task, reply *Task) error {
	// 维护task状态
	c.taskInfo[req.ID].status = Finish

	switch req.TaskType {
	case MapTask:
		calNumLock.Lock()
		defer calNumLock.Unlock()
		MapFinishTaskNum++
	case ReduceTask:
		calNumLock.Lock()
		defer calNumLock.Unlock()
		ReduceFinishTaskNum++
	}
	if len(c.files) == MapFinishTaskNum && c.phase == Map {
		c.toNextPhase()
	} else if c.ReduceNum == ReduceFinishTaskNum && c.phase == Reduce {
		c.toNextPhase()
	}

	logrus.Infof("markFinish---,mapFinishTask:%d,reduceFinishTask:%d,currentPhase:%d",
		MapFinishTaskNum, ReduceFinishTaskNum, c.phase)
	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.phase == Map {
		c.phase = Reduce
		for i := 0; i < c.ReduceNum; i++ {
			task := &Task{
				TaskType:  ReduceTask,
				ReduceNum: c.ReduceNum,
				ID:        TaskID,
			}
			c.taskInfo[TaskID] = task
			TaskID++
			c.reduceTaskChan <- task
		}
	} else if c.phase == Reduce {
		c.phase = Done
		c.doneTaskChan <- &Task{
			TaskType: ExitTask,
			ID:       TaskID,
		}
	}

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
		mapTaskChan:    make(chan *Task, len(files)),
		reduceTaskChan: make(chan *Task, nReduce),
		doneTaskChan:   make(chan *Task, 1),
		taskInfo:       make(map[int]*Task, len(files)+nReduce),
	}
	initLogCoordinator()
	c.genTaskInfo()

	c.server()
	return &c
}

func initLogCoordinator() {
	logPath, _ := os.Getwd()
	// 按照pid输出
	logName := fmt.Sprintf("%s/coordinator.%d.%s.", logPath, os.Getpid(), time.Now().Format("20060102-150405"))
	r, _ := rotatelogs.New(logName + "%Y%m%d")
	mw := io.MultiWriter(r)
	logrus.SetOutput(mw)
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
	logrus.Info("init log ....")
}
