package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	prefixed "github.com/x-cray/logrus-prefixed-formatter"
)

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
	StartTime time.Time
	Status    TaskStatus
	TaskType  TaskType
	ReduceNum int
}

var (
	calNumLock    sync.RWMutex
	TIME_OUT      = 10 * time.Second
	taskIdLock    sync.Mutex
	taskInfoLock  sync.RWMutex
	taskFinishMap = make(map[int]bool)
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
	taskList := make([]*Task, 0, len(c.files))
	for _, file := range c.files {
		task := &Task{
			File:      file,
			Status:    Prepare,
			ID:        TaskID,
			TaskType:  MapTask,
			ReduceNum: c.ReduceNum,
		}
		taskList = append(taskList, task)

		genTaskId()
		c.mapTaskChan <- task
	}
	c.setTask(taskList)
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
		task.StartTime = time.Now()
		task.Status = Running
		c.setTask([]*Task{task})
		*reply = *task
		logrus.Info("assign task ,task:", task)
	default:
		time.Sleep(100 * time.Microsecond)
	}
	return nil
}

func (c *Coordinator) setTask(tasks []*Task) {
	taskInfoLock.Lock()
	defer taskInfoLock.Unlock()
	for _, task := range tasks {
		c.taskInfo[task.ID] = task
	}
}

func (c *Coordinator) FinishTask(req *Task, reply *Task) error {
	if req.Status != Running {
		return fmt.Errorf("task %v status is not Running", req.ID)
	}
	// 维护task状态
	req.Status = Finish
	c.setTask([]*Task{req})
	logrus.Infof("finish task ,task.ID:%v,StartTime:%v,currentPhase:%v,taskType:%v", req.ID, req.StartTime, c.phase, req.TaskType)

	calNumLock.Lock()
	taskFinishMap[req.ID] = true
	calNumLock.Unlock()

	calNumLock.RLock()
	defer calNumLock.RUnlock()

	logrus.Infof("markFinish---,taskId:%v,finish:%v,currentPhase:%d",
		req.ID, marshal(taskFinishMap), c.phase)
	if c.phase == Map {
		for i := 0; i < len(c.files); i++ {
			if !taskFinishMap[i] {
				return nil
			}
		}
		c.toNextPhase()
	} else if c.phase == Reduce {
		for i := len(c.files); i < len(c.files)+c.ReduceNum; i++ {
			if !taskFinishMap[i] {
				return nil
			}
		}
		c.toNextPhase()
	}
	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.phase == Map {
		c.phase = Reduce
		taskList := make([]*Task, 0, c.ReduceNum)
		for i := 0; i < c.ReduceNum; i++ {
			task := &Task{
				TaskType:  ReduceTask,
				ReduceNum: c.ReduceNum,
				ID:        TaskID,
				Status:    Prepare,
			}
			genTaskId()
			taskList = append(taskList, task)
			c.reduceTaskChan <- task
		}

		c.setTask(taskList)
	} else if c.phase == Reduce {
		c.phase = Done
		c.doneTaskChan <- &Task{
			TaskType: ExitTask,
			ID:       TaskID,
		}
	}
	logrus.Infof("toNextPhase,phase:%v,finish:%v", c.phase, marshal(taskFinishMap))
}

func (c *Coordinator) crashDetect() {
	for c.phase != Done {
		time.Sleep(3 * time.Second)
		overTimeTask := []*Task{}
		taskInfoLock.RLock()
		// 获取超时任务
		for taskId := range c.taskInfo {
			taskInfo := c.taskInfo[taskId]
			//logrus.Debugf("taskId:%v,runningTime:%v", taskId, time.Now().Sub(taskInfo.StartTime))
			if taskInfo.Status == Running && time.Now().Sub(taskInfo.StartTime) > TIME_OUT {
				logrus.Infof("detect crash task,taskId:%v,holdTime:%v", taskId, time.Now().Sub(taskInfo.StartTime))
				overTimeTask = append(overTimeTask, taskInfo)
			}
		}
		taskInfoLock.RUnlock()

		// 维护任务状态
		for _, taskInfo := range overTimeTask {
			taskInfo.Status = Prepare
		}
		c.setTask(overTimeTask)

		// 重新推入chan
		for _, taskInfo := range overTimeTask {
			if taskInfo.TaskType == MapTask {
				if c.phase == Reduce {
					logrus.Errorf("error phase,error crash task,taskId:%v,phase:%v,task.Type:%v", taskInfo.ID, c.phase, taskInfo.TaskType)
					continue
				}
				c.mapTaskChan <- taskInfo
			} else if taskInfo.TaskType == ReduceTask {
				if c.phase == Map {
					logrus.Errorf("error phase,error crash task,taskId:%v,phase:%v,task.Type:%v", taskInfo.ID, c.phase, taskInfo.TaskType)
					continue
				}
				c.reduceTaskChan <- taskInfo
			}
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

	go c.crashDetect()
	c.server()

	return &c
}

func initLogCoordinator() {
	logPath, _ := os.Getwd()
	// 按照pid输出
	logName := fmt.Sprintf("%s/coordinator.%d.%s.", logPath, os.Getpid(), time.Now().Format("20060102-150405"))
	writer, _ := rotatelogs.New(logName + "%Y%m%d")
	fileFormatter = &prefixed.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02.15:04:05.000000",
		ForceFormatting: true,
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

func marshal(v interface{}) string {
	bytes, _ := json.Marshal(v)
	return string(bytes)
}

func genTaskId() {
	taskIdLock.Lock()
	defer taskIdLock.Unlock()
	TaskID++
}
