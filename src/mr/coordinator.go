package mr

import (
	"fmt"
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
	lock sync.Mutex // 全局锁
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

func (holder TaskMetaHolder) changeStatus(taskID int) bool {
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
		mapUndoneNum    int = 0
		mapDoneNum      int = 0
		reduceUndoneNum     = 0
		reduceDoneNum       = 0
	)
	for _, t := range holder.MetaMap {
		if t.TaskAdr.TaskType == MapTask {
			if t.status == Done {
				mapDoneNum++
			} else {
				mapUndoneNum++
			}
		} else if t.TaskAdr.TaskType == ReduceTask {
			if t.status == Done {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	fmt.Printf("Coordinator:[INFO]check task is done,mapRunning=%d,mapDone=%d,reduceRunning=%d,reduceDone=%d\n", mapUndoneNum, mapDoneNum, reduceUndoneNum, reduceDoneNum)

	if mapUndoneNum == 0 && mapDoneNum > 0 && reduceUndoneNum == 0 && reduceDoneNum == 0 {
		// map阶段运行完毕
		return true
	} else if reduceDoneNum > 0 && reduceUndoneNum == 0 {
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
	Status         Status `json:"status"` // 整个mapreduce任务的状态，任务完成后，所有工作线程也可以退出
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

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *Task) error {
	// 整个过程加锁保证线程安全
	lock.Lock()
	defer lock.Unlock()
	fmt.Printf("Coordinator:[INFO]worker call coordinator for task,current phase is %v\n", c.Phase)

	switch c.Phase {
	case MapPhase:
		if len(c.MapChannel) > 0 {
			// 从通道中取出的任务对象内容会被复制到 reply 所指向的对象中
			*reply = *<-c.MapChannel
			fmt.Printf("Coordinator:[DEBUG]AssignTask task from chan,current phase is map,taskID=%d,len(input)=%d\n", reply.TaskID, len(reply.Input))
			c.TaskMetaHolder.changeStatus(reply.TaskID)
			//	fmt.Printf("[WARN] map-taskID[%d] is running", reply.TaskID)
			//}
		} else {
			// map任务分发完毕，等待任务执行
			reply.TaskType = WaitingType
			if c.TaskMetaHolder.isTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case ReducePhase:
		if len(c.ReduceChannel) > 0 {
			*reply = *<-c.ReduceChannel
			fmt.Printf("Coordinator:[DEBUG]AssignTask task from chan,current phase is reduce,taskID=%d,len(input)=%d\n", reply.TaskID, len(reply.Input))
			c.TaskMetaHolder.changeStatus(reply.TaskID)
			//	fmt.Printf("[WARN] reduce-taskID[%d] is running", reply.TaskID)
			//}
		} else {
			reply.TaskType = WaitingType
			if c.TaskMetaHolder.isTaskDone() {
				c.toNextPhase()
			}
			return nil
		}
	case AllDone:
		reply.TaskType = ExitType
	default:
		panic("error phase")
	}

	return nil
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	fmt.Printf("Coordinator:[INFO]worker call coordinator for Mark finish,current taskType is %v,taskid=%d\n", args.TaskType, args.TaskID)
	lock.Lock()
	defer lock.Unlock()

	if args.TaskType == MapTask || args.TaskType == ReduceTask {
		meta, ok := c.TaskMetaHolder.MetaMap[args.TaskID]
		if ok && meta.status == Running {
			meta.status = Done
		} else {
			fmt.Printf("Coordinator:[WARN]Map task Id[%d] is finished,already ! ! !\n", args.TaskID)
		}
	} else {
		panic("MarkFinished error,unknown taskType")
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
	fmt.Printf("Coordinator:[INFO]coordinator server alreay start...\n")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// mrcoordinator.go will exit
func (c *Coordinator) Done() bool {
	//ret := false
	lock.Lock()
	defer lock.Unlock()
	if c.Phase == AllDone {
		return true
	}
	return false
}

// toNextPhase mapreduce的阶段切换
func (c *Coordinator) toNextPhase() {
	fmt.Printf("Coordinator:[INFO]change to next phase,current phase is %v\n", c.Phase)
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
	fmt.Printf("Coordinator:[INFO]push map task to channel,len(chan)=%d\n", len(c.MapChannel))
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.generateTaskID()
		task := Task{
			TaskType: ReduceTask,
			TaskID:   id,
			Input:    selectReduceFile(i),
		}
		taskMetaInfo := TaskMetaInfo{
			status:  Waiting,
			TaskAdr: &task,
		}
		c.TaskMetaHolder.acceptMeta(&taskMetaInfo)
		c.ReduceChannel <- &task
	}
}

func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2)
		lock.Lock()
		if c.Phase == AllDone {
			lock.Unlock()
			break
		}
		for _, task := range c.TaskMetaHolder.MetaMap {
			// 超时检测
			if task.status == Running && time.Since(task.StartTime) > 9*time.Second {
				fmt.Printf("Coordinator:[WARN]the task is crash,taskid=%d,taskType=%d\n", task.TaskAdr.TaskID, task.TaskAdr.TaskType)

				if task.TaskAdr.TaskType == MapTask {
					c.MapChannel <- task.TaskAdr
					task.status = Waiting

				}
				if task.TaskAdr.TaskType == ReduceTask {
					c.ReduceChannel <- task.TaskAdr
					task.status = Waiting
				}
			}
		}
		lock.Unlock()
	}
}

func selectReduceFile(reduceNum int) []string {
	// 读取当前目录下所有文件
	reduceFiles := []string{}
	path, _ := os.Getwd()
	files, _ := os.ReadDir(path)
	for _, file := range files {
		// 根据文件名称匹配
		if strings.HasPrefix(file.Name(), MAP_FILE_PREFIX) &&
			strings.HasSuffix(file.Name(), strconv.Itoa(reduceNum)) {
			reduceFiles = append(reduceFiles, file.Name())
		}
	}
	return reduceFiles
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
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
	fmt.Printf("Coordinator:[INFO]make map tasks by files,len(files)=%d\n", len(files))
	c.makeMapTasks(files)
	// Your code here.

	c.server()

	go c.CrashDetector()

	return &c
}
