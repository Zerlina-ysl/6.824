package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"github.com/sirupsen/logrus"
)

type Coordinator struct {
	Phase        int
	MapTaskCh    chan *TaskInfo
	ReduceTaskCh chan *TaskInfo
}

// startMapTask 启动map任务
func (c *Coordinator) startMapTask(file string) *TaskInfo {
	return &TaskInfo{
		TaskType: MapTaskType,
	}

}

func (c *Coordinator) AssignTask(req *ReqArgs, reply *TaskInfo) error {
	switch c.Phase {
	case MapPhase:

		break
	case ReducePhase:
		break
	case Complete:
		break
	default:
		logrus.Warn("error phase:", c.Phase)
	}

	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Phase: MapPhase,
		MapTaskCh:    make(chan *TaskInfo, len(files)),
		ReduceTaskCh: make(chan *TaskInfo, nReduce),
	}

	// 启动map任务
	for _, file := range files {
		mapTaskInfo := c.startMapTask(file)
		c.MapTaskCh <- mapTaskInfo
	}

	c.server()
	// 检测
	return &c
}
