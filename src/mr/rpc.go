package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskArgs struct{}
type TaskType int  // 任务类型
type Status int    // 任务状态
type TaskPhase int // 当前mapReduce程序所处阶段

const (
	MapPhase TaskPhase = iota
	ReducePhase
	AllDone
)

const (
	Waiting Status = iota
	Running
	// TODO 从running到done的状态转换是在哪里完成的
	Done
)
const (
	MapTask TaskType = iota
	ReduceTask
	WaitingType // 任务分发完毕，等待任务执行完成
	ExitType    // 任务全部执行完毕，程序可以退出
)

type Task struct {
	TaskID int `json:"task_id"` // 任务id
	//TypeStatus TaskStatus `json:"type_status"` // 任务状态
	TaskType TaskType `json:"task_type"` // 任务类型
	Input    []string `json:"values"`    // 函数输入，map任务的输入是文件，reduce函数的输入是编号对应的中间值文件数组
	// 不可删除 worker需要通过num hash得到运行的任务
	ReduceNum int // reduce任务的数量
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
