package mapreduce

import (
	"sync"
	"time"
)

// Captures intermediate  state after map
type KeyValue struct {
	Key   string
	Value string
}

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
	TaskTypeNoop
	TaskTypeExit
)

type Task struct {
	ID        int
	Type      TaskType
	InputFile string // input file that should be processed by this task
	nMappers  int    // total number of map tasks
	nReducers int    // total number of reducers
}

type TaskStatus int

const (
	TaskStatusInProgress TaskStatus = iota
	TaskStatusCompleted
	TaskStatusIdle
)

type TaskState struct {
	status    TaskStatus
	startTime time.Time
	taskId    int
}

func (t *TaskState) SetToIdle() {
	t.status = TaskStatusIdle
}

func (t *TaskState) SetToInProgress() {
	t.status = TaskStatusInProgress
}
func (t *TaskState) SetToCompleted() {
	t.status = TaskStatusCompleted
}
func (t *TaskState) IsIdle() bool {
	return t.status == TaskStatusIdle
}

func (t *TaskState) Completed() bool {
	return t.status == TaskStatusCompleted
}

func (t *TaskState) IsInProgress() bool {
	return t.status == TaskStatusInProgress
}

type TaskDone struct {
	taskId   int
	taskType TaskType
}

func (t *TaskDone) isMapTask() bool {
	return t.taskType == TaskTypeMap
}

func (t *TaskDone) isReduceTask() bool {
	return t.taskType == TaskTypeReduce
}

type Coordinator struct {
	mu sync.Mutex

	inputFiles []string

	nMappers  int
	nReducers int

	mapTasks     []TaskState
	mapTasksDone bool

	reduceTasks     []TaskState
	reduceTasksDone bool

	taskQueue chan Task
	doneChan  chan TaskDone
}
