package mapreduce

import (
	"log"
	"sync"
	"time"
)

func NewCoordinator(files []string, nReducers int) *Coordinator {
	nMap := len(files)
	coordinator := &Coordinator{
		mu:              sync.Mutex{},
		inputFiles:      files,
		nMappers:        nMap,
		nReducers:       nReducers,
		mapTasks:        make([]TaskState, nMap),
		mapTasksDone:    false,
		reduceTasks:     make([]TaskState, nReducers),
		reduceTasksDone: false,
		taskQueue:       make(chan Task, max(nMap, nReducers)),
		doneChan:        make(chan TaskDone, max(nMap, nReducers)),
	}
	// should eliminate this probably

	coordinator.initializeTasks()
	go coordinator.schedule()
	go coordinator.handleCompletions()

	return coordinator
}

func (c *Coordinator) initializeTasks() {
	for i := range c.mapTasks {
		c.mapTasks[i].status = TaskStatusIdle // ✓ Modifies the actual element
	}

	for i := range c.reduceTasks {
		c.reduceTasks[i].status = TaskStatusIdle // ✓ Modifies the actual element
	}
}

func (c *Coordinator) schedule() {
	log.Println("Starting dispatcher")
	for {
		c.mu.Lock()

		// Check if reduceTasks are done.
		// if yes then close the queue

		if c.reduceTasksDone {
			close(c.taskQueue)
			c.mu.Unlock()
			return
		}

		if !c.mapTasksDone {
			c.scheduleMapTasks()

		} else {
			c.scheduleReduceTasks()
		}

		c.mu.Unlock()
		// delay next schedule for 100 milliseconds
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *Coordinator) scheduleMapTasks() {
	assigned := false
	// assign only one at a time
	for i, mapTask := range c.mapTasks {

		if mapTask.IsIdle() {
			task := Task{
				ID:        i,
				Type:      TaskTypeMap,
				InputFile: c.inputFiles[i],
				nMappers:  c.nMappers,
				nReducers: c.nReducers,
			}

			c.mapTasks[i].SetToInProgress()
			c.mapTasks[i].startTime = time.Now()
			c.taskQueue <- task
			assigned = true
			break
		}

	}

	if !assigned {
		mapTasksCompleted := true

		for i := range c.mapTasks {
			if !c.mapTasks[i].Completed() {
				mapTasksCompleted = false
				break
			}
		}

		if mapTasksCompleted {
			c.mapTasksDone = true
			log.Println("All Map tasks complete")
		}

	}

}

func (c *Coordinator) scheduleReduceTasks() {
	// start processinfg reduce tasks
	assigned := false

	for i, reduceTask := range c.reduceTasks {

		if reduceTask.IsIdle() {
			task := Task{
				ID:        i,
				Type:      TaskTypeReduce,
				nMappers:  c.nMappers,
				nReducers: c.nReducers,
			}
			c.taskQueue <- task
			c.reduceTasks[i].SetToInProgress()
			c.reduceTasks[i].startTime = time.Now()
			assigned = true
			break
		}
	}

	if !assigned {
		allDone := true

		for i := range c.reduceTasks {
			if !c.reduceTasks[i].Completed() {
				allDone = false
			}
		}

		if allDone {
			c.reduceTasksDone = true
			log.Println("All Reduce tasks complete")
		}
	}
}

func (c *Coordinator) handleCompletions() {
	log.Println("Starting completion handler")
	for doneTask := range c.doneChan {
		c.mu.Lock()
		if doneTask.isMapTask() {
			c.mapTasks[doneTask.taskId].SetToCompleted()
			log.Printf("MapTask %d complete", doneTask.taskId)
		}

		if doneTask.isReduceTask() {
			c.reduceTasks[doneTask.taskId].SetToCompleted()
			log.Printf("ReduceTask %d complete", doneTask.taskId)
		}
		c.mu.Unlock()
	}
}

func (c *Coordinator) GetTask() Task {
	task, ok := <-c.taskQueue
	if !ok {
		return Task{Type: TaskTypeExit}
	}
	return task
}

func (c *Coordinator) NotifyTaskDone(task Task) {
	c.doneChan <- TaskDone{
		taskId:   task.ID,
		taskType: task.Type,
	}
}

// Done returns true when all work is complete
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceTasksDone
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
