package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"
)

func NewCoordinator(files []string, nReducers int) *Coordinator {
	nMap := len(files)
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal("failed to get current directory")
	}
	coordinator := &Coordinator{
		mu:              sync.Mutex{},
		inputFiles:      files,
		nMappers:        nMap,
		nReducers:       nReducers,
		mapTasks:        make([]TaskState, nMap),
		mapTasksDone:    false,
		reduceTasks:     make([]TaskState, nReducers),
		reduceTasksDone: false,
		taskQueue:       make(chan Task),
		doneChan:        make(chan TaskDone),
		rootPath:        path.Join(pwd, "store"),
	}

	coordinator.initializeTasks()
	coordinator.schedule()
	coordinator.handleCompletions()

	return coordinator
}

func (c *Coordinator) createFolders() error {
	folders := []string{"intermediate", "output"}
	for _, folder := range folders {
		err := os.MkdirAll(path.Join(c.rootPath, folder), os.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) initializeTasks() {

	for _, mapTask := range c.mapTasks {
		mapTask.status = TaskStatusIdle
	}

	for _, reduceTask := range c.reduceTasks {
		reduceTask.status = TaskStatusIdle
	}
}

func (c *Coordinator) schedule() {
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

		for _, task := range c.mapTasks {
			if !task.Completed() {
				mapTasksCompleted = false
				break
			}
		}

		if mapTasksCompleted {
			c.mapTasksDone = true
			fmt.Println("All Map tasks complete")
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

		for _, task := range c.reduceTasks {
			if !task.Completed() {
				allDone = false
			}
		}

		if allDone {
			c.reduceTasksDone = true
			fmt.Println("All Reduce tasks complete")
		}
	}
}

func (c *Coordinator) handleCompletions() {
	for doneTask := range c.doneChan {
		c.mu.Lock()
		if doneTask.isMapTask() {
			c.mapTasks[doneTask.taskId].SetToCompleted()
			fmt.Printf("MapTask %d complete", doneTask.taskId)
		}

		if doneTask.isReduceTask() {
			c.reduceTasks[doneTask.taskId].SetToCompleted()
			fmt.Printf("ReduceTask %d complete", doneTask.taskId)
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
