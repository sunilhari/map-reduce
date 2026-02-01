package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
)

func Worker(id int, coord *Coordinator, mapFunc MapFunc, reduceFunc ReduceFunc) {

	fmt.Printf("Starting worker %d", id)

	for {
		task := coord.GetTask()
		// have to check if its a map task or reduce task or exit task
		// if its a worker task call the mapfunction and do the work
		// if its a reduce task call the reducefunction and do the work

		switch task.Type {
		case TaskTypeMap:
			doMapTask(task, mapFunc, coord.rootPath)
			// notify coordinator that certain task is complete
			coord.NotifyTaskDone(task)
		case TaskTypeReduce:
			doReduceTask(task, reduceFunc, coord.rootPath)
			// notify coordinator that certain task is complete
			coord.NotifyTaskDone(task)
		case TaskTypeExit:
			fmt.Printf("Exiting worker %d", id)
			return
		}
	}
}

func doMapTask(task Task, mapFunc MapFunc, rootPath string) {
	// read file,because map function requires file name and content
	fileName := task.InputFile
	nReducer := task.nReducers

	b, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("Failed to read file %s", fileName)
	}
	keyValues := mapFunc(fileName, string(b))

	// should create one file for each reducer
	intermediateFiles := make([]*json.Encoder, nReducer)
	fileHandles := make([]*os.File, nReducer)
	for i := 0; i < nReducer; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", task.ID, i)
		fileLocation := path.Join(rootPath, "intermediate", fileName)
		iFile, err := os.Create(fileLocation)

		fileHandles[i] = iFile

		if err != nil {
			log.Fatalf("Failed to create intermediate file %v with following error %v", fileLocation, err)
		}
		intermediateFiles[i] = json.NewEncoder(iFile)
	}

	for _, keyValue := range keyValues {
		reducerId := Ihash(keyValue.Key) % nReducer
		err := intermediateFiles[reducerId].Encode(keyValue)
		if err != nil {
			log.Fatalf("Failed to encode kv:%v", keyValue)
		}
	}

	for _, file := range fileHandles {
		file.Close()
	}
	log.Printf("Map task completed for task:%d and file:%s", task.ID, task.InputFile)
}
func doReduceTask(task Task, reduceFunc ReduceFunc, rootPath string) {
	// read all files ending with task id
	var intermediate []KeyValue
	for mapTaskID := 0; mapTaskID <= task.nMappers; mapTaskID++ {
		fileName := fmt.Sprintf("mr-%d-%d", mapTaskID, task.ID)
		fileLocation := path.Join(rootPath, "intermediate", fileName)

		file, err := os.Open(fileLocation)
		if err != nil {
			log.Fatalf("failed to open intermediate file %s for reduce task %d", fileLocation, task.ID)
		}

		decoder := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// Intermediate would contain all the KeyValue that needs to be processed by  a specific reducer

	// sort by keys
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// group by keys and write to a file

	fileName := fmt.Sprintf("mr-out-%d", task.ID)
	fileLocation := path.Join(rootPath, "output", fileName)

	outputFile, err := os.Create(fileLocation)
	if err != nil {
		log.Fatalf("failed to create output file at %s", fileLocation)
	}
	defer outputFile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1

		// Find all values for the same key
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// Collect all values
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunc(intermediate[i].Key, values)
		// Write to output
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	log.Printf("Reduce task completed for task:%d", task.ID)
}
