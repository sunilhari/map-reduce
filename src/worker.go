package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
)

func Worker(id int, coord *Coordinator, mapFunc MapFunc, reduceFunc ReduceFunc) {

	log.Printf("Starting worker %d", id)

	for {
		task := coord.GetTask()
		// have to check if its a map task or reduce task or exit task
		// if its a worker task call the mapfunction and do the work
		// if its a reduce task call the reducefunction and do the work
		log.Printf("Worker[%d] Processing task:%d\n", id, task.ID)
		switch task.Type {
		case TaskTypeMap:
			doMapTask(task, mapFunc)
			// notify coordinator that certain task is complete
			coord.NotifyTaskDone(task)
		case TaskTypeReduce:
			doReduceTask(task, reduceFunc)
			// notify coordinator that certain task is complete
			coord.NotifyTaskDone(task)
		case TaskTypeExit:
			log.Printf("Exiting worker %d", id)
			return
		}
	}
}

func doMapTask(task Task, mapFunc MapFunc) {
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
		fileName := fmt.Sprintf("mr-%d-%d.txt", task.ID, i)
		fileLocation := filepath.Join(getDefaultPath(), "intermediate", fileName)

		dir := filepath.Dir(fileLocation)
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			log.Fatalf("failed to create intermediate directory")
		}

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
	log.Printf("\nMap task completed for task:%d and file:%s", task.ID, task.InputFile)
}
func doReduceTask(task Task, reduceFunc ReduceFunc) {
	// read all files ending with task id
	var intermediate []KeyValue
	for mapTaskID := 0; mapTaskID < task.nMappers; mapTaskID++ {
		fileName := fmt.Sprintf("mr-%d-%d.txt", mapTaskID, task.ID)
		fileLocation := filepath.Join(getDefaultPath(), "intermediate", fileName)

		file, err := os.Open(fileLocation)
		if err != nil {
			log.Fatalf("failed to open intermediate file %s for reduce task %d due to error %v", fileLocation, task.ID, err)
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

	fileName := fmt.Sprintf("mr-out-%d.txt", task.ID)
	fileLocation := filepath.Join(getDefaultPath(), "output", fileName)
	dir := filepath.Dir(fileLocation)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		log.Fatalf("failed to create output directory")
	}

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
		_, err := fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("failed to write output file for key : %d to location %s due to error %v", intermediate[i].Key, fileLocation, err)
		}

		i = j
	}
	log.Printf("Reduce task completed for task:%d", task.ID)
}
