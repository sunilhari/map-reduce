package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	mapreduce "github.com/sunilhari/map-reduce/src"
)

func main() {

	// read input files directory from command line
	inputFileDir := os.Args[1]
	// outputPath := os.Args[2]

	// read all .txt files from the directory
	inputFilePath := filepath.Join(inputFileDir, "*.txt")
	files, err := filepath.Glob(inputFilePath)

	if err != nil {
		log.Fatalf("failed to read input files directory %s", inputFileDir)
	}

	if len(files) == 0 {
		log.Fatalf("could not find any .txt files in %s", inputFileDir)
	}

	// should be configurable
	// nReducers := 3
	nWorkers := 5

	coordinator := mapreduce.NewCoordinator(files, 3)
	// Start workers

	for i := 0; i < nWorkers; i++ {
		go mapreduce.Worker(i, coordinator, mapreduce.WordCountMap, mapreduce.WordCountReduce)
	}

	// Wait for completion
	for !coordinator.Done() {
		time.Sleep(1 * time.Second)
	}

	log.Println("MapReduce job completed!")
	log.Println("Check output/ directory for results")
	// combineOutputs(nReducers, outputPath)
}

func combineOutputs(nReduce int, outputPath string) {
	outputPath, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	finalOutput, err := os.Create(filepath.Join(outputPath, "final-output.txt"))
	if err != nil {
		log.Fatal(err)
	}
	defer finalOutput.Close()

	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("output/mr-out-%d", i)
		content, err := os.ReadFile(filename)
		if err != nil {
			continue
		}
		finalOutput.Write(content)
	}

	log.Println("Combined output written to output/final-output.txt")
}
