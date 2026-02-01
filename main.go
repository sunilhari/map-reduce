package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {

	// read input files directory from command line
	inputFileDir := os.Args[2]

	// read all .txt files from the directory

	files, err := filepath.Glob(filepath.Join(inputFileDir, "*.txt"))

	if err != nil {
		log.Fatalf("failed to read input files directory %s", inputFileDir)
	}

	if len(files) == 0 {
		log.Fatalf("could not find any .txt files in %s", inputFileDir)
	}

	// should be configurable
	nReducers := 3
	nWorkers := 5

	coordinator := NewCoordinator(files, 3)
	// Start workers

	for i := 0; i < nWorkers; i++ {
		go Worker(i, coordinator, WordCountMap, WordCountReduce)
	}

	// Wait for completion
	for !coordinator.Done() {
		time.Sleep(1 * time.Second)
	}

	log.Println("MapReduce job completed!")
	log.Println("Check output/ directory for results")
	combineOutputs(nReducers)
}

func combineOutputs(nReduce int) {
	outputPath, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	finalOutput, err := os.Create(filepath.Join(outputPath, "output", "final-output.txt"))
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

	fmt.Println("Combined output written to output/final-output.txt")
}

func getDefaultPath() string {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal("failed to get current directory")
	}
	rootPath := filepath.Join(pwd, "store")
	return rootPath
}
