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
	// nReduces := 3
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

	fmt.Println("MapReduce job completed!")
	fmt.Println("Check output/ directory for results")
}
