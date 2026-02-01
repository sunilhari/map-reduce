package mapreduce

import (
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
)

// Hash function for partitioning keys.
// Creates a 32bit number out of string using fnv
// Bitwise & with 0xfffffff to convert it to positive number

func Ihash(key string) int {
	h := fnv.New32()
	h.Write([]byte(key))
	nSum := h.Sum32()
	return (int(nSum & 0xffffffff))
}

func getDefaultPath() string {
	homedir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal("failed to get current directory")
	}
	rootPath := filepath.Join(homedir, "temp", ".mapreduce", "store")
	return rootPath
}
