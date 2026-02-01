package main

import (
	"fmt"
	"hash/fnv"
)

// Hash function for partitioning keys.
// Creates a 32bit number out of string using fnv
// Bitwise & with 0xfffffff to convert it to positive number

func Ihash(key string) int {
	h := fnv.New32()
	n, _ := h.Write([]byte(key))
	nSum := h.Sum32()
	fmt.Printf("h.write:%d,h.Sum32:%d", n, nSum) // for debugging
	return (int(nSum & 0xffffffff))
}
