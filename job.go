package main

import (
	"strconv"
	"strings"
)

type MapFunc func(fileName string, contents string) []KeyValue
type ReduceFunc func(key string, values []string) string

// iterate through each words and creates key value pairs
func WordCountMap(fileName string, contents string) []KeyValue {
	words := strings.Fields(contents)
	kv := []KeyValue{}

	for _, word := range words {
		kva := KeyValue{
			Key:   word,
			Value: "1",
		}
		kv = append(kv, kva)
	}
	return kv
}

func ReduceCountMap(key, values []string) string {
	count := 0
	for _, value := range values {
		n, _ := strconv.Atoi(value)
		count += n
	}
	return strconv.Itoa(count)
}
