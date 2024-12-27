package main

import (
	"fmt"
	"reflect"
	"runtime"
	"time"
)

// BRC functions, each function processes a file
type brcFunc func(string) string

var brcFuncs = []brcFunc{
	// processFile_1,
	// processFile_2,
	processFile_3,
	processFile_4,
}

var maxGoroutines = runtime.NumCPU()

func main() {
	// const fileName = "measurements_s.txt"
	const fileName = "measurements.txt"
	const numRuns = 3

	for _, f := range brcFuncs {
		var totalDuration time.Duration

		funcName := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		fmt.Println("\nRunning", funcName)
		for i := 0; i < numRuns; i++ {
			duration, _ := runAndTime(f, fileName)
			totalDuration += duration

			fmt.Println("\tRun", i+1, "| Elapsed time:", duration)
		}
		averageDuration := totalDuration / numRuns
		fmt.Println("Average time:", averageDuration)
	}
}

func runAndTime(f brcFunc, fileName string) (time.Duration, string) {
	start := time.Now()
	output := f(fileName)
	duration := time.Since(start)
	return duration, output
}
