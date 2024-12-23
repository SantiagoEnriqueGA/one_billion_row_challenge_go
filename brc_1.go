package main

// Packages:
// 	bufio: implements buffered I/O. It wraps an io.Reader or io.Writer object, creating another object (Reader or Writer) that also implements the interface but provides buffering and some help for textual I/O.
// 	fmt: implements formatted I/O with functions analogous to C's printf and scanf. The format 'verbs' are derived from C's but are simpler.
// 	os: provides a platform-independent interface to operating system functionality, including file I/O, command-line arguments, and environment variables.
// 	strconv: implements conversions to and from string representations of basic data types.
// 	strings: implements simple functions to manipulate UTF-8 encoded strings.
// 	sort: provides primitives for sorting slices and user-defined collections.

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

func processFile_1(fileName string) string {
	if fileName == "" {
		fmt.Println("No file name provided, using default: measurements.txt")
		fileName = "measurements.txt"
	}

	result := make(map[string][]float64) // create a map to store the results

	file, err := os.Open(fileName) // open the file
	if err != nil {
		return fmt.Sprintf("Error opening file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file) // create a scanner to read the file line by line
	for scanner.Scan() {
		line := scanner.Text()            // read the line
		parts := strings.Split(line, ";") // split the line by comma

		if len(parts) < 2 { // check if there are at least two parts
			fmt.Println("Invalid line format: ", line)
			continue
		}

		location := parts[0]                                 // get the location
		measurement, err := strconv.ParseFloat(parts[1], 64) // get the measurement, convert to float

		if err != nil {
			return fmt.Sprintf("Error parsing measurement: %v", err)
		}

		if _, exists := result[location]; !exists { // if the location is not in the map, add it
			result[location] = []float64{measurement, measurement, measurement, 1} // [min, max, sum, count]
		} else { // if the location is in the map, update the values
			_result := result[location]

			if measurement < _result[0] { // update min
				_result[0] = measurement
			}
			if measurement > _result[1] { // update max
				_result[1] = measurement
			}
			_result[2] += measurement // update sum
			_result[3] += 1           // update count
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Sprintf("Error reading file: %v", err)
	}

	// Extract keys and sort them
	keys := make([]string, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var resultBuilder strings.Builder
	resultBuilder.WriteString("{")
	for _, location := range keys {
		measurements := result[location]

		avg := measurements[2] / measurements[3] // calculate the average
		resultBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", location, measurements[0], avg, measurements[1]))
	}
	out := resultBuilder.String()
	return strings.TrimSuffix(out, ", ") + "}"
}
