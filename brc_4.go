package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// processFile_4 processes the input file and returns the result as a string
func processFile_4(fileName string) string {
	// Split the file into parts for concurrent processing
	parts, err := splitFile(fileName, maxGoroutines)
	if err != nil {
		return fmt.Sprintf("Error splitting file: %v", err)
	}

	// Channel to collect results from goroutines
	resultsCh := make(chan map[string]*stats3, len(parts))
	for _, part := range parts {
		// Process each part concurrently
		go processPart_4(fileName, part.offset, part.size, resultsCh)
	}

	// Aggregate results from all parts
	totals := make(map[string]*stats3)
	for i := 0; i < len(parts); i++ {
		result := <-resultsCh
		for station, s := range result {
			ts := totals[station]
			if ts == nil {
				totals[station] = s
				continue
			}
			// Update totals with new data
			ts.min = min(ts.min, s.min)
			ts.max = max(ts.max, s.max)
			ts.sum += s.sum
			ts.count += s.count
		}
	}

	// Sort stations alphabetically
	stations := make([]string, 0, len(totals))
	for station := range totals {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	// Build the result string
	var resultBuilder strings.Builder
	resultBuilder.WriteString("{")

	for i, station := range stations {
		if i > 0 {
			resultBuilder.WriteString(", ")
		}
		s := totals[station]
		mean := float64(s.sum) / float64(s.count) / 10
		resultBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, float64(s.min)/10, mean, float64(s.max)/10))
	}
	resultBuilder.WriteString("}\n")
	return resultBuilder.String()
}

// processPart_4 processes a part of the file and sends the result to the channel
func processPart_4(inputPath string, fileOffset, fileSize int64, resultsCh chan map[string]*stats3) {
	file, err := os.Open(inputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	f := io.LimitedReader{R: file, N: fileSize}

	// Define item structure for hash buckets
	type item struct {
		key  []byte
		stat *stats3
	}
	const numBuckets = 1 << 17        // number of hash buckets (power of 2)
	items := make([]item, numBuckets) // hash buckets, linearly probed
	size := 0                         // number of active items in items slice

	buf := make([]byte, 1024*1024)
	readStart := 0
	for {
		n, err := f.Read(buf[readStart:])
		if err != nil && err != io.EOF {
			panic(err)
		}
		if readStart+n == 0 {
			break
		}
		chunk := buf[:readStart+n]

		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			break
		}
		remaining := chunk[newline+1:]
		chunk = chunk[:newline+1]

	chunkLoop:
		for {
			var hash uint64
			var station, after []byte

			if len(chunk) < 8 {
				break chunkLoop
			}

			nameWord0 := binary.NativeEndian.Uint64(chunk)
			matchBits := semicolonMatchBits(nameWord0)
			if matchBits != 0 {
				// semicolon is in the first 8 bytes
				nameLen := calcNameLen(matchBits)
				nameWord0 = maskWord(nameWord0, matchBits)
				station = chunk[:nameLen]
				after = chunk[nameLen+1:]
				hash = calcHash(nameWord0)
			} else {
				// station name is longer so keep looking for the semicolon in
				// uint64 chunks
				nameLen := 8
				hash = calcHash(nameWord0)
				for {
					if nameLen > len(chunk)-8 {
						break chunkLoop
					}
					lastNameWord := binary.NativeEndian.Uint64(chunk[nameLen:])
					matchBits = semicolonMatchBits(lastNameWord)
					if matchBits != 0 {
						nameLen += calcNameLen(matchBits)
						station = chunk[:nameLen]
						after = chunk[nameLen+1:]
						break
					}
					nameLen += 8
				}
			}
			index := 0
			negative := false
			if after[index] == '-' {
				negative = true
				index++
			}
			temp := int32(after[index] - '0')
			index++
			if after[index] != '.' {
				temp = temp*10 + int32(after[index]-'0')
				index++
			}
			index++ // skip '.'
			temp = temp*10 + int32(after[index]-'0')
			index += 2 // skip last digit and '\n'
			if negative {
				temp = -temp
			}
			chunk = after[index:]

			hashIndex := int(hash & (numBuckets - 1))
			for {
				if items[hashIndex].key == nil {
					// Found empty slot, add new item (copying key).
					key := make([]byte, len(station))
					copy(key, station)
					items[hashIndex] = item{
						key: key,
						stat: &stats3{
							min:   temp,
							max:   temp,
							sum:   int64(temp),
							count: 1,
						},
					}
					size++
					if size > numBuckets/2 {
						panic("too many items in hash table")
					}
					break
				}
				if bytes.Equal(items[hashIndex].key, station) {
					// Found matching slot, add to existing stats.
					s := items[hashIndex].stat
					s.min = min(s.min, temp)
					s.max = max(s.max, temp)
					s.sum += int64(temp)
					s.count++
					break
				}
				// Slot already holds another key, try next slot (linear probe).
				hashIndex++
				if hashIndex >= numBuckets {
					hashIndex = 0
				}
			}
		}

		readStart = copy(buf, remaining)
	}

	// Collect results into a map
	result := make(map[string]*stats3, size)
	for _, item := range items {
		if item.key == nil {
			continue
		}
		result[string(item.key)] = item.stat
	}
	resultsCh <- result
}
