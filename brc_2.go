package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// stats2 holds statistical data for each station
type stats2 struct {
	min, max, sum float64 // Minimum, maximum, and sum of temperatures
	count         int64   // Count of temperature readings
}

// processFile_2 processes the input file and returns the result as a string
func processFile_2(fileName string) string {
	// Split the file into parts for concurrent processing
	parts, err := splitFile(fileName, maxGoroutines)
	if err != nil {
		return fmt.Sprintf("Error splitting file: %v", err)
	}

	// Channel to collect results from goroutines
	resultsCh := make(chan map[string]stats2)
	for _, part := range parts {
		// Process each part concurrently
		go processPart_2(fileName, part.offset, part.size, resultsCh)
	}

	// Aggregate results from all parts
	totals := make(map[string]stats2)
	for i := 0; i < len(parts); i++ {
		result := <-resultsCh
		for station, s := range result {
			ts, ok := totals[station]
			if !ok {
				// Initialize totals for the station if not present
				totals[station] = stats2{
					min:   s.min,
					max:   s.max,
					sum:   s.sum,
					count: s.count,
				}
				continue
			}
			// Update totals with new data
			ts.min = min(ts.min, s.min)
			ts.max = max(ts.max, s.max)
			ts.sum += s.sum
			ts.count += s.count
			totals[station] = ts
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
		mean := s.sum / float64(s.count)
		resultBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", station, s.min, mean, s.max))
	}
	resultBuilder.WriteString("}\n")
	return resultBuilder.String()
}

// processPart_2 processes a part of the file and sends the result to the channel
func processPart_2(inputPath string, fileOffset, fileSize int64, resultsCh chan map[string]stats2) {
	// Open the file and seek to the specified offset
	file, err := os.Open(inputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Create a limited reader for the part of the file
	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	f := io.LimitedReader{R: file, N: fileSize}

	// Initialize station statistics
	stationStats := make(map[string]stats2)

	// Read the part line by line and update station statistics
	scanner := bufio.NewScanner(&f)
	for scanner.Scan() {
		line := scanner.Text()
		// Split the line by semicolon to get station and temperature
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue // Skip lines without semicolon
		}

		// Parse the temperature as a float
		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			panic(err)
		}

		// Update station statistics
		s, ok := stationStats[station]
		if !ok {
			// Initialize stats for the station if not present
			s.min = temp
			s.max = temp
			s.sum = temp
			s.count = 1
		} else {
			// Update stats with new data
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++
		}
		// Store the updated stats
		stationStats[station] = s
	}

	// Send the result to the channel
	resultsCh <- stationStats
}

// part represents a part of the file with offset and size
type part struct {
	offset, size int64 // Offset and size of the file part
}

// splitFile splits the input file into parts for concurrent processing
func splitFile(inputPath string, numParts int) ([]part, error) {
	const maxLineLength = 100 // Maximum length of a line

	f, err := os.Open(inputPath)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	splitSize := size / int64(numParts)

	buf := make([]byte, maxLineLength)

	parts := make([]part, 0, numParts)
	offset := int64(0)
	for i := 0; i < numParts; i++ {
		if i == numParts-1 {
			// Last part takes the remaining size
			if offset < size {
				parts = append(parts, part{offset, size - offset})
			}
			break
		}

		seekOffset := max(offset+splitSize-maxLineLength, 0)
		_, err := f.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		n, _ := io.ReadFull(f, buf)
		chunk := buf[:n]
		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			return nil, fmt.Errorf("newline not found at offset %d", offset+splitSize-maxLineLength)
		}
		remaining := len(chunk) - newline - 1
		nextOffset := seekOffset + int64(len(chunk)) - int64(remaining)
		parts = append(parts, part{offset, nextOffset - offset})
		offset = nextOffset
	}
	return parts, nil
}
