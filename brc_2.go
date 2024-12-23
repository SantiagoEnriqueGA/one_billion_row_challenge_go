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

type stats struct {
	min, max, sum float64
	count         int64
}

func processFile_2(fileName string) string {
	maxGoroutines := 12
	parts, err := splitFile(fileName, maxGoroutines)
	if err != nil {
		return fmt.Sprintf("Error splitting file: %v", err)
	}

	resultsCh := make(chan map[string]stats)
	for _, part := range parts {
		go processPart(fileName, part.offset, part.size, resultsCh)
	}

	totals := make(map[string]stats)
	for i := 0; i < len(parts); i++ {
		result := <-resultsCh
		for station, s := range result {
			ts, ok := totals[station]
			if !ok {
				totals[station] = stats{
					min:   s.min,
					max:   s.max,
					sum:   s.sum,
					count: s.count,
				}
				continue
			}
			ts.min = min(ts.min, s.min)
			ts.max = max(ts.max, s.max)
			ts.sum += s.sum
			ts.count += s.count
			totals[station] = ts
		}
	}

	stations := make([]string, 0, len(totals))
	for station := range totals {
		stations = append(stations, station)
	}
	sort.Strings(stations)

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

func processPart(inputPath string, fileOffset, fileSize int64, resultsCh chan map[string]stats) {
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

	stationStats := make(map[string]stats)

	scanner := bufio.NewScanner(&f)
	for scanner.Scan() {
		line := scanner.Text()
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			panic(err)
		}

		s, ok := stationStats[station]
		if !ok {
			s.min = temp
			s.max = temp
			s.sum = temp
			s.count = 1
		} else {
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++
		}
		stationStats[station] = s
	}

	resultsCh <- stationStats
}

type part struct {
	offset, size int64
}

func splitFile(inputPath string, numParts int) ([]part, error) {
	const maxLineLength = 100

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
