package loadgen

import (
	"bytes"
	"os"
	"strconv"
	"time"
)

const clockTicks = 100

func ProcessCPU() (time.Duration, bool) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}
	return parseProcStat(data)
}

func parseProcStat(data []byte) (time.Duration, bool) {
	i := bytes.LastIndexByte(data, ')')
	if i < 0 {
		return 0, false
	}
	fields := bytes.Fields(data[i+1:])
	if len(fields) < 13 {
		return 0, false
	}
	utime, err := strconv.ParseInt(string(fields[11]), 10, 64)
	if err != nil {
		return 0, false
	}
	stime, err := strconv.ParseInt(string(fields[12]), 10, 64)
	if err != nil {
		return 0, false
	}
	return time.Duration(utime+stime) * time.Second / clockTicks, true
}
