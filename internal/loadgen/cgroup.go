package loadgen

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type CgroupCPU struct {
	Usage  time.Duration
	User   time.Duration
	System time.Duration
}

func (a CgroupCPU) Sub(b CgroupCPU) CgroupCPU {
	return CgroupCPU{Usage: a.Usage - b.Usage, User: a.User - b.User, System: a.System - b.System}
}

func FindCgroup(root, containerID string) (string, error) {
	for _, dir := range []string{
		filepath.Join(root, "docker", containerID),
		filepath.Join(root, "system.slice", "docker-"+containerID+".scope"),
	} {
		if _, err := os.Stat(filepath.Join(dir, "cpu.stat")); err == nil {
			return dir, nil
		}
	}
	return "", fmt.Errorf("no cgroup for container %.12s under %s", containerID, root)
}

func ReadCgroupCPU(dir string) (CgroupCPU, error) {
	data, err := os.ReadFile(filepath.Join(dir, "cpu.stat"))
	if err != nil {
		return CgroupCPU{}, err
	}
	return ParseCPUStat(string(data))
}

func ParseCPUStat(s string) (CgroupCPU, error) {
	var c CgroupCPU
	fields := map[string]*time.Duration{"usage_usec": &c.Usage, "user_usec": &c.User, "system_usec": &c.System}
	seen := 0
	for line := range strings.Lines(s) {
		name, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		dst, known := fields[name]
		if !ok || !known {
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return c, fmt.Errorf("cpu.stat %s: %w", name, err)
		}
		*dst = time.Duration(n) * time.Microsecond
		seen++
	}
	if seen != len(fields) {
		return c, errors.New("cpu.stat lacks usage, user or system time")
	}
	return c, nil
}

func CgroupCores(dir string) int {
	data, err := os.ReadFile(filepath.Join(dir, "cpuset.cpus.effective"))
	if err != nil {
		return 0
	}
	return countCPUs(strings.TrimSpace(string(data)))
}

func countCPUs(list string) int {
	n := 0
	for part := range strings.SplitSeq(list, ",") {
		if part == "" {
			continue
		}
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil {
			return 0
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil || b < a {
				return 0
			}
		}
		n += b - a + 1
	}
	return n
}

type Watched struct {
	Seconds       float64 `json:"seconds"`
	UserSeconds   float64 `json:"userSeconds"`
	SystemSeconds float64 `json:"systemSeconds"`
	Cores         int     `json:"cores"`
	Utilization   float64 `json:"utilization,omitempty"`
}

type watcher struct {
	dirs  map[string]string
	start map[string]CgroupCPU
}

func newWatcher(dirs map[string]string) *watcher {
	return &watcher{dirs: dirs, start: map[string]CgroupCPU{}}
}

func (w *watcher) begin() {
	for name, dir := range w.dirs {
		if c, err := ReadCgroupCPU(dir); err == nil {
			w.start[name] = c
		}
	}
}

func (w *watcher) end(window float64) map[string]Watched {
	if len(w.start) == 0 {
		return nil
	}
	out := map[string]Watched{}
	for name, before := range w.start {
		after, err := ReadCgroupCPU(w.dirs[name])
		if err != nil {
			continue
		}
		d := after.Sub(before)
		v := Watched{
			Seconds:       round(d.Usage.Seconds(), 3),
			UserSeconds:   round(d.User.Seconds(), 3),
			SystemSeconds: round(d.System.Seconds(), 3),
			Cores:         CgroupCores(w.dirs[name]),
		}
		if v.Cores > 0 && window > 0 {
			v.Utilization = round(d.Usage.Seconds()/(window*float64(v.Cores)), 3)
		}
		out[name] = v
	}
	return out
}
