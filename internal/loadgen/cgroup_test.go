package loadgen

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeCgroup(t *testing.T, dir string, usage, user, system int64, cpus string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	stat := fmt.Sprintf("usage_usec %d\nuser_usec %d\nsystem_usec %d\nnr_periods 0\n", usage, user, system)
	if err := os.WriteFile(filepath.Join(dir, "cpu.stat"), []byte(stat), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cpuset.cpus.effective"), []byte(cpus+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFindCgroupKnowsBothDrivers(t *testing.T) {
	root := t.TempDir()
	writeCgroup(t, filepath.Join(root, "docker", "abc"), 1, 1, 0, "0")
	writeCgroup(t, filepath.Join(root, "system.slice", "docker-def.scope"), 1, 1, 0, "0")
	if dir, err := FindCgroup(root, "abc"); err != nil || filepath.Base(dir) != "abc" {
		t.Fatalf("cgroupfs layout: %q %v", dir, err)
	}
	if dir, err := FindCgroup(root, "def"); err != nil || filepath.Base(dir) != "docker-def.scope" {
		t.Fatalf("systemd layout: %q %v", dir, err)
	}
	if _, err := FindCgroup(root, "missing"); err == nil {
		t.Fatal("found a container that isn't there")
	}
}

func TestParseCPUStat(t *testing.T) {
	got, err := ParseCPUStat("usage_usec 1500000\nuser_usec 1000000\nsystem_usec 500000\nnr_periods 0\n")
	want := CgroupCPU{Usage: 1500 * time.Millisecond, User: time.Second, System: 500 * time.Millisecond}
	if err != nil || got != want {
		t.Fatalf("got %+v %v", got, err)
	}
	if _, err := ParseCPUStat("usage_usec 10\nnr_periods 0\n"); err == nil {
		t.Fatal("missing user and system time accepted")
	}
}

func TestCountCPUs(t *testing.T) {
	for in, want := range map[string]int{"0": 1, "1-2": 2, "0-3,6,8-9": 7, "": 0, "3-1": 0, "x": 0} {
		if got := countCPUs(in); got != want {
			t.Errorf("%q: got %d, want %d", in, got, want)
		}
	}
}

func TestWatcherMeasuresTheWindow(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "docker", "sut")
	writeCgroup(t, dir, 1_000_000, 600_000, 400_000, "3")
	w := newWatcher(map[string]string{"system": dir})
	w.begin()
	writeCgroup(t, dir, 3_000_000, 1_500_000, 1_500_000, "3")
	got := w.end(4)["system"]
	if got.Seconds != 2 || got.UserSeconds != 0.9 || got.SystemSeconds != 1.1 || got.Cores != 1 || got.Utilization != 0.5 {
		t.Fatalf("watched %+v", got)
	}
}

func TestRunReportsWatchedCgroups(t *testing.T) {
	srv := dimensionServer(t, nil)
	dir := filepath.Join(t.TempDir(), "docker", "sut")
	writeCgroup(t, dir, 0, 0, 0, "0-1")
	res, err := Run(context.Background(), Config{URLs: []string{srv.URL}, Rate: 100, Duration: 100 * time.Millisecond, Watch: map[string]string{"system": dir}})
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := res.Summary.Watched["system"]; !ok || got.Cores != 2 {
		t.Fatalf("watched %+v", res.Summary.Watched)
	}
}
