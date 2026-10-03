package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

type Compose struct {
	File string
	Env  []string
}

func (c Compose) command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", c.File}, args...)...)
	cmd.Env = append(os.Environ(), c.Env...)
	return cmd
}

func (c Compose) run(ctx context.Context, args ...string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd := c.command(ctx, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("docker compose %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func (c Compose) Up(ctx context.Context) error {
	_, err := c.run(ctx, "up", "-d", "--build", "--force-recreate", "--remove-orphans")
	return err
}

func (c Compose) Down(ctx context.Context) error {
	_, err := c.run(ctx, "down", "--remove-orphans")
	return err
}

func (c Compose) Exec(ctx context.Context, service string, args ...string) (string, error) {
	out, err := c.run(ctx, append([]string{"exec", "-T", service}, args...)...)
	return strings.TrimSpace(string(out)), err
}

type CPUTime struct {
	Total  time.Duration
	User   time.Duration
	System time.Duration
}

func (a CPUTime) Sub(b CPUTime) CPUTime {
	return CPUTime{Total: a.Total - b.Total, User: a.User - b.User, System: a.System - b.System}
}

func (c Compose) CPU(ctx context.Context, service string) (CPUTime, error) {
	out, err := c.Exec(ctx, service, "cat", "/sys/fs/cgroup/cpu.stat")
	if err != nil {
		return CPUTime{}, err
	}
	return parseCPUStat(out)
}

func parseCPUStat(s string) (CPUTime, error) {
	var t CPUTime
	fields := map[string]*time.Duration{"usage_usec": &t.Total, "user_usec": &t.User, "system_usec": &t.System}
	seen := 0
	for line := range strings.Lines(s) {
		name, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		dst, known := fields[name]
		if !ok || !known {
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return t, fmt.Errorf("cpu.stat %s: %w", name, err)
		}
		*dst = time.Duration(n) * time.Microsecond
		seen++
	}
	if seen != len(fields) {
		return t, fmt.Errorf("cpu.stat lacks usage, user or system time")
	}
	return t, nil
}

func (c Compose) CPUs(ctx context.Context, services []string) (map[string]CPUTime, error) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	out := make(map[string]CPUTime, len(services))
	var firstErr error
	for _, s := range services {
		wg.Go(func() {
			d, err := c.CPU(ctx, s)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			out[s] = d
		})
	}
	wg.Wait()
	return out, firstErr
}

func (c Compose) Loadgen(ctx context.Context, args ...string) (loadgen.Summary, error) {
	var sum loadgen.Summary
	out, err := c.run(ctx, append([]string{"run", "--rm", "-T", "loadgen"}, args...)...)
	if err != nil {
		return sum, err
	}
	if err := json.Unmarshal(out, &sum); err != nil {
		return sum, fmt.Errorf("loadgen output: %w", err)
	}
	return sum, nil
}
