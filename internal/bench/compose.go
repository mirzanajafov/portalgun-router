package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"

	"github.com/mirzanajafov/portalgun-router/internal/loadgen"
)

type Compose struct {
	File string
	Env  []string
}

func (c Compose) with(env ...string) Compose {
	c.Env = append(slices.Clone(c.Env), env...)
	return c
}

func (c Compose) command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "-f", c.File}, args...)...)
	cmd.Env = append(os.Environ(), c.Env...)
	return cmd
}

func capture(cmd *exec.Cmd, what string) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w: %s", what, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

func (c Compose) run(ctx context.Context, args ...string) ([]byte, error) {
	return capture(c.command(ctx, args...), "docker compose "+strings.Join(args, " "))
}

func docker(ctx context.Context, args ...string) (string, error) {
	out, err := capture(exec.CommandContext(ctx, "docker", args...), "docker "+strings.Join(args, " "))
	return strings.TrimSpace(string(out)), err
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

func (c Compose) ContainerIDs(ctx context.Context, services []string) (map[string]string, error) {
	ids := map[string]string{}
	for _, s := range services {
		out, err := c.run(ctx, "ps", "-q", "--no-trunc", s)
		if err != nil {
			return nil, err
		}
		id := strings.TrimSpace(string(out))
		if id == "" || strings.ContainsAny(id, " \n") {
			return nil, fmt.Errorf("expected one container for %s, got %q", s, id)
		}
		ids[s] = id
	}
	return ids, nil
}

func (c Compose) Loadgen(ctx context.Context, args ...string) (loadgen.Summary, error) {
	var sum loadgen.Summary
	out, err := c.run(ctx, append([]string{"exec", "-T", "loadgen", "loadgen"}, args...)...)
	if err != nil {
		return sum, err
	}
	if err := json.Unmarshal(out, &sum); err != nil {
		return sum, fmt.Errorf("loadgen output: %w", err)
	}
	return sum, nil
}
