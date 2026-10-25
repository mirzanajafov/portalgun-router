package bench

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"text/tabwriter"
	"time"
)

func WriteJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func writeTable(w io.Writer, text string, flags uint) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', flags)
	if _, err := io.WriteString(tw, text); err != nil {
		return err
	}
	return tw.Flush()
}

func runDir(parent, suffix string) (string, error) {
	dir, err := filepath.Abs(filepath.Join(parent, time.Now().UTC().Format("20060102-150405")+suffix))
	if err != nil {
		return "", err
	}
	return dir, os.MkdirAll(dir, 0o755)
}

func writeConfigs(dir string, configs map[string]any) (string, error) {
	path := filepath.Join(dir, "configs")
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", err
	}
	for name, cfg := range configs {
		if err := WriteJSON(filepath.Join(path, name+".json"), cfg); err != nil {
			return "", err
		}
	}
	return path, nil
}

func startRuns(dir string, meta map[string]any) (*os.File, error) {
	if err := WriteJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		return nil, err
	}
	return os.Create(filepath.Join(dir, "runs.jsonl"))
}

func loadRuns[T any](dir string) ([]T, error) {
	path := filepath.Join(dir, "runs.jsonl")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var runs []T
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		var r T
		err := dec.Decode(&r)
		if errors.Is(err, io.EOF) {
			return runs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		runs = append(runs, r)
	}
}
