package bench

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type Merged struct {
	MergedFrom []string `json:"mergedFrom"`
	Records    []int    `json:"records"`
}

func MergeRuns(out string, runs []string) error {
	if len(runs) < 2 {
		return errors.New("merge needs at least two runs")
	}
	if _, err := os.Stat(filepath.Join(out, "runs.jsonl")); err == nil {
		return fmt.Errorf("%s already holds runs", out)
	}
	var all bytes.Buffer
	var meta Merged
	for _, dir := range runs {
		b, err := os.ReadFile(filepath.Join(dir, "runs.jsonl"))
		if err != nil {
			return err
		}
		lines := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
		for i, line := range lines {
			if !json.Valid(line) {
				return fmt.Errorf("%s: record %d is not JSON", dir, i+1)
			}
			all.Write(line)
			all.WriteByte('\n')
		}
		meta.MergedFrom = append(meta.MergedFrom, filepath.Base(filepath.Clean(dir)))
		meta.Records = append(meta.Records, len(lines))
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	if err := WriteJSON(filepath.Join(out, "meta.json"), meta); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "runs.jsonl"), all.Bytes(), 0o644)
}
