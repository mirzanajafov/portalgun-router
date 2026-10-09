package bench

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeRuns(t *testing.T, dir, records string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "runs.jsonl"), []byte(records), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestMergeRuns(t *testing.T) {
	dir := t.TempDir()
	a, b, out := filepath.Join(dir, "a"), filepath.Join(dir, "b"), filepath.Join(dir, "merged")
	writeRuns(t, a, "{\"cell\":\"x\"}\n{\"cell\":\"y\"}\n")
	writeRuns(t, b, "{\"cell\":\"z\"}")
	if err := MergeRuns(out, []string{a, b}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(out, "runs.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "{\"cell\":\"x\"}\n{\"cell\":\"y\"}\n{\"cell\":\"z\"}\n" {
		t.Fatalf("runs %q", got)
	}
	raw, err := os.ReadFile(filepath.Join(out, "meta.json"))
	if err != nil {
		t.Fatal(err)
	}
	var meta Merged
	if err := json.Unmarshal(raw, &meta); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(meta, Merged{MergedFrom: []string{"a", "b"}, Records: []int{2, 1}}) {
		t.Fatalf("meta %+v", meta)
	}
	if err := MergeRuns(out, []string{a, b}); err == nil {
		t.Fatal("merged over a directory that already holds runs")
	}
}

func TestMergeRunsRefusesACutOffRecord(t *testing.T) {
	dir := t.TempDir()
	a, b := filepath.Join(dir, "a"), filepath.Join(dir, "b")
	writeRuns(t, a, "{\"cell\":\"x\"}\n")
	writeRuns(t, b, "{\"cell\":\"z\"}\n{\"cel")
	out := filepath.Join(dir, "merged")
	if err := MergeRuns(out, []string{a, b}); err == nil {
		t.Fatal("merged a run whose last record was cut off")
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Fatalf("left %s behind: %v", out, err)
	}
}
