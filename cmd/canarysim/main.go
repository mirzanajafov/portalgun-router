package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/canary/sim"
)

func main() {
	out := flag.String("out", filepath.Join("bench", "results", "e4"), "results directory")
	aaTrials := flag.Int("aa-trials", 2000, "trials per A/A cell")
	trials := flag.Int("trials", 500, "trials per regression cell")
	seed := flag.Uint64("seed", 1, "seed")
	step := flag.Duration("step", 5*time.Minute, "length of one canary step")
	flag.Parse()

	cfg := sim.Config{Step: *step, MinStep: 30 * time.Second, Seed: *seed, Alpha: 0.01, Beta: 0.05, Rho: 2}
	began := time.Now()
	results := sim.Run(sim.Grid(*aaTrials, *trials), cfg)
	elapsed := time.Since(began)

	dir := filepath.Join(*out, "sim-"+time.Now().UTC().Format("20060102-150405"))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail(err)
	}
	payload := map[string]any{
		"config":     map[string]any{"step": step.String(), "minStep": cfg.MinStep.String(), "seed": cfg.Seed, "alpha": cfg.Alpha, "beta": cfg.Beta, "rho": cfg.Rho},
		"elapsedSec": elapsed.Seconds(),
		"cells":      results,
	}
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		fail(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "results.json"), append(b, '\n'), 0o644); err != nil {
		fail(err)
	}
	print(results)
	fmt.Printf("\n%d cells in %v, written to %s\n", len(results), elapsed.Round(time.Millisecond), dir)
}

func print(results []sim.CellResult) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "group\tbase\tshare\trps\tcanary\tblip\tsprt (undecided)\tfixed-absolute\tfixed-relative\tpeeking-z\tfixed-horizon\tsprt+terminal\t")
	for _, r := range results {
		c := r.Cell
		canaryDesc := fmt.Sprintf("%.1fx", c.Rho)
		switch {
		case c.TotalFailure:
			canaryDesc = "fails"
		case c.CanarySlow > 0:
			canaryDesc = fmt.Sprintf("slow %.0f%%", c.CanarySlow*100)
		}
		blip := ""
		if c.Blip {
			blip = "blip"
		}
		cols := []string{c.Group, pct(c.BaseError), pct(c.Share), fmt.Sprintf("%.0f", c.RPS), canaryDesc, blip}
		for _, p := range r.Policies {
			cell := fmt.Sprintf("%s rb", pct(p.RollbackRate))
			if p.Rollbacks > 0 {
				cell += fmt.Sprintf(" @%.0fs", p.MedianRollbackS)
			}
			if p.Policy == "sprt" {
				cell += fmt.Sprintf(" (%s)", pct(float64(p.Truncated)/float64(p.Trials)))
			}
			cols = append(cols, cell)
		}
		fmt.Fprintln(tw, strings.Join(cols, "\t")+"\t")
	}
	tw.Flush()
}

func pct(v float64) string {
	return strings.TrimSuffix(strings.TrimSuffix(fmt.Sprintf("%.1f", 100*v), "0"), ".") + "%"
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "canarysim:", err)
	os.Exit(1)
}
