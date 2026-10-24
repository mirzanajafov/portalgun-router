package bench

import (
	"encoding/json"
	"io"
	"os"
	"text/tabwriter"
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
