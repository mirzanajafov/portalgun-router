package httpx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

var errTrailingData = errors.New("the body has more after its JSON value")

func ReadJSON(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	_, err := dec.Token()
	var syntax *json.SyntaxError
	switch {
	case errors.Is(err, io.EOF):
		return nil
	case err == nil, errors.As(err, &syntax):
		return errTrailingData
	default:
		return err
	}
}

func WriteError(w http.ResponseWriter, status int, msg string) {
	WriteJSON(w, status, map[string]string{"error": msg})
}
