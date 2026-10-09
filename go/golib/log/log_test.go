package log

import (
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

// The text of an error is logged verbatim, whatever it contains: MySQL errors carry accounts like 'user'@'%'.
func TestErroreDoesNotInterpretPercentSigns(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	orig := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = orig }()

	want := "Error 1044 (42000): Access denied for user 'orcbk'@'%' to database 'x' like '%a%'"
	_ = Errore(errors.New(want))
	w.Close()
	out, _ := io.ReadAll(r)
	if got := string(out); !strings.Contains(got, want) {
		t.Errorf("logged %q, want it to contain %q", got, want)
	}
}
