package text

import (
	"errors"
	"strings"
	"testing"
)

func TestSafe(t *testing.T) {
	cases := []struct {
		in    string
		limit int
		want  string
	}{
		{"", 64, ""},
		{"ok", 64, "ok"},
		{"a\x00b", 64, "a\uFFFDb"},
		{"bad\xffbyte", 64, "bad\uFFFDbyte"},
		{"éééé", 3, "é..."}, // cut on a rune boundary
	}
	for _, c := range cases {
		if got := Safe(c.in, c.limit); got != c.want {
			t.Errorf("Safe(%q, %d) = %q, want %q", c.in, c.limit, got, c.want)
		}
	}
}

func TestCheckName(t *testing.T) {
	bad := errors.New("bad")
	for _, s := range []string{"", strings.Repeat("x", MaxName+1), "a\x00", "\xff"} {
		if err := CheckName(bad, "name", s); !errors.Is(err, bad) {
			t.Errorf("CheckName(%q) = %v", s, err)
		}
	}
	if err := CheckName(bad, "name", "order-42"); err != nil {
		t.Fatal(err)
	}
}
