// Package text makes caller-supplied text safe to store (ported from saga's
// internal/store/text.go): Postgres text columns reject NUL bytes and
// invalid UTF-8, and a checkpoint carrying such text would fail on every
// attempt.
package text

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Safe makes s storable: invalid sequences and NULs become U+FFFD, and the
// text is cut to at most limit bytes on a rune boundary ("..." appended).
// Valid text within the limit is returned as is, without allocating.
func Safe(s string, limit int) string {
	if s == "" {
		return s
	}
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "�")
	}
	if len(s) <= limit {
		return s
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "..."
}

// MaxName bounds names and ids supplied by the caller: indexed columns cap
// a btree entry at about 2.7 KB, and NOTIFY payloads at 8000 bytes.
const MaxName = 1024

// CheckName validates a name or id the engine stores and returns an error
// wrapping invalid that says why: empty, too long, not valid UTF-8, or
// containing a NUL byte.
func CheckName(invalid error, what, s string) error {
	switch {
	case s == "":
		return fmt.Errorf("%w: empty %s", invalid, what)
	case len(s) > MaxName:
		return fmt.Errorf("%w: %s of %d bytes (at most %d)", invalid, what, len(s), MaxName)
	case !utf8.ValidString(s) || strings.IndexByte(s, 0) >= 0:
		return fmt.Errorf("%w: %s %q is not valid UTF-8 text", invalid, what, s)
	}
	return nil
}
