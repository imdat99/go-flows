//go:build race

package pg_test

// raceEnabled: the race detector randomly drops sync.Pool items, so pooled
// buffers are allocated again and allocation counts mean nothing.
const raceEnabled = true
