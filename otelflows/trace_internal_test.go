package otelflows

import "testing"

// T10.10: deriving an execution's span context allocates nothing, and
// distinct executions get distinct ones.
func TestExecutionSpanContext(t *testing.T) {
	if n := testing.AllocsPerRun(100, func() { _ = executionSpanContext("instance", 42) }); n != 0 {
		t.Fatalf("%v allocs", n)
	}
	a, b := executionSpanContext("i", 1), executionSpanContext("i", 2)
	c := executionSpanContext("j", 1)
	if !a.IsValid() || a.TraceID() == b.TraceID() || a.TraceID() == c.TraceID() || a.SpanID() == b.SpanID() {
		t.Fatalf("%v %v %v", a, b, c)
	}
	if !a.Equal(executionSpanContext("i", 1)) {
		t.Fatal("not deterministic")
	}
}
