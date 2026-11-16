package mem

import (
	"context"
	"fmt"
	"testing"
)

// manyExecutions returns a store holding n running executions on queue
// "bulk" and a parent with 3 running children on queue "c" (T10.8).
func manyExecutions(b *testing.B, n int) (s *Store, parent int64, children []*execution) {
	b.Helper()
	ctx := context.Background()
	s = New(&ManualClock{})
	for i := range n {
		if _, _, err := s.CreateExecution(ctx, newExec(fmt.Sprintf("bulk-%d", i), "bulk")); err != nil {
			b.Fatal(err)
		}
	}
	p, _, err := s.CreateExecution(ctx, newExec("parent", "p"))
	if err != nil {
		b.Fatal(err)
	}
	for i := range 3 {
		ne := newExec(fmt.Sprintf("c%d", i), "c")
		ne.ParentID, ne.ParentSched = p.ID, int32(i+1)
		children = append(children, s.create(ne, s.clock.Now()))
	}
	return s, p.ID, children
}

func BenchmarkWake(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(fmt.Sprintf("executions=%d", n), func(b *testing.B) {
			s, _, _ := manyExecutions(b, n)
			queues := []string{"c"}
			b.ReportAllocs()
			for b.Loop() {
				if ready, _, _ := s.Wake("o", queues); !ready {
					b.Fatal("not ready")
				}
			}
		})
	}
}

func BenchmarkCancelChildrenOf(b *testing.B) {
	for _, n := range []int{100, 10000} {
		b.Run(fmt.Sprintf("executions=%d", n), func(b *testing.B) {
			s, parent, children := manyExecutions(b, n)
			now := s.clock.Now()
			b.ReportAllocs()
			for b.Loop() {
				s.cancelChildrenOf(parent, now)
				for _, ch := range children {
					ch.pending = ch.pending[:1] // keep ExecutionStarted only
				}
			}
		})
	}
}
