// Package errgroup is a stub of golang.org/x/sync/errgroup for the analyzer tests.
package errgroup

type Group struct{}

func (g *Group) Go(f func() error) {}
func (g *Group) Wait() error       { return nil }
