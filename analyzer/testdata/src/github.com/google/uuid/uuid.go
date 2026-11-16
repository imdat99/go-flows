// Package uuid is a stub of github.com/google/uuid for the analyzer tests.
package uuid

type UUID [16]byte

func New() UUID                            { return UUID{} }
func NewString() string                    { return "" }
func NewRandom() (UUID, error)             { return UUID{}, nil }
func NewV7() (UUID, error)                 { return UUID{}, nil }
func NewSHA1(space UUID, data []byte) UUID { return UUID{} }
func MustParse(s string) UUID              { return UUID{} }
