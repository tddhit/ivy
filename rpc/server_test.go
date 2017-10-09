package rpc

import (
	"testing"
)

type Math struct {
}

func (m Math) Add(i, j int) int {
	return i + j
}

func TestServer(t *testing.T) {
	m := Math{}
	s := NewServer(":3870")
	s.Register(m)
	s.Start()
}
