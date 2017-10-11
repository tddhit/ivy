package rpc

import (
	"testing"
)

type Math struct {
}

func (m Math) Add(i, j, k int) int {
	return i + j + k
}

func TestServer(t *testing.T) {
	m := Math{}
	s := NewServer(":3870")
	s.Register(m)
	s.Start()
}
