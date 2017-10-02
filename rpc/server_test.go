package rpc

import (
	"testing"
)

func TestServer(t *testing.T) {
	s := NewRPCServer(":3870")
	s.Start()
}
