package rpc

import (
	"testing"
)

func TestClient(t *testing.T) {
	c := NewClient(":3870")
	args := make([]interface{}, 2)
	args[0] = 1
	args[1] = 2
	req := &Request{"Add", args}
	c.Call(req)
}
