package rpc

import (
	"testing"
)

func TestClient(t *testing.T) {
	c, _ := NewClient(":3870")
	args := make([]interface{}, 2)
	args[0] = 1
	args[1] = 2
	c.Call("Math.Add", args)
	select {}
}
