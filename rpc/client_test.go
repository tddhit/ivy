package rpc

import (
	"fmt"
	"testing"
)

func TestClient(t *testing.T) {
	c, _ := NewClient(":3870")
	reply := c.Call("Math.Add", 1, 5, 6)
	reply2 := c.Call("Math.Add", 1, 5, 7)
	for {
		select {
		case res := <-reply.Done:
		case res2 := <-reply2.Done:
		}
	}
}
