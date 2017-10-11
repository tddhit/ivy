package util

import (
	"fmt"
	"testing"
)

func TestConsistentHash(t *testing.T) {
	rnodes := make([]RNode, 0)
	rnodes = append(rnodes, RNode{"127.0.0.1", 1})
	rnodes = append(rnodes, RNode{"127.0.0.2", 2})
	rnodes = append(rnodes, RNode{"127.0.0.3", 1})
	rnodes = append(rnodes, RNode{"127.0.0.4", 2})
	rnodes = append(rnodes, RNode{"127.0.0.5", 1})
	hashRing := NewConsistentHash(rnodes, 10)
	hashRing.AddNode(RNode{"127.0.0.6", 2})
	hashRing.RemoveNode(RNode{"127.0.0.5", 1})
}
