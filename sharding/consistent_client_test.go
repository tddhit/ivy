package sharding

import (
	. "github.com/tddhit/ivy/util"
	"log"
	"testing"
)

func TestCClient(t *testing.T) {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	rnodes := make([]RNode, 0)
	rnodes = append(rnodes, RNode{"127.0.0.1:3870", 1})
	rnodes = append(rnodes, RNode{"127.0.0.1:3880", 1})
	c, err := NewCClient(rnodes)
	if err != nil {
		panic(err)
	}
	c.Set("name", "dandang")
	c.Set("sex", "male")
	c.Set("age", "26")
	log.Println(c.Get("name"))
	log.Println(c.Get("sex"))
	log.Println(c.Get("age"))

}
