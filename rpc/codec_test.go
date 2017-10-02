package rpc

import (
	"fmt"
	"testing"
)

type Int struct {
	X int
}

func TestCodec(t *testing.T) {
	args := make([]interface{}, 2)
	args[0] = Int{1}
	args[1] = Int{2}
	req := &Request{"Add", args}
	Register(Int{})
	if data, err := Encode(req); err != nil {
		panic(err)
	} else {
		if req, err := Decode(data); err != nil {
			panic(err)
		} else {
			fmt.Println(req.Name)
			fmt.Println(req.Args[0].(Int).X)
			fmt.Println(req.Args[1].(Int).X)
		}
	}
}
