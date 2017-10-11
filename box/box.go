package main

import (
	"errors"
	"github.com/tddhit/ivy/rpc"
	"os"
)

type Box map[string]string

func (b Box) Set(key, value string) {
	b[key] = value
}

func (b Box) Get(key string) (string, error) {
	if value, ok := b[key]; ok {
		return value, nil
	} else {
		return "", errors.New("not found.")
	}
}

func (b Box) Delete(key string) error {
	if _, ok := b[key]; ok {
		delete(b, key)
		return nil
	} else {
		return errors.New("not found.")
	}
}

func main() {
	b := Box{}
	if len(os.Args) < 2 {
		panic("usage:./box ip:port")
	}
	s := rpc.NewServer(os.Args[1])
	s.Register(b)
	s.Start()
}
