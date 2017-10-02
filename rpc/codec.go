package rpc

import (
	"bytes"
	"encoding/gob"
)

type Request struct {
	Name string
	Args []interface{}
}

func Register(value interface{}) {
	gob.Register(value)
}

func Decode(data []byte) (*Request, error) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	req := &Request{}
	if err := dec.Decode(req); err != nil {
		return nil, err
	}
	return req, nil
}

func Encode(req *Request) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(req); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
