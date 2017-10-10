package rpc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"net"
)

type Request struct {
	seq  uint64
	Name string
	Args interface{}
}

type Response struct {
	seq   uint64
	Name  string
	Reply interface{}
}

func read(ob interface{}, conn net.Conn) (err error) {
	buf := make([]byte, 4)
	if _, err = io.ReadFull(conn, buf); err != nil {
		return
	}
	length := binary.LittleEndian.Uint32(buf)
	buf = make([]byte, length)
	if _, err = io.ReadFull(conn, buf); err != nil {
		return
	}
	decbuf := bytes.NewBuffer(buf)
	dec := gob.NewDecoder(decbuf)
	if err = dec.Decode(ob); err != nil {
		return
	}
	return
}

func write(ob interface{}, conn net.Conn) (err error) {
	var encbuf bytes.Buffer
	enc := gob.NewEncoder(&encbuf)
	if err = enc.Encode(ob); err != nil {
		return
	}
	buf := make([]byte, 4+len(encbuf.Bytes()))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(encbuf.Bytes())))
	copy(buf[4:], encbuf.Bytes())
	_, err = conn.Write(buf)
	return
}