package rpc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"log"
	"net"
)

type Request struct {
	Seq  uint64
	Name string
	Args []interface{}
}

type Response struct {
	Seq   uint64
	Name  string
	Reply []interface{}
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
	log.Println(ob)
	var encbuf bytes.Buffer
	enc := gob.NewEncoder(&encbuf)
	if err = enc.Encode(ob); err != nil {
		log.Println(err)
		conn.Write([]byte(err.Error()))
		return
	}
	buf := make([]byte, 4+len(encbuf.Bytes()))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(encbuf.Bytes())))
	copy(buf[4:], encbuf.Bytes())
	if conn != nil {
		_, err = conn.Write(buf)
	} else {
		err = errors.New("can't connect")
	}
	return
}
