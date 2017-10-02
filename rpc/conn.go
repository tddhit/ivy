package rpc

import (
	//"encoding/binary"
	"encoding/binary"
	"fmt"
	//"io/ioutil"
	"io"
	"net"
)

type iConn struct {
	addr string
	conn net.Conn
}

func NewConn(conn net.Conn) *iConn {
	c := new(iConn)
	c.conn = conn
	return c
}

func (c *iConn) read() []byte {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		panic(err)
	}
	length := binary.LittleEndian.Uint32(buf)
	fmt.Println(length)
	buf = make([]byte, length)
	if _, err := io.ReadFull(c.conn, buf); err != nil {
		panic(err)
	}
	return buf
}

func (c *iConn) write(data []byte) {
	buf := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(data)))
	copy(buf[4:], data)
	c.conn.Write(buf)
}
