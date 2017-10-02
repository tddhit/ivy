package rpc

import (
	"net"
)

type Client struct {
	peer string
	conn *iConn
}

func NewClient(peer string) *Client {
	c := new(Client)
	c.peer = peer
	conn, err := net.Dial("tcp", c.peer)
	if err != nil {
		panic(err)
	}
	c.conn = NewConn(conn)
	return c
}

func (c *Client) Call(req *Request) {
	data, err := Encode(req)
	if err != nil {
		panic(err)
	}
	c.conn.write(data)
}
