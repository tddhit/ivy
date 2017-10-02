package rpc

import (
	"fmt"
	"net"
)

type RPCServer struct {
	addr     string
	listener net.Listener
}

func NewRPCServer(addr string) *RPCServer {
	rpcServer := new(RPCServer)
	rpcServer.addr = addr
	return rpcServer
}

func (s *RPCServer) Start() {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
	fmt.Println("Start Listen.")
	if err != nil {
		panic(err)
	}
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			panic(err)
		} else {
			go s.handleConn(conn)
		}
	}
}

func (s *RPCServer) handleConn(conn net.Conn) {
	c := NewConn(conn)
	fmt.Println("New Connection")
	data := c.read()
	req, err := Decode(data)
	if err != nil {
		panic(err)
	} else {
		fmt.Println(req)
	}
}
