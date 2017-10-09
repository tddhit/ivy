package rpc

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
)

type Request struct {
	Name string
	Args []interface{}
}

type iConn struct {
	addr string
	conn net.Conn
}

type iMethod struct {
	method reflect.Method
	args   reflect.Type
	reply  reflect.Type
}

type iService struct {
	name   string
	itype  reflect.Type
	ivalue reflect.Value
	method map[string]*iMethod
}

type Server struct {
	addr     string
	listener net.Listener
	service  map[string]*iService
}

func NewServer(addr string) *Server {
	srv := new(Server)
	srv.addr = addr
	srv.service = make(map[string]*iService)
	return srv
}

func (s *Server) Register(receiver interface{}) {
	itype := reflect.TypeOf(receiver)
	methods := make(map[string]*iMethod)
	for i := 0; i < itype.NumMethod(); i++ {
		m := itype.Method(i)
		methods[m.Name] = &iMethod{
			method: m,
			//args:   m.Type.In(1),
			//reply:  m.Type.In(2),
		}
	}
	sve := new(iService)
	sve.method = methods
	sve.itype = itype
	sve.ivalue = reflect.ValueOf(receiver)
	sve.name = itype.Name()
	s.service[sve.name] = sve
}

func (s *Server) call(conn *iConn, req *Request) {
	dot := strings.LastIndex(req.Name, ".")
	sveName := req.Name[:dot]
	methodName := req.Name[dot+1:]
	if sve, ok := s.service[sveName]; ok {
		if imethod, ok := sve.method[methodName]; ok {
			args := make([]reflect.Value, 0)
			args = append(args, sve.ivalue)
			for _, v := range req.Args {
				args = append(args, reflect.ValueOf(v))
			}
			rtn := imethod.method.Func.Call(args)
			data, _ := Encode(rtn[0].Int())
			conn.write(data)
		}
	}
}

func (s *Server) Start() {
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

func (s *Server) handleConn(conn net.Conn) {
	c := NewConn(conn)
	fmt.Println("New Connection")
	data := c.read()
	req := &Request{}
	err := Decode(data, req)
	if err != nil {
		panic(err)
	} else {
		go s.call(c, req)
	}
}

func Decode(data []byte, req interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(req); err != nil {
		return err
	}
	return nil
}

func Encode(req interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(req); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
