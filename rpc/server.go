package rpc

import (
	"net"
	"reflect"
	"strings"
	"sync"
)

type iMethod struct {
	method reflect.Method
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
	srv := &Server{
		addr:    addr,
		service: make(map[string]*iService),
	}
	return srv
}

func (s *Server) Start() {
	var err error
	s.listener, err = net.Listen("tcp", s.addr)
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

func (s *Server) Register(receiver interface{}) {
	itype := reflect.TypeOf(receiver)
	methods := make(map[string]*iMethod)
	for i := 0; i < itype.NumMethod(); i++ {
		m := itype.Method(i)
		methods[m.Name] = &iMethod{
			method: m,
		}
	}
	sve := &iService{
		method: methods,
		itype:  itype,
		ivalue: reflect.ValueOf(receiver),
		name:   itype.Name(),
	}
	s.service[sve.name] = sve
}

func (s *Server) handleConn(conn net.Conn) {
	sendMutex := new(sync.Mutex)
	for {
		req := &Request{}
		err := read(req, conn)
		if err != nil {
			break
		} else {
			go s.call(conn, req, sendMutex)
		}
	}
	conn.Close()
}

func (s *Server) call(conn net.Conn, req *Request, sendMutex *sync.Mutex) {
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
			replys := make([]interface{}, 0)
			for _, v := range rtn {
				replys = append(replys, v.Interface())
			}
			rsp := &Response{
				Seq:   req.Seq,
				Name:  req.Name,
				Reply: replys,
			}
			sendMutex.Lock()
			write(rsp, conn)
			sendMutex.Unlock()
		}
	}
}
