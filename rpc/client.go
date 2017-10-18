package rpc

import (
	"errors"
	"net"
	"sync"
	"time"
)

type Call struct {
	Req   *Request
	Rsp   *Response
	Done  chan *Call
	Error error
}

type Client struct {
	addr     string
	conn     net.Conn
	reqMutex sync.Mutex
	mutex    sync.Mutex
	seq      uint64
	pending  map[uint64]*Call
}

func NewClient(addr string) *Client {
	c := &Client{
		addr:    addr,
		pending: make(map[uint64]*Call),
	}
	return c
}

//func (c *Client) IsAlive() bool {
//	return c.conn != nil
//}

func (c *Client) Dial() (err error) {
	conn, err := net.DialTimeout("tcp", c.addr, 100*time.Millisecond)
	if err != nil {
		return
	}
	c.conn = conn
	go c.handleRsp()
	return
}

func (c *Client) handleRsp() {
	var err error
	//var rsp Response
	for err == nil {
		rsp := &Response{}
		err = read(rsp, c.conn)
		if err != nil {
			break
		}
		seq := rsp.Seq
		c.mutex.Lock()
		call := c.pending[seq]
		delete(c.pending, seq)
		c.mutex.Unlock()
		call.Rsp = rsp
		call.Done <- call
	}
	c.Close()
}

func (c *Client) Close() {
	c.reqMutex.Lock()
	c.mutex.Lock()
	for _, call := range c.pending {
		call.Error = errors.New("close.")
		call.Done <- call
	}
	c.conn.Close()
	c.mutex.Unlock()
	c.reqMutex.Unlock()
}

func (c *Client) Call(name string, args ...interface{}) *Call {
	call := &Call{
		Req:  &Request{Name: name, Args: args},
		Done: make(chan *Call, 100),
	}
	c.send(call)
	return call
}

func (c *Client) send(call *Call) {
	c.reqMutex.Lock()
	defer c.reqMutex.Unlock()

	c.mutex.Lock()
	seq := c.seq
	c.seq++
	c.pending[seq] = call
	c.mutex.Unlock()

	call.Req.Seq = seq
	if c.conn == nil {
		err := c.Dial()
		if err != nil {
			call.Error = err
			call.Done <- call
			return
		}
	}
	if err := write(call.Req, c.conn); err != nil {
		c.mutex.Lock()
		call = c.pending[seq]
		delete(c.pending, seq)
		c.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.Done <- call
		}
	}
}
