package rpc

import "fmt"

type CClient struct {
	conn map[string]*Conn
}

func NewCClient(addr []string) (*CClient, error) {
	c := &CClient{
		conn: make(map[string]*Conn),
	}
	for _, v := range addr {
		conn, err := NewConn(v)
		if err != nil {
			fmt.Println(err)
			continue
		}
		c.conn[v] = conn
	}
	if len(c.conn) > 0 {
		return c, nil
	} else {
		return nil, errors.New("connect all peer fail.")
	}
}

func (c *CClient) Close() {
	for _, v := range c.conn {
		v.Close()
	}
}
