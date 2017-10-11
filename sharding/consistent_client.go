package sharding

import (
	"errors"
	"github.com/tddhit/ivy/rpc"
	. "github.com/tddhit/ivy/util"
)

type CClient struct {
	hashRing *ConsistentHash
	client   map[string]*rpc.Client
}

func NewCClient(nodes []RNode) (*CClient, error) {
	c := &CClient{
		hashRing: NewConsistentHash(nodes, 10),
		client:   make(map[string]*rpc.Client),
	}
	for _, v := range nodes {
		client, err := rpc.NewClient(v.Id)
		if err != nil {
			panic(err)
		}
		c.client[v.Id] = client
	}
	if len(c.client) > 0 {
		return c, nil
	} else {
		return nil, errors.New("connect all peer fail.")
	}
}

func (c *CClient) Set(key, value string) error {
	node := c.hashRing.GetNode(key)
	if client, ok := c.client[node.Id]; ok {
		reply := client.Call("Box.Set", key, value)
		call := <-reply.Done
		return call.Error
	}
	return errors.New("no suitable node.")
}

func (c *CClient) Get(key string) (string, error) {
	node := c.hashRing.GetNode(key)
	if client, ok := c.client[node.Id]; ok {
		reply := client.Call("Box.Get", key)
		call := <-reply.Done
		if len(call.Rsp.Reply) == 2 {
			return call.Rsp.Reply[0].(string), call.Error
		}
	}
	return "", errors.New("no suitable node.")
}

func (c *CClient) Delete(key string) error {
	node := c.hashRing.GetNode(key)
	if client, ok := c.client[node.Id]; ok {
		reply := client.Call("Box.Delete", key)
		call := <-reply.Done
		return call.Error
	}
	return errors.New("no suitable node.")
}

func (c *CClient) Close() {
	for _, v := range c.client {
		v.Close()
	}
}
