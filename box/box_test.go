package main

import (
	"github.com/tddhit/ivy/rpc"
	"log"
	"testing"
)

func TestBox(t *testing.T) {
	client := rpc.NewClient("127.0.0.1:5870")
	client.Dial()
	log.Println("Set...")
	reply := client.Call("Box.Set", "1name", "dd")
	reply2 := client.Call("Box.Set", "1name2", "dd2")
	reply3 := client.Call("Box.Set", "1name3", "dd3")
	reply4 := client.Call("Box.Set", "1name4", "dd4")
	count := 0
	for {
		select {
		case <-reply.Done:
			log.Println("1")
			count++
		case <-reply2.Done:
			log.Println("2")
			count++
		case <-reply3.Done:
			log.Println("3")
			count++
		case <-reply4.Done:
			log.Println("4")
			count++
		}
		if count == 4 {
			break
		}
	}
	log.Println("Get...")
	reply = client.Call("Box.Get", "1name")
	reply2 = client.Call("Box.Get", "1name2")
	reply3 = client.Call("Box.Get", "1name3")
	reply4 = client.Call("Box.Get", "1name4")
	count = 0
	for {
		select {
		case call := <-reply.Done:
			log.Println(call.Rsp.Reply[0])
			count++
		case call := <-reply2.Done:
			log.Println(call.Rsp.Reply[0])
			count++
		case call := <-reply3.Done:
			log.Println(call.Rsp.Reply[0])
			count++
		case call := <-reply4.Done:
			log.Println(call.Rsp.Reply[0])
			count++
		}
		if count == 4 {
			break
		}
	}
}
