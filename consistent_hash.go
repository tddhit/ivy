package util

import (
	"crypto/sha1"
	"encoding/hex"
	//	"fmt"
	"math"
	"sort"
	"strconv"
)

type ConsistentHash struct {
	vprNum int // vnode num per rnode
	vnodes iVNodeArray
	rnodes map[string]RNode
}

type RNode struct {
	id     string
	weight int
}

type iVNode struct {
	key int
	id  string
}

type iVNodeArray []iVNode

func (n iVNodeArray) Len() int {
	return len(n)
}

func (n iVNodeArray) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n iVNodeArray) Less(i, j int) bool {
	return n[i].key < n[j].key
}

func (n iVNodeArray) Sort() {
	sort.Sort(n)
}

func NewConsistentHash(rnodes []RNode, vprNum int) *ConsistentHash {
	hashRing := new(ConsistentHash)
	hashRing.vprNum = vprNum
	hashRing.rnodes = make(map[string]RNode)
	for _, v := range rnodes {
		hashRing.rnodes[v.id] = v
	}
	hashRing.adjust()
	return hashRing
}

func (this *ConsistentHash) AddNode(rnode RNode) {
	this.rnodes[rnode.id] = rnode
	this.adjust()
}

func (this *ConsistentHash) RemoveNode(rnode RNode) {
	delete(this.rnodes, rnode.id)
	this.adjust()
}

func (this *ConsistentHash) GetNode(key string) RNode {
	k := hash(key)
	i := sort.Search(len(this.vnodes), func(i int) bool { return this.vnodes[i].key >= k })
	if i == len(this.vnodes) {
		i = 0
	}
	return this.rnodes[this.vnodes[i].id]
}

func (this *ConsistentHash) adjust() {
	this.vnodes = make(iVNodeArray, 2*len(this.rnodes)*this.vprNum)
	totalWeight := 0
	for _, v := range this.rnodes {
		totalWeight += v.weight
	}
	for k, v := range this.rnodes {
		j := int(math.Floor((float64(v.weight) / float64(totalWeight)) * float64(len(this.rnodes)) * float64(this.vprNum)))
		for i := 0; i < j; i++ {
			key := hash(k + "#" + strconv.Itoa(i))
			this.vnodes = append(this.vnodes, iVNode{key, k})
		}
	}
	this.vnodes.Sort()
}

func hash(key string) int {
	rs := sha1.Sum([]byte(key))
	hexrs := hex.EncodeToString(rs[:])
	h, _ := strconv.ParseInt(hexrs[8:12], 16, 32)
	return int(h)
}
