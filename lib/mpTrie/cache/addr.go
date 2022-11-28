package cache

import "github.com/openGemini/openGemini/lib/mpTrie/obj"

type AddrNode struct {
	key   uint64
	value *obj.AddrListBlock
	prev  *AddrNode
	next  *AddrNode
}

func NewAddrNode(key uint64, value *obj.AddrListBlock) *AddrNode {
	return &AddrNode{key: key, value: value}
}

type AddrCache struct {
	capicity   int
	used       int
	blk        map[uint64]*AddrNode
	list       *AddrNode
	head, tail *AddrNode
}

func InitAddrCache(capicity int) *AddrCache {
	cache := &AddrCache{
		capicity: capicity,
		used:     0,
		blk:      make(map[uint64]*AddrNode),
		head:     NewAddrNode(0, nil),
		tail:     NewAddrNode(0, nil),
	}
	cache.head.next = cache.tail
	cache.tail.prev = cache.head
	return cache
}

func (this *AddrCache) Put(offset uint64, blk *obj.AddrListBlock) {
	node, ok := this.blk[offset]
	if !ok {
		tmp := NewAddrNode(offset, blk)
		this.blk[offset] = tmp
		this.AddToHead(tmp)
		this.UpSize()
		for this.used > this.capicity {
			real := this.DeleteTail()
			this.DeleteEntry(real)
			delete(this.blk, real.key)
			this.DownSize()
		}
	} else {
		node.value = blk
		this.DeleteEntry(node)
		this.AddToHead(node)
	}
}

func (this *AddrCache) Get(offset uint64) *obj.AddrListBlock {
	if node, ok := this.blk[offset]; ok {
		this.DeleteEntry(node)
		this.AddToHead(node)
		return node.value
	} else {
		return nil
	}

}

func (this *AddrCache) UpSize() {
	this.used++
}
func (this *AddrCache) DownSize() {
	this.used--
}

func (this *AddrCache) DeleteEntry(entry *AddrNode) {
	entry.prev.next = entry.next
	entry.next.prev = entry.prev

}

func (this *AddrCache) DeleteTail() *AddrNode {
	realnode := this.tail.prev
	this.DeleteEntry(realnode)
	return realnode
}

func (this *AddrCache) AddToHead(node *AddrNode) {
	head := this.head
	node.next = head.next
	node.prev = head
	head.next.prev = node
	head.next = node
}
