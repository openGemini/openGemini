package decode

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
)

type SearchTreeNode struct {
	data      string
	children  map[int]*SearchTreeNode
	addrlen   int
	addrInfo  *AddrInfo
	invtdlen  int
	invtdInfo *InvtdInfo
	isleaf    bool
}

func (node *SearchTreeNode) Data() string {
	return node.data
}

func (node *SearchTreeNode) SetData(data string) {
	node.data = data
}

func (node *SearchTreeNode) Children() map[int]*SearchTreeNode {
	return node.children
}

func (node *SearchTreeNode) SetChildren(children map[int]*SearchTreeNode) {
	node.children = children
}

func (node *SearchTreeNode) Addrlen() int {
	return node.addrlen
}

func (node *SearchTreeNode) SetAddrlen(addrlen int) {
	node.addrlen = addrlen
}

func (node *SearchTreeNode) AddrInfo() *AddrInfo {
	return node.addrInfo
}

func (node *SearchTreeNode) SetAddrInfo(addrInfo *AddrInfo) {
	node.addrInfo = addrInfo
}

func (node *SearchTreeNode) Invtdlen() int {
	return node.invtdlen
}

func (node *SearchTreeNode) SetInvtdlen(invtdlen int) {
	node.invtdlen = invtdlen
}

func (node *SearchTreeNode) InvtdInfo() *InvtdInfo {
	return node.invtdInfo
}

func (node *SearchTreeNode) SetInvtdInfo(invtdInfo *InvtdInfo) {
	node.invtdInfo = invtdInfo
}

func (node *SearchTreeNode) Isleaf() bool {
	return node.isleaf
}

func (node *SearchTreeNode) SetIsleaf(isleaf bool) {
	node.isleaf = isleaf
}

type AddrInfo struct {
	addrblkOffset uint64
	addrblksize   uint64
}

func (a *AddrInfo) AddrblkOffset() uint64 {
	return a.addrblkOffset
}

func (a *AddrInfo) SetAddrblkOffset(addrblkOffset uint64) {
	a.addrblkOffset = addrblkOffset
}

func (a *AddrInfo) Addrblksize() uint64 {
	return a.addrblksize
}

func (a *AddrInfo) SetAddrblksize(addrblksize uint64) {
	a.addrblksize = addrblksize
}

type InvtdInfo struct {
	ivtdblkOffset uint64
	ivtdblksize   uint64
}

func (i *InvtdInfo) IvtdblkOffset() uint64 {
	return i.ivtdblkOffset
}

func (i *InvtdInfo) SetIvtdblkOffset(ivtdblkOffset uint64) {
	i.ivtdblkOffset = ivtdblkOffset
}

func (i *InvtdInfo) Ivtdblksize() uint64 {
	return i.ivtdblksize
}

func (i *InvtdInfo) SetIvtdblksize(ivtdblksize uint64) {
	i.ivtdblksize = ivtdblksize
}

func NewSearchTreeNode(data string) *SearchTreeNode {
	return &SearchTreeNode{
		data:      data,
		children:  make(map[int]*SearchTreeNode),
		addrInfo:  &AddrInfo{},
		invtdInfo: &InvtdInfo{},
		isleaf:    false,
	}
}

func (node *SearchTreeNode) printsearchTreeNode(level int, addrcache *cache.AddrCache, invtdcache *cache.InvertedCache) {
	fmt.Println()
	for i := 0; i < level; i++ {
		fmt.Print("      ")
	}
	addrblk := addrcache.Get(node.addrInfo.addrblkOffset)
	invtdblk := invtdcache.Get(node.invtdInfo.ivtdblkOffset)
	fmt.Print(node.data, " - ", node.isleaf, " - ")
	if invtdblk != nil && node.invtdlen != 0 {
		blk := invtdblk.Mpblk()
		fmt.Print(" -invt: ")
		//obj.PrintInvertedBlk(blk)
		fmt.Print(blk)
	} else {
		fmt.Print(" - ", node.invtdInfo.ivtdblkOffset)
	}
	if addrblk != nil && node.addrlen != 0 {
		blk := addrblk.Mpblk()
		fmt.Print(" -addr: ")
		//obj.PrintAddrBlk(blk)
		fmt.Print(blk)
	} else {
		fmt.Print(" - ", node.addrInfo.addrblkOffset)
	}
	for _, node := range node.children {
		node.printsearchTreeNode(level+1, addrcache, invtdcache)
	}
}

func (node *SearchTreeNode) GetInvertedListBlock(buffer []byte) *obj.InvertedListBlock {
	off := node.invtdInfo.ivtdblkOffset
	size := node.invtdInfo.ivtdblksize
	invtbuf := buffer[off : off+size]
	blk := new(obj.InvertedListBlock)
	invtblk := blk.Blk()
	blksize, _ := mpTrie.BytesToInt(invtbuf[:obj.DEFAULT_SIZE], false)
	invtbuf = invtbuf[obj.DEFAULT_SIZE:]
	invtbuf = invtbuf[:blksize]
	for len(invtbuf) != 0 {
		itemsize, _ := mpTrie.BytesToInt(invtbuf[:obj.DEFAULT_SIZE], false)
		invtbuf = invtbuf[obj.DEFAULT_SIZE:]
		itembuf := invtbuf[:itemsize]
		tsid, _ := mpTrie.BytesToInt(itembuf[:obj.DEFAULT_SIZE], false)
		timestamp, _ := mpTrie.BytesToInt(itembuf[obj.DEFAULT_SIZE:2*obj.DEFAULT_SIZE], false)
		itembuf = itembuf[2*obj.DEFAULT_SIZE:]
		pos := make([]uint16, 0)
		for len(itembuf) != 0 {
			p, _ := mpTrie.BytesToInt(itembuf[:2], false)
			pos = append(pos, uint16(p))
			itembuf = itembuf[2:]
		}
		itm := obj.NewInvertedItem(uint64(tsid), int64(timestamp), pos, uint64(itemsize))
		invtblk = append(invtblk, itm)
		invtbuf = invtbuf[itemsize:]
	}
	blk.SetBlk(invtblk)
	return blk
}

func (node *SearchTreeNode) GetAddrListBlock(buffer []byte) *obj.AddrListBlock {
	off := node.addrInfo.addrblkOffset
	size := node.addrInfo.addrblksize
	addrbuf := buffer[off : off+size]
	blksize, _ := mpTrie.BytesToInt(addrbuf[:obj.DEFAULT_SIZE], false)
	addrbuf = addrbuf[obj.DEFAULT_SIZE:]
	addrbuf = addrbuf[:blksize]
	//decode addrlistblock
	blk := new(obj.AddrListBlock)
	addrblk := blk.Blk()
	for len(addrbuf) != 0 {
		itemsize, _ := mpTrie.BytesToInt(addrbuf[:obj.DEFAULT_SIZE], false)
		addrbuf = addrbuf[obj.DEFAULT_SIZE:]
		itemdata, _ := mpTrie.BytesToInt(addrbuf[:obj.DEFAULT_SIZE], false)
		//decode invtdblk
		itemoff, _ := mpTrie.BytesToInt(addrbuf[obj.DEFAULT_SIZE:], false)
		addrbuf = addrbuf[itemsize:]
		item := obj.NewAddrItem(uint64(itemdata), uint16(itemoff))
		addrblk = append(addrblk, item)
	}
	blk.SetBlk(addrblk)
	blk.SetBlksize(uint64(blksize))
	return blk
}
