package decode

import (
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
)

/**
* @ Author: Yaixihn
* @ Dec:map+trie unserialized
* @ Date: 2022/10/4 13:40
 */

func UnserializeAddrListBlk(offset uint64, buffer []byte) *obj.AddrListBlock {
	buffer = buffer[offset:]
	sizebyte := buffer[:obj.DEFAULT_SIZE]
	blksize, _ := mpTrie.BytesToInt(sizebyte, false)
	buffer = buffer[obj.DEFAULT_SIZE:]
	buffer = buffer[:blksize]
	//items := unserializeAddrItemList(buffer)
	mp := UnserializeAddrMap(buffer)
	blk := obj.InitAddrListBlock(mp, uint64(blksize))
	return blk
}

func unserializeAddrItemList(buffer []byte) []*obj.AddrItem {
	list := make([]*obj.AddrItem, 0)
	for len(buffer) != 0 {
		//size
		sizebyte := buffer[:obj.DEFAULT_SIZE]
		itemsize, _ := mpTrie.BytesToInt(sizebyte, false)
		buffer = buffer[obj.DEFAULT_SIZE:]
		item := unserializeAddrItem(buffer)
		list = append(list, item)
		buffer = buffer[itemsize:]
	}
	return list
}

func unserializeAddrItem(buffer []byte) *obj.AddrItem {
	//data
	databyte := buffer[:obj.DEFAULT_SIZE]
	data, _ := mpTrie.BytesToInt(databyte, false)
	buffer = buffer[obj.DEFAULT_SIZE:]
	//offset
	offbyte := buffer[:2]
	off, _ := mpTrie.BytesToInt(offbyte, false)
	buffer = buffer[2:]
	item := obj.NewAddrItem(uint64(data), uint16(off))
	return item
}

func UnserializeAddrMap(buffer []byte) map[uint64]uint16 {
	res := make(map[uint64]uint16)
	for len(buffer) != 0 {
		//size
		sizebyte := buffer[:obj.DEFAULT_SIZE]
		itemsize, _ := mpTrie.BytesToInt(sizebyte, false)
		buffer = buffer[obj.DEFAULT_SIZE:]
		data, offset := getUnserializeAddrData(buffer)
		res[data] = offset
		buffer = buffer[itemsize:]
	}
	return res
}

func getUnserializeAddrData(buffer []byte) (uint64, uint16) {
	//data
	databyte := buffer[:obj.DEFAULT_SIZE]
	data, _ := mpTrie.BytesToInt(databyte, false)
	buffer = buffer[obj.DEFAULT_SIZE:]
	//offset
	offbyte := buffer[:2]
	off, _ := mpTrie.BytesToInt(offbyte, false)
	buffer = buffer[2:]
	return uint64(data), uint16(off)
}
