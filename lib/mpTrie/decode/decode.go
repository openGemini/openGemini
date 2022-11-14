package decode

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
	"os"
)

/**
* @ Author: Yaixihn
* @ Dec:map+trie unserialized
* @ Date: 2022/10/4 13:40
 */
func UnserializeFromFile(buffer []byte, filesize int64, addrCachesize, invtdCachesize int) (*SearchTree, *cache.AddrCache, *cache.InvertedCache) {
	raw := buffer
	if buffer == nil || filesize == 0 || filesize < 2*obj.DEFAULT_SIZE {
		return nil, nil, nil
	}
	invtdTotalbyte := buffer[filesize-2*obj.DEFAULT_SIZE : filesize-obj.DEFAULT_SIZE]
	addrTotalbyte := buffer[filesize-obj.DEFAULT_SIZE:]
	invtdTotal, _ := mpTrie.BytesToInt(invtdTotalbyte, true)
	addrTotal, _ := mpTrie.BytesToInt(addrTotalbyte, true)
	clvdataStart := invtdTotal + addrTotal
	clvdatabuf := buffer[clvdataStart : filesize-2*obj.DEFAULT_SIZE]

	//decode obj
	tree, addrcache, invtdcache := unserializeObj(clvdatabuf, raw, addrCachesize, invtdCachesize)
	return tree, addrcache, invtdcache
}
func GetBytesFromFile(filename string) ([]byte, int64) {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	filesize := fileinfo.Size()
	buffer := make([]byte, filesize)
	_, err = file.Read(buffer)
	if err != nil {
		fmt.Println(err)
		return nil, 0
	}
	return buffer, filesize
}

func unserializeObj(buffer, raw []byte, addrCachesize, invtdCachesize int) (*SearchTree, *cache.AddrCache, *cache.InvertedCache) {
	//init tree
	tree := NewSearchTree()
	stdlen := obj.DEFAULT_SIZE
	clvdata := make(map[string]*obj.SerializeObj)
	addrcache := cache.InitAddrCache(addrCachesize)
	invtdcache := cache.InitInvertedCache(invtdCachesize)
	//offinvtdobjmp := make(map[uint64]*obj.InvertedListBlock)
	//offaddrObjmp := make(map[uint64]*obj.AddrListBlock)
	for len(buffer) > 0 {
		tmp := buffer[:stdlen]
		objsize, _ := mpTrie.BytesToInt(tmp, false)
		objsize += stdlen
		objbuff := buffer[stdlen:objsize]
		data, obj := decodeSerializeObj(objbuff)
		clvdata[data] = obj
		buffer = buffer[objsize:]
		//tree.Insert(offinvtdobjmp, offaddrObjmp, raw, data, obj)
		tree.Insert(addrcache, invtdcache, raw, data, obj)
	}
	return tree, addrcache, invtdcache
}

func decodeSerializeObj(buffer []byte) (string, *obj.SerializeObj) {
	stdlen := obj.DEFAULT_SIZE
	objaddrlen, _ := mpTrie.BytesToInt(buffer[:stdlen], false)
	buffer = buffer[stdlen:]
	var addrListEntry = new(obj.AddrListEntry)
	if objaddrlen != 0 {
		size, _ := mpTrie.BytesToInt(buffer[:stdlen], false)
		off, _ := mpTrie.BytesToInt(buffer[stdlen:2*stdlen], false)
		addrListEntry.SetSize(uint64(size))
		addrListEntry.SetBlockoffset(uint64(off))
		buffer = buffer[2*stdlen:]
	}
	objinvtdlen, _ := mpTrie.BytesToInt(buffer[:stdlen], false)
	buffer = buffer[stdlen:]
	var invtdListEntry = new(obj.InvertedListEntry)
	if objinvtdlen != 0 {
		size, _ := mpTrie.BytesToInt(buffer[:stdlen], false)
		mintime, _ := mpTrie.BytesToInt(buffer[stdlen:2*stdlen], false)
		maxtime, _ := mpTrie.BytesToInt(buffer[2*stdlen:3*stdlen], false)
		off, _ := mpTrie.BytesToInt(buffer[3*stdlen:4*stdlen], false)
		invtdListEntry.SetSize(uint64(size))
		invtdListEntry.SetMinTime(int64(mintime))
		invtdListEntry.SetMaxTime(int64(maxtime))
		invtdListEntry.SetBlockoffset(uint64(off))
		buffer = buffer[4*stdlen:]
	}
	data := string(buffer)
	obj := obj.NewSerializeObj(data, uint64(objaddrlen), addrListEntry, uint64(objinvtdlen), invtdListEntry)
	obj.UpdateSeializeObjSize()
	return data, obj
}
