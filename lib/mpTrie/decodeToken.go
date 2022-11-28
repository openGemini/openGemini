package mpTrie

import (
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
)

func UnserializeTokenIndexFromFile(buffer []byte, filesize int64, addrCachesize, invtdCachesize int) (*SearchTree, *cache.AddrCache, *cache.InvertedCache) {
	raw := buffer
	if buffer == nil || filesize == 0 || filesize < 2*obj.DEFAULT_SIZE {
		return nil, nil, nil
	}
	invtdTotalbyte := buffer[filesize-2*obj.DEFAULT_SIZE : filesize-obj.DEFAULT_SIZE]
	addrTotalbyte := buffer[filesize-obj.DEFAULT_SIZE:]
	invtdTotal, _ := BytesToInt(invtdTotalbyte, true)
	addrTotal, _ := BytesToInt(addrTotalbyte, true)
	clvdataStart := invtdTotal + addrTotal
	clvdatabuf := buffer[clvdataStart : filesize-2*obj.DEFAULT_SIZE]

	//decode obj
	tree, addrcache, invtdcache := unserializeTokenObj(clvdatabuf, raw, addrCachesize, invtdCachesize)
	return tree, addrcache, invtdcache
}

func unserializeTokenObj(buffer, raw []byte, addrCachesize, invtdCachesize int) (*SearchTree, *cache.AddrCache, *cache.InvertedCache) {
	//init tree
	tree := NewSearchTree()
	stdlen := obj.DEFAULT_SIZE
	clvdata := make(map[string]*obj.SerializeObj)
	addrcache := cache.InitAddrCache(addrCachesize)
	invtdcache := cache.InitInvertedCache(invtdCachesize)
	for len(buffer) > 0 {
		tmp := buffer[:stdlen]
		objsize, _ := BytesToInt(tmp, false)
		objsize += stdlen
		objbuff := buffer[stdlen:objsize]
		data, obj := decodeSerializeObj(objbuff)
		clvdata[data] = obj
		buffer = buffer[objsize:]
		tree.InsertToken(addrcache, invtdcache, raw, data, obj)
	}
	return tree, addrcache, invtdcache
}
