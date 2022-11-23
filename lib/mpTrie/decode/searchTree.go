package decode

import (
	"github.com/openGemini/openGemini/lib/mpTrie/cache"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
)

type SearchTree struct {
	root *SearchTreeNode
}

func (tree *SearchTree) Root() *SearchTreeNode {
	return tree.root
}

func (tree *SearchTree) SetRoot(root *SearchTreeNode) {
	tree.root = root
}

func NewSearchTree() *SearchTree {
	return &SearchTree{root: NewSearchTreeNode("")}
}
func (tree *SearchTree) Insert(addrcache *cache.AddrCache, invtdcache *cache.InvertedCache, buffer []byte, data string, obj *obj.SerializeObj) {
	root := tree.root
	for i, c := range data {
		cur := int(c)
		if root.children[cur] == nil {
			root.children[cur] = NewSearchTreeNode(data[i : i+1])
		}
		root = root.children[cur]
		root.data = data[i : i+1]
	}
	//Add addrInfo
	addrlen := int(obj.AddrListlen())
	root.addrlen = addrlen
	if addrlen != 0 {
		addr := root.addrInfo
		off := obj.AddrListEntry().Blockoffset()
		addr.addrblkOffset = off
		addrInfo := UnserializeAddrListBlk(off, buffer)
		addrcache.Put(off, addrInfo)
		addr.addrblksize = obj.AddrListEntry().Size()
	}
	//Add invtdInfo
	invtdlen := int(obj.InvertedListlen())
	root.invtdlen = invtdlen
	if invtdlen != 0 {
		invt := root.invtdInfo
		off := obj.InvertedListEntry().Blockoffset()
		invt.ivtdblkOffset = off
		invtdInfo := UnserializeInvertedListBlk(off, buffer)
		invtdcache.Put(off, invtdInfo)
		invt.ivtdblksize = obj.InvertedListEntry().Size()
	}
	root.isleaf = true
}

func (tree *SearchTree) SearchPrefix(prefix string) *SearchTreeNode {
	root := tree.root
	for _, c := range prefix {
		cur := int(c)
		if root.children[cur] == nil {
			root.children[cur] = &SearchTreeNode{}
		}
		root = root.children[cur]
	}
	return root
}

func (tree *SearchTree) Search(data string) bool {
	node := tree.SearchPrefix(data)
	return node.isleaf && node != nil
}

func (tree *SearchTree) PrintSearchTree(addrcache *cache.AddrCache, invtdcache *cache.InvertedCache) {
	tree.root.printsearchTreeNode(0, addrcache, invtdcache)
}

/*
func (tree *SearchTree) InsertToken(addrcache *cache.AddrCache, invtdcache *cache.InvertedCache, buffer []byte, data string, obj *obj.SerializeObj) {
	root := tree.root
	tokens := strings.Split(data, " ")
	for _, token := range tokens {
		cur := utils.StringToHashCode(token)
		if root.children[cur] == nil {
			root.children[cur] = NewSearchTreeNode(token)
		}
		root = root.children[cur]
		root.data = token
	}
	//Add addrInfo
	addrlen := int(obj.AddrListlen())
	root.addrlen = addrlen
	if addrlen != 0 {
		addr := root.addrInfo
		off := obj.AddrListEntry().Blockoffset()
		addr.addrblkOffset = off
		addrInfo := unserializeAddrListBlk(off, buffer)
		addrcache.Put(off, addrInfo)
		addr.addrblksize = obj.AddrListEntry().Size()
	}
	//Add invtdInfo
	invtdlen := int(obj.InvertedListlen())
	root.invtdlen = invtdlen
	if invtdlen != 0 {
		invt := root.invtdInfo
		off := obj.InvertedListEntry().Blockoffset()
		invt.ivtdblkOffset = off
		invtdInfo := unserializeInvertedListBlk(off, buffer)
		invtdcache.Put(off, invtdInfo)
		invt.ivtdblksize = obj.InvertedListEntry().Size()
	}
	root.isleaf = true
}

func (tree *SearchTree) SearchTokenPrefix(prefix string) *SearchTreeNode {
	root := tree.root
	tokens := strings.Split(prefix, " ")
	for _, token := range tokens {
		cur := utils.StringToHashCode(token)
		if root.children[cur] == nil {
			root.children[cur] = &SearchTreeNode{}
		}
		root = root.children[cur]
	}
	return root
}

func (tree *SearchTree) SearchToken(data string) bool {
	node := tree.SearchTokenPrefix(data)
	return node.isleaf && node != nil
}
*/
