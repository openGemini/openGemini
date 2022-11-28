package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
	"github.com/openGemini/openGemini/lib/vToken/tokenIndex"
	"os"
	"sort"
	"strings"
)

func SerializeTokenIndexToFile(tree *tokenIndex.IndexTree, filename string) {
	fb := make([]byte, 0)
	res, mp_invertedblk, res_addrctr := getIndexTokenData(tree)
	var addrTotal uint64
	//1. serialize invertedlistblock
	ivtdIdxData := make([]string, 0)
	for data, _ := range mp_invertedblk {
		ivtdIdxData = append(ivtdIdxData, data)
	}
	sort.Strings(ivtdIdxData)
	invtdblkToOff := make(map[*obj.InvertedListBlock]uint64, 0) //<invertedlistblock,offset> of the invetedlistblock
	var start_invtblk uint64 = 0
	for _, data := range ivtdIdxData {
		invetdblk := mp_invertedblk[data]
		invtdblkToOff[invetdblk] = start_invtblk
		tmpbytes := serializeInvertedListBlk(invetdblk)
		fb = append(fb, tmpbytes...)
		start_invtblk += invetdblk.Blksize() + obj.DEFAULT_SIZE
	}

	//2. serialize encodeaddrblk
	addrIdxData := make([]string, 0)
	for data, _ := range res_addrctr {
		addrIdxData = append(addrIdxData, data)
	}
	sort.Strings(addrIdxData)
	//2.1 addrblk convert
	mp_addblk, addrblkToOff := addrCenterStatusToBLK(start_invtblk, addrIdxData, res_addrctr, invtdblkToOff)
	for _, data := range addrIdxData {
		addrblk := mp_addblk[data]
		tmpbytes := serializeAddrListBlock(addrblk)
		fb = append(fb, tmpbytes...)
		addrTotal += addrblk.Blksize() + obj.DEFAULT_SIZE
	}
	//3. serialize clv data block
	idxData := make([]string, 0)
	for data, _ := range res {
		idxData = append(idxData, data)
	}
	sort.Strings(idxData)

	for _, data := range idxData {
		obj := res[data]
		var addrblkoff, addrblksize, invtdblkroff, invtdblksize uint64
		if obj.InvertedListlen() == 0 {
			obj.InvertedListEntry().SetSize(0)
		} else if blk, ok := mp_invertedblk[data]; ok {
			invtdblkroff = invtdblkToOff[blk]
			invtdblksize = blk.Blksize()
			obj.InvertedListEntry().SetBlockoffset(invtdblkroff)
			obj.InvertedListEntry().SetSize(invtdblksize)
		}
		if obj.AddrListlen() == 0 {
			obj.AddrListEntry().SetSize(0)
		} else if blk, ok := mp_addblk[data]; ok {
			addrblkoff = addrblkToOff[blk]
			addrblksize = blk.Blksize()
			obj.AddrListEntry().SetBlockoffset(addrblkoff)
			obj.AddrListEntry().SetSize(addrblksize)
		}
		obj.UpdateSeializeObjSize()
		tmpbyte := serializeObj(obj)
		if tmpbyte == nil {
			fmt.Println(fmt.Errorf("the process of serialized obj had some error."))
			return
		}
		fb = append(fb, tmpbyte...)
	}

	//file tailer
	invtdTotalbyte, _ := IntToBytes(int(start_invtblk), stdlen)
	addrTotalbyte, _ := IntToBytes(int(addrTotal), stdlen)
	fb = append(fb, invtdTotalbyte...)
	fb = append(fb, addrTotalbyte...)

	file, err := os.Create(filename)
	defer file.Close()
	if err != nil {
		fmt.Println("file open fail when mptrie serialize.", err)
		return
	}
	//fmt.Println(fb)
	_, err = file.Write(fb)
	if err != nil {
		fmt.Println("file write fail when mptrie serialize", err)
		return
	}

}

//get more than qmin index grams,and write the file
func getIndexTokenData(tree *tokenIndex.IndexTree) (map[string]*obj.SerializeObj, map[string]*obj.InvertedListBlock, map[string][]*obj.AddrCenterStatus) {
	res := make(map[string]*obj.SerializeObj)
	res_invetedblk := make(map[string]*obj.InvertedListBlock)
	res_addrCntStatus := make(map[string][]*obj.AddrCenterStatus)
	var dfs func(node *tokenIndex.IndexTreeNode, path []string)
	dfs = func(node *tokenIndex.IndexTreeNode, path []string) {
		if node.Isleaf() == true {
			temp := ""
			for _, s := range path {
				temp += s + " "
			}
			temp = strings.TrimSpace(temp)
			//process addr
			addrmp := node.AddrOffset()
			arrlen := uint64(len(addrmp))
			if arrlen != 0 {
				res_addrCntStatus[temp] = encodeTokenAddrCntStatus(addrmp) //addrlistblock
			}

			//process inverted
			inverted := node.InvertedIndex()
			invtdlen := uint64(len(inverted))
			if invtdlen != 0 {
				res_invetedblk[temp] = encodeInvertedBlk(inverted) //inverted list block
			}
			//obj
			min, max := GetMaxAndMinTime(inverted)
			addrEntry := obj.NewAddrListEntry(0)
			invertedEntry := obj.NewInvertedListEntry(min, max, 0)
			obj := obj.NewSerializeObj(temp, arrlen, addrEntry, invtdlen, invertedEntry)
			res[temp] = obj
			//fmt.Println(temp, node.AddrOffset())
		}
		if len(node.Children()) == 0 {
			return
		}
		for _, child := range node.Children() {
			path = append(path, child.Data())
			dfs(child, path)
			path = path[:len(path)-1]
		}
	}
	root := tree.Root()
	path := make([]string, 0)
	dfs(root, path)
	return res, res_invetedblk, res_addrCntStatus
}
