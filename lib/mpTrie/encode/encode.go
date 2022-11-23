package encode

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"os"
	"sort"
)

/**
* @ Author: Yaixihn
* @ Dec:map+trie落盘
* @ Date: 2022/9/18 13:40
 */
var stdlen byte = obj.DEFAULT_SIZE

//get more than qmin index grams,and write the file
func getIndexData(tree *gramIndex.IndexTree) (map[string]*obj.SerializeObj, map[string]*obj.InvertedListBlock, map[string][]*obj.AddrCenterStatus) {
	res := make(map[string]*obj.SerializeObj)
	res_invetedblk := make(map[string]*obj.InvertedListBlock)
	res_addrCntStatus := make(map[string][]*obj.AddrCenterStatus)
	var dfs func(node *gramIndex.IndexTreeNode, path []string)
	dfs = func(node *gramIndex.IndexTreeNode, path []string) {
		if node.Isleaf() == true {
			temp := ""
			for _, s := range path {
				temp += s
			}
			//process addr
			addrmp := node.AddrOffset()
			arrlen := uint64(len(addrmp))
			if arrlen != 0 {
				res_addrCntStatus[temp] = encodeAddrCntStatus(addrmp) //addrlistblock
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

func SerializeToFile(tree *gramIndex.IndexTree, filename string) {
	fb := make([]byte, 0)
	res, mp_invertedblk, res_addrctr := getIndexData(tree)
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
	invtdTotalbyte, _ := mpTrie.IntToBytes(int(start_invtblk), stdlen)
	addrTotalbyte, _ := mpTrie.IntToBytes(int(addrTotal), stdlen)
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

func serializeObj(obj *obj.SerializeObj) []byte {
	res := make([]byte, 0)
	size, err := mpTrie.IntToBytes(int(obj.Size()), stdlen)
	res = append(res, size...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	addrlen, err := mpTrie.IntToBytes(int(obj.AddrListlen()), stdlen)
	res = append(res, addrlen...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if obj.AddrListlen() == 0 {
		//
	} else {
		//maybe don`t have record size
		entrysize, err := mpTrie.IntToBytes(int(obj.AddrListEntry().Size()), stdlen)
		res = append(res, entrysize...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		entryoff, err := mpTrie.IntToBytes(int(obj.AddrListEntry().Blockoffset()), stdlen)
		res = append(res, entryoff...)
		if err != nil {
			fmt.Println(err)
			return nil
		}

	}
	invtdlen, err := mpTrie.IntToBytes(int(obj.InvertedListlen()), stdlen)
	res = append(res, invtdlen...)
	if obj.InvertedListlen() == 0 {
		//
	} else {

		entrysize, err := mpTrie.IntToBytes(int(obj.InvertedListEntry().Size()), stdlen)
		res = append(res, entrysize...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		entryMinTime, err := mpTrie.IntToBytes(int(obj.InvertedListEntry().MinTime()), stdlen)
		res = append(res, entryMinTime...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		entryMaxTime, err := mpTrie.IntToBytes(int(obj.InvertedListEntry().MaxTime()), stdlen)
		res = append(res, entryMaxTime...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
		entryoff, err := mpTrie.IntToBytes(int(obj.InvertedListEntry().Blockoffset()), stdlen)
		res = append(res, entryoff...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
	}
	res = append(res, []byte(obj.Data())...)
	return res
}
