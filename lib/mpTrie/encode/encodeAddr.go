package encode

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
)

//the center status of addrlistblock turn to file layout format
func encodeAddrCntStatus(addoff map[*gramIndex.IndexTreeNode]uint16) []*obj.AddrCenterStatus {
	res := make([]*obj.AddrCenterStatus, 0)
	for node, off := range addoff {
		cur := *node
		inverted := cur.InvertedIndex()
		blk := encodeInvertedBlk(inverted)
		tmp := obj.NewAddrCenterStatus(blk, off)
		res = append(res, tmp)
	}
	return res
}

//update addrlistblock offset
func addrCenterStatusToBLK(startoff uint64, idxData []string, res_addrctr map[string][]*obj.AddrCenterStatus, invtdblkToOff map[*obj.InvertedListBlock]uint64) (map[string]*obj.AddrListBlock, map[*obj.AddrListBlock]uint64) {
	mp_addrblk := make(map[string]*obj.AddrListBlock)
	addrblkToOff := make(map[*obj.AddrListBlock]uint64, 0) //<addrlistblock,offset> of the addrlistblock
	for _, data := range idxData {
		addrblk := new(obj.AddrListBlock)
		ctrstatusArry := res_addrctr[data]
		items := make([]*obj.AddrItem, 0)
		var blksize uint64 = 0
		for _, ctrstatus := range ctrstatusArry {
			blk := ctrstatus.Blk()
			logoff := ctrstatus.Offset()
			invtdoffset := invtdblkToOff[blk]
			item := obj.NewAddrItem(invtdoffset, logoff)
			items = append(items, item)
			blksize += item.Size() + obj.DEFAULT_SIZE
		}
		addrblk.SetBlk(items)
		addrblk.SetBlksize(blksize)
		mp_addrblk[data] = addrblk
		addrblkToOff[addrblk] = startoff
		startoff += addrblk.Blksize() + obj.DEFAULT_SIZE
	}
	return mp_addrblk, addrblkToOff
}

//serialize addrlistblock
func serializeAddrListBlock(addrblk *obj.AddrListBlock) []byte {
	res := make([]byte, 0)
	addrblksize, err := mpTrie.IntToBytes(int(addrblk.Blksize()), stdlen)
	res = append(res, addrblksize...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	blk := addrblk.Blk()
	for _, item := range blk {
		b := serializeAddrItem(item)
		res = append(res, b...)
	}
	return res
}

func serializeAddrItem(item *obj.AddrItem) []byte {
	res := make([]byte, 0)
	itemsize, err := mpTrie.IntToBytes(int(item.Size()), stdlen)
	res = append(res, itemsize...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	itemdata, err := mpTrie.IntToBytes(int(item.Addrdata()), stdlen)
	res = append(res, itemdata...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	itemoff, err := mpTrie.IntToBytes(int(item.IndexEntryOffset()), 2)
	res = append(res, itemoff...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return res
}
