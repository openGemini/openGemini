package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
	"github.com/openGemini/openGemini/lib/utils"
	"sort"
)

//serialize to file in bytes array format
func serializeInvertedListBlk(invtdblk *obj.InvertedListBlock) []byte {
	res := make([]byte, 0)
	// size  store
	invtdblksize, err := IntToBytes(int(invtdblk.Blksize()), stdlen)
	res = append(res, invtdblksize...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	//block
	blk := invtdblk.Blk()
	for _, item := range blk {
		b := serializeInvertedItem(item)
		res = append(res, b...)
	}
	return res
}
func serializeInvertedItem(item *obj.InvertedItem) []byte {
	res := make([]byte, 0)
	// record the size in the start position
	itemsize, err := IntToBytes(int(item.Size()), stdlen)
	res = append(res, itemsize...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	itemTsid, err := IntToBytes(int(item.Tsid()), stdlen)
	res = append(res, itemTsid...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	itemTime, err := IntToBytes(int(item.Timestamp()), stdlen)
	res = append(res, itemTime...)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	pos := item.PosBlock()
	tmp := make([]byte, 0)
	for _, num := range pos {
		n, err := IntToBytes(int(num), 2)
		tmp = append(tmp, n...)
		if err != nil {
			fmt.Println(err)
			return nil
		}
	}
	res = append(res, tmp...)

	return res
}

//turn to file layout
func encodeInvertedBlk(inverted map[utils.SeriesId][]uint16) *obj.InvertedListBlock {
	var total uint64 = 0
	res := new(obj.InvertedListBlock)
	blk := make([]*obj.InvertedItem, 0)
	for s, pos := range inverted {
		tmp := getInvtdItemSize(pos)
		item := obj.NewInvertedItem(s.Id, s.Time, pos, tmp)
		total += tmp + obj.DEFAULT_SIZE
		blk = append(blk, item)
	}
	res = obj.NewInvertedListBlock(blk, total)
	return res
}
func getInvtdItemSize(pos []uint16) uint64 {
	//size of bytes
	sidstd, timestd := 64/8, 64/8 //sid and time is int64
	posstd := 16 / 8              //pos item is uint16
	return uint64(sidstd + timestd + len(pos)*posstd)
}

func GetMaxAndMinTime(index map[utils.SeriesId][]uint16) (min, max int64) {
	sidArr := make([]utils.SeriesId, 0)
	if len(index) <= 0 {
		return -1, -1
	}
	for series, _ := range index {
		sidArr = append(sidArr, series)
	}
	sort.Slice(sidArr, func(i, j int) bool {
		if sidArr[i].Time < sidArr[j].Time {
			return true
		} else {
			return false
		}
	})
	return sidArr[0].Time, sidArr[len(sidArr)-1].Time
}
