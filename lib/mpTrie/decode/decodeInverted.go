package decode

import (
	"github.com/openGemini/openGemini/lib/mpTrie"
	"github.com/openGemini/openGemini/lib/mpTrie/obj"
	"github.com/openGemini/openGemini/lib/utils"
)

func UnserializeInvertedListBlk(offset uint64, buffer []byte) *obj.InvertedListBlock {
	buf := buffer[offset:]
	sizeByte := buf[:obj.DEFAULT_SIZE]
	blksize, _ := mpTrie.BytesToInt(sizeByte, false)
	data := buf[obj.DEFAULT_SIZE:]
	data = data[:blksize]
	//items := unserializeInvertedItem(data)
	mp := UnserializeInvertedMap(data)
	blk := obj.InitInvertedListBlock(mp, uint64(blksize))
	return blk
}

func unserializeInvertedItem(buffer []byte) []*obj.InvertedItem {
	list := make([]*obj.InvertedItem, 0)
	for len(buffer) != 0 {
		sizeByte := buffer[:obj.DEFAULT_SIZE]
		itemsize, _ := mpTrie.BytesToInt(sizeByte, false)
		buffer = buffer[obj.DEFAULT_SIZE:]
		data := buffer[:itemsize]
		//tsid
		tsidbyte := data[:obj.DEFAULT_SIZE]
		tsid, _ := mpTrie.BytesToInt(tsidbyte, false)
		//timestamp
		timebyte := data[obj.DEFAULT_SIZE : 2*obj.DEFAULT_SIZE]
		time, _ := mpTrie.BytesToInt(timebyte, false)
		data = data[2*obj.DEFAULT_SIZE:]
		//pos
		posbuf := make([]uint16, 0)
		for len(data) != 0 {
			tmpbyte := data[:2]
			pos, _ := mpTrie.BytesToInt(tmpbyte, false)
			data = data[2:]
			posbuf = append(posbuf, uint16(pos))
		}
		item := obj.NewInvertedItem(uint64(tsid), int64(time), posbuf, uint64(itemsize))
		list = append(list, item)
		buffer = buffer[itemsize:]
	}
	return list
}
func UnserializeInvertedMap(buffer []byte) map[utils.SeriesId][]uint16 {
	res := make(map[utils.SeriesId][]uint16)
	for len(buffer) != 0 {
		sizeByte := buffer[:obj.DEFAULT_SIZE]
		itemsize, _ := mpTrie.BytesToInt(sizeByte, false)
		buffer = buffer[obj.DEFAULT_SIZE:]
		data := buffer[:itemsize]
		//tsid
		tsidbyte := data[:obj.DEFAULT_SIZE]
		tsid, _ := mpTrie.BytesToInt(tsidbyte, false)
		//timestamp
		timebyte := data[obj.DEFAULT_SIZE : 2*obj.DEFAULT_SIZE]
		time, _ := mpTrie.BytesToInt(timebyte, false)
		data = data[2*obj.DEFAULT_SIZE:]
		//pos
		posbuf := make([]uint16, 0)
		for len(data) != 0 {
			tmpbyte := data[:2]
			pos, _ := mpTrie.BytesToInt(tmpbyte, false)
			data = data[2:]
			posbuf = append(posbuf, uint16(pos))
		}
		sid := utils.NewSeriesId(uint64(tsid), int64(time))
		res[sid] = posbuf
		buffer = buffer[itemsize:]
	}
	return res
}
