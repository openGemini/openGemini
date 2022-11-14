package obj

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/utils"
)

type InvertedItem struct {
	tsid      uint64
	timestamp int64
	posBlock  []uint16
	size      uint64
}

func (i InvertedItem) Tsid() uint64 {
	return i.tsid
}

func (i InvertedItem) Timestamp() int64 {
	return i.timestamp
}

func (i InvertedItem) PosBlock() []uint16 {
	return i.posBlock
}

func (i InvertedItem) Size() uint64 {
	return i.size
}

type InvertedListBlock struct {
	blk     []*InvertedItem
	mpblk   map[utils.SeriesId][]uint16
	blksize uint64
}

func InitInvertedListBlock(mpblk map[utils.SeriesId][]uint16, blksize uint64) *InvertedListBlock {
	return &InvertedListBlock{mpblk: mpblk, blksize: blksize}
}

func (i *InvertedListBlock) Mpblk() map[utils.SeriesId][]uint16 {
	return i.mpblk
}

func (i *InvertedListBlock) SetMpblk(mpblk map[utils.SeriesId][]uint16) {
	i.mpblk = mpblk
}

func (i *InvertedListBlock) SetBlk(blk []*InvertedItem) {
	i.blk = blk
}

func (i *InvertedListBlock) SetBlksize(blksize uint64) {
	i.blksize = blksize
}

func (i InvertedListBlock) Blk() []*InvertedItem {
	return i.blk
}

func (i InvertedListBlock) Blksize() uint64 {
	return i.blksize
}

func PrintInvertedBlk(blk []*InvertedItem) {
	for _, item := range blk {
		fmt.Print("[{", item.Tsid(), item.Timestamp(), "}:", item.PosBlock(), "]")
	}
}

func NewInvertedListBlock(blk []*InvertedItem, size uint64) *InvertedListBlock {
	return &InvertedListBlock{blk: blk, blksize: size}
}

func NewInvertedItem(tsid uint64, timestamp int64, posBlock []uint16, size uint64) *InvertedItem {
	return &InvertedItem{tsid: tsid, timestamp: timestamp, posBlock: posBlock, size: size}
}
