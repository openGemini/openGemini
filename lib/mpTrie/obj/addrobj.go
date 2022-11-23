package obj

import "fmt"

const deafult_addrItemSize = 10 //(64/8)+16/8
type AddrItem struct {
	addrdata         uint64
	indexEntryOffset uint16
	size             uint64
}

func (a *AddrItem) Addrdata() uint64 {
	return a.addrdata
}

func (a *AddrItem) SetAddrdata(addrdata uint64) {
	a.addrdata = addrdata
}

func (a *AddrItem) IndexEntryOffset() uint16 {
	return a.indexEntryOffset
}

func (a *AddrItem) SetIndexEntryOffset(indexEntryOffset uint16) {
	a.indexEntryOffset = indexEntryOffset
}

func (a *AddrItem) Size() uint64 {
	return a.size
}

func (a *AddrItem) SetSize(size uint64) {
	a.size = size
}

func NewAddrItem(addrdata uint64, indexEntryOffset uint16) *AddrItem {
	return &AddrItem{addrdata: addrdata, indexEntryOffset: indexEntryOffset, size: deafult_addrItemSize}
}

type AddrListBlock struct {
	blk     []*AddrItem
	mpblk   map[uint64]uint16 //offset 有什么用，能否找到倒排？内存中是一个节点地址，直接可获取倒排表，磁盘中存放是invetoffset还是 addroffset，能否找到倒排
	blksize uint64            // UnserializeInvertedListBlk函数？
}

func InitAddrListBlock(mpblk map[uint64]uint16, blksize uint64) *AddrListBlock {
	return &AddrListBlock{mpblk: mpblk, blksize: blksize}
}

func (a *AddrListBlock) Mpblk() map[uint64]uint16 {
	return a.mpblk
}

func (a *AddrListBlock) SetMpblk(mpblk map[uint64]uint16) {
	a.mpblk = mpblk
}

func (a *AddrListBlock) SetBlk(blk []*AddrItem) {
	a.blk = blk
}

func (a *AddrListBlock) SetBlksize(blksize uint64) {
	a.blksize = blksize
}

func (a AddrListBlock) Blk() []*AddrItem {
	return a.blk
}

func (a AddrListBlock) Blksize() uint64 {
	return a.blksize
}
func NewAddrListBlock(blk []*AddrItem, blksize uint64) *AddrListBlock {
	return &AddrListBlock{blk: blk, blksize: blksize}
}

func PrintAddrBlk(blk []*AddrItem) {
	for _, item := range blk {
		fmt.Print("[", item.Addrdata(), ":", item.IndexEntryOffset(), "]")
	}
}

// turn from addrOffset->map[*IndexTreeNode]uint16
type AddrCenterStatus struct {
	blk    *InvertedListBlock
	offset uint16
}

func (a AddrCenterStatus) Blk() *InvertedListBlock {
	return a.blk
}

func (a AddrCenterStatus) Offset() uint16 {
	return a.offset
}

func NewAddrCenterStatus(blk *InvertedListBlock, offset uint16) *AddrCenterStatus {
	return &AddrCenterStatus{blk: blk, offset: offset}
}
