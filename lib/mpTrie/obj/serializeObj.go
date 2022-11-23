package obj

/**
* @ Author: Yaixihn
* @ Dec:
* @ Date: 2022/9/18 13:51
 */
//serialize object information
const DEFAULT_SIZE = 64 / 8

type SerializeObj struct {
	data              string
	addrListlen       uint64
	addrListEntry     *AddrListEntry
	invertedListlen   uint64
	invertedListEntry *InvertedListEntry
	size              uint64
}

func (s *SerializeObj) AddrListEntry() *AddrListEntry {
	return s.addrListEntry
}

func (s *SerializeObj) SetAddrListEntry(addrListEntry *AddrListEntry) {
	s.addrListEntry = addrListEntry
}

func (s *SerializeObj) InvertedListEntry() *InvertedListEntry {
	return s.invertedListEntry
}

func (s *SerializeObj) SetInvertedListEntry(invertedListEntry *InvertedListEntry) {
	s.invertedListEntry = invertedListEntry
}

func (s *SerializeObj) Data() string {
	return s.data
}

func (s *SerializeObj) AddrListlen() uint64 {
	return s.addrListlen
}

func (s *SerializeObj) SetAddrListlen(addrListlen uint64) {
	s.addrListlen = addrListlen
}

func (s *SerializeObj) InvertedListlen() uint64 {
	return s.invertedListlen
}

func (s *SerializeObj) SetInvertedListlen(invertedListlen uint64) {
	s.invertedListlen = invertedListlen
}

func (s *SerializeObj) Size() uint64 {
	return s.size
}

func (s *SerializeObj) SetSize(size uint64) {
	s.size = size
}

func NewSerializeObj(data string, addrListlen uint64, addrListEntry *AddrListEntry, invertedListlen uint64, invertedListEntry *InvertedListEntry) *SerializeObj {
	tmp := uint64(len(data)) + DEFAULT_SIZE*2 //no affection
	return &SerializeObj{
		data:        data,
		addrListlen: addrListlen, addrListEntry: addrListEntry,
		invertedListlen: invertedListlen, invertedListEntry: invertedListEntry,
		size: tmp}
}

func (s *SerializeObj) UpdateSeializeObjSize() {
	res := uint64(len([]byte(s.data))) + DEFAULT_SIZE*2
	if s.addrListlen != 0 {
		res += DEFAULT_SIZE * 2
	}
	if s.invertedListlen != 0 {
		res += DEFAULT_SIZE * 4
	}
	s.SetSize(res)
}

//addrlist block information
type AddrListEntry struct {
	blockoffset uint64
	size        uint64
}

func (a *AddrListEntry) Blockoffset() uint64 {
	return a.blockoffset
}

func (a *AddrListEntry) Size() uint64 {
	return a.size
}

func NewAddrListEntry(blockoffset uint64) *AddrListEntry {
	return &AddrListEntry{blockoffset: blockoffset, size: DEFAULT_SIZE}
}

func (a *AddrListEntry) SetBlockoffset(blockoffset uint64) {
	a.blockoffset = blockoffset
}

func (a *AddrListEntry) SetSize(size uint64) {
	a.size = size
}

//invertedlist block information
type InvertedListEntry struct {
	minTime     int64
	maxTime     int64
	blockoffset uint64
	size        uint64
}

func (i *InvertedListEntry) SetMinTime(minTime int64) {
	i.minTime = minTime
}

func (i *InvertedListEntry) SetMaxTime(maxTime int64) {
	i.maxTime = maxTime
}

func (i *InvertedListEntry) MinTime() int64 {
	return i.minTime
}

func (i *InvertedListEntry) MaxTime() int64 {
	return i.maxTime
}

func (i *InvertedListEntry) Blockoffset() uint64 {
	return i.blockoffset
}

func (i *InvertedListEntry) Size() uint64 {
	return i.size
}

func (i *InvertedListEntry) SetBlockoffset(blockoffset uint64) {
	i.blockoffset = blockoffset
}

func (i *InvertedListEntry) SetSize(size uint64) {
	i.size = size
}

func NewInvertedListEntry(minTime int64, maxTime int64, blockoffset uint64) *InvertedListEntry {
	return &InvertedListEntry{minTime: minTime, maxTime: maxTime, blockoffset: blockoffset, size: DEFAULT_SIZE * 3}
}
