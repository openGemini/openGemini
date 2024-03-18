/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package tokenizer

import (
	"encoding/binary"
	"math/bits"

	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const (
	ConjunctionLength           = 3
	VersionLatest        uint32 = 4
	VersionBefore        uint32 = 3
	CONTENT_SPLITTER            = " \n\t`-=~!@#$%^&*()_+[]{}\\|;':\",.<>/?"
	TAGS_SPLITTER_CHAR          = byte(6)
	TAGS_SPLITTER               = string(TAGS_SPLITTER_CHAR)
	TAGS_SPLITTER_BEFORE        = " \t,"
)

var (
	ZeroConjunction = []uint64{0, 0, 0}
	ZeroSymbol      = []bool{false, false, false}
	SetSymbol       = []bool{true, true, true}
)

var CONTENT_SPLIT_TABLE []byte = make([]byte, 256)
var TAGS_SPLIT_TABLE []byte = make([]byte, 256)
var TAGS_SPLIT_TABLE_BEFORE []byte = make([]byte, 256)
var ROTATE_TABLE []byte = make([]byte, 256)
var MISS_CONTENT_SPLIT_TABLE_INDEX uint8
var MISS_TAGS_SPLIT_TABLE_INDEX uint8
var MISS_TAGS_SPLIT_TABLE_INDEX_BEFORE uint8
var table = make([]uint64, 512)

func init() {
	CONTENT_SPLIT_TABLE, MISS_CONTENT_SPLIT_TABLE_INDEX = BuildSplitTable(CONTENT_SPLITTER)
	TAGS_SPLIT_TABLE, MISS_TAGS_SPLIT_TABLE_INDEX = BuildSplitTable(TAGS_SPLITTER)
	TAGS_SPLIT_TABLE_BEFORE, MISS_TAGS_SPLIT_TABLE_INDEX_BEFORE = BuildSplitTable(TAGS_SPLITTER_BEFORE)

	for i := 1; i < 256; i++ {
		ROTATE_TABLE[i] = 1
	}

	idx := 0
	for i := 0; i < 31; i++ {
		for j := i + 1; j < 32; j++ {
			var v uint64 = 1 << i
			v |= (1 << j)
			table[idx] = v
			idx++
		}
	}
	for i := 496; i < 512; i++ {
		table[i] = table[i-496]
	}
}

func BuildSplitTable(splitChar string) ([]byte, uint8) {
	var splitTable = make([]byte, 256)
	var missIndex uint8
	for _, c := range splitChar {
		splitTable[c] = 1
	}
	for k := range splitChar {
		if splitChar[k] == 0 {
			missIndex = uint8(k)
			break
		}
	}
	return splitTable, missIndex
}

func GetFullTextOption(ir *influxql.IndexRelation) *influxql.IndexOption {
	if ir != nil {
		if option := ir.FindIndexOption(uint32(index.BloomFilterFullText), ""); option != nil {
			return option
		}
	}
	return &influxql.IndexOption{
		Tokens:      CONTENT_SPLITTER,
		TokensTable: CONTENT_SPLIT_TABLE,
	}
}

type Tokenizer interface {
	InitInput([]byte)
	Next() bool
	CurrentHash() uint64
	ProcessTokenizerBatch(input, output []byte, offsets, lens []int32) int
	FreeSimpleGramTokenizer()
}

type SimpleTokenizer struct {
	splitTable []byte
	input      []byte
	hashValue  uint64
	seed       uint64
	start      int
	end        int
}

type SimpleUtf8Tokenizer struct {
	SimpleTokenizer
	preSplitChar    byte
	prePreSplitChar byte
}

type SimpleGramTokenizer struct {
	SimpleUtf8Tokenizer
	preHash           uint64
	prePreHash        uint64
	prePrePreHash     uint64
	conjunctionHash   []uint64
	conjunctionSymbol []bool
	n                 int
	conjunction       bool
	ip                bool
}

type SimpleGramTokenizerV0 struct {
	*SimpleGramTokenizer
}

func NewSimpleTokenizer(splitTable []byte) *SimpleTokenizer {
	t := &SimpleTokenizer{}
	t.splitTable = splitTable
	return t
}

func NewSimpleUtf8Tokenizer(splitTable []byte) *SimpleUtf8Tokenizer {
	t := &SimpleUtf8Tokenizer{}
	t.splitTable = splitTable
	return t
}

func NewSimpleGramTokenizer(splitTable []byte, version uint32, defaultNilSplit uint8) Tokenizer {
	return NewSimpleGramTokenizerWithSeed(splitTable, version, defaultNilSplit, 0)
}

func NewSimpleGramTokenizerWithSeed(splitTable []byte, version uint32, defaultNilSplit uint8, seed uint64) Tokenizer {
	t := &SimpleGramTokenizer{conjunctionHash: make([]uint64, ConjunctionLength), conjunctionSymbol: make([]bool, ConjunctionLength)}
	t.splitTable = splitTable
	t.seed = seed
	switch version {
	case 0:
		return &SimpleGramTokenizerV0{t}
	case 1, 2, 3:
		s := NewSimpleGramTokenizerV1(splitTable, defaultNilSplit)
		return s
	case 4:
		s := NewSimpleGramTokenizerV1(splitTable, defaultNilSplit)
		s.seed = seed
		return s
	default:
		return t
	}
}

func (t *SimpleTokenizer) ProcessTokenizerBatch(input, output []byte, offsets, lens []int32) int {
	for i := range offsets {
		t.InitInput(input[offsets[i] : offsets[i]+lens[i]])
		for t.Next() {
			hash := t.CurrentHash()
			target := uint32(hash >> 46)
			var offsetLow int = int((hash >> 28) & 0x1ff)
			var offsetHigh int = int((hash >> 37) & 0x1ff)
			v := table[offsetLow] | (table[offsetHigh] << 32)
			s := binary.LittleEndian.Uint64(output[target : target+8])
			if (v & s) == v {
				continue
			}
			binary.LittleEndian.PutUint64(output[target:target+8], v|s)
		}
	}
	return 0
}

func (t *SimpleTokenizer) FreeSimpleGramTokenizer() {}

func (t *SimpleTokenizer) InitInput(bytes []byte) {
	t.input = bytes
	t.start = 0
	t.end = len(bytes)
}

func (t *SimpleTokenizer) Next() bool {
	t.hashValue = t.seed
	for t.start < t.end {
		if t.splitTable[(t.input)[t.start]] > 0 {
			t.start++
			continue
		}

		for (t.start < t.end) && (t.splitTable[(t.input)[t.start]] == 0) {
			t.hashValue ^= bits.RotateLeft64(t.hashValue, 11) ^ (uint64((t.input)[t.start]) * Prime_64)
			t.start++
		}
		return true
	}
	return false
}

func (t *SimpleTokenizer) CurrentHash() uint64 {
	return t.hashValue
}

func (t *SimpleUtf8Tokenizer) Next() bool {
	t.hashValue = t.seed
	for t.start < t.end {
		b := (t.input)[t.start]
		if t.isSplit(b) {
			t.preSplitChar = b
			t.start++
			continue
		}
		if t.isChar(b) {
			for (t.start < t.end) && t.isChar((t.input)[t.start]) {
				t.hashValue ^= bits.RotateLeft64(t.hashValue, 11) ^ (uint64((t.input)[t.start]) * Prime_64)
				t.start++
			}
			return true
		}
		if (b & 0xff) <= 0xdf {
			t.updateHash(2)
			t.preSplitChar = 1
			return true
		} else if (b & 0xff) <= 0xef {
			t.updateHash(3)
			t.preSplitChar = 1
			return true
		} else if (b & 0xff) <= 0xf7 {
			t.updateHash(4)
			t.preSplitChar = 1
			return true
		}
		t.start++
	}
	return false
}

func (t *SimpleUtf8Tokenizer) updateHash(count int) {
	for i := 0; i < count; i++ {
		t.hashValue ^= bits.RotateLeft64(t.hashValue, 11) ^ (uint64((t.input)[t.start+i]) * Prime_64)
	}
	t.start += count
}

func (t *SimpleUtf8Tokenizer) isSplit(b byte) bool {
	return (b&0x80 == 0) && (t.splitTable[b] > 0)
}

func (t *SimpleUtf8Tokenizer) isChar(b byte) bool {
	return (b&0x80 == 0) && (t.splitTable[b] == 0)
}

func (t *SimpleGramTokenizer) InitInput(bytes []byte) {
	t.SimpleTokenizer.InitInput(bytes)
	t.preHash = 0
	t.prePreHash = 0
	t.prePrePreHash = 0
	t.preSplitChar = 0
	t.prePreSplitChar = 0
	copy(t.conjunctionHash, ZeroConjunction)
	copy(t.conjunctionSymbol, ZeroSymbol)
	t.n = 0
	t.conjunction = false
	//t.ip = true
}

func (t *SimpleGramTokenizer) Next() bool {
	if !t.conjunction && t.prePreSplitChar != 0 && t.preSplitChar != '"' && t.prePreSplitChar == t.preSplitChar {
		if t.prePreSplitChar == 1 {
			// Perform word segmentation on"华为.华为.华为", the semantics represented by different hash values
			t.n++
			if t.n > 2 {
				t.conjunctionHash[1] = bits.RotateLeft64(t.preHash, 11) ^ (t.conjunctionHash[2] * Prime_64) //华为华为
				t.conjunctionHash[2] = bits.RotateLeft64(t.preHash, 11) ^ (t.conjunctionHash[0] * Prime_64) //为华为
				t.conjunctionHash[0] = bits.RotateLeft64(t.preHash, 11) ^ (t.prePreHash * Prime_64)         //华为
				copy(t.conjunctionSymbol, SetSymbol)
			} else if t.n > 1 {
				t.conjunctionHash[2] = bits.RotateLeft64(t.preHash, 11) ^ (t.conjunctionHash[0] * Prime_64) //华为华
				t.conjunctionHash[0] = bits.RotateLeft64(t.preHash, 11) ^ (t.prePreHash * Prime_64)         //为华
				t.conjunctionSymbol[0] = true
				t.conjunctionSymbol[2] = true
			} else {
				t.conjunctionHash[0] = bits.RotateLeft64(t.preHash, 11) ^ (t.prePreHash * Prime_64) //华为
				t.conjunctionSymbol[0] = true
			}
			t.conjunction = true
		} else if t.prePrePreHash != 0 {
			t.n++
			//1.2.3.4.5.6
			if t.n > 1 {
				t.conjunctionHash[1] = bits.RotateLeft64(t.preHash, 11) ^ (t.conjunctionHash[2] * Prime_64) //1.2.3.4
				t.conjunctionHash[2] = bits.RotateLeft64(t.preHash, 11) ^ (t.conjunctionHash[0] * Prime_64) //2.3.4
				t.conjunctionHash[0] = bits.RotateLeft64(t.preHash, 11) ^ (t.prePreHash * Prime_64)         //3.4
				copy(t.conjunctionSymbol, ZeroSymbol)
				t.conjunctionSymbol[1] = true
			} else {
				t.conjunctionHash[1] = bits.RotateLeft64(t.prePreHash, 11) ^ (t.prePrePreHash * Prime_64)   //1.2
				t.conjunctionHash[2] = bits.RotateLeft64(t.preHash, 11) ^ (t.conjunctionHash[1] * Prime_64) //1.2.3
				t.conjunctionHash[0] = bits.RotateLeft64(t.preHash, 11) ^ (t.prePreHash * Prime_64)         //2.3
				copy(t.conjunctionSymbol, ZeroSymbol)
			}
			if !t.ip {
				copy(t.conjunctionSymbol, SetSymbol)
			}
			t.conjunction = true
		}
	} else if t.prePreSplitChar != t.preSplitChar {
		t.n = 0
	}

	if t.conjunctionSymbol[0] {
		t.hashValue, t.conjunctionSymbol[0] = t.conjunctionHash[0], false
		return true
	}
	if t.conjunctionSymbol[1] {
		t.hashValue, t.conjunctionSymbol[1] = t.conjunctionHash[1], false
		return true
	}
	if t.conjunctionSymbol[2] {
		t.hashValue, t.conjunctionSymbol[2] = t.conjunctionHash[2], false
		return true
	}

	t.conjunction = false
	t.prePreSplitChar = t.preSplitChar
	if t.SimpleUtf8Tokenizer.Next() {
		t.prePrePreHash, t.prePreHash, t.preHash = t.prePreHash, t.preHash, t.hashValue
		if t.ip {
			t.hashValue = 0
		}
		return true
	}
	return false
}

func (t *SimpleGramTokenizerV0) Next() bool {
	if t.preHash != 0 && t.prePreHash != 0 {
		t.hashValue = bits.RotateLeft64(t.preHash, 11) ^ (t.prePreHash * Prime_64)
		t.prePreHash = 0
		return true
	}

	if t.SimpleUtf8Tokenizer.Next() {
		t.prePreHash, t.preHash = t.preHash, t.hashValue
		return true
	}
	return false
}

type SimpleGramTokenizerV1 struct {
	SimpleUtf8Tokenizer
	currSplit    uint8
	currIndex    int
	currSpiltNum uint32
	hashValue    uint64
	prepreHash   uint64
	preHash      uint64
	allHashes    []uint64
}

func NewSimpleGramTokenizerV1(splitTable []byte, defaultNilSplit uint8) *SimpleGramTokenizerV1 {
	t := &SimpleGramTokenizerV1{currSplit: defaultNilSplit, allHashes: make([]uint64, 0), currSpiltNum: 0, currIndex: -1}
	t.splitTable = splitTable
	return t
}

func (t *SimpleGramTokenizerV1) addHashes(tokenizer *SimpleUtf8Tokenizer) {
	if t.currSpiltNum == 1 {
		if tokenizer.preSplitChar == 1 {
			t.hashValue = bits.RotateLeft64(t.preHash, 11) ^ (t.prepreHash * Prime_64)
			t.allHashes = append(t.allHashes, t.hashValue)
		} else {
			t.allHashes = append(t.allHashes, t.preHash)
			t.allHashes = append(t.allHashes, t.prepreHash)
		}
	} else {
		t.allHashes = append(t.allHashes, t.hashValue)
	}
	t.preHash, t.prepreHash, t.hashValue, t.currSplit, t.currSpiltNum = tokenizer.CurrentHash(), 0, tokenizer.CurrentHash(), 0, 0
}

func (t *SimpleGramTokenizerV1) InitInput(bytes []byte) {
	t.input = bytes
	t.start = 0
	t.end = len(bytes)
	tokenizer := NewSimpleUtf8Tokenizer(t.splitTable)
	tokenizer.seed = t.seed
	tokenizer.InitInput(t.input)
	isInit := false
	t.preHash = 0
	t.prepreHash = 0
	for tokenizer.Next() {
		if tokenizer.CurrentHash() == 0 {
			continue
		}
		if !isInit {
			t.hashValue = tokenizer.CurrentHash()
			t.preHash = tokenizer.CurrentHash()
			isInit = true
			continue
		}
		if t.currSpiltNum == 0 {
			t.prepreHash = t.preHash
			t.preHash = tokenizer.CurrentHash()
			t.hashValue = bits.RotateLeft64(tokenizer.CurrentHash(), 11) ^ (t.hashValue * Prime_64)
			t.currSplit = tokenizer.preSplitChar
			t.currSpiltNum += 1
			continue
		}
		if t.currSplit != tokenizer.preSplitChar {
			t.addHashes(tokenizer)
			isInit = false
			continue
		}
		if t.currSpiltNum >= 3 {
			t.addHashes(tokenizer)
			isInit = false
		} else {
			t.prepreHash = t.preHash
			t.preHash = tokenizer.CurrentHash()
			t.hashValue = bits.RotateLeft64(tokenizer.CurrentHash(), 11) ^ (t.hashValue * Prime_64)
			t.currSplit = tokenizer.preSplitChar
			t.currSpiltNum += 1
		}
	}
	if t.hashValue != t.seed {
		t.addHashes(tokenizer)
	}
}

func (t *SimpleGramTokenizerV1) Next() bool {
	if t.currIndex >= len(t.allHashes)-1 {
		return false
	}
	t.currIndex += 1
	return true
}

func (t *SimpleGramTokenizerV1) CurrentHash() uint64 {
	return t.allHashes[t.currIndex]
}
