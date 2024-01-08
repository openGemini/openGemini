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

package analyzer

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type CompressAlgo struct {
	name         string
	total        int
	originSize   int
	compressSize int
}

func (p *ColumnIterator) analyzeSegment(offset int64, size uint32, ref *record.Field) error {
	buf, err := p.fi.ReadData(offset, size)
	if err != nil {
		return err
	}

	p.result.compressSize[ref.Type] += len(buf)
	p.analyzeCompressAlgo(buf, ref)
	return nil
}

func (p *ColumnIterator) analyzeCompressAlgo(data []byte, ref *record.Field) {
	dataType := int(data[0])
	typ := data[0]
	if encoding.IsBlockOne(typ) {
		p.analyzeOneRowCompressAlgo(ref)
		return
	}

	if encoding.IsBlockEmpty(typ) || encoding.IsBlockFull(typ) {
		dataType = int(encTypeToNormal[typ])
		data = data[5:]
	} else {
		data = data[1:]
		bitmapSize := int(numberenc.UnmarshalUint32(data))
		p.result.bitmapSize += bitmapSize

		// 4 byte nilBitmapLen, 4 byte bitmapOffset, 4 byte nilCount
		data = data[bitmapSize+12:]
	}

	if dataType != ref.Type {
		panic(fmt.Sprintf("type(%v) in table not eq select type(%v)", dataType, ref.Type))
	}

	p.currentAlgo = nil
	if ref.Name == record.TimeField {
		p.analyzeTimeCompressAlgo(data)

		block, err := encoding.DecodeTimestampBlock(data, &([]byte{}), &encoding.CoderContext{})
		if err != nil {
			panic(err)
		}
		p.currentAlgo.originSize += len(block) * 8
		return
	}

	switch ref.Type {
	case influx.Field_Type_Float:
		p.analyzeFloatCompressAlgo(data)
	case influx.Field_Type_Int:
		p.analyzeIntCompressAlgo(data)
	case influx.Field_Type_String:
	default:
		break
	}
}

func (p *ColumnIterator) analyzeFloatCompressAlgo(data []byte) {
	key := getFloatAlgo(data[0])
	p.currentAlgo = allocAlgo(p.result.floatCompressAlgo, key, len(data))
}

func (p *ColumnIterator) analyzeIntCompressAlgo(data []byte) {
	key := getIntAlgo(data[0])
	p.currentAlgo = allocAlgo(p.result.intCompressAlgo, key, len(data))
}

func (p *ColumnIterator) analyzeTimeCompressAlgo(data []byte) {
	key := getTimeAlgo(data[0])
	p.currentAlgo = allocAlgo(p.result.timeCompressAlgo, key, len(data))
}

var floatOneRow = []byte{0, 0, 0, 0, 0, 0, 0, 0}
var intOneRow = []byte{4 << 4, 0, 0, 0, 0, 0, 0, 0}

func (p *ColumnIterator) analyzeOneRowCompressAlgo(ref *record.Field) {
	switch ref.Type {
	case influx.Field_Type_Float:
		p.analyzeFloatCompressAlgo(floatOneRow)
	case influx.Field_Type_Int:
		p.analyzeIntCompressAlgo(intOneRow)
	default:
		break
	}
}

func allocAlgo(m map[string]*CompressAlgo, key string, size int) *CompressAlgo {
	algo, ok := m[key]
	if !ok || algo == nil {
		algo = &CompressAlgo{
			name:         key,
			total:        0,
			originSize:   0,
			compressSize: 0,
		}
		m[key] = algo
	}
	algo.total++
	algo.compressSize += size
	return algo
}

func printCompressAlgo(name string, algo map[string]*CompressAlgo) {
	if len(algo) == 0 {
		return
	}

	fmt.Println("")
	fmt.Println("--------------- " + name + " --------------")
	total := 0
	for _, item := range algo {
		total += item.total
	}
	for _, item := range algo {
		fmt.Println("[" + item.name + "]")
		printRatio("  Percentage", item.total, total)
		printSizeRatio("  CompressionRatio", item.compressSize, item.originSize)
	}
}

func getTimeAlgo(b byte) string {
	b = b >> 4
	switch b {
	case 1:
		return "ConstDelta"
	case 2:
		return "Simple8b"
	case 3:
		return "Snappy"
	case 4:
		return "Uncompressed"
	default:
		return "Unknown"
	}
}

func getIntAlgo(b byte) string {
	b = b >> 4
	switch b {
	case 1:
		return "ConstDelta"
	case 2:
		return "Simple8b"
	case 3:
		return "ZSTD"
	case 4:
		return "Uncompressed"
	default:
		return "Unknown"
	}
}

func getFloatAlgo(b byte) string {
	b = b >> 4
	switch b {
	case 0:
		return "Uncompressed"
	case 1:
		return "OldGorilla"
	case 2:
		return "Snappy"
	case 3:
		return "InfluxDBGorilla"
	case 4:
		return "Same"
	case 5:
		return "RLE"
	default:
		return "Unknown"
	}
}

var encTypeToNormal = map[uint8]uint8{
	encoding.BlockFloat64One:   encoding.BlockFloat64,
	encoding.BlockIntegerOne:   encoding.BlockInteger,
	encoding.BlockBooleanOne:   encoding.BlockBoolean,
	encoding.BlockStringOne:    encoding.BlockString,
	encoding.BlockFloat64Full:  encoding.BlockFloat64,
	encoding.BlockIntegerFull:  encoding.BlockInteger,
	encoding.BlockBooleanFull:  encoding.BlockBoolean,
	encoding.BlockStringFull:   encoding.BlockString,
	encoding.BlockFloat64Empty: encoding.BlockFloat64,
	encoding.BlockIntegerEmpty: encoding.BlockInteger,
	encoding.BlockBooleanEmpty: encoding.BlockBoolean,
	encoding.BlockStringEmpty:  encoding.BlockString,
}
