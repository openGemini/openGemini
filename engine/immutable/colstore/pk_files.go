// Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colstore

import (
	"fmt"
	"hash/crc32"

	"github.com/RoaringBitmap/roaring"
	"github.com/openGemini/openGemini/lib/fragment"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

const (
	PKMetaPrefixSize = util.Uint64SizeBytes*2 + util.Uint32SizeBytes*2
	CRCLen           = 4
)

type ClusterIndex interface {
	Query(expr influxql.Expr) (*roaring.Bitmap, error)
	GetRowCount(expr influxql.Expr) (uint64, error)
}

type PKInfo struct {
	pkType     string
	tcLocation int8
	rec        *record.Record
	mark       fragment.IndexFragment
	ci         ClusterIndex
	columns    map[string]int
}

func NewPKInfo(rec *record.Record, mark fragment.IndexFragment, pkType string, tcLocation int8) *PKInfo {
	pki := &PKInfo{
		pkType:     pkType,
		tcLocation: tcLocation,
		rec:        rec,
		mark:       mark,
	}
	pki.BuildColumnMap()
	return pki
}

func (p *PKInfo) IsClusterIndex() bool {
	return p.pkType == meta.PrimaryKeyTypeCluster
}

func (p *PKInfo) SetPkType(typ string) {
	p.pkType = typ
}

func (p *PKInfo) GetPkType() string {
	return p.pkType
}

func (p *PKInfo) GetTCLocation() int8 {
	return p.tcLocation
}

func (p *PKInfo) SetRec(rec *record.Record) {
	p.rec = rec
	p.BuildColumnMap()
}

func (p *PKInfo) GetRec() *record.Record {
	return p.rec
}

func (p *PKInfo) GetMark() fragment.IndexFragment {
	return p.mark
}

func (p *PKInfo) SetClusterIndex(ci ClusterIndex) {
	p.ci = ci
}

func (p *PKInfo) GetClusterIndex() ClusterIndex {
	return p.ci
}

func (p *PKInfo) GetColumnMap() map[string]int {
	return p.columns
}

func (p *PKInfo) BuildColumnMap() {
	p.columns = make(map[string]int)
	if p.rec == nil {
		return
	}

	for i := range p.rec.Schema {
		field := &p.rec.Schema[i]
		if field.Name == record.TimeField || field.Name == record.FragmentField {
			continue
		}
		p.columns[field.Name] = i
	}
}

type PkMetaBlock struct {
	StartBlockId uint64
	EndBlockId   uint64
	Offset       uint32
	Size         uint32
}

func MarshalPkMetaBlock(startId, endId uint64, offset, size uint32, meta, colsOffset []byte) []byte {
	pos := uint32(len(meta))
	meta = numberenc.MarshalUint32Append(meta, 0) // reserve crc32
	meta = numberenc.MarshalUint64Append(meta, startId)
	meta = numberenc.MarshalUint64Append(meta, endId)
	meta = numberenc.MarshalUint32Append(meta, offset)
	meta = numberenc.MarshalUint32Append(meta, size)
	meta = append(meta, colsOffset...)
	crc := crc32.ChecksumIEEE(meta[pos+crcSize:])
	numberenc.MarshalUint32Copy(meta[pos:pos+crcSize], crc)
	return meta
}

func UnmarshalPkMetaBlock(src []byte) (*PkMetaBlock, error) {
	if len(src) < PKMetaPrefixSize {
		return nil, fmt.Errorf("not enough data for unmarshal PkMetaBlock; want %d bytes; remained %d bytes", PKMetaPrefixSize, len(src))
	}

	pk := &PkMetaBlock{}
	src = src[CRCLen:]
	pk.StartBlockId, src = numberenc.UnmarshalUint64(src), src[util.Uint64SizeBytes:]
	pk.EndBlockId, src = numberenc.UnmarshalUint64(src), src[util.Uint64SizeBytes:]
	pk.Offset, src = numberenc.UnmarshalUint32(src), src[util.Uint32SizeBytes:]
	pk.Size = numberenc.UnmarshalUint32(src)
	// colsOffset is parsed on demand outside
	return pk, nil
}
