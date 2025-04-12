// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package immutable

import (
	"unsafe"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const (
	tableMagic = "53ac2021"
	version    = uint64(2)

	trailerSize           = int(unsafe.Sizeof(Trailer{})) - 24*2 + (2 + 0) + (2 + 1) - int(unsafe.Sizeof(ExtraData{}))
	fileHeaderSize        = len(tableMagic) + int(unsafe.Sizeof(version))
	kb                    = 1024
	maxImmTablePercentage = 85
)

var (
	falsePositive = 0.08
)

type TableData struct {
	// id bloom filter data
	bloomFilter []byte
	// include version | each section offset | measurement name | key and time range etc...
	trailerData    []byte
	metaIndexItems []MetaIndex
}

func (t *TableData) reset() {
	t.trailerData = t.trailerData[:0]
	t.bloomFilter = t.bloomFilter[:0]
	t.metaIndexItems = t.metaIndexItems[:0]
}

func minTableSize() int64 {
	return int64(len(tableMagic)+trailerSize) + 8 + 8
}

func pow2(v uint64) uint64 {
	for i := uint64(8); i < 1<<62; i *= 2 {
		if i >= v {
			return i
		}
	}
	panic("unreachable")
}

func WriteIntoFile(msb *MsBuilder, tmp bool, withPKIndex bool, ir *influxql.IndexRelation) error {
	f, err := msb.NewTSSPFile(tmp)
	if err != nil {
		panic(err)
	}
	if f != nil {
		msb.Files = append(msb.Files, f)
		fileInfo := genFileInfo(f, msb)
		msb.FilesInfo = append(msb.FilesInfo, fileInfo)
	}

	if !withPKIndex {
		err = RenameTmpFiles(msb.Files)
	} else {
		err = RenameTmpFilesWithPKIndex(msb.Files, ir)
	}

	if err != nil {
		return err
	}

	return RenameTmpFullTextIdxFile(msb)
}
