// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package tsreader

import (
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
)

type TSSPReader interface {
	Ref()
	Unref()
	RefFileReader()
	UnrefFileReader()
	MetaIndex(id uint64, tr util.TimeRange) (int, *immutable.MetaIndex, error)
	ChunkMeta(id uint64, offset int64, size, itemCount uint32, metaIdx int,
		ctx *immutable.ChunkMetaContext, ioPriority int) (*immutable.ChunkMeta, error)
	ReadAt(cm *immutable.ChunkMeta, segment int, dst *record.Record,
		ctx *immutable.ReadContext, ioPriority int) (*record.Record, error)
}

func UnrefTSSPFile(readers ...immutable.TSSPFile) {
	for _, reader := range readers {
		reader.UnrefFileReader()
		reader.Unref()
	}
}
