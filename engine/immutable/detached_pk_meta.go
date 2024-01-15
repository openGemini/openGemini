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

package immutable

import (
	"sort"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	PKMetaLimitNum   = 16
	PKMetaPrefixSize = util.Uint64SizeBytes*2 + util.Uint32SizeBytes*2
)

type DetachedPKMetaReader struct {
	r fileops.BasicFileReader
}

func NewDetachedPKMetaReader(path string, opts *obs.ObsOptions) (*DetachedPKMetaReader, error) {
	fd, err := obs.OpenObsFile(path, PrimaryMetaFile, opts)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	return &DetachedPKMetaReader{r: dr}, nil
}

func (reader *DetachedPKMetaReader) Read(offset, length []int64) ([]*colstore.DetachedPKMeta, error) {
	c := make(chan *request.StreamReader, 1)
	reader.r.StreamReadBatch(offset, length, c, PKMetaLimitNum)
	pkItems := make([]*colstore.DetachedPKMeta, len(offset))
	i := 0
	for r := range c {
		if r.Err != nil {
			return nil, r.Err
		}
		pkItems[i] = &colstore.DetachedPKMeta{}
		r.Content = r.Content[crcSize:]
		_, err := pkItems[i].Unmarshal(r.Content)
		if err != nil {
			return nil, err
		}
		i += 1
	}
	sort.Slice(pkItems, func(i, j int) bool {
		return pkItems[i].StartBlockId < pkItems[j].StartBlockId
	})
	return pkItems, nil
}

func GetPKMetaOffsetLengthByChunkId(pkMetaInfo *colstore.DetachedPKMetaInfo, chunkId int) (offset, length int64) {
	publicInfoSize := pkMetaInfo.Size()
	colOffsetSize := pkMetaInfo.Schema.Len() * util.Uint32SizeBytes
	pkMetaSize := PKMetaPrefixSize + colOffsetSize
	return int64(publicInfoSize + (pkMetaSize+crcSize)*chunkId), int64(pkMetaSize + crcSize)
}
