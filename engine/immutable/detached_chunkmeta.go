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
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
)

type DetachedChunkMetaReader struct {
	r fileops.BasicFileReader
}

func NewDetachedChunkMetaReader(path string, obsOpts *obs.ObsOptions) (*DetachedChunkMetaReader, error) {
	fd, err := obs.OpenObsFile(path, ChunkMetaFile, obsOpts)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	return &DetachedChunkMetaReader{r: dr}, nil
}

func (reader *DetachedChunkMetaReader) ReadChunkMeta(offset, length []int64) ([]*ChunkMeta, error) {
	c := make(chan *request.StreamReader, 1)
	reader.r.StreamReadBatch(offset, length, c, MetaIndexLimitNum)
	chunkMetas := make([]*ChunkMeta, len(offset))
	i := 0
	var err error
	for r := range c {
		if r.Err != nil {
			return nil, r.Err
		}
		chunkMetas[i] = &ChunkMeta{}
		if len(r.Content) < crcSize {
			err = fmt.Errorf("get wrong data")
			break
		}
		if binary.BigEndian.Uint32(r.Content[:crcSize]) != crc32.ChecksumIEEE(r.Content[crcSize:]) {
			err = fmt.Errorf("check detached chunkMeta crc failed")
			break
		}
		_, err := chunkMetas[i].unmarshal(r.Content[crcSize:])
		if err != nil {
			return nil, err
		}
		i += 1
	}
	if err != nil {
		for range c {
		}
		return nil, err
	}
	sort.Slice(chunkMetas, func(i, j int) bool {
		return chunkMetas[i].offset < chunkMetas[j].offset
	})
	return chunkMetas, nil
}

type SegmentMeta struct {
	id        int
	chunkMeta *ChunkMeta
}

func NewSegmentMeta(id int, c *ChunkMeta) *SegmentMeta {
	return &SegmentMeta{id: id, chunkMeta: c}
}

func (s *SegmentMeta) GetMinTime() int64 {
	return s.chunkMeta.timeRange[s.id].minTime()
}

func (s *SegmentMeta) GetMaxTime() int64 {
	return s.chunkMeta.timeRange[s.id].maxTime()
}
