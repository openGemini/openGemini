/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
	"io"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/pool"
	"go.uber.org/zap"
)

type TagSets struct {
	//   map[tag key][tag value]
	sets       map[string]map[string]struct{}
	totalCount int
}

func NewTagSets() *TagSets {
	return &TagSets{
		sets: make(map[string]map[string]struct{}),
	}
}

func (s *TagSets) Add(key, val string) {
	if valueSet, keyExist := s.sets[key]; !keyExist {
		s.sets[key] = map[string]struct{}{val: {}}
		s.totalCount++
	} else if _, valueExist := valueSet[val]; !valueExist {
		valueSet[val] = struct{}{}
		s.totalCount++
	}
}

func (s *TagSets) ForEach(process func(tagKey, tagValue string)) {
	for tagKey, tagValues := range s.sets {
		for tagValue := range tagValues {
			process(tagKey, tagValue)
		}
	}
}

func (s *TagSets) TagKVCount() int {
	return s.totalCount
}

type SequenceIteratorHandler interface {
	Init(map[string]interface{}) error
	Begin()
	NextFile(TSSPFile)
	NextChunkMeta([]byte) error
	Limited() bool
	Finish()
}

type SequenceIterator interface {
	SetChunkMetasReader(reader SequenceIteratorChunkMetaReader)
	Release()
	AddFiles(files []TSSPFile)
	Stop()
	Run() error
	Buffer() *pool.Buffer
}

type sequenceIterator struct {
	handler         SequenceIteratorHandler
	chunkMetaReader SequenceIteratorChunkMetaReader
	buf             pool.Buffer
	files           []TSSPFile
	logger          *Log.Logger
}

func NewSequenceIterator(handler SequenceIteratorHandler, logger *Log.Logger) SequenceIterator {
	itr := &sequenceIterator{
		handler: handler,
		buf:     pool.Buffer{},
	}
	reader := &chunkMetasReader{itr: itr}
	itr.chunkMetaReader = reader
	itr.logger = logger
	return itr
}

func (itr *sequenceIterator) SetChunkMetasReader(reader SequenceIteratorChunkMetaReader) {
	itr.chunkMetaReader = reader
}

func (itr *sequenceIterator) Release() {
	if len(itr.files) > 0 {
		UnrefFiles(itr.files...)
		itr.files = itr.files[:0]
	}
}

func (itr *sequenceIterator) AddFiles(files []TSSPFile) {
	itr.files = append(itr.files, files...)
}

func (itr *sequenceIterator) Stop() {

}

func (itr *sequenceIterator) Run() error {
	itr.handler.Begin()
	defer itr.handler.Finish()

	for _, f := range itr.files {
		err := itr.iterateFile(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func (itr *sequenceIterator) Buffer() *pool.Buffer {
	return &itr.buf
}

func (itr *sequenceIterator) iterateFile(f TSSPFile) error {
	f.RefFileReader()
	defer f.UnrefFileReader()

	itr.handler.NextFile(f)

	n := int(f.MetaIndexItemNum())
	for i := 0; i < n; i++ {
		buf, offsets, err := itr.chunkMetaReader.ReadChunkMetas(f, i)
		if err != nil {
			return err
		}

		offsets = append(offsets, uint32(len(buf)))
		for k := 0; k < len(offsets)-1; k++ {
			err = itr.handler.NextChunkMeta(buf[offsets[k]:offsets[k+1]])
			if err != nil && errno.Equal(err, errno.ErrSearchSeriesKey) {
				// sid not found in index, skip
				itr.logger.Warn("sequenceIterator.iterateFile NextChunkMeta search sid failed", zap.Error(err))
				continue
			} else if err != nil {
				return err
			}

			// reach the limit
			if itr.handler.Limited() {
				return io.EOF
			}
		}
	}

	return nil
}

type SequenceIteratorChunkMetaReader interface {
	ReadChunkMetas(f TSSPFile, idx int) ([]byte, []uint32, error)
}

type chunkMetasReader struct {
	itr SequenceIterator
}

func (r *chunkMetasReader) ReadChunkMetas(f TSSPFile, idx int) ([]byte, []uint32, error) {
	mi, err := f.MetaIndexAt(idx)
	if err != nil {
		return nil, nil, err
	}

	var buf []byte

	buffer := r.itr.Buffer()
	buf, err = f.ReadData(mi.offset, mi.size, &buffer.B, fileops.IO_PRIORITY_HIGH)
	if err != nil {
		return nil, nil, err
	}

	buf, err = decompressChunkMeta(f.ChunkMetaCompressMode(), buffer, buf)
	if err != nil {
		return nil, nil, err
	}

	return chunkMetaDataAndOffsets(buf, mi.count)
}
