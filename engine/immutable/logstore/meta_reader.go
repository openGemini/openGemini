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
package logstore

import (
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/util"
)

const (
	META_NAME            = "segment.meta"
	MAX_SCAN_META_COUNT  = 16 * 1024
	MAX_SCAN_META_Length = MAX_SCAN_META_COUNT * META_STORE_N_BYTES
)

type MetaReader interface {
	next() (*MetaData, error)
	Close()
}

// todo: cache reader
func NewMetaReader(option *obs.ObsOptions, path string, offset int64, length int64, tr util.TimeRange, isCache bool, isStat bool) (MetaReader, error) {
	return NewMetaStorageReader(option, path, offset, length, tr, isStat)
}

type MetaStorageReader struct {
	isStat            bool
	metaFileReader    fileops.BasicFileReader
	offset            int64
	length            int64
	tr                util.TimeRange
	readChan          chan *request.StreamReader
	readBuffer        []byte
	currentBlockIndex int64
}

func NewMetaStorageReader(option *obs.ObsOptions, path string, offset int64, length int64, tr util.TimeRange, isStat bool) (*MetaStorageReader, error) {
	metaReader := &MetaStorageReader{isStat: isStat}
	metaReader.tr = tr
	fd, err := obs.OpenObsFile(path, META_NAME, option)
	if err != nil {
		return nil, err
	}
	dr := fileops.NewFileReader(fd, nil)
	metaReader.offset = offset
	metaReader.length = length
	metaReader.metaFileReader = dr
	readChan := make(chan *request.StreamReader, RecordPoolNum)
	metaReader.metaFileReader.StreamReadBatch([]int64{metaReader.offset}, []int64{metaReader.length}, readChan, -1)
	metaReader.readChan = readChan
	return metaReader, nil
}

func (m *MetaStorageReader) next() (*MetaData, error) {
	metaData, err := m.getDataFromBuffer()
	if err != nil {
		return nil, err
	}
	if metaData != nil {
		if metaData.minTimestamp <= m.tr.Max && metaData.maxTimestamp >= m.tr.Min {
			return metaData, nil
		}
	}
	for r := range m.readChan {
		if r.Err != nil {
			return nil, r.Err
		}
		m.readBuffer = r.Content
		m.currentBlockIndex = r.Offset / int64(META_STORE_N_BYTES)
		metaData, err := m.getDataFromBuffer()
		if err != nil {
			return nil, err
		}
		if metaData != nil {
			if metaData.minTimestamp > m.tr.Max || metaData.maxTimestamp < m.tr.Min {
				continue
			}
			return metaData, nil
		}
	}
	return nil, nil
}

func (m *MetaStorageReader) getDataFromBuffer() (*MetaData, error) {
	for m.readBuffer != nil && len(m.readBuffer) > 0 {
		metaData, err := NewMetaData(m.currentBlockIndex, m.readBuffer[:META_STORE_N_BYTES], 0)
		if err != nil {
			return nil, err
		}
		m.readBuffer = m.readBuffer[META_STORE_N_BYTES:]
		m.currentBlockIndex++
		if err != nil {
			return nil, err
		}
		if metaData.maxTimestamp < m.tr.Min || metaData.minTimestamp > m.tr.Max {
			continue
		} else {
			return metaData, nil
		}
	}
	return nil, nil
}

func (m *MetaStorageReader) Close() {
	if m.metaFileReader != nil {
		m.metaFileReader.Close()
	}
}

func GetMetaLen(option *obs.ObsOptions, path string) (int64, error) {
	fd, err := obs.OpenObsFile(path, META_NAME, option)
	if err != nil {
		return 0, err
	}
	dr := fileops.NewFileReader(fd, nil)
	defer dr.Close()
	return dr.Size()
}
