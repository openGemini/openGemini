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
package obs

import (
	"path"
	"path/filepath"
	"strings"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logstore"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/request"
	"github.com/openGemini/openGemini/lib/tracing"
)

const (
	ObsPathSeparator = "/"
	RecordPoolNum    = 3
	OBSSuffix        = ".obs"
)

var (
	FileCursorPool = record.NewRecordPool(record.LogStoreReaderPool)
)

type StoreReader interface {
	Read(offs []int64, sizes []int64, minBlockSize int64, result map[int64][]byte, obsRangeSize int) error
	ReadTo(off int64, receiver []byte) error
	StreamRead(offs []int64, sizes []int64, minBlockSize int64, c chan *request.StreamReader, obsRangeSize int)
	Len() (int64, error)
	Close()
	StartSpan(span *tracing.Span)
	EndSpan()
}

type FsReader struct {
	fd fileops.File
}

type ObsReader struct {
	obsClient  *ObjectClient
	objectName string
	logPath    *LogPath
}

func NewFsReader(objectDir *LogPath, fileName string) (*FsReader, error) {
	fd, err := fileops.Open(filepath.Join(objectDir.SegmentPath, fileName))
	if err != nil {
		return nil, err
	}
	return &FsReader{
		fd: fd,
	}, nil
}

func (r *FsReader) Read(offs []int64, sizes []int64, minBlockSize int64, result map[int64][]byte, obsRangeSize int) error {
	readChan := make(chan *request.StreamReader, RecordPoolNum)
	go r.StreamRead(offs, sizes, minBlockSize, readChan, -1)
	for re := range readChan {
		if re.Err != nil {
			return re.Err
		}
		result[re.Offset] = re.Content
	}
	return nil
}

func (r *FsReader) ReadTo(off int64, receiver []byte) error {
	_, err := r.fd.ReadAt(receiver, off)
	return err
}

func (r *FsReader) StreamRead(offs []int64, sizes []int64, minBlockSize int64, c chan *request.StreamReader, obsRangeSize int) {
	for i, offset := range offs {
		content := make([]byte, sizes[i])
		_, err := r.fd.ReadAt(content, offset)
		c <- &request.StreamReader{
			Offset:  offset,
			Err:     err,
			Content: content,
		}
		if err != nil {
			break
		}
	}
	close(c)
}

func (r *FsReader) Close() {
	if r.fd != nil {
		r.fd.Close()
	}
}

func (r *FsReader) Len() (int64, error) {
	fileInfo, err := r.fd.Stat()
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func (r *FsReader) StartSpan(span *tracing.Span) {
}

func (r *FsReader) EndSpan() {
}

func NewReader(logPath *LogPath, fileName string) (StoreReader, error) {
	if logPath.IsObsEnabled() && (fileName == logstore.OBSMetaFileName || fileName == logstore.OBSVLMFileName || fileName == logstore.OBSContentFileName || strings.HasSuffix(fileName, OBSSuffix)) {
		return NewObsReader(logPath, fileName)
	} else {
		return NewFsReader(logPath, fileName)
	}
}

func NewObsReader(logPath *LogPath, fileName string) (*ObsReader, error) {
	obsClient, err := GetObsClient(logPath)
	if err != nil {
		return nil, err
	}
	return &ObsReader{obsClient: obsClient, objectName: Join(logPath.BasePath, logPath.SegmentPath, fileName), logPath: logPath}, nil
}

func Join(elem ...string) string {
	for i, e := range elem {
		if e != "" {
			return path.Clean(strings.Join(elem[i:], ObsPathSeparator))
		}
	}
	return ""
}

func (r *ObsReader) Read(offs []int64, sizes []int64, minBlockSize int64, result map[int64][]byte, obsRangeSize int) error {
	readChan := make(chan *request.StreamReader, RecordPoolNum)
	go r.StreamRead(offs, sizes, minBlockSize, readChan, obsRangeSize)
	for re := range readChan {
		if re.Err != nil {
			return re.Err
		}
		result[re.Offset] = re.Content
	}
	return nil
}

func (r *ObsReader) ReadTo(off int64, receiver []byte) error {
	return r.obsClient.ReadAt(r.objectName, off, int64(len(receiver)), receiver)
}

func (r *ObsReader) StreamRead(offs []int64, sizes []int64, minBlockSize int64, c chan *request.StreamReader, obsRangeSize int) {
	_ = r.obsClient.StreamReadMultiRange(r.objectName, offs, sizes, minBlockSize, c, obsRangeSize)
}

func (r *ObsReader) Len() (int64, error) {
	objectLen, err := r.obsClient.GetLength(r.objectName)
	if err != nil {
		return 0, err
	}
	return objectLen, nil
}

func (r *ObsReader) Close() {
}

func (r *ObsReader) StartSpan(span *tracing.Span) {
}

func (r *ObsReader) EndSpan() {
}
