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

package colstore

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/numberenc"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

func init() {
	InitDecFunctions()
}

type PrimaryKeyReader struct {
	inited   int32
	version  uint32
	fileSize int64
	reader   fileops.BasicFileReader
}

var (
	fileReaderPool = sync.Pool{}
)

func getPrimaryKeyReader() *PrimaryKeyReader {
	v := fileReaderPool.Get()
	if v == nil {
		return &PrimaryKeyReader{}
	}

	r, ok := v.(*PrimaryKeyReader)
	if !ok {
		return &PrimaryKeyReader{}
	}

	return r
}

func putPrimaryKeyReader(r *PrimaryKeyReader) {
	r.reset()
	fileReaderPool.Put(r)
}

func NewPrimaryKeyReader(name string, lockPath *string) (*PrimaryKeyReader, error) {
	var header [headerSize]byte
	fi, err := fileops.Stat(name)
	if err != nil {
		log.Error("stat file failed", zap.String("file", name), zap.Error(err))
		err = errno.NewError(errno.OpenFileFailed, err)
		return nil, err
	}
	if fi.Size() < int64(headerSize) {
		err = fmt.Errorf("invalid file(%v) size:%v", name, fi.Size())
		log.Error(err.Error())
		err = errno.NewError(errno.OpenFileFailed, err)
		_ = fileops.Remove(name, fileops.FileLockOption(*lockPath))
		return nil, err
	}
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	lock := fileops.FileLockOption("")
	fd, err := fileops.Open(name, lock, pri)
	if err != nil {
		err = errno.NewError(errno.OpenFileFailed, err)
		log.Error("open file failed", zap.String("file", name), zap.Error(err))
		return nil, err
	}
	dr := fileops.NewFileReader(fd, lockPath)
	hb := header[:]
	_, err = dr.ReadAt(0, uint32(headerSize), &hb)
	if err != nil {
		_ = dr.Close()
		log.Error("read file header failed", zap.String("file", name), zap.Error(err))
		return nil, err
	}
	if util.Bytes2str(header[:fileMagicSize]) != primaryKeyMagic {
		_ = dr.Close()
		err = fmt.Errorf("invalid file(%v) magic: %v", name, util.Bytes2str(header[:fileMagicSize]))
		log.Error(err.Error())
		err = errno.NewError(errno.OpenFileFailed, err)
		return nil, err
	}

	r := getPrimaryKeyReader()
	r.fileSize = fi.Size()
	r.version = numberenc.UnmarshalUint32(header[fileMagicSize:])
	r.reader = dr
	atomic.StoreInt32(&r.inited, 1)

	return r, nil
}

func (r *PrimaryKeyReader) reset() {
	r.version = version
	r.fileSize = 0
	r.reader = nil
	atomic.StoreInt32(&r.inited, 0)
}

func (r *PrimaryKeyReader) Open() error {
	return nil
}

func (r *PrimaryKeyReader) Close() error {
	err := r.reader.Close()
	putPrimaryKeyReader(r)
	return err
}

func (r *PrimaryKeyReader) FileSize() int64 {
	return r.fileSize
}

func (r *PrimaryKeyReader) Version() uint32 {
	return r.version
}

func (r *PrimaryKeyReader) Read(offset int64, size uint32, dst *[]byte) ([]byte, error) {
	b, err := r.reader.ReadAt(offset, size, dst)

	if err != nil {
		log.Error("read file failed", zap.String("file", r.reader.Name()), zap.Error(err))
		return nil, err
	}

	return b, nil
}

func (r *PrimaryKeyReader) ReadData() (*record.Record, error) {
	var err error
	buf := []byte{}
	data, err := r.Read(0, uint32(r.FileSize()), &buf)
	if err != nil {
		return nil, err
	}
	metaSize := int(numberenc.UnmarshalUint32(data[headerSize : headerSize+util.Uint32SizeBytes]))
	schema, offset := unmarshalMeta(data[headerSize : headerSize+metaSize])
	dst := record.NewRecord(schema, false)
	coder := encoding.NewCoderContext()
	for i := range offset {
		ref := &schema[i]
		colBuilder := dst.Column(i)
		if i < len(offset)-1 {
			err = decodeColumnData(ref, data[offset[i]+4:offset[i+1]], colBuilder, coder)
		} else {
			err = decodeColumnData(ref, data[offset[i]+4:], colBuilder, coder)
		}
		if err != nil {
			log.Error("decode column fail", zap.Error(err))
			return nil, err
		}
		crc := crc32.ChecksumIEEE(colBuilder.Val)
		crcWritten := numberenc.UnmarshalUint32(data[offset[i] : offset[i]+4])
		if crc != crcWritten { // check crc
			return nil, errors.New("failed to verify checksum")
		}
	}

	return dst, nil
}
