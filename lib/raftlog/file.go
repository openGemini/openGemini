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

package raftlog

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/fileops"
)

const unit32Size = 4 // the byte size of unit32
const unit64Size = 8 // the byte size of unit64

var NewFile = errors.New("Create a new file")

type FileWrapper interface {
	Name() string
	Size() int
	GetEntryData(start, end int) []byte
	Write(dat []byte) (int, error)
	WriteAt(offset int64, dat []byte) (int, error)
	WriteSlice(offset int64, dat []byte) error
	ReadSlice(offset int64) []byte
	SliceSize(offset int) int
	Truncate(size int64) error
	TrySync() error
	Delete() error
	Close() error
}

// FileWrap represents a file and includes both the buffer to the data
// and the file descriptor.
type FileWrap struct {
	fd   fileops.File
	data []byte
}

// OpenFile opens an existing file or creates a new file. If the file is
// created, it would truncate the file to maxSz. In case the file is created, z.NewFile is
// returned.
func OpenFile(fpath string, flag int, maxSz int) (FileWrapper, error) {
	fd, err := fileops.OpenFile(fpath, flag, 0640, fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL))
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open: %s", fpath)
	}

	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "cannot stat file: %s", filename)
	}

	fw := &FileWrap{
		fd: fd,
	}

	var rerr error
	fileSize := fi.Size()
	if maxSz > 0 && fileSize == 0 {
		// If file is empty, truncate it to sz.
		var buff = make([]byte, maxSz)
		if _, err = fw.Write(buff); err != nil {
			return nil, errors.Wrapf(err, "error while truncation")
		}
		if err = fw.TrySync(); err != nil {
			return nil, errors.Wrapf(err, "error while trySync")
		}
		rerr = NewFile
	} else {
		reader := bufio.NewReader(fd)
		content, err := io.ReadAll(reader)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading file:%s", fw.Name())
		}
		fw.data = content
	}
	return fw, rerr
}

func (fw *FileWrap) Name() string {
	if fw.fd == nil {
		return ""
	}
	return fw.fd.Name()
}

func (fw *FileWrap) Size() int {
	return len(fw.data)
}

// GetEntryData returns entry data
func (fw *FileWrap) GetEntryData(start, end int) []byte {
	return fw.data[start:end]
}

func (fw *FileWrap) Write(dat []byte) (int, error) {
	n, err := fw.fd.Write(dat)
	if err != nil {
		return 0, errors.Wrapf(err, "write failed:%s", fw.Name())
	}
	fw.data = append(fw.data, dat...)
	return n, err
}

func (fw *FileWrap) WriteAt(offset int64, dat []byte) (int, error) {
	_, err := fw.fd.Seek(offset, 0)
	if err != nil {
		return 0, errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
	}
	n, err := fw.fd.Write(dat)
	if err != nil {
		return 0, errors.Wrapf(err, "write failed for file:%s", fw.Name())
	}
	copy(fw.data[offset:], dat)
	_, err = fw.fd.Seek(0, io.SeekEnd)
	return n, errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
}

func (fw *FileWrap) WriteSlice(offset int64, dat []byte) error {
	_, err := fw.fd.Seek(offset, 0)
	if err != nil {
		return errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
	}
	var buff = make([]byte, 0, unit32Size)
	buff = encoding.MarshalUint32(buff, uint32(len(dat))) // size
	_, err = fw.fd.Write(buff)
	if err != nil {
		return errors.Wrapf(err, "write failed for file:%s", fw.Name())
	}
	_, err = fw.fd.Write(dat) // data
	if err != nil {
		return errors.Wrapf(err, "write failed for file:%s", fw.Name())
	}
	_, err = fw.fd.Seek(0, io.SeekEnd)

	if int(offset)+unit32Size+len(dat) >= len(fw.data) {
		fw.data = append(fw.data, make([]byte, int(offset)-len(fw.data)+unit32Size+len(dat))...)
	}
	dst := fw.data[offset:]
	binary.BigEndian.PutUint32(dst[:unit32Size], uint32(len(dat))) // size
	copy(dst[unit32Size:], dat)                                    // data

	return errors.Wrapf(err, "seek unreachable file:%s", fw.Name())
}

func (fw *FileWrap) ReadSlice(offset int64) []byte {
	sz := encoding.UnmarshalUint32(fw.data[offset:])
	start := offset + unit32Size
	next := int(start) + int(sz)
	if next > len(fw.data) {
		return []byte{}
	}
	return fw.data[start:next]
}

func (fw *FileWrap) SliceSize(offset int) int {
	sz := encoding.UnmarshalUint32(fw.data[offset:])
	return unit32Size + int(sz)
}

func (fw *FileWrap) Truncate(size int64) error {
	return fw.fd.Truncate(size)
}

func (fw *FileWrap) Delete() error {
	// Badger can set the m.Data directly, without setting any Fd. In that case, this should be a
	// NOOP.
	if fw.fd == nil {
		return nil
	}

	fw.data = nil
	if err := fw.fd.Close(); err != nil {
		return errors.Wrapf(err, "while close file:%s", fw.Name())
	}
	return fileops.Remove(fw.fd.Name())
}

func (fw *FileWrap) Close() error {
	if err := fw.TrySync(); err != nil {
		return errors.Wrapf(err, "sync error:%s", fw.Name())
	}
	return fw.fd.Close()
}

func (fw *FileWrap) TrySync() error {
	if fw.fd == nil {
		return nil
	}
	return fw.fd.Sync()
}
