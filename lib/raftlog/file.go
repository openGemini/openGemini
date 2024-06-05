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
	"io"

	"github.com/cockroachdb/errors"
	"github.com/openGemini/openGemini/lib/fileops"
)

var NewFile = errors.New("Create a new file")

type FileWrapper interface {
	Name() string
	Size() int
	Write(dat []byte) (int, error)
	WriteAt(offset int64, dat []byte) (int, error)
	TrySync() error
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
		return 0, errors.Wrapf(err, "seek unreachable file:%s", fw.fd.Name())
	}
	n, err := fw.fd.Write(dat)
	if err != nil {
		return 0, errors.Wrapf(err, "write failed for file:%s", fw.fd.Name())
	}
	copy(fw.data[offset:], dat)
	_, err = fw.fd.Seek(0, io.SeekEnd)
	return n, errors.Wrapf(err, "seek unreachable file:%s", fw.fd.Name())
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
