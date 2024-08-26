// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package colstore

import (
	"io"
	"os"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	Log "github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

type SkipIndexBuilder struct {
	encodeChunk []byte
	fd          fileops.File
	writer      fileops.FileWriter
	log         *Log.Logger
}

func NewSkipIndexBuilder(lockPath *string, filePath string) *SkipIndexBuilder {
	indexBuilder := &SkipIndexBuilder{}
	var err error
	lock := fileops.FileLockOption(*lockPath)
	pri := fileops.FilePriorityOption(fileops.IO_PRIORITY_NORMAL)
	indexBuilder.fd, err = fileops.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0640, lock, pri)
	if err != nil {
		log.Error("create file fail", zap.String("name", filePath), zap.Error(err))
		panic(err)
	}
	indexBuilder.log = Log.NewLogger(errno.ModuleCompact).SetZapLogger(log)
	indexBuilder.writer = newIndexWriter(indexBuilder.fd, lockPath)
	return indexBuilder
}

func (b *SkipIndexBuilder) WriteData(data []byte) error {
	var num int
	var err error
	if num, err = b.writer.WriteData(data); err != nil {
		err = errno.NewError(errno.WriteFileFailed, err)
		b.log.Error("write chunk data fail", zap.Error(err))
		return err
	}

	if num != len(data) {
		b.log.Error("write chunk data fail", zap.String("file", b.fd.Name()),
			zap.Ints("size", []int{len(b.encodeChunk), num}))
		return io.ErrShortWrite
	}
	return nil
}

func (b *SkipIndexBuilder) Reset() {
	b.encodeChunk = b.encodeChunk[:0]
	if b.writer != nil {
		_ = b.writer.Close()
		b.writer = nil
	}
	if b.fd != nil {
		_ = b.fd.Close()
		b.fd = nil
	}
}
