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
	"fmt"
	"os"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
)

type FsStoreAppender struct {
	file   *os.File
	logger *logger.Logger
}

func NewFsStoreAppender(name string) (*FsStoreAppender, error) {
	log := logger.NewLogger(errno.ModuleLogStore)
	file, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0640)
	if err != nil {
		log.Error(fmt.Sprintf("create fs appender [%s] failed, error: %s", name, err.Error()))
		return nil, err
	}
	return &FsStoreAppender{file: file, logger: log}, nil
}

func (f *FsStoreAppender) AppendWithCall(data []byte, call func()) error {
	defer func() {
		if call != nil {
			call()
		}
	}()
	return f.Append(data)
}

func NewFsStoreAppenderWithTrunc(name string, size int64) (*FsStoreAppender, error) {
	log := logger.NewLogger(errno.ModuleLogStore)
	err := os.Truncate(name, size)
	if err != nil {
		log.Error(fmt.Sprintf("trunc file [%s] with size=%d failed, error: %s", name, size, err))
		return nil, err
	}
	file, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		log.Error(fmt.Sprintf("create fs appender [%s] failed, error: %s", name, err))
		return nil, err
	}
	return &FsStoreAppender{file: file, logger: log}, nil
}

func (f *FsStoreAppender) Append(data []byte) error {
	_, err := f.file.Write(data)
	if err != nil {
		f.logger.Error(fmt.Sprintf("append to [%s] with size=%d failed, error: %s", f.file.Name(), len(data), err))
		return err
	}
	return nil
}

func (f *FsStoreAppender) Commit() error {
	err := f.file.Sync()
	if err != nil {
		f.logger.Error(fmt.Sprintf("commit to [%s] failed, error: %s", f.file.Name(), err))
	}
	return err
}

func (f *FsStoreAppender) TryCommit() error {
	return f.Commit()
}

func (f *FsStoreAppender) Close() error {
	err := f.file.Sync()
	if err != nil {
		f.logger.Error(fmt.Sprintf("fsync to [%s] for close failed, error: %s", f.file.Name(), err))
		return err
	}
	err = f.file.Close()
	if err != nil {
		f.logger.Error(fmt.Sprintf("close file [%s] failed, error: %s", f.file.Name(), err))
		return err
	}
	return nil
}
