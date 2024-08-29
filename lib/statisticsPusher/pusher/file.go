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

package pusher

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"go.uber.org/zap"
)

const (
	fileExpire    = time.Hour * 24 * 2
	splitDuration = time.Hour
)

var fileSeq = time.Now().Unix()

type File struct {
	compress bool
	conf     *FileConfig
	logger   *logger.Logger
	done     chan struct{}

	path string
	mu   sync.RWMutex

	writer *SnappyWriter
}

type FileConfig struct {
	App  string
	Path string

	dir  string
	name string
	ext  string
}

func (c *FileConfig) Parser() {
	base := filepath.Base(c.Path)
	c.dir = filepath.Dir(c.Path)
	c.ext = filepath.Ext(c.Path)
	c.name = strings.TrimSuffix(base, c.ext)
}

// GetSavePath generate file names by day
func (c *FileConfig) GetSavePath() string {
	return filepath.Join(c.dir, fmt.Sprintf("%s-%s-%d%s", c.App, c.name, fileSeq, c.ext))
}

// GetName get name of the metric file
func (c *FileConfig) GetName() string {
	return c.name
}

// GetDir get directory of the metric file
func (c *FileConfig) GetDir() string {
	return c.dir
}

// GlobPattern get pattern to match metric files
func (c *FileConfig) GlobPattern() string {
	return fmt.Sprintf("%s/*", c.dir)
}

func NewFile(conf *FileConfig, compress bool, logger *logger.Logger) *File {
	f := &File{
		compress: compress,
		conf:     conf,
		logger:   logger,
	}
	f.init()
	go f.run()
	return f
}

func (f *File) run() {
	tickerUpdate := time.NewTicker(time.Minute)
	tickerRemove := time.NewTicker(time.Hour)
	tickerSplit := time.NewTicker(splitDuration)

	for {
		select {
		case <-tickerSplit.C:
			fileSeq++
		case <-tickerUpdate.C:
			path := f.conf.GetSavePath()
			if path != f.path {
				f.path = path
				f.openFile(f.path)
			}
		case <-tickerRemove.C:
			go f.removeExpireFiles(f.conf.GlobPattern(), fileExpire)
		case <-f.done:
			tickerUpdate.Stop()
			tickerRemove.Stop()
			tickerSplit.Stop()
			return
		}
	}
}

func (f *File) init() {
	f.done = make(chan struct{})
	f.conf.Parser()

	err := os.MkdirAll(f.conf.dir, 0700)
	if err != nil {
		f.logger.Error("failed to invoke os.MkdirAll", zap.Error(err), zap.String("dir", f.conf.dir))
	}
	f.path = f.conf.GetSavePath()
	f.openFile(f.path)
}

func (f *File) openFile(path string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writer != nil {
		_ = f.writer.Close()
		f.writer = nil
	}

	f.writer = NewSnappyWriter()
	if f.compress {
		f.writer.EnableCompress()
	}
	if err := f.writer.OpenFile(path); err != nil {
		f.logger.Error("failed to open file", zap.Error(err), zap.String("path", path))
		return
	}
}

func (f *File) Push(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writer == nil {
		return nil
	}

	err := f.writer.WriteBlock(bytes.TrimSpace(data))
	return err
}

func (f *File) Stop() {
	f.mu.Lock()
	if f.writer != nil {
		_ = f.writer.Close()
		f.writer = nil
	}
	f.mu.Unlock()

	close(f.done)
}

// removeExpireFiles periodically delete expired files.
func (f *File) removeExpireFiles(pattern string, expire time.Duration) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		f.logger.Error("failed to remove old files", zap.Error(err))
		return
	}

	for _, path := range matches {
		stat, err := os.Stat(path)
		if err != nil {
			f.logger.Error("failed to read file stat", zap.Error(err), zap.String("path", path))
			continue
		}

		if time.Now().After(stat.ModTime().Add(expire)) {
			f.logger.Info("remove expired file", zap.String("path", path))
			_ = os.Remove(path)
		}
	}
}
