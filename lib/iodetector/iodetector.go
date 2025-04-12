// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package iodetector

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/openGemini/openGemini/lib/sysinfo"
)

const (
	defaultSuicideTimeout = 30
	defaultFlushInterval  = 10
)

type Config struct {
	Paths          []string `toml:"paths"`
	SuicideTimeout int      `toml:"timeout"`
	FlushInterval  int      `toml:"flush-interval"`
}

func NewIODetector() *Config {
	return &Config{
		SuicideTimeout: defaultSuicideTimeout,
		FlushInterval:  defaultFlushInterval,
	}
}

func (c *Config) Validate() error {
	if c.FlushInterval < 0 {
		return errors.New("IODetector flush interval can not be negative")
	}
	if c.SuicideTimeout < 0 {
		return errors.New("IODetector suicide timeout can not be negative")
	}
	return nil
}

func OpenIODetection(cfg *Config) *IODetector {
	if cfg == nil || len(cfg.Paths) == 0 {
		return nil
	}
	// start I/O Detection
	return startIODetection(cfg)
}

func startIODetection(conf *Config) *IODetector {
	ioDetector := &IODetector{
		detectFilePath:   conf.Paths,
		detectCh:         make(chan time.Time),
		detectCloseCh:    make(chan bool),
		flushCloseCh:     make(chan bool),
		timeoutThreshold: conf.SuicideTimeout,
		flushInterval:    conf.FlushInterval,
	}

	ioDetector.processIODetection()
	return ioDetector
}

type IODetector struct {
	detectFilePath   []string
	detectCh         chan time.Time
	detectCloseCh    chan bool
	flushCloseCh     chan bool
	timeoutThreshold int // Suicide Timeout. The unit is second.
	flushInterval    int // Disk Flush Interval. The unit is second.
}

func (d *IODetector) processIODetection() {
	go d.flushDiskForDetectIO()
	go d.detectIO()
}

// flushDiskForDetectIO
// Flush Logs to disks every 10 seconds.
func (d *IODetector) flushDiskForDetectIO() {
	ticker := time.NewTicker(time.Duration(d.flushInterval) * time.Second)
	for {
		select {
		case <-d.flushCloseCh:
			return
		case <-ticker.C:
			d.flush()
			d.detectCh <- time.Now()
		}
	}
}

// detectIO checks the disk flushing time every second.
// If the time is not updated within 30 seconds, the process kills itself.
func (d *IODetector) detectIO() {
	ticker := time.NewTicker(1 * time.Second)
	beforeTime := time.Now()
	for {
		select {
		case <-d.detectCloseCh:
			return
		case beforeTime = <-d.detectCh:
		case <-ticker.C:
			if time.Since(beforeTime) > time.Duration(d.timeoutThreshold)*time.Second {
				log.Println("Suicide for I/O detection failed")
				// process suicide
				sysinfo.Suicide()
				return
			}
		}
	}
}

func (d *IODetector) flush() {
	for _, filePath := range d.detectFilePath {
		file, err := os.OpenFile(filepath.Clean(filePath), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil || file == nil {
			return
		}

		_, err = file.WriteAt([]byte("I/O detection"), 0)
		if err != nil {
			fmt.Printf("I/O Detection: detectFile.WriteAt() error:%v\n", err)
		}

		err = file.Sync()
		if err != nil {
			fmt.Printf("I/O Detection: detectFile.Sync() error:%v\n", err)
		}
		if err := file.Close(); err != nil {
			fmt.Printf("Fail to close file error:%v\n", err)
		}
	}
}

func (d *IODetector) Close() {
	close(d.detectCloseCh)
	close(d.flushCloseCh)
}
