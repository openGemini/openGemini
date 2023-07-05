/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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

package iodetector

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
)

const (
	defaultLogDiskFilePath    = "/opt/tsdb/log/io_detection.log"
	defaultSystemDiskFilePath = "/home/Ruby/io_detection.log"
)

func newIODetector(detectFilePath []string, timeout, flushInterval int) *IODetector {
	return &IODetector{
		detectFilePath:   detectFilePath,
		detectCh:         make(chan time.Time),
		detectCloseCh:    make(chan bool),
		flushCloseCh:     make(chan bool),
		timeoutThreshold: timeout,
		flushInterval:    flushInterval,
	}
}

func TestIODetector_flushDiskForDetectIO(t *testing.T) {
	testFunc := func() {
		detector := newIODetector([]string{"mock_path1", "mock_path2"}, defaultSuicideTimeout, defaultFlushInterval)
		ticker := time.NewTicker(100 * time.Millisecond)
		patch1 := gomonkey.ApplyPrivateMethod(detector, "flush", func(_ *IODetector) { return })
		patch2 := gomonkey.ApplyFunc(time.NewTicker,
			func(d time.Duration) *time.Ticker {
				return ticker
			})
		defer func() {
			patch1.Reset()
			patch2.Reset()
		}()
		go detector.flushDiskForDetectIO()
		beforeTime := <-detector.detectCh
		convey.So(beforeTime, convey.ShouldNotBeNil)
		detector.flushCloseCh <- true
		for len(detector.flushCloseCh) > 0 {
		}
	}
	convey.Convey("TestIODetector_flushDiskForDetectIO", t, testFunc)
}

func TestIODetector_detectIO_1(t *testing.T) {
	testFunc := func() {
		detector := newIODetector([]string{"mock_path1", "mock_path2"}, defaultSuicideTimeout, defaultFlushInterval)
		patch1 := gomonkey.ApplyFunc(syscall.Kill, func(pid int, signal syscall.Signal) error {
			return nil
		})
		defer func() {
			patch1.Reset()
		}()
		go detector.detectIO()
		//detector.detectCh <- time.Now()
		detector.detectCloseCh <- true
	}
	convey.Convey("TestIODetector_detectIO", t, testFunc)
}

func TestIODetector_detectIO_2(t *testing.T) {
	testFunc := func() {
		var killed bool
		detector := newIODetector([]string{"mock_path1", "mock_path2"}, defaultSuicideTimeout, defaultFlushInterval)
		patch1 := gomonkey.ApplyFunc(syscall.Kill, func(pid int, signal syscall.Signal) error {
			killed = true
			return nil
		})
		defer func() {
			patch1.Reset()
		}()
		go detector.detectIO()
		detector.detectCh <- time.Now().Add(-100 * time.Second)
		for !killed {

		}
		convey.So(killed, convey.ShouldEqual, true)
	}
	convey.Convey("TestIODetector_detectIO", t, testFunc)
}

func TestIODetector_newIODetector(t *testing.T) {
	testFunc := func() {
		detector := newIODetector([]string{"mock_path1", "mock_path2"}, defaultSuicideTimeout, defaultFlushInterval)
		convey.So(detector, convey.ShouldNotBeNil)
		if detector != nil {
			convey.So(detector.detectFilePath, convey.ShouldNotBeNil)
			convey.So(detector.detectCh, convey.ShouldNotBeNil)
			convey.So(detector.flushCloseCh, convey.ShouldNotBeNil)
			convey.So(detector.detectCloseCh, convey.ShouldNotBeNil)
			convey.So(detector.timeoutThreshold, convey.ShouldEqual, defaultSuicideTimeout)
			convey.So(detector.flushInterval, convey.ShouldEqual, defaultFlushInterval)
		}
	}
	convey.Convey("TestIODetector_newIODetector", t, testFunc)
}

func TestIODetector_processIODetection(t *testing.T) {
	testFunc := func() {
		detector := &IODetector{}
		f1, f2 := make(chan bool), make(chan bool)
		patch1 := gomonkey.ApplyPrivateMethod(detector, "flushDiskForDetectIO", func(_ *IODetector) { f1 <- true })
		patch2 := gomonkey.ApplyPrivateMethod(detector, "detectIO", func(_ *IODetector) { f2 <- true })
		defer func() {
			patch1.Reset()
			patch2.Reset()
		}()
		detector.processIODetection()
		f1Res := <-f1
		f2Res := <-f2
		convey.So(f1Res, convey.ShouldEqual, true)
		convey.So(f2Res, convey.ShouldEqual, true)
	}
	convey.Convey("TestIODetector_processIODetection", t, testFunc)
}

func TestIODetector_StartIODetection(t *testing.T) {
	testFunc := func() {
		detector := &IODetector{}
		patch1 := gomonkey.ApplyPrivateMethod(detector, "processIODetection", func(_ *IODetector) {})
		defer func() {
			patch1.Reset()
		}()
		config := NewIODetector()
		config.Paths = []string{defaultSystemDiskFilePath, defaultLogDiskFilePath}
		detectorForAssert := startIODetection(config)
		convey.So(detectorForAssert.detectFilePath, convey.ShouldResemble,
			[]string{defaultSystemDiskFilePath, defaultLogDiskFilePath})
		convey.So(detectorForAssert.detectCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.detectCloseCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.flushCloseCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.timeoutThreshold, convey.ShouldEqual, defaultSuicideTimeout)
		convey.So(detectorForAssert.flushInterval, convey.ShouldEqual, defaultFlushInterval)

		detectorForAssert = startIODetection(config)
		convey.So(detectorForAssert.detectFilePath, convey.ShouldResemble,
			[]string{defaultSystemDiskFilePath, defaultLogDiskFilePath})
		convey.So(detectorForAssert.detectCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.detectCloseCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.flushCloseCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.timeoutThreshold, convey.ShouldEqual, defaultSuicideTimeout)
		convey.So(detectorForAssert.flushInterval, convey.ShouldEqual, defaultFlushInterval)

		detectorForAssert = startIODetection(&Config{
			Paths:          []string{"mockpath1", "mockpath2"},
			SuicideTimeout: 60,
			FlushInterval:  5,
		})
		convey.So(detectorForAssert.detectFilePath, convey.ShouldResemble,
			[]string{"mockpath1", "mockpath2"})
		convey.So(detectorForAssert.detectCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.detectCloseCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.flushCloseCh, convey.ShouldNotBeNil)
		convey.So(detectorForAssert.timeoutThreshold, convey.ShouldEqual, 60)
		convey.So(detectorForAssert.flushInterval, convey.ShouldEqual, 5)
	}
	convey.Convey("TestIODetector_StartIODetection", t, testFunc)
}

func TestIODetector_flush_1(t *testing.T) {
	testFunc := func() {
		mockDetector := &IODetector{detectFilePath: []string{"filepath"}}
		patch1 := gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return nil, errors.New("test err")
		})
		defer func() {
			patch1.Reset()
		}()
		mockDetector.flush()
	}
	convey.Convey("TestIODetector_flush_1", t, testFunc)
}

func TestIODetector_flush_2(t *testing.T) {
	testFunc := func() {
		mockDetector := &IODetector{detectFilePath: []string{"filepath"}}
		mockFile := &os.File{}
		patch1 := gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return &os.File{}, nil
		})
		patch2 := gomonkey.ApplyMethod(mockFile, "WriteAt", func(_ *os.File, b []byte, off int64) (n int, err error) {
			return 0, errors.New("test err")
		})
		patch3 := gomonkey.ApplyMethod(mockFile, "Sync", func(_ *os.File) error {
			return errors.New("test err")
		})
		patch4 := gomonkey.ApplyMethod(mockFile, "Close", func(_ *os.File) error {
			return errors.New("test err")
		})
		defer func() {
			patch1.Reset()
			patch2.Reset()
			patch3.Reset()
			patch4.Reset()
		}()
		mockDetector.flush()
	}
	convey.Convey("TestIODetector_flush_2", t, testFunc)
}

func TestIODetector_ConfigValidate(t *testing.T) {
	config := NewIODetector()
	config.FlushInterval = -1
	config.SuicideTimeout = -1
	err := config.Validate()
	if err == nil {
		t.Fatal("valid config fail")
	}
	config.FlushInterval = 10
	err = config.Validate()
	if err == nil {
		t.Fatal("valid config fail")
	}
}

func TestIODetector_StopFlush(t *testing.T) {
	d := &IODetector{
		detectCh:      make(chan time.Time),
		detectCloseCh: make(chan bool),
		flushCloseCh:  make(chan bool),
		flushInterval: 1,
	}

	go d.flushDiskForDetectIO()
	time.Sleep(1 * time.Second)
	d.Close()
}

func TestIODetector_StopFlush2(t *testing.T) {
	d := &IODetector{
		detectCh:      make(chan time.Time),
		detectCloseCh: make(chan bool),
		flushCloseCh:  make(chan bool),
		flushInterval: 1,
	}

	go d.flushDiskForDetectIO()
	time.Sleep(10 * time.Millisecond)
	d.Close()
}
