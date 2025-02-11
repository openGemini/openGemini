// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

//go:build linux || darwin || freebsd
// +build linux darwin freebsd

package sherlock

import (
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestSherlock_isContainer(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithMemRule(25, 10, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))

	convey.Convey("docker env file exist", t, func() {
		patches := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
			return nil, nil
		})
		defer patches.Reset()
		isContainer := sh.isContainer()
		assert.Equal(t, true, isContainer)
	})
	convey.Convey("docker env file not exist", t, func() {
		patches := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
			return nil, errors.New("file not exist")
		})
		defer patches.Reset()
		convey.Convey("read cgroup file failed", func() {
			patchesB := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
				return nil, errors.New("read file failed")
			})
			defer patchesB.Reset()
			isContainer := sh.isContainer()
			assert.Equal(t, false, isContainer)
		})
		convey.Convey("read cgroup ok", func() {
			patchesB := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
				return []byte("kubepods"), nil
			})
			defer patchesB.Reset()
			isContainer := sh.isContainer()
			assert.Equal(t, true, isContainer)
		})
	})
}

func TestSherlock_getContainerMemoryLimit(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithMemRule(25, 10, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))

	convey.Convey("read file failed", t, func() {
		patchesB := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
			return nil, errors.New("read file failed")
		})
		defer patchesB.Reset()
		limit, err := sh.getContainerMemoryLimit()
		assert.ErrorContains(t, err, "read file failed")
		assert.Equal(t, uint64(0), limit)
	})
	patchesB := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
		return []byte("123456"), nil
	})
	defer patchesB.Reset()
	convey.Convey("parse uint failed", t, func() {
		patchesC := gomonkey.ApplyFunc(strconv.ParseUint, func(s string, base int, bitSize int) (uint64, error) {
			return 0, errors.New("parse failed")
		})
		defer patchesC.Reset()
		limit, err := sh.getContainerMemoryLimit()
		assert.ErrorContains(t, err, "parse failed")
		assert.Equal(t, uint64(0), limit)
	})

	convey.Convey("ok", t, func() {
		limit, err := sh.getContainerMemoryLimit()
		assert.NoError(t, err)
		assert.Equal(t, uint64(123456), limit)
	})
}

func TestSherlock_getMemoryLimit(t *testing.T) {
	tmpDir := t.TempDir()
	sh := New(
		WithMonitorInterval(time.Millisecond),
		WithSavePath(tmpDir),
		WithMemRule(25, 10, 0, time.Minute),
	)
	sh.Set(WithLogger(logger.NewLogger(errno.ModuleUnknown)))

	convey.Convey("is container", t, func() {
		patchesA := gomonkey.ApplyPrivateMethod(reflect.TypeOf(sh), "isContainer", func() bool {
			return true
		})
		defer patchesA.Reset()
		patchesB := gomonkey.ApplyPrivateMethod(reflect.TypeOf(sh), "getContainerMemoryLimit", func() (uint64, error) {
			return 0, nil
		})
		defer patchesB.Reset()
		limit, err := sh.getMemoryLimit()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), limit)
	})

	convey.Convey("is vm", t, func() {
		patchesA := gomonkey.ApplyPrivateMethod(reflect.TypeOf(sh), "isContainer", func() bool {
			return false
		})
		defer patchesA.Reset()
		patchesB := gomonkey.ApplyPrivateMethod(reflect.TypeOf(sh), "getVMMemoryLimit", func() (uint64, error) {
			return 0, nil
		})
		defer patchesB.Reset()
		limit, err := sh.getMemoryLimit()
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), limit)
	})
}
