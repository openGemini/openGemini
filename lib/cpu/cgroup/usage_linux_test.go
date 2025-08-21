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

//go:build linux
// +build linux

package cgroup

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

func TestIsCgroup2(t *testing.T) {
	convey.Convey("is cgroup v2", t, func() {
		patches := gomonkey.ApplyFunc(unix.Statfs, func(path string, buf *unix.Statfs_t) (err error) {
			buf.Type = unix.CGROUP2_SUPER_MAGIC
			return nil
		})
		defer patches.Reset()
		isCgroup2 := IsCgroup2()
		assert.EqualValues(t, true, isCgroup2)
	})

	convey.Convey("get statfs failed", t, func() {
		patches := gomonkey.ApplyFunc(unix.Statfs, func(path string, buf *unix.Statfs_t) (err error) {
			return errors.New("patch error")
		})
		defer patches.Reset()
		isCgroup2 := IsCgroup2()
		assert.EqualValues(t, false, isCgroup2)
	})
}

func TestGetCPULimit(t *testing.T) {
	// set data
	patchV2 := gomonkey.ApplyFunc(getCPULimitV2, func() (float64, error) {
		return 2, nil
	})
	defer patchV2.Reset()
	patchV1 := gomonkey.ApplyFunc(getCPULimitV1, func() (float64, error) {
		return 1, nil
	})
	defer patchV1.Reset()
	convey.Convey("cgroup v2", t, func() {
		patchIs := gomonkey.ApplyFunc(IsCgroup2, func() bool {
			return true
		})
		defer patchIs.Reset()

		cpuLimit, err := GetCPULimit()
		assert.NoError(t, err)
		assert.EqualValues(t, 2, int(cpuLimit))
	})

	convey.Convey("cgroup v1", t, func() {
		patchNo := gomonkey.ApplyFunc(IsCgroup2, func() bool {
			return false
		})
		defer patchNo.Reset()

		cpuLimit, err := GetCPULimit()
		assert.NoError(t, err)
		assert.EqualValues(t, 1, int(cpuLimit))
	})
}

func TestGetUsage(t *testing.T) {
	// set data
	patchV2 := gomonkey.ApplyFunc(getUsageV2, func() (float64, error) {
		return 2, nil
	})
	defer patchV2.Reset()
	patchV1 := gomonkey.ApplyFunc(getUsageV1, func() (float64, error) {
		return 1, nil
	})
	defer patchV1.Reset()
	convey.Convey("cgroup v2", t, func() {
		patchIs := gomonkey.ApplyFunc(IsCgroup2, func() bool {
			return true
		})
		defer patchIs.Reset()

		usage, err := GetUsage(time.Second)
		assert.NoError(t, err)
		assert.EqualValues(t, 2, usage)
	})

	convey.Convey("cgroup v1", t, func() {
		patchNo := gomonkey.ApplyFunc(IsCgroup2, func() bool {
			return false
		})
		defer patchNo.Reset()

		usage, err := GetUsage(time.Second)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, usage)
	})
}

func Test_getCPULimitV1(t *testing.T) {
	convey.Convey("read quota cgroup failed", t, func() {
		patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
			return "", errors.New("patch error")
		})
		defer patches.Reset()
		limitV1, err := getCPULimitV1()
		assert.ErrorContains(t, err, "patch error")
		assert.EqualValues(t, 0, int(limitV1))
	})

	convey.Convey("parse quota failed", t, func() {
		// set mock data
		patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
			if name == cpuQuotaUs {
				return "20000", nil
			}
			return "", nil
		})
		defer patches.Reset()
		patchQuota := gomonkey.ApplyFunc(strconv.ParseInt, func(s string, base int, bitSize int) (i int64, err error) {
			return 0, errors.New("parse quota failed")
		})
		defer patchQuota.Reset()
		limitV1, err := getCPULimitV1()
		assert.ErrorContains(t, err, "parse quota failed")
		assert.EqualValues(t, 0, int(limitV1))
	})

	convey.Convey("read period failed", t, func() {
		patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
			if name == cpuQuotaUs {
				return "20000", nil
			}
			return "", errors.New("read period failed")
		})
		defer patches.Reset()
		limitV1, err := getCPULimitV1()
		assert.ErrorContains(t, err, "read period failed")
		assert.EqualValues(t, 0, int(limitV1))
	})

	patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
		if name == cpuQuotaUs {
			return "20000", nil
		}
		if name == cpuPeriodUs {
			return "10000", nil
		}
		return "", nil
	})
	defer patches.Reset()

	convey.Convey("parse period failed", t, func() {
		patchPeriod := gomonkey.ApplyFunc(strconv.ParseInt, func(s string, base int, bitSize int) (i int64, err error) {
			if s == "10000" {
				return 0, errors.New("parse period failed")
			}
			return 0, nil
		})
		defer patchPeriod.Reset()
		limitV1, err := getCPULimitV1()
		assert.ErrorContains(t, err, "parse period failed")
		assert.EqualValues(t, 0, int(limitV1))
	})

	limitV1, err := getCPULimitV1()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, int(limitV1))
}

func Test_getCPULimitV2(t *testing.T) {
	convey.Convey("read file failed", t, func() {
		patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
			return "", errors.New("read file failed")
		})
		defer patches.Reset()
		limitV2, err := getCPULimitV2()
		assert.ErrorContains(t, err, "read file failed")
		assert.EqualValues(t, 0, int(limitV2))
	})

	// set mock data
	patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
		return "20000 10000", nil
	})
	defer patches.Reset()

	convey.Convey("split failed", t, func() {
		patch3 := gomonkey.ApplyFunc(strings.Split, func(s, sep string) []string {
			return []string{"", "", ""}
		})
		defer patch3.Reset()

		limitV2, err := getCPULimitV2()
		assert.ErrorContains(t, err, "unexpected cpu.max format")
		assert.EqualValues(t, 0, int(limitV2))
	})

	convey.Convey("no cpu limit", t, func() {
		patch3 := gomonkey.ApplyFunc(strings.Split, func(s, sep string) []string {
			return []string{"max", ""}
		})
		defer patch3.Reset()

		limitV2, err := getCPULimitV2()
		assert.NoError(t, err)
		assert.EqualValues(t, -1, int(limitV2))
	})

	convey.Convey("parse quota failed", t, func() {
		applyFunc := gomonkey.ApplyFunc(strconv.ParseInt, func(s string, base int, bitSize int) (i int64, err error) {
			return 0, errors.New("parse quota failed")
		})
		defer applyFunc.Reset()
		limitV2, err := getCPULimitV2()
		assert.ErrorContains(t, err, "cannot parse cpu.max quota")
		assert.EqualValues(t, 0, int(limitV2))
	})

	convey.Convey("parse period failed", t, func() {
		applyFunc := gomonkey.ApplyFunc(strconv.ParseInt, func(s string, base int, bitSize int) (i int64, err error) {
			if s == "10000" {
				return 0, errors.New("parse period failed")
			}
			return 0, nil
		})
		defer applyFunc.Reset()
		limitV2, err := getCPULimitV2()
		assert.ErrorContains(t, err, "cannot parse cpu.max period")
		assert.EqualValues(t, 0, int(limitV2))
	})

	limitV2, err := getCPULimitV2()
	assert.NoError(t, err)
	assert.EqualValues(t, 2, int(limitV2))
}

func Test_getUsageV1(t *testing.T) {
	// set mock data
	patches := gomonkey.ApplyFunc(parseUsageV1, func() (float64, error) {
		return 10, nil
	})
	defer patches.Reset()
	usageV1, err := getUsageV1(time.Millisecond)
	assert.NoError(t, err)
	assert.NotEqualValues(t, 10, int(usageV1))
}

func Test_getUsageV2(t *testing.T) {
	// set mock data
	patches := gomonkey.ApplyFunc(parseUsageV2, func() (float64, error) {
		return 10, nil
	})
	defer patches.Reset()
	usageV2, err := getUsageV2(time.Millisecond)
	assert.NoError(t, err)
	assert.NotEqualValues(t, 10, int(usageV2))
}

func Test_parseUsageV1(t *testing.T) {
	convey.Convey("read file failed", t, func() {
		patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
			return "", errors.New("read file failed")
		})
		defer patches.Reset()
		_, err := parseUsageV1()
		assert.ErrorContains(t, err, "read file failed")
	})
	patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
		return "10000", nil
	})
	defer patches.Reset()
	convey.Convey("parse failed", t, func() {
		patchParse := gomonkey.ApplyFunc(strconv.ParseInt, func(s string, base int, bitSize int) (i int64, err error) {
			return 0, errors.New("parse failed")
		})
		defer patchParse.Reset()
		_, err := parseUsageV1()
		assert.ErrorContains(t, err, "parse failed")
	})
	v1, err := parseUsageV1()
	assert.NoError(t, err)
	assert.EqualValues(t, 10000, int(v1))
}

func Test_parseUsageV2(t *testing.T) {
	convey.Convey("read file failed", t, func() {
		patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
			return "", errors.New("read file failed")
		})
		defer patches.Reset()
		_, err := parseUsageV2()
		assert.ErrorContains(t, err, "read file failed")
	})
	// set mock data
	patches := gomonkey.ApplyFunc(readFile, func(subsystem string, name string) (string, error) {
		return "usage_usec 10000", nil
	})
	defer patches.Reset()
	convey.Convey("split line failed", t, func() {
		patchSplit := gomonkey.ApplyFunc(strings.Split, func(s, sep string) []string {
			return nil
		})
		defer patchSplit.Reset()
		_, err := parseUsageV2()
		assert.ErrorContains(t, err, "read cpu.stat failed")
	})
	convey.Convey("split space failed", t, func() {
		patchSplit := gomonkey.ApplyFunc(strings.Split, func(s, sep string) []string {
			if sep == " " {
				return nil
			}
			return []string{""}
		})
		defer patchSplit.Reset()
		_, err := parseUsageV2()
		assert.ErrorContains(t, err, "unexpected cpu.stat format")
	})
	convey.Convey("parse failed", t, func() {
		patchParse := gomonkey.ApplyFunc(strconv.ParseInt, func(s string, base int, bitSize int) (i int64, err error) {
			return 0, errors.New("parse failed")
		})
		defer patchParse.Reset()
		_, err := parseUsageV2()
		assert.ErrorContains(t, err, "parse failed")
	})
	v1, err := parseUsageV2()
	assert.NoError(t, err)
	assert.EqualValues(t, 10000, int(v1))
}

func Test_readFile(t *testing.T) {
	convey.Convey("read file failed", t, func() {
		patches := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
			return nil, errors.New("read file failed")
		})
		defer patches.Reset()
		_, err := readFile("a", "b")
		assert.ErrorContains(t, err, "read file failed")
	})
	convey.Convey("read file ok", t, func() {
		patches := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
			return []byte("hello"), nil
		})
		defer patches.Reset()
		body, err := readFile("a", "b")
		assert.NoError(t, err)
		assert.EqualValues(t, "hello", body)
	})
}
