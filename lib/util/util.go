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

package util

import (
	"io"
	"reflect"
	"time"

	"github.com/influxdata/influxdb/toml"
	"go.uber.org/zap"
)

const (
	TierBegin = 0
	Hot       = 1
	Warm      = 2
	Cold      = 3
	TierEnd   = 4
)

var logger *zap.Logger

func SetLogger(lg *zap.Logger) {
	logger = lg
}

func MustClose(obj io.Closer) {
	if obj == nil || IsObjectNil(obj) {
		return
	}

	err := obj.Close()
	if err != nil && logger != nil {
		logger.WithOptions(zap.AddCallerSkip(1)).
			Error("failed to close", zap.Error(err))
	}
}

func IsObjectNil(obj interface{}) bool {
	val := reflect.ValueOf(obj)
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map,
		reflect.UnsafePointer, reflect.Ptr,
		reflect.Interface, reflect.Slice:

		return val.IsNil()
	}

	return false
}

type Corrector struct {
	intMin   int64
	floatMin float64
}

func NewCorrector(intMin int64, floatMin float64) *Corrector {
	return &Corrector{intMin: intMin, floatMin: floatMin}
}

func (c *Corrector) Int(v *int, def int) {
	if int64(*v) <= c.intMin {
		*v = def
	}
}

func (c *Corrector) Uint64(v *uint64, def uint64) {
	if *v <= uint64(c.intMin) {
		*v = def
	}
}

func (c *Corrector) Float64(v *float64, def float64) {
	if *v <= c.floatMin {
		*v = def
	}
}

func (c *Corrector) String(v *string, def string) {
	if *v == "" {
		*v = def
	}
}

func (c *Corrector) TomlDuration(v *toml.Duration, def toml.Duration) {
	if int64(*v) <= c.intMin {
		*v = def
	}
}

func (c *Corrector) TomlSize(v *toml.Size, def toml.Size) {
	if int64(*v) <= c.intMin {
		*v = def
	}
}

func TimeCost(option string) func() {
	start := time.Now()
	return func() {
		tc := time.Since(start)
		logger.Debug(option, zap.Duration("time cost", tc))
	}
}

func CeilToPower2(num uint32) uint32 {
	if num > (1 << 31) {
		return 1 << 31
	}
	num--
	num |= num >> 1
	num |= num >> 2
	num |= num >> 4
	num |= num >> 8
	num |= num >> 16
	return num + 1
}

func IntLimit(min, max int, v int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
