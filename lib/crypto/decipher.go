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

package crypto

import (
	"os"
	"sync"

	"go.uber.org/zap"
)

var defaultDecipher Decipher
var logger *zap.Logger
var cacheData = make(map[string]string)
var cacheMu sync.RWMutex

func SetLogger(lg *zap.Logger) {
	logger = lg
}

func SetDecipher(d Decipher) {
	defaultDecipher = d
}

var initOnce = sync.Once{}
var destructOnce = sync.Once{}

func Initialize(conf string) {
	if defaultDecipher == nil {
		return
	}

	initOnce.Do(func() {
		defaultDecipher.Initialize(conf)
	})
}

func Destruct() {
	if defaultDecipher == nil {
		return
	}

	destructOnce.Do(func() {
		defaultDecipher.Destruct()
	})
}

type Decipher interface {
	Initialize(string)
	Decrypt(string) (string, error)
	Destruct()
}

func DecryptFromFile(path string) string {
	if path == "" {
		return ""
	}

	cacheMu.RLock()
	s, ok := cacheData[path]
	cacheMu.RUnlock()

	if !ok {
		buf, err := os.ReadFile(path)
		if err != nil {
			logger.Error("failed to read file", zap.Error(err), zap.String("file", path))
			return ""
		}
		s = string(buf)
		cacheMu.Lock()
		cacheData[path] = s
		cacheMu.Unlock()
	}

	return Decrypt(s)
}

func Decrypt(s string) string {
	if s == "" || defaultDecipher == nil {
		return s
	}

	out, err := defaultDecipher.Decrypt(s)
	if err != nil {
		out = ""
		logger.Error("decrypt failed", zap.Error(err), zap.String("input", s))
	}
	return out
}
