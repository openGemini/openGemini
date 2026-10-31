// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package failpoint

import (
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
)

const (
	WriteCurrentMetaError = "write-current-meta-error"
	SetQueryId            = "set-query-id"
	ColumnWriteError      = "column-writer-error"
)

// Enable fail point
// The parameter name consists of the package name and the fault point name, for example:
// github.com/openGemini/openGemini/engine/write-current-meta-error
func Enable(name string, val any) {
	switch v := val.(type) {
	case string:
		val = FetchValue(v)
	default:
		break
	}

	points.Enable(name, val)
}

func Disable(name string) {
	points.Disable(name)
}

func GetValue(name string, fn func()) *Value {
	name = RewriteFailPointName(name, fn)
	p, ok := points.GetPoint(name)
	if !ok {
		return nil
	}

	return &Value{val: p.Value()}
}

func RewriteFailPointName(name string, fn interface{}) string {
	value := reflect.ValueOf(fn)
	ptr := value.Pointer()
	ffp := runtime.FuncForPC(ptr)

	ffpName := ffp.Name()
	dir := filepath.Dir(ffpName)
	base := filepath.Base(ffpName)
	idx := strings.Index(base, ".")
	name = filepath.Join(dir, base[:idx], name)

	return strings.ReplaceAll(name, "\\", "/")
}

var valRegexp = regexp.MustCompile(`(?i)(?:term=)?(?:sleep|return)\("?(.*?)"?\)`)

func FetchValue(s string) string {
	arr := valRegexp.FindStringSubmatch(s)
	if len(arr) < 2 {
		return s
	}

	return arr[1]
}
