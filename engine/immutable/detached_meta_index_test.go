/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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
package immutable

import (
	"math"
	"path"
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/interruptsignal"
	"github.com/openGemini/openGemini/lib/util"
)

func TestGetMetaIndexAndBlockId(t *testing.T) {
	testCompDir := t.TempDir()
	_ = fileops.RemoveAll(testCompDir)
	sig := interruptsignal.NewInterruptSignal()
	defer func() {
		sig.Close()
		_ = fileops.RemoveAll(testCompDir)
	}()
	mstName := "mst"
	err := writeData(testCompDir, mstName)
	if err != nil {
		t.Errorf(err.Error())
	}

	p := path.Join(testCompDir, mstName)
	_, _, err = GetMetaIndexAndBlockId("", nil, 1, util.TimeRange{Min: math.MinInt64, Max: math.MinInt64})
	if err == nil {
		t.Errorf(" get wrong meta")
	}

	_, _, err = GetMetaIndexAndBlockId(p, nil, 1, util.TimeRange{Min: math.MinInt64, Max: math.MinInt64})
	if err != nil {
		t.Errorf("get wrong meta")
	}

	_, _, err = GetPKItems("", nil, []int64{0})
	if err == nil {
		t.Errorf(" get wrong primary")
	}

	_, _, err = GetPKItems(p, nil, []int64{0})
	if err != nil {
		t.Errorf("get wrong primay")
	}
}
