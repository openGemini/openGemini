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

package handler

import (
	"testing"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/stretchr/testify/assert"
)

func TestParseTagKeyCondition(t *testing.T) {
	condition, tr, err := parseTagKeyCondition("val = 100 and ((a= 'abc' or b = 1.1 or c=*) and d=e or f=false or g=true)")
	if err != nil {
		t.Fatalf("%v \n", err)
	}

	exp := `val::tag = '100' AND ((a::tag = 'abc' OR b::tag = '1.1' OR c::tag =~ /.*/) AND d::tag = e::tag OR f::tag = 'false' OR g::tag = 'true')`

	if condition.String() != exp {
		t.Fatalf("parse tag key condition failed, \n exp: %s, \n got: %s \n", exp, condition.String())
	}

	if !tr.Min.IsZero() || !tr.Max.IsZero() {
		t.Fatalf("parse time failed")
	}

	// no condition
	_, _, err = parseTagKeyCondition("")
	assert.Nil(t, err)

	// incorrect param
	_, _, err = parseTagKeyCondition("tag == 'abc'")
	assert.NotNil(t, err)
}

func TestCreateDir(t *testing.T) {
	dataPath := t.TempDir()
	if err := fileops.MkdirAll(dataPath, 0750); err != nil {
		t.Fatalf("mkdir(%v) failed, err: %v", dataPath, err)
	}

	err := createDir(dataPath+"/none", "db0", 1, "rp0")
	assert.NotNil(t, err)

	if err = createDir(dataPath, "db0", 1, "rp0"); err != nil {
		t.Fatalf("createDir failed, err, err: %v", err)
	}
}
