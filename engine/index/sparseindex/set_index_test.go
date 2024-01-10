/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package sparseindex_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestSetIndexReader(t *testing.T) {
	schema := record.Schemas{{Name: "region", Type: influx.Field_Type_String}}
	option := &query.ProcessorOptions{}
	reader, err := sparseindex.NewSetIndexReader(schema, option, true)
	if err != nil {
		t.Fatal(err)
	}
	dataFile := "00000001-0001-00000001.tssp"
	assert.Equal(t, reader.ReInit(dataFile), nil)
	ok, err := reader.MayBeInFragment(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, ok, false)
	assert.Equal(t, reader.Close(), nil)
}
