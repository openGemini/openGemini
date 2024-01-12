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

package immutable

import (
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

func TestNewTSSPFileAttachedReader(t *testing.T) {
	reader := &TSSPFileAttachedReader{reader: NewLocationCursor(1)}
	assert.Equal(t, reader.Name(), "TSSPFileAttachedReader")
	reader.StartSpan(nil)
	reader.EndSpan()
	reader.SinkPlan(nil)
	reader.SetOps(nil)
	err := reader.Close()
	assert.Equal(t, err, nil)
	_, _, err = reader.NextAggData()
	assert.Equal(t, err, nil)
	decs := NewFileReaderContext(util.TimeRange{Min: 0, Max: 1635732519000000000}, schema, NewReadContext(true), NewFilterOpts(nil, nil, nil, nil), nil, false)
	reader.ctx = decs
	reader.schema = &query.ProcessorOptions{}
	err = reader.initReader(nil, nil)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(reader.GetSchema()), len(schema))
	err = reader.ResetBy(nil, nil)
	assert.Equal(t, err, nil)
	reader.recordPool = record.NewCircularRecordPool(record.NewRecordPool(record.ColumnReaderPool), BatchReaderRecordNum, schema, false)
	err = reader.Close()
	assert.Equal(t, err, nil)
}
