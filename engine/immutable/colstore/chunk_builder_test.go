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

package colstore

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func genData(rows int) *record.Record {
	tm := time.Now().Truncate(time.Minute).UnixNano()

	schema := []record.Field{
		{Name: "field1_int64", Type: influx.Field_Type_Int},
		{Name: "field2_float", Type: influx.Field_Type_Float},
		{Name: "field3_string", Type: influx.Field_Type_String},
		{Name: "field4_bool", Type: influx.Field_Type_Boolean},
		{Name: "time", Type: influx.Field_Type_Int},
	}

	genRecFn := func() *record.Record {
		b := record.NewRecordBuilder(schema)

		f1Builder := b.Column(0) // int64
		f2Builder := b.Column(1) // float
		f3Builder := b.Column(2) // string
		f4Builder := b.Column(3) // bool
		tmBuilder := b.Column(4) // timestamp
		for i := 1; i <= rows; i++ {
			f1 := rand.Int63n(11)
			f2 := 1.1 * float64(f1)
			if i%7 == 0 {
				f1Builder.AppendInteger(f1 / 11)
			} else {
				f1Builder.AppendInteger(f1)
			}

			f2Builder.AppendFloat(f2)

			if i%11 == 0 {
				f3 := fmt.Sprintf("test_%d", f1/7)
				f3Builder.AppendString(f3)
			} else {
				f3 := fmt.Sprintf("test_%d", f1)
				f3Builder.AppendString(f3)
			}

			if i%3 == 0 || i%5 == 0 {
				f4Builder.AppendBoolean(false)
			} else {
				f4Builder.AppendBoolean(true)
			}

			tmBuilder.AppendInteger(tm)
			tm += time.Millisecond.Milliseconds()
		}

		return b
	}
	data := genRecFn()

	return data
}

func TestMeta(t *testing.T) {
	data := genData(100)
	meta := marshalMeta(&data.Schema)
	haha, _ := unmarshalMeta(meta)
	require.EqualValues(t, data.Schemas(), haha)
}

func TestChunkBuilder(t *testing.T) {
	testCompDir := t.TempDir()
	filePath := filepath.Join(testCompDir, "chunkBuilder.test")
	_ = fileops.MkdirAll(testCompDir, 0755)
	defer func() {
		_ = fileops.Remove(filePath)
	}()
	data := genData(100)
	lockPath := ""
	indexBuilder := NewIndexBuilder(&lockPath, filePath)
	defer indexBuilder.Reset()
	err := indexBuilder.WriteData(data)
	haha, _ := unmarshalMeta(indexBuilder.meta)
	require.EqualValues(t, data.Schemas(), haha)
	if err != nil {
		t.Fatal("write data error")
	}
}
