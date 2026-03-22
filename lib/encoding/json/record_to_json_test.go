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

package json_test

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/lib/encoding"
	libJson "github.com/openGemini/openGemini/lib/encoding/json"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestMarshalFloat64(t *testing.T) {
	values := []float64{1.1, math.Inf(1), math.Inf(-1), math.NaN(), 0}

	sw := libJson.NewStreamWriter(nil)
	sw.WriteArrayStart("")
	for _, value := range values {
		sw.WriteValueFloat64(value)
		sw.WriteComma()
	}
	sw.TrimLeftOne()
	sw.WriteArrayEnd()
	got := sw.Bytes()

	require.Equal(t, `[1.1,"+Inf","-Inf","NaN",0]`, string(got))
}

func TestWriteJson(t *testing.T) {
	rec := buildRecord(10)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	rj := libJson.NewRecord2ResultsJSON(loc, "rfc3339")
	rj.Reset()
	defer rj.Release()

	rj.WriteRecords("cpu", []*record.Record{rec})
	ret := rj.Bytes()

	rows, err := RecToRows(rec, loc, "cpu", rec.Schema)
	require.NoError(t, err)

	resp := httpd.Response{
		Results: []*query.Result{{
			StatementID: 0,
			Series:      rows,
		}},
	}

	exp, err := json.Marshal(resp)
	require.NoError(t, err)

	require.Equal(t, string(exp), string(ret))
}

func BenchmarkAppendInt64(b *testing.B) {
	now := time.Now().UnixNano()
	b.Run("normal", func(b *testing.B) {
		buf := make([]byte, 8)
		for i := 0; i < b.N; i++ {
			buf = strconv.AppendInt(buf[:0], 109991, 10)
			buf = strconv.AppendInt(buf, now, 10)
			buf = strconv.AppendInt(buf, 19, 10)
		}
	})
	b.Run("sonic-avx2", func(b *testing.B) {
		sw := libJson.NewStreamWriter(nil)
		for i := 0; i < b.N; i++ {
			sw.WriteValueInt64(109991)
			sw.WriteValueInt64(now)
			sw.WriteValueInt64(19)
			sw.Reset()
		}
	})
}

func BenchmarkAppendFloat64(b *testing.B) {
	b.Run("normal", func(b *testing.B) {
		buf := make([]byte, 8)
		for i := 0; i < b.N; i++ {
			buf = strconv.AppendFloat(buf[:0], 15.22, 'f', -1, 64)
			buf = strconv.AppendFloat(buf, 0, 'f', -1, 64)
			buf = strconv.AppendFloat(buf, 365842.15668, 'f', -1, 64)
		}
	})
	b.Run("sonic-avx2", func(b *testing.B) {
		sw := libJson.NewStreamWriter(nil)
		for i := 0; i < b.N; i++ {
			sw.WriteValueFloat64(15.22)
			sw.WriteValueFloat64(0)
			sw.WriteValueFloat64(365842.15668)
			sw.Reset()
		}
	})
}

func BenchmarkJson(b *testing.B) {
	rec := buildRecord(1000)
	loc, _ := time.LoadLocation("Asia/Shanghai")

	b.Run("stream", func(b *testing.B) {
		rj := libJson.NewRecord2ResultsJSON(loc, "rfc3339")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rj.WriteRecord("cpu", rec)
			rj.Reset()
		}
	})

	b.Run("sonic", func(b *testing.B) {
		b.ReportAllocs()
		rows, _ := RecToRows(rec, loc, "cpu", rec.Schema)
		for i := 0; i < b.N; i++ {
			_, _ = encoding.JSONConfig.Marshal(rows)
		}
	})

	b.Run("default", func(b *testing.B) {
		b.ReportAllocs()
		rows, _ := RecToRows(rec, loc, "cpu", rec.Schema)
		for i := 0; i < b.N; i++ {
			_, _ = json.Marshal(rows)
		}
	})

}

func buildRecord(rows int) *record.Record {
	rec := record.Record{}
	schema := record.Schemas{
		record.Field{
			Type: influx.Field_Type_Float,
			Name: "usage",
		},
		record.Field{
			Type: influx.Field_Type_String,
			Name: "util",
		},
		record.Field{
			Type: influx.Field_Type_Boolean,
			Name: "switch",
		},
		record.Field{
			Type: influx.Field_Type_Int,
			Name: "value",
		},
		record.Field{
			Type: influx.Field_Type_Int,
			Name: "time",
		},
	}
	rec.ResetWithSchema(schema)
	strValues := []string{"a", "中文字符", "-\n-\r-\t-\\-\"-'-&-#-<->", "1234", "", string([]byte{17, 18})}

	for i := range rows {
		rec.ColVals[0].AppendFloat(float64(int64(i*777)%20877) / 10)
		rec.ColVals[1].AppendString(strValues[i%len(strValues)])
		rec.ColVals[2].AppendBoolean(i%3 == 0)
		if i%5 > 0 {
			rec.ColVals[3].AppendInteger(int64(i))
		} else {
			rec.ColVals[3].AppendIntegerNull()
		}
		rec.ColVals[4].AppendInteger(1e10 + int64(i))
	}

	return &rec
}

func RecToRows(rec *record.Record, loc *time.Location, name string, schema record.Schemas) (models.Rows, error) {
	rowsGenerator := executor.NewRowsGenerator()
	defer rowsGenerator.Release()
	rows := rowsGenerator.GenerateFromRecord(rec, loc, name, schema)
	return rows, nil
}
