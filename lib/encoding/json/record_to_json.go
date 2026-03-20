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

package json

import (
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var bufferPool = pool.NewDefaultUnionPool[[]byte](func() *[]byte {
	buf := make([]byte, 0, 32*config.KB)
	return &buf
})

// Record2ResultsJSON Encode the Record directly into a JSON string format of httpd.Response
type Record2ResultsJSON struct {
	sw          *StreamWriter
	loc         *time.Location
	rfc3339     bool
	divisor     int64
	statementID int64
}

func NewRecord2ResultsJSON(loc *time.Location, epoch string) *Record2ResultsJSON {
	epoch = strings.ToLower(epoch)
	return &Record2ResultsJSON{
		loc:     loc,
		rfc3339: epoch == "rfc3339",
		divisor: util.EpochDivisor(epoch),
		sw:      NewStreamWriter(*bufferPool.Get()),
	}
}

func (rj *Record2ResultsJSON) Release() {
	buf := rj.sw.Bytes()
	bufferPool.PutWithMemSize(&buf, len(buf))
}

func (rj *Record2ResultsJSON) Bytes() []byte {
	return rj.sw.Bytes()
}

func (rj *Record2ResultsJSON) Reset() {
	rj.statementID = 0
	rj.sw.Reset()
}

func (rj *Record2ResultsJSON) Start() {
	w := rj.sw
	w.WriteObjectStart()
	w.WriteArrayStart("results")
}

func (rj *Record2ResultsJSON) End() {
	w := rj.sw
	w.WriteArrayEnd()
	w.WriteObjectEnd()
}

func (rj *Record2ResultsJSON) StartStatement() {
	w := rj.sw
	w.WriteObjectStart()
	w.WriteIntKV("statement_id", rj.statementID)
	w.WriteArrayStart("series")

	rj.statementID++
}

func (rj *Record2ResultsJSON) EndStatement() {
	w := rj.sw
	w.WriteArrayEnd()
	w.WriteObjectEnd()
}

func (rj *Record2ResultsJSON) WriteRecords(name string, recs []*record.Record) {
	rj.Start()
	rj.StartStatement()
	for _, rec := range recs {
		rj.WriteRecord(name, rec)
		rj.sw.WriteComma()
	}
	rj.sw.TrimLeftOne()
	rj.EndStatement()
	rj.End()
}

func (rj *Record2ResultsJSON) WriteRecord(name string, rec *record.Record) {
	times := rec.Times()
	w := rj.sw
	w.WriteObjectStart()
	w.WriteStringKV("name", name)
	rj.writeColumnName(rec.Schema)
	w.WriteArrayStart("values")
	rj.writeColumns(times, rec.ColVals, rec.Schema)
	w.WriteArrayEnd()
	w.WriteObjectEnd()
}

func (rj *Record2ResultsJSON) writeColumnName(schema record.Schemas) {
	w := rj.sw
	w.WriteArrayStart("columns")
	w.WriteValueString("time")
	w.WriteComma()

	for i := range schema.Len() - 1 {
		w.WriteValueStringEscape(schema[i].Name)
		w.WriteComma()
	}

	w.TrimLeftOne()
	w.WriteArrayEnd()
	w.WriteComma()
}

func (rj *Record2ResultsJSON) writeColumns(times []int64, colVals []record.ColVal, schema record.Schemas) {
	w := rj.sw
	for i := range len(times) {
		w.WriteArrayStart("")
		rj.writeTime(times[i])
		w.WriteComma()
		for j := 0; j < len(colVals)-1; j++ {
			rj.writeColValue(colVals[j], i, schema[j])
			w.WriteComma()
		}
		w.TrimLeftOne()
		w.WriteArrayEnd()
		w.WriteComma()
	}
	w.TrimLeftOne()
}

func (rj *Record2ResultsJSON) writeTime(ts int64) {
	if rj.rfc3339 {
		rj.sw.WriteTimeRFC3339(time.Unix(0, ts).In(rj.loc))
	} else {
		rj.sw.WriteValueInt64(ts / rj.divisor)
	}
}

func (rj *Record2ResultsJSON) writeColValue(col record.ColVal, idx int, ref record.Field) {
	w := rj.sw
	if col.NilCount > 0 {
		if col.IsNil(idx) {
			w.WriteNil()
			return
		}
	}
	switch ref.Type {
	case influx.Field_Type_Float:
		v, _ := col.FloatValue(idx)
		w.WriteValueFloat64(v)
	case influx.Field_Type_Int:
		v, _ := col.IntegerValue(idx)
		w.WriteValueInt64(v)
	case influx.Field_Type_Boolean:
		v, _ := col.BooleanValue(idx)
		w.WriteValueBool(v)
	case influx.Field_Type_String, influx.Field_Type_Tag:
		v, _ := col.StringValue(idx)
		w.WriteValueStringEscape(util.Bytes2str(v))
	}
}
