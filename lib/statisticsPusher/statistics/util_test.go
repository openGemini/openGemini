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

package statistics_test

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

func TestAddPointToBuffer(t *testing.T) {
	mst := "mst"
	tags := map[string]string{"tid": "t01", "cid": "c01"}
	fields := map[string]interface{}{
		"age":  int64(1),
		"name": "sandy",
	}
	statistics.NewTimestamp().Init(time.Second)
	buf := bytes.TrimSpace(statistics.AddPointToBuffer(mst, tags, fields, nil))
	err := compareBuffer(mst, tags, fields, buf)
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func BenchmarkAddPointToBuffer(b *testing.B) {
	mst := "mst"
	tags := map[string]string{"tid": "t01", "cid": "c01"}
	fields := map[string]interface{}{
		"age":  int64(1),
		"name": "sandy abc",
	}
	statistics.NewTimestamp().Init(time.Second)

	var buf []byte
	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		buf = bytes.TrimSpace(statistics.AddPointToBuffer(mst, tags, fields, buf))
	}
}

var compareRowIndex = 0

func compareBuffer(mst string, tags map[string]string, fields map[string]interface{}, buf []byte) error {
	var err error
	w := influx.GetUnmarshalWork()
	w.Callback = func(db string, rows []influx.Row, e error) {
		if e != nil {
			err = e
			return
		}

		if len(rows) < (compareRowIndex + 1) {
			err = fmt.Errorf("error length, exp: %d; got: %d", compareRowIndex+1, len(rows))
			return
		}

		err = compareRow(mst, tags, fields, rows[compareRowIndex])
	}
	w.Db = ""
	w.ReqBuf = buf
	w.Unmarshal()

	return err
}

func compareRow(mst string, tags map[string]string, fields map[string]interface{}, row influx.Row) error {
	if row.Name != mst {
		return fmt.Errorf("error measurement, exp: %s; got: %s", mst, row.Name)
	}

	if fmt.Sprintf("%d", row.Timestamp) != string(statistics.NewTimestamp().Bytes()) {
		return fmt.Errorf("error timestamp, exp: %s; got: %d", string(statistics.NewTimestamp().Bytes()), row.Timestamp)
	}

	gotTags := map[string]string{}
	for _, item := range row.Tags {
		gotTags[item.Key] = item.Value
	}
	if !reflect.DeepEqual(gotTags, tags) {
		return fmt.Errorf("error tags, \nexp: %+v;\ngot: %+v", tags, gotTags)
	}

	var expFields = convertFields(fields)
	var gotFields = row.Fields
	if len(expFields) != len(gotFields) {
		return fmt.Errorf("error fields length, exp: %d; got: %d", len(expFields), len(gotFields))
	}

	sort.Sort(expFields)
	sort.Sort(row.Fields)

	for i, item := range gotFields {
		if item.StrValue != "" {
			if item.StrValue != expFields[i].StrValue {
				return fmt.Errorf("error fields value(string), index: %d, key: %s, exp: %s; got: %s",
					i, expFields[i].Key, expFields[i].StrValue, item.StrValue)
			}
			continue
		}

		if item.NumValue != expFields[i].NumValue {
			return fmt.Errorf("error fields value(number), index: %d,  key: %s, exp: %v; got: %v",
				i, expFields[i].Key, expFields[i].NumValue, item.NumValue)
		}
	}
	return nil
}

func convertFields(fields map[string]interface{}) influx.Fields {
	var ret influx.Fields
	for k, v := range fields {
		switch vv := v.(type) {
		case string:
			ret = append(ret, influx.Field{
				Key:      k,
				NumValue: 0,
				StrValue: vv,
				Type:     0,
			})
		case int64:
			ret = append(ret, influx.Field{
				Key:      k,
				NumValue: float64(vv),
				StrValue: "",
				Type:     0,
			})
		case float64:
			ret = append(ret, influx.Field{
				Key:      k,
				NumValue: vv,
				StrValue: "",
				Type:     0,
			})

		}
	}
	return ret
}

func TestIncrDuration(t *testing.T) {
	ts := statistics.NewTimestamp()
	ts.Init(time.Second)

	tm1, _ := strconv.ParseInt(string(ts.Bytes()), 10, 64)

	ts.Incr()
	tm2, _ := strconv.ParseInt(string(ts.Bytes()), 10, 64)

	assert.Equal(t, time.Second.Nanoseconds(), tm2-tm1)
}
