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

package record_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestRecordGroup(t *testing.T) {
	rgs, release := record.NewMemGroups(4, 2)
	defer release()

	rows := []*influx.Row{
		buildRow(100, "foo1", 1),
		buildRow(101, "foo1", 2),
		buildRow(101, "foo1", 3),
		buildRow(100, "foo2", 2),
		buildRow(100, "foo3", 2),
		buildRow(100, "foo4", 2),
		buildRow(100, "foo5", 2),
	}

	for _, row := range rows {
		require.NoError(t, rgs.Add(row))
	}

	total := 0
	for i := range rgs.GroupNum() {
		rg := rgs.RowGroup(i)
		total += len(rg.Records())
	}
	require.Equal(t, len(rows), total)

	rgs.BeforeWrite()
	for i := range rgs.GroupNum() {
		rg := rgs.RowGroup(i)
		rg.SetShardID(100)
		require.Equal(t, uint64(i), rg.Hash())
		require.Equal(t, uint64(100), rg.ShardID())
		rg.Done(nil)
	}

	rgs.Wait()
	require.NoError(t, rgs.Error())
}

func TestRecordGroupError(t *testing.T) {
	rgs, release := record.NewMemGroups(4, 1)
	defer release()

	row := buildRow(100, "foo1", 1)
	row.Fields[0].Type = 100 // invalid type
	require.NotEmpty(t, rgs.Add(row))

	require.NoError(t, rgs.Add(buildRow(100, "foo1", 1)))
	rgs.BeforeWrite()

	err := errors.New("some error")
	for i := range rgs.GroupNum() {
		rg := rgs.RowGroup(i)
		rg.Done(err)
	}

	rgs.Wait()
	require.EqualError(t, rgs.Error(), err.Error())
}

func buildRow(ts int64, name string, i int) *influx.Row {
	row := &influx.Row{
		Timestamp: ts,
		Name:      name,
		Tags: []influx.Tag{
			{
				Key:   "tag1",
				Value: fmt.Sprintf("value%d", i),
			},
			{
				Key:   "tag2",
				Value: fmt.Sprintf("value%d", i),
			},
		},
		Fields: influx.Fields{
			influx.Field{
				Key:      fmt.Sprintf("float_%d", i),
				Type:     influx.Field_Type_Float,
				NumValue: float64(i),
			},
			influx.Field{
				Key:      fmt.Sprintf("int_%d", i%3),
				Type:     influx.Field_Type_Int,
				NumValue: float64(i),
			},
			influx.Field{
				Key:      fmt.Sprintf("string_%d", i%7),
				Type:     influx.Field_Type_String,
				StrValue: fmt.Sprintf("value%d", i),
			},
		},
	}
	row.UnmarshalIndexKeys(nil)
	return row
}
