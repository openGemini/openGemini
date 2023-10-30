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

package engine

// Run single case for Table Test Suite:
// go test -v -run Test_PreAggregation_FullData_SingleCal\/select_min.*?float\]$
// go test -v -run Test_PreAggregation_FullData_SingleCal\/select_min.*?int\]_with_time_range

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	ast "github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/comm"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/engine/mutable"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/resourceallocator"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func init() {
	immutable.EnableMergeOutOfOrder = false
	logger.InitLogger(config.NewLogger(config.AppStore))
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ChunkReaderRes, 0, 0)
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.ShardsParallelismRes, 0, 0)
	_ = resourceallocator.InitResAllocator(math.MaxInt64, 1, 1, resourceallocator.GradientDesc, resourceallocator.SeriesParallelismRes, 0, 0)
}

func RemoveTimeCondition(stmt *influxql.SelectStatement) influxql.TimeRange {
	valuer := &influxql.NowValuer{Location: stmt.Location}
	condition, timeRange, _ := influxql.ConditionExpr(stmt.Condition, valuer)
	stmt.Condition = condition
	return timeRange
}

func Test_PreAggregation_FullData_SingleCall(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	//pts, minT, maxT := GenDataRecord_FullFields(msNames, 5, 10000, time.Millisecond*10, startTime)
	pts, minT, maxT := GenDataRecord(msNames, 5, 10000, time.Millisecond*10, startTime, true, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****
	enableStates := []bool{true, false}
	for _, v := range enableStates {
		executor.EnableFileCursor(v)
		for _, tt := range []struct {
			name              string
			q                 string
			tr                util.TimeRange
			fields            map[string]influxql.DataType
			skip              bool
			outputRowDataType *hybridqp.RowDataTypeImpl
			readerOps         []hybridqp.ExprOptions
			aggOps            []hybridqp.ExprOptions
			expect            func(chunks []executor.Chunk) bool
		}{
			/* min */
			// select min[int]
			{
				name:   "select min[int]",
				q:      fmt.Sprintf(`SELECT min(field2_int) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
					return success
				},
				// skip: true,
			},
			// select min[int] with time range
			{
				name:   "select min[int] with time range",
				q:      fmt.Sprintf(`SELECT min(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxT),
				tr:     util.TimeRange{Min: minT, Max: maxT},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
					return success
				},
				// skip: true,
			},
			// select min[float]
			{
				name:   "select min[float]",
				q:      fmt.Sprintf(`SELECT min(field4_float) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 1.1) && success
					return success
				},
				// skip: true,
			},
			// select min[float] with time range
			{
				name:   "select min[float] with time range",
				q:      fmt.Sprintf(`SELECT min(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxT),
				tr:     util.TimeRange{Min: minT, Max: maxT},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 1.1) && success
					return success
				},
				// skip: true,
			},
			// select min[int] with aux and time range
			{
				name:   "select min[int] with aux and time range",
				q:      fmt.Sprintf(`SELECT min(field2_int),field3_bool, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val3", Type: influxql.Boolean},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462815000000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
					//success = ast.Equal(t, ck.Column(1).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(2).FloatValue(0), 1.1) && success
					success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-0") && success
					return success
				},
				// skip: true,
			},
			// select min[bool]
			{
				name:   "select min[bool]",
				q:      fmt.Sprintf(`SELECT min(field3_bool) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val3", Type: influxql.Boolean},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).BooleanValue(0), false) && success
					return success
				},
				// skip: true,
			},
			// select min[bool] with aux and time range
			{
				name:   "select min[bool] with aux and time range",
				q:      fmt.Sprintf(`SELECT min(field3_bool),field2_int,field4_float,field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
					{
						Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 4) && success
					success = ast.Equal(t, ck.Column(0).BooleanValue(0), false) && success
					success = ast.Equal(t, len(ck.Column(1).IntegerValues()), 1) && success
					success = ast.Equal(t, len(ck.Column(2).FloatValues()), 1) && success
					success = ast.Equal(t, len(ck.Column(3).StringValuesV2(nil)), 1) && success
					return success
				},
				// skip: true,
			},

			/* max */
			// select max[int]
			{
				name:   "select max[int]",
				q:      fmt.Sprintf(`SELECT max(field2_int) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462800040000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				// skip: true,
			},
			// select max[int] with time range
			{
				name:   "select max[int] with time range",
				q:      fmt.Sprintf(`SELECT max(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxT-time.Second.Nanoseconds()*30),
				tr:     util.TimeRange{Min: minT, Max: maxT - time.Second.Nanoseconds()*30},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462800040000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				// skip: true,
			},
			// select max[float]
			{
				name:   "select max[float]",
				q:      fmt.Sprintf(`SELECT max(field4_float) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462800040000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select max[float] with time range
			{
				name:   "select max[float] with time range",
				q:      fmt.Sprintf(`SELECT max(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxT-time.Second.Nanoseconds()*5),
				tr:     util.TimeRange{Min: minT, Max: maxT},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462800040000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select max[float] with aux and time range
			{
				name:   "select max[float] with aux and time range",
				q:      fmt.Sprintf(`SELECT max(field4_float), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val3", Type: influxql.Float},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val3", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462815000000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(1).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},
			// select max[bool]
			{
				name:   "select max[bool]",
				q:      fmt.Sprintf(`SELECT max(field3_bool) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val3", Type: influxql.Boolean},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).BooleanValue(0), true) && success
					return success
				},
				// skip: true,
			},
			// select max[bool] with aux and time range
			{
				name:   "select max[bool] with aux and time range",
				q:      fmt.Sprintf(`SELECT max(field3_bool),field2_int,field4_float,field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					},
					{
						Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 4) && success
					success = ast.Equal(t, ck.Column(0).BooleanValue(0), true) && success
					success = ast.Equal(t, len(ck.Column(1).IntegerValues()), 1) && success
					success = ast.Equal(t, len(ck.Column(2).FloatValues()), 1) && success
					success = ast.Equal(t, len(ck.Column(3).StringValuesV2(nil)), 1) && success
					return success
				},
				// skip: true,
			},
			/* count */
			// select count[int]
			{
				// count to sum
				name:   "select count[int]",
				q:      fmt.Sprintf(`SELECT count(field2_int) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(50000)) && success
					return success
				},
				// skip: true,
			},
			// select count[bool]
			{
				name:   "select count[bool]",
				q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val3", Type: influxql.Integer}, // Be Integer
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(50000)) && success
					return success
				},
				// skip: true,
			},
			// select count[float]
			{
				name:   "select count[float]",
				q:      fmt.Sprintf(`SELECT count(field4_float) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Integer}, // Be Integer
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(50000)) && success
					return success
				},
				// skip: true,
			},
			// select count[string]
			{
				name:   "select count[string]",
				q:      fmt.Sprintf(`SELECT count(field1_string) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Integer}, // Be Integer
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(50000)) && success
					return success
				},
				// skip: true,
			},
			// select count[int] with time range
			{
				name:   "select count[int] with time range",
				q:      fmt.Sprintf(`SELECT count(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*30, maxT-time.Second.Nanoseconds()*30),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*30, Max: maxT - time.Second.Nanoseconds()*30},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(20020)) && success
					return success
				},
				// skip: true,
			},
			// select count[bool] with time range
			{
				name:   "select count[bool] with time range",
				q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*10, maxT-time.Second.Nanoseconds()*10),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*10, Max: maxT - time.Second.Nanoseconds()*10},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val3", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(40020)) && success
					return success
				},
				// skip: true,
			},
			// select count[float] with time range
			{
				name:   "select count[float] with time range",
				q:      fmt.Sprintf(`SELECT count(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*20, maxT-time.Second.Nanoseconds()*20),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*20, Max: maxT - time.Second.Nanoseconds()*20},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(30020)) && success
					return success
				},
				// skip: true,
			},
			// select count[string] with time range
			{
				name:   "select count[string] with time range",
				q:      fmt.Sprintf(`SELECT count(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(45020)) && success
					return success
				},
				// skip: true,
			},

			/* sum */
			// select sum[int]
			{
				name:   "select sum[int]",
				q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(150000)) && success
					return success
				},
				// skip: true,
			},
			// select sum[float]
			{
				name:   "select sum[float]",
				q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(165000.00000000105)) && success
					return success
				},
				// skip: true,
			},
			// select sum[int] with time range
			{
				name:   "select sum[int] with time range",
				q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(135060)) && success
					return success
				},
				// skip: true,
			},
			// select sum[float] with time range
			{
				name:   "select sum[float] with time range",
				q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*25, maxT-time.Second.Nanoseconds()*25),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*25, Max: maxT - time.Second.Nanoseconds()*25},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(82566.0000000005)) && success
					return success
				},
				// skip: true,
			},

			/* first */
			// select first[int]
			{
				name:   "select first[int]",
				q:      fmt.Sprintf(`SELECT first(field2_int) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
					return success
				},
				// skip: true,
			},
			// select first[float]
			{
				name:   "select first[float]",
				q:      fmt.Sprintf(`SELECT first(field4_float) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 1.1) && success
					return success
				},
				// skip: true,
			},
			// select first[string]
			{
				name:   "select first[string]",
				q:      fmt.Sprintf(`SELECT first(field1_string) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-0") && success
					return success
				},
				// skip: true,
			},
			// select first[int] with time range
			{
				name:   "select first[int] with time range",
				q:      fmt.Sprintf(`SELECT first(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462815000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				// skip: true,
			},
			// select first[float] with time range
			{
				name:   "select first[float] with time range",
				q:      fmt.Sprintf(`SELECT first(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462815000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select first[string] with time range
			{
				name:   "select first[string] with time range",
				q:      fmt.Sprintf(`SELECT first(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462805000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},
			// select first[string] with aux and time range
			{
				name:   "select first[string] with aux and time range",
				q:      fmt.Sprintf(`SELECT first(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val3", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
					{
						Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462815000000000)) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},

			/* last */
			// select last[int]
			{
				name:   "select last[int]",
				q:      fmt.Sprintf(`SELECT last(field2_int) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462900030000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				// skip: true,
			},
			// select last[float]
			{
				name:   "select last[float]",
				q:      fmt.Sprintf(`SELECT last(field4_float) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462900030000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select last[string]
			{
				name:   "select last[string]",
				q:      fmt.Sprintf(`SELECT last(field1_string) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462900030000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},
			// select last[int] with time range
			{
				name:   "select last[int] with time range",
				q:      fmt.Sprintf(`SELECT last(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, 1609462801030000000),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: 1609462801030000000},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462801030000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				// skip: true,
			},
			// select last[float] with time range
			{
				name:   "select last[float] with time range",
				q:      fmt.Sprintf(`SELECT last(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, 1609462801030000000),
				tr:     util.TimeRange{Min: minT, Max: 1609462801030000000},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462801030000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select last[string] with time range
			{
				name:   "select last[string] with time range",
				q:      fmt.Sprintf(`SELECT last(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT, 1609462801030000000),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: 1609462801030000000},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462801030000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},
			// select last[string] with aux and time range
			{
				name:   "select last[string] with aux and time range",
				q:      fmt.Sprintf(`select last(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val3", Type: influxql.String},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609462885030000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(1).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},

			/* mean */
			// select mean[int]
			{
				name:   "select mean[int]",
				q:      fmt.Sprintf(`SELECT count(field2_int),sum(field2_int) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Integer},
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 2) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(50000)) && success
					success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(150000)) && success
					return success
				},
				// skip: true,
			},
			// select mean[float]
			{
				name:   "select mean[float]",
				q:      fmt.Sprintf(`SELECT count(field4_float),sum(field4_float) from cpu`),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Integer},
					influxql.VarRef{Val: "val2", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 2) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(50000)) && success
					success = ast.Equal(t, math.Round(ck.Column(1).FloatValue(0)), math.Round(165000.00000000105)) && success
					return success
				},
				// skip: true,
			},
			// select mean[int] with time range
			{
				name:   "select mean[int] with time range",
				q:      fmt.Sprintf(`SELECT count(field2_int),sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Integer},
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 2) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(45020)) && success
					success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(135060)) && success
					return success
				},
				// skip: true,
			},
			// select mean[float] with time range
			{
				name:   "select mean[float] with time range",
				q:      fmt.Sprintf(`SELECT count(field4_float),sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Integer},
					influxql.VarRef{Val: "val2", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 2) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(45020)) && success
					success = ast.Equal(t, math.Round(ck.Column(1).FloatValue(0)), math.Round(148566.0000000009)) && success
					return success
				},
				// skip: true,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, nil, querySchema, keyCursors, false)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)

				agg, _ := executor.NewStreamAggregateTransform(
					[]hybridqp.RowDataType{tt.outputRowDataType},
					[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
				agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
				agg.GetOutputs()[0].Connect(outPutPort)

				chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])
				go func() {
					chunkReader.Work(ctx)
				}()
				go agg.Work(ctx)

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				if !tt.expect(chunks) {
					t.Fail()
				}
			})
		}
	}
}

func Test_PreAggregation_MissingData_SingleCall(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z")
	pts, minT, maxT := GenDataRecord(msNames, 5, 10000, time.Millisecond*100, startTime, false, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 2)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []executor.Chunk) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int]",
			q:      fmt.Sprintf(`SELECT min(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// // skip: true,
		},
		// select min[int] with time range
		{
			name:   "select min[int] with time range",
			q:      fmt.Sprintf(`SELECT min(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxT),
			tr:     util.TimeRange{Min: minT, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// skip: true,
		},
		// select min[float]
		{
			name:   "select min[float]",
			q:      fmt.Sprintf(`SELECT min(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
				return success
			},
			// skip: true,
		},
		// select min[float] with time range
		{
			name:   "select min[float] with time range",
			q:      fmt.Sprintf(`SELECT min(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxT),
			tr:     util.TimeRange{Min: minT, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
				return success
			},
			// skip: true,
		},
		// select min[int] with aux and time range
		{
			name:   "select min[int] with aux and time range",
			q:      fmt.Sprintf(`SELECT min(field2_int),field3_bool, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Boolean},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.Boolean},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Boolean},
				},
				{
					Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				//success = ast.Equal(t, ck.Column(1).BooleanValue(0), true) && success
				success = ast.Equal(t, ck.Column(2).FloatValues(), []float64(nil)) && success
				success = ast.Equal(t, ck.Column(3).StringValuesV2(nil), []string(nil)) && success
				return success
			},
			// skip: true,
		},

		// BUG2022071101301
		// select min[bool] with bool aux and time range
		{
			name:   "select min[bool] with bool aux and time range",
			q:      fmt.Sprintf(`SELECT min(field3_bool),field3_bool from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Boolean},
				influxql.VarRef{Val: "val1", Type: influxql.Boolean},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Boolean},
				},
				{
					Expr: &influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Boolean},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Boolean},
				},
				{
					Expr: &influxql.VarRef{Val: "val1", Type: influxql.Boolean},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Boolean},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609462800000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 2) && success
				success = ast.Equal(t, ck.Column(0).BooleanValue(0), false) && success
				success = ast.Equal(t, ck.Column(1).BooleanValue(0), false) && success
				return success
			},
			// skip: true,
		},

		/* max */
		// select max[int]
		{
			name:   "select max[int]",
			q:      fmt.Sprintf(`SELECT max(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			//// skip: true,
		},
		// select max[int] with time range
		{
			name:   "select max[int] with time range",
			q:      fmt.Sprintf(`SELECT max(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxT-time.Second.Nanoseconds()*30),
			tr:     util.TimeRange{Min: minT, Max: maxT - time.Second.Nanoseconds()*30},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select max[float]
		{
			name:   "select max[float]",
			q:      fmt.Sprintf(`SELECT max(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select max[float] with time range
		{
			name:   "select max[float] with time range",
			q:      fmt.Sprintf(`SELECT max(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxT-time.Second.Nanoseconds()*5),
			tr:     util.TimeRange{Min: minT, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select max[float] with aux and time range
		{
			name:   "select max[float] with aux and time range",
			q:      fmt.Sprintf(`SELECT max(field4_float), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				success = ast.Equal(t, ck.Column(1).FloatValue(0), 5.5) && success
				success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
				success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},

		/* count */
		// select count[int]
		{
			// count to sum
			name:   "select count[int]",
			q:      fmt.Sprintf(`SELECT count(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(50000)) && success
				return success
			},
			// skip: true,
		},
		// select count[bool]
		{
			name:   "select count[bool]",
			q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val3", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(40000)) && success
				return success
			},
			// skip: true,
		},
		// select count[float]
		{
			name:   "select count[float]",
			q:      fmt.Sprintf(`SELECT count(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(40000)) && success
				return success
			},
			// skip: true,
		},
		// select count[string]
		{
			name:   "select count[string]",
			q:      fmt.Sprintf(`SELECT count(field1_string) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(40000)) && success
				return success
			},
			// skip: true,
		},
		// select count[int] with time range
		{
			name:   "select count[int] with time range",
			q:      fmt.Sprintf(`SELECT count(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*30, maxT-time.Second.Nanoseconds()*30),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*30, Max: maxT - time.Second.Nanoseconds()*30},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(47020)) && success
				return success
			},
			// skip: true,
		},
		// select count[bool] with time range
		{
			name:   "select count[bool] with time range",
			q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*10, maxT-time.Second.Nanoseconds()*10),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*10, Max: maxT - time.Second.Nanoseconds()*10},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val3", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(39216)) && success
				return success
			},
			// skip: true,
		},
		// select count[float] with time range
		{
			name:   "select count[float] with time range",
			q:      fmt.Sprintf(`SELECT count(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*20, maxT-time.Second.Nanoseconds()*20),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*20, Max: maxT - time.Second.Nanoseconds()*20},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(38416)) && success
				return success
			},
			// skip: true,
		},
		// select count[string] with time range
		{
			name:   "select count[string] with time range",
			q:      fmt.Sprintf(`SELECT count(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(39616)) && success
				return success
			},
			// skip: true,
		},

		/* sum */
		// select sum[int]
		{
			name:   "select sum[int]",
			q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(150000)) && success
				return success
			},
			// skip: true,
		},
		// select sum[float]
		{
			name:   "select sum[float]",
			q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(154000.000000001)) && success
				return success
			},
			// skip: true,
		},
		// select sum[int] with time range
		{
			name:   "select sum[int] with time range",
			q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(148560)) && success
				return success
			},
			// skip: true,
		},
		// select sum[float] with time range
		{
			name:   "select sum[float] with time range",
			q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*25, maxT-time.Second.Nanoseconds()*25),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*25, Max: maxT - time.Second.Nanoseconds()*25},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(146361.60000000094)) && success
				return success
			},
			// skip: true,
		},

		/* first */
		// select first[int]
		{
			name:   "select first[int]",
			q:      fmt.Sprintf(`SELECT first(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// skip: true,
		},
		// select first[float]
		{
			name:   "select first[float]",
			q:      fmt.Sprintf(`SELECT first(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
				return success
			},
			// skip: true,
		},
		// select first[string]
		{
			name:   "select first[string]",
			q:      fmt.Sprintf(`SELECT first(field1_string) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-1") && success
				return success
			},
			// skip: true,
		},
		// select first[int] with time range
		{
			name:   "select first[int] with time range",
			q:      fmt.Sprintf(`SELECT first(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select first[float] with time range
		{
			name:   "select first[float] with time range",
			q:      fmt.Sprintf(`SELECT first(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select first[string] with time range
		{
			name:   "select first[string] with time range",
			q:      fmt.Sprintf(`SELECT first(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459205000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		// select first[string] with aux and time range
		{
			name:   "select first[string] with aux and time range",
			q:      fmt.Sprintf(`SELECT first(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.String},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				success = ast.Equal(t, ck.Column(1).StringValue(0), "test-test-test-test-4") && success
				success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
				success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},

		/* last */
		// select last[int]
		{
			name:   "select last[int]",
			q:      fmt.Sprintf(`SELECT last(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609460200300000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select last[float]
		{
			name:   "select last[float]",
			q:      fmt.Sprintf(`SELECT last(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609460200300000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select last[string]
		{
			name:   "select last[string]",
			q:      fmt.Sprintf(`SELECT last(field1_string) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609460200300000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		// select last[int] with time range
		{
			name:   "select last[int] with time range",
			q:      fmt.Sprintf(`SELECT last(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, 1609462801030000000),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: 1609462801030000000},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609460200300000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select last[float] with time range
		{
			name:   "select last[float] with time range",
			q:      fmt.Sprintf(`SELECT last(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, 1609462801030000000),
			tr:     util.TimeRange{Min: minT, Max: 1609462801030000000},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609460200300000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select last[string] with time range
		{
			name:   "select last[string] with time range",
			q:      fmt.Sprintf(`SELECT last(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT, 1609462801030000000),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: 1609462801030000000},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609460200300000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		// select last[string] with aux and time range
		{
			name:   "select last[string] with aux and time range",
			q:      fmt.Sprintf(`select last(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.String},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609460185300000000)) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				success = ast.Equal(t, ck.Column(1).StringValue(0), "test-test-test-test-4") && success
				success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
				success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
	} {
		enableStates := []bool{true, false}
		for _, v := range enableStates {
			executor.EnableFileCursor(v)
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, nil, querySchema, keyCursors, false)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)

				agg, _ := executor.NewStreamAggregateTransform(
					[]hybridqp.RowDataType{tt.outputRowDataType},
					[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
				agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
				agg.GetOutputs()[0].Connect(outPutPort)

				chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])
				go func() {
					chunkReader.Work(ctx)
				}()
				go agg.Work(ctx)

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				if !tt.expect(chunks) {
					t.Errorf("`%s` failed", tt.name)
				}
			})
		}
	}
}

func Test_PreAggregation_MemTableMissingData_SingleCall(t *testing.T) {
	Run_MissingData_SingCall(t, false)
}

func Test_PreAggregation_OutOfOrderMissingData_SingleCall(t *testing.T) {
	Run_MissingData_SingCall(t, true)
}

func Run_MissingData_SingCall(t *testing.T, isFlush bool) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z")
	pts, minT, maxT := GenDataRecord(msNames, 5, 10000, time.Millisecond*100, startTime, false, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	sh.SetWriteColdDuration(100000 * time.Second)
	mutable.SetSizeLimit(10 * 1024 * 1024 * 1024)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	startTimeMemTable := mustParseTime(time.RFC3339Nano, "2020-01-01T00:00:00Z")
	ptsOut, minTOut, maxTOut := GenDataRecord(msNames, 5, 1000, time.Millisecond*100, startTimeMemTable, false, false, true)
	if err := sh.WriteRows(ptsOut, nil); err != nil {
		t.Fatal(err)
	}
	if isFlush {
		sh.ForceFlush()
		time.Sleep(time.Second * 1)
	} else {
		idx, ok := sh.indexBuilder.GetPrimaryIndex().(*tsi.MergeSetIndex)
		if !ok {
			t.Fatal("not a MergeSetIndex")
		}
		idx.DebugFlush()
	}
	var minFalseTOut int64
	var minFalseVOut int
	first := true
	for _, value := range ptsOut {
		state := false
		for _, field := range value.Fields {
			if first && field.Key == "field3_bool" {
				minFalseTOut = value.Timestamp
				minFalseVOut = int(field.NumValue)
				first = false
				if minFalseVOut == 0 {
					state = true
				}
				break
			}

			if field.Key == "field3_bool" && field.NumValue == 0 {
				state = true
				minFalseVOut = int(field.NumValue)
			}
		}
		if state {
			minFalseTOut = value.Timestamp
			break
		}
	}
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []executor.Chunk) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "select min[int]",
			q:      fmt.Sprintf(`SELECT min(field2_int) from cpu where time <= %v`, maxTOut),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], minTOut) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// // skip: true,
		},
		// select min[bool]
		{
			name:   "select min[bool]",
			q:      fmt.Sprintf(`SELECT min(field3_bool) from cpu where time <= %v`, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Boolean},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Boolean},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Boolean},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], minFalseTOut) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).BooleanValue(0), minFalseVOut == 1) && success
				return success
			},
			// // skip: true,
		},
		// select min[int] with time range
		{
			name:   "select min[int] with time range",
			q:      fmt.Sprintf(`SELECT min(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2020-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// skip: true,
		},
		// select min[float]
		{
			name:   "select min[float]",
			q:      fmt.Sprintf(`SELECT min(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
				return success
			},
			// skip: true,
		},
		// select min[float] with time range
		{
			name:   "select min[float] with time range",
			q:      fmt.Sprintf(`SELECT min(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
				return success
			},
			// skip: true,
		},
		// select min[int] with aux and time range
		{
			name:   "select min[int] with aux and time range",
			q:      fmt.Sprintf(`SELECT min(field2_int),field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*15, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Integer},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836815000000000)) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(1)) && success
				success = ast.Equal(t, ck.Column(2).FloatValues(), []float64(nil)) && success
				success = ast.Equal(t, ck.Column(3).StringValuesV2(nil), []string(nil)) && success
				return success
			},
			// skip: true,
		},

		/* max */
		// select max[int]
		{
			name:   "select max[int]",
			q:      fmt.Sprintf(`SELECT max(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			//// skip: true,
		},
		// select max[int] with time range
		{
			name:   "select max[int] with time range",
			q:      fmt.Sprintf(`SELECT max(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select max[float]
		{
			name:   "select max[float]",
			q:      fmt.Sprintf(`SELECT max(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select max[float] with time range
		{
			name:   "select max[float] with time range",
			q:      fmt.Sprintf(`SELECT max(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select max[float] with aux and time range
		{
			name:   "select max[float] with aux and time range",
			q:      fmt.Sprintf(`SELECT max(field4_float), field2_int, field4_float, field1_string from cpu  WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*15, Max: maxTOut - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836815000000000)) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				success = ast.Equal(t, ck.Column(1).FloatValue(0), 5.5) && success
				success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
				success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},

		/* count */
		// select count[int]
		{
			// count to sum
			name:   "select count[int]",
			q:      fmt.Sprintf(`SELECT count(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5000)) && success
				return success
			},
			// skip: true,
		},
		// select count[bool]
		{
			name:   "select count[bool]",
			q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val3", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(4000)) && success
				return success
			},
			// skip: true,
		},
		// select count[float]
		{
			name:   "select count[float]",
			q:      fmt.Sprintf(`SELECT count(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(4000)) && success
				return success
			},
			// skip: true,
		},
		// select count[string]
		{
			name:   "select count[string]",
			q:      fmt.Sprintf(`SELECT count(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(44000)) && success
				return success
			},
			// skip: true,
		},
		// select count[int] with time range
		{
			name:   "select count[int] with time range",
			q:      fmt.Sprintf(`SELECT count(field2_int) from cpu  WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*30, Max: maxTOut - time.Second.Nanoseconds()*30},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(2020)) && success
				return success
			},
			// skip: true,
		},
		// select count[bool] with time range
		{
			name:   "select count[bool] with time range",
			q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*10, Max: maxTOut - time.Second.Nanoseconds()*10},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val3", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(3216)) && success
				return success
			},
			// skip: true,
		},
		// select count[float] with time range
		{
			name:   "select count[float] with time range",
			q:      fmt.Sprintf(`SELECT count(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*20, Max: maxTOut - time.Second.Nanoseconds()*20},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(2416)) && success
				return success
			},
			// skip: true,
		},
		// select count[string] with time range
		{
			name:   "select count[string] with time range",
			q:      fmt.Sprintf(`SELECT count(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*5, Max: maxTOut - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(3616)) && success
				return success
			},
			// skip: true,
		},

		/* sum */
		// select sum[int]
		{
			name:   "select sum[int]",
			q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(15000)) && success
				return success
			},
			// skip: true,
		},
		// select sum[float]
		{
			name:   "select sum[float]",
			q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(169400.000000001)) && success
				return success
			},
			// skip: true,
		},
		// select sum[int] with time range
		{
			name:   "select sum[int] with time range",
			q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*5, Max: maxTOut - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(13560)) && success
				return success
			},
			// skip: true,
		},
		// select sum[float] with time range
		{
			name:   "select sum[float] with time range",
			q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*25, Max: maxTOut - time.Second.Nanoseconds()*25},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(7761.60000000094)) && success
				return success
			},
			// skip: true,
		},

		/* first */
		// select first[int]
		{
			name:   "select first[int]",
			q:      fmt.Sprintf(`SELECT first(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2020-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// skip: true,
		},
		// select first[float]
		{
			name:   "select first[float]",
			q:      fmt.Sprintf(`SELECT first(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
				return success
			},
			// skip: true,
		},
		// select first[string]
		{
			name:   "select first[string]",
			q:      fmt.Sprintf(`SELECT first(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-1") && success
				return success
			},
			// skip: true,
		},
		// select first[int] with time range
		{
			name:   "select first[int] with time range",
			q:      fmt.Sprintf(`SELECT first(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select first[float] with time range
		{
			name:   "select first[float] with time range",
			q:      fmt.Sprintf(`SELECT first(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select first[string] with time range
		{
			name:   "select first[string] with time range",
			q:      fmt.Sprintf(`SELECT first(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459205000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		// select first[string] with aux and time range
		{
			name:   "select first[string] with aux",
			q:      fmt.Sprintf(`SELECT first(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.String},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1577836800100000000)) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(2)) && success
				success = ast.Equal(t, ck.Column(1).StringValue(0), "test-test-test-test-1") && success
				success = ast.Equal(t, ck.Column(2).FloatValue(0), 2.2) && success
				success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-1") && success
				return success
			},
			// skip: true,
		},

		/* last */
		// select last[int]
		{
			name:   "select last[int]",
			q:      fmt.Sprintf(`SELECT last(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTOut) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select last[float]
		{
			name:   "select last[float]",
			q:      fmt.Sprintf(`SELECT last(field4_float) from cpu  WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTOut) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select last[string]
		{
			name:   "select last[string]",
			q:      fmt.Sprintf(`SELECT last(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTOut) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		// select last[string] with time range
		{
			name:   "select last[string] with time range",
			q:      fmt.Sprintf(`SELECT last(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTOut) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		// select last[string] with aux and time range
		{
			name:   "select last[string] with aux",
			q:      fmt.Sprintf(`select last(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
			tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.String},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTOut) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				success = ast.Equal(t, ck.Column(1).StringValue(0), "test-test-test-test-4") && success
				success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
				success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
	} {
		enableStates := []bool{true, false}
		for _, v := range enableStates {
			executor.EnableFileCursor(v)
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, nil, querySchema, keyCursors, false)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)

				agg, _ := executor.NewStreamAggregateTransform(
					[]hybridqp.RowDataType{tt.outputRowDataType},
					[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
				agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
				agg.GetOutputs()[0].Connect(outPutPort)

				chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])

				go func() {
					chunkReader.Work(ctx)
				}()
				go agg.Work(ctx)

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				if !tt.expect(chunks) {
					t.Errorf("`%s` failed", tt.name)
				}
			})
		}
	}
}

func getMinMaxBool(rows []influx.Row) (int64, int, int64, int) {
	var minFalseTOut, maxTrueTime int64
	var minFalseVOut, maxTrueVOut int
	first := true
	for _, value := range rows {
		state := false
		for _, field := range value.Fields {
			if first && field.Key == "field3_bool" {
				minFalseTOut = value.Timestamp
				minFalseVOut = int(field.NumValue)
				first = false
				if minFalseVOut == 0 {
					state = true
				}
				break
			}

			if field.Key == "field3_bool" && field.NumValue == 0 {
				state = true
				minFalseVOut = int(field.NumValue)
			}
		}
		if state {
			minFalseTOut = value.Timestamp
			break
		}
	}

	first = true
	for _, value := range rows {
		state := false
		for _, field := range value.Fields {
			if first && field.Key == "field3_bool" {
				maxTrueTime = value.Timestamp
				maxTrueVOut = int(field.NumValue)
				first = false
				if field.NumValue == 1 {
					state = true
				}
				break
			}

			if field.Key == "field3_bool" && field.NumValue == 1 {
				maxTrueTime = value.Timestamp
				maxTrueVOut = int(field.NumValue)
				state = true
				break
			}
		}
		if state {
			maxTrueTime = value.Timestamp
			break
		}
	}
	return minFalseTOut, minFalseVOut, maxTrueTime, maxTrueVOut
}

func getMinMaxBoolWithMemtable(rows []influx.Row, outRows []influx.Row) (int64, int, int64, int) {
	var minFalseTOut, maxTrueTime int64
	var minFalseVOut, maxTrueVOut int
	minFalseTOut, minFalseVOut, maxTrueTime, maxTrueVOut = getMinMaxBool(rows)

	if minFalseVOut != 0 {
		for _, value := range outRows {
			state := false
			for _, field := range value.Fields {
				if field.Key == "field3_bool" && field.NumValue == 0 {
					state = true
					minFalseVOut = int(field.NumValue)
				}
			}
			if state {
				minFalseTOut = value.Timestamp
				break
			}
		}
	}

	if maxTrueVOut != 1 {
		for _, value := range outRows {
			state := false
			for _, field := range value.Fields {
				if field.Key == "field3_bool" && field.NumValue == 1 {
					maxTrueTime = value.Timestamp
					maxTrueVOut = int(field.NumValue)
					state = true
					break
				}
			}
			if state {
				maxTrueTime = value.Timestamp
				break
			}
		}
	}
	return minFalseTOut, minFalseVOut, maxTrueTime, maxTrueVOut
}

func Test_PreAggregation_Memtable_After_Order_SingCall(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z")
	pts, minT, maxT := GenDataRecord(msNames, 5, 10000, time.Millisecond*100, startTime, false, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	sh.SetWriteColdDuration(100000 * time.Second)
	mutable.SetSizeLimit(10 * 1024 * 1024 * 1024)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	startTimeMemTable := mustParseTime(time.RFC3339Nano, "2021-02-01T00:00:00Z")
	ptsOut, minTOut, maxTOut := GenDataRecord(msNames, 5, 1000, time.Millisecond*100, startTimeMemTable, false, false, true)
	if err := sh.WriteRows(ptsOut, nil); err != nil {
		t.Fatal(err)
	}
	idx, ok := sh.indexBuilder.GetPrimaryIndex().(*tsi.MergeSetIndex)
	if !ok {
		t.Fatal("not a MergeSetIndex type")
	}
	idx.DebugFlush()

	var minFalseTOut, maxTrueTime int64
	var minFalseVOut, maxTrueVOut int

	minFalseTOut, minFalseVOut, maxTrueTime, maxTrueVOut = getMinMaxBoolWithMemtable(pts, ptsOut)
	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****
	enableStates := []bool{true, false}
	for _, v := range enableStates {
		executor.EnableFileCursor(v)
		for _, tt := range []struct {
			name              string
			q                 string
			tr                util.TimeRange
			fields            map[string]influxql.DataType
			skip              bool
			outputRowDataType *hybridqp.RowDataTypeImpl
			readerOps         []hybridqp.ExprOptions
			aggOps            []hybridqp.ExprOptions
			expect            func(chunks []executor.Chunk) bool
		}{
			/* min */
			// select min[int]
			{
				name:   "select min[int]",
				q:      fmt.Sprintf(`SELECT min(field2_int) from cpu where time <= %v`, maxTOut),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], minT) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
					return success
				},
				// // skip: true,
			},
			// select min[bool]
			{
				name:   "select min[bool]",
				q:      fmt.Sprintf(`SELECT min(field3_bool) from cpu where time <= %v`, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Boolean},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Boolean},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Boolean},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], minFalseTOut) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).BooleanValue(0), minFalseVOut == 1) && success
					return success
				},
				// // skip: true,
			},
			// select min[int] with time range
			{
				name:   "select min[int] with time range",
				q:      fmt.Sprintf(`SELECT min(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
					return success
				},
				// skip: true,
			},
			// select min[float]
			{
				name:   "select min[float]",
				q:      fmt.Sprintf(`SELECT min(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
					return success
				},
				// skip: true,
			},
			// select min[float] with time range
			{
				name:   "select min[float] with time range",
				q:      fmt.Sprintf(`SELECT min(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
					return success
				},
				// skip: true,
			},
			// select min[int] with aux and time range
			{
				name:   "select min[int] with aux and time range",
				q:      fmt.Sprintf(`SELECT min(field2_int),field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val3", Type: influxql.Integer},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val3", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
					success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(1)) && success
					success = ast.Equal(t, ck.Column(2).FloatValues(), []float64(nil)) && success
					success = ast.Equal(t, ck.Column(3).StringValuesV2(nil), []string(nil)) && success
					return success
				},
				// skip: true,
			},

			/* max */
			// select max[int]
			{
				name:   "select max[int]",
				q:      fmt.Sprintf(`SELECT max(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				//// skip: true,
			},
			// select max[bool]
			{
				name:   "select max[bool]",
				q:      fmt.Sprintf(`SELECT max(field3_bool) from cpu where time <= %v`, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Boolean},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Boolean},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Boolean},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], maxTrueTime) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).BooleanValue(0), maxTrueVOut == 1) && success
					return success
				},
				// // skip: true,
			},
			// select max[int] with time range
			{
				name:   "select max[int] with time range",
				q:      fmt.Sprintf(`SELECT max(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTOut),
				tr:     util.TimeRange{Min: minTOut, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1612137600400000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				// skip: true,
			},
			// select max[float]
			{
				name:   "select max[float]",
				q:      fmt.Sprintf(`SELECT max(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select max[float] with time range
			{
				name:   "select max[float] with time range",
				q:      fmt.Sprintf(`SELECT max(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select max[float] with aux and time range
			{
				name:   "select max[float] with aux and time range",
				q:      fmt.Sprintf(`SELECT max(field4_float), field2_int, field4_float, field1_string from cpu  WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxTOut - time.Second.Nanoseconds()*15},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val3", Type: influxql.Float},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val3", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.VarRef{Val: "val1", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(1).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},

			/* count */
			// select count[int]
			{
				// count to sum
				name:   "select count[int]",
				q:      fmt.Sprintf(`SELECT count(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(55000)) && success
					return success
				},
				// skip: true,
			},
			// select count[bool]
			{
				name:   "select count[bool]",
				q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val3", Type: influxql.Integer}, // Be Integer
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(44000)) && success
					return success
				},
				// skip: true,
			},
			// select count[float]
			{
				name:   "select count[float]",
				q:      fmt.Sprintf(`SELECT count(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Integer}, // Be Integer
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(44000)) && success
					return success
				},
				// skip: true,
			},
			// select count[string]
			{
				name:   "select count[string]",
				q:      fmt.Sprintf(`SELECT count(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Integer}, // Be Integer
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(44000)) && success
					return success
				},
				// skip: true,
			},
			// select count[int] with time range
			{
				name:   "select count[int] with time range",
				q:      fmt.Sprintf(`SELECT count(field2_int) from cpu  WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*30, Max: maxTOut - time.Second.Nanoseconds()*30},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(52020)) && success
					return success
				},
				// skip: true,
			},
			// select count[bool] with time range
			{
				name:   "select count[bool] with time range",
				q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*10, Max: maxTOut - time.Second.Nanoseconds()*10},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val3", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(43216)) && success
					return success
				},
				// skip: true,
			},
			// select count[float] with time range
			{
				name:   "select count[float] with time range",
				q:      fmt.Sprintf(`SELECT count(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*20, Max: maxTOut - time.Second.Nanoseconds()*20},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(42416)) && success
					return success
				},
				// skip: true,
			},
			// select count[string] with time range
			{
				name:   "select count[string] with time range",
				q:      fmt.Sprintf(`SELECT count(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxTOut - time.Second.Nanoseconds()*5},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(43616)) && success
					return success
				},
				// skip: true,
			},

			/* sum */
			// select sum[int]
			{
				name:   "select sum[int]",
				q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(165000)) && success
					return success
				},
				// skip: true,
			},
			// select sum[float]
			{
				name:   "select sum[float]",
				q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(169400.000000001)) && success
					return success
				},
				// skip: true,
			},
			// select sum[int] with time range
			{
				name:   "select sum[int] with time range",
				q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxTOut - time.Second.Nanoseconds()*5},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(163560)) && success
					return success
				},
				// skip: true,
			},
			// select sum[float] with time range
			{
				name:   "select sum[float] with time range",
				q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*25, Max: maxTOut - time.Second.Nanoseconds()*25},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(161761.60000000094)) && success
					return success
				},
				// skip: true,
			},

			/* first */
			// select first[int]
			{
				name:   "select first[int]",
				q:      fmt.Sprintf(`SELECT first(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
					return success
				},
				// skip: true,
			},
			// select first[float]
			{
				name:   "select first[float]",
				q:      fmt.Sprintf(`SELECT first(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 2.2) && success
					return success
				},
				// skip: true,
			},
			// select first[string]
			{
				name:   "select first[string]",
				q:      fmt.Sprintf(`SELECT first(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-1") && success
					return success
				},
				// skip: true,
			},
			// select first[int] with time range
			{
				name:   "select first[int] with time range",
				q:      fmt.Sprintf(`SELECT first(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				// skip: true,
			},
			// select first[float] with time range
			{
				name:   "select first[float] with time range",
				q:      fmt.Sprintf(`SELECT first(field4_float) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select first[string] with time range
			{
				name:   "select first[string] with time range",
				q:      fmt.Sprintf(`SELECT first(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459205000000000)) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},
			// select first[string] with aux and time range
			{
				name:   "select first[string] with aux",
				q:      fmt.Sprintf(`SELECT first(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val3", Type: influxql.String},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(2)) && success
					success = ast.Equal(t, ck.Column(1).StringValue(0), "test-test-test-test-1") && success
					success = ast.Equal(t, ck.Column(2).FloatValue(0), 2.2) && success
					success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-1") && success
					return success
				},
				// skip: true,
			},

			/* last */
			// select last[int]
			{
				name:   "select last[int]",
				q:      fmt.Sprintf(`SELECT last(field2_int) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], maxTOut) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					return success
				},
				// skip: true,
			},
			// select last[float]
			{
				name:   "select last[float]",
				q:      fmt.Sprintf(`SELECT last(field4_float) from cpu  WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], maxTOut) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select last[string]
			{
				name:   "select last[string]",
				q:      fmt.Sprintf(`SELECT last(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], maxTOut) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},
			// select last[string] with time range
			{
				name:   "select last[string] with time range",
				q:      fmt.Sprintf(`SELECT last(field1_string) from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], maxTOut) && success
					success = ast.Equal(t, len(ck.Columns()), 1) && success
					success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},
			// select last[string] with aux and time range
			{
				name:   "select last[string] with aux",
				q:      fmt.Sprintf(`select last(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minT, maxTOut),
				tr:     util.TimeRange{Min: minT, Max: maxTOut},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val3", Type: influxql.String},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
					influxql.VarRef{Val: "val1", Type: influxql.String},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
						Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
						Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], maxTOut) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(1).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
					return success
				},
				// skip: true,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, nil, querySchema, keyCursors, false)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)

				agg, _ := executor.NewStreamAggregateTransform(
					[]hybridqp.RowDataType{tt.outputRowDataType},
					[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
				agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
				agg.GetOutputs()[0].Connect(outPutPort)

				chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])
				go func() {
					chunkReader.Work(ctx)
				}()
				go agg.Work(ctx)

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				if !tt.expect(chunks) {
					t.Errorf("`%s` failed", tt.name)
				}
			})
		}
	}
}

func Test_PreAggregation_MemTableData_SingleCall(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	pts, _, maxT := GenDataRecord(msNames, 5, 10000, time.Millisecond*100, startTime, true, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	sh.SetWriteColdDuration(1000 * time.Second)
	compWorker.RegisterShard(sh)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	startTimeMemTable := mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z")
	ptsOut, minTOut, maxTout := GenDataRecord(msNames, 5, 1000, time.Millisecond*100, startTimeMemTable, true, false, true)
	if err := sh.WriteRows(ptsOut, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 2)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []executor.Chunk) bool
	}{
		{
			name:   "select min[int]",
			q:      fmt.Sprintf(`SELECT min(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// // skip: true,
		},
		// select min[int] with time range
		{
			name:   "select min[int] with time range",
			q:      fmt.Sprintf(`SELECT min(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxT),
			tr:     util.TimeRange{Min: minTOut, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// skip: true,
		},
		// select min[float]
		{
			name:   "select min[float]",
			q:      fmt.Sprintf(`SELECT min(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 1.1) && success
				return success
			},
			// skip: true,
		},
		// select min[float] with time range
		{
			name:   "select min[float] with time range",
			q:      fmt.Sprintf(`SELECT min(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxT),
			tr:     util.TimeRange{Min: minTOut, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 1.1) && success
				return success
			},
			// skip: true,
		},
		{
			name:   "select max[int]",
			q:      fmt.Sprintf(`SELECT max(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			//// skip: true,
		},
		// select max[int] with time range
		{
			name:   "select max[int] with time range",
			q:      fmt.Sprintf(`SELECT max(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxT-time.Second.Nanoseconds()*30),
			tr:     util.TimeRange{Min: minTOut, Max: maxT - time.Second.Nanoseconds()*30},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select max[float]
		{
			name:   "select max[float]",
			q:      fmt.Sprintf(`SELECT max(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select max[float] with time range
		{
			name:   "select max[float] with time range",
			q:      fmt.Sprintf(`SELECT max(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxT-time.Second.Nanoseconds()*5),
			tr:     util.TimeRange{Min: minTOut, Max: maxT},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459200400000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		{
			name:   "select first[int]",
			q:      fmt.Sprintf(`SELECT first(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(1)) && success
				return success
			},
			// skip: true,
		},
		// select first[float]
		{
			name:   "select first[float]",
			q:      fmt.Sprintf(`SELECT first(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 1.1) && success
				return success
			},
			// skip: true,
		},
		// select first[string]
		{
			name:   "select first[string]",
			q:      fmt.Sprintf(`SELECT first(field1_string) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z").UnixNano()) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-0") && success
				return success
			},
			// skip: true,
		},
		// select first[int] with time range
		{
			name:   "select first[int] with time range",
			q:      fmt.Sprintf(`SELECT first(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select first[float] with time range
		{
			name:   "select first[float] with time range",
			q:      fmt.Sprintf(`SELECT first(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459215000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select first[string] with time range
		{
			name:   "select first[string] with time range",
			q:      fmt.Sprintf(`SELECT first(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*15, maxT-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459205000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		{
			// count to sum
			name:   "select count[int]",
			q:      fmt.Sprintf(`SELECT count(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200000000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(55000)) && success
				return success
			},
			// skip: true,
		},
		// select count[bool]
		{
			name:   "select count[bool]",
			q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val3", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(55000)) && success
				return success
			},
			// skip: true,
		},
		// select count[float]
		{
			name:   "select count[float]",
			q:      fmt.Sprintf(`SELECT count(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(55000)) && success
				return success
			},
			// skip: true,
		},
		// select count[string]
		{
			name:   "select count[string]",
			q:      fmt.Sprintf(`SELECT count(field1_string) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer}, // Be Integer
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				//success = ast.Equal(t, ck.Time()[0], int64(1609459200100000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(55000)) && success
				return success
			},
			// skip: true,
		},
		// select count[int] with time range
		{
			name:   "select count[int] with time range",
			q:      fmt.Sprintf(`SELECT count(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*30, maxT-time.Second.Nanoseconds()*30),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*30, Max: maxT - time.Second.Nanoseconds()*30},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(52020)) && success
				return success
			},
			// skip: true,
		},
		// select count[bool] with time range
		{
			name:   "select count[bool] with time range",
			q:      fmt.Sprintf(`SELECT count(field3_bool) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*10, maxT-time.Second.Nanoseconds()*10),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*10, Max: maxT - time.Second.Nanoseconds()*10},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val3", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.Boolean}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(54020)) && success
				return success
			},
			// skip: true,
		},
		// select count[float] with time range
		{
			name:   "select count[float] with time range",
			q:      fmt.Sprintf(`SELECT count(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*20, maxT-time.Second.Nanoseconds()*20),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*20, Max: maxT - time.Second.Nanoseconds()*20},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(53020)) && success
				return success
			},
			// skip: true,
		},
		// select count[string] with time range
		{
			name:   "select count[string] with time range",
			q:      fmt.Sprintf(`SELECT count(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(54520)) && success
				return success
			},
			// skip: true,
		},

		/* sum */
		// select sum[int]
		{
			name:   "select sum[int]",
			q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(165000)) && success
				return success
			},
			// skip: true,
		},
		// select sum[float]
		{
			name:   "select sum[float]",
			q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(181500.000000001)) && success
				return success
			},
			// skip: true,
		},
		// select sum[int] with time range
		{
			name:   "select sum[int] with time range",
			q:      fmt.Sprintf(`SELECT sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(163560)) && success
				return success
			},
			// skip: true,
		},
		// select sum[float] with time range
		{
			name:   "select sum[float] with time range",
			q:      fmt.Sprintf(`SELECT sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*25, maxT-time.Second.Nanoseconds()*25),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*25, Max: maxT - time.Second.Nanoseconds()*25},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, math.Round(ck.Column(0).FloatValue(0)), math.Round(173316)) && success
				return success
			},
			// skip: true,
		},
		/* last */
		// select last[int]
		{
			name:   "select last[int]",
			q:      fmt.Sprintf(`SELECT last(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxT) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select last[float]
		{
			name:   "select last[float]",
			q:      fmt.Sprintf(`SELECT last(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxT) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select last[string]
		{
			name:   "select last[string]",
			q:      fmt.Sprintf(`SELECT last(field1_string) from cpu`),
			tr:     util.TimeRange{Min: minTOut, Max: maxTout},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTout) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		// select last[int] with time range
		{
			name:   "select last[int] with time range",
			q:      fmt.Sprintf(`SELECT last(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTout),
			tr:     util.TimeRange{Min: minTOut, Max: maxTout},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTout) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				return success
			},
			// skip: true,
		},
		// select last[float] with time range
		{
			name:   "select last[float] with time range",
			q:      fmt.Sprintf(`SELECT last(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut, 1609462801030000000),
			tr:     util.TimeRange{Min: minTOut, Max: maxTout},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val4", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], int64(1609459300300000000)) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).FloatValue(0), 5.5) && success
				return success
			},
			// skip: true,
		},
		// select last[string] with time range
		{
			name:   "select last[string] with time range",
			q:      fmt.Sprintf(`SELECT last(field1_string) from cpu WHERE time>=%v AND time<=%v`, minTOut, maxTout),
			tr:     util.TimeRange{Min: minTOut, Max: maxTout},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTout) && success
				success = ast.Equal(t, len(ck.Columns()), 1) && success
				success = ast.Equal(t, ck.Column(0).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},
		// select last[string] with aux and time range
		{
			name:   "select last[string] with aux and time range",
			q:      fmt.Sprintf(`select last(field1_string), field2_int, field4_float, field1_string from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*15, maxTout-time.Second.Nanoseconds()*15),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*15, Max: maxTout + time.Second.Nanoseconds()*15},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.String},
				influxql.VarRef{Val: "val4", Type: influxql.Float},
				influxql.VarRef{Val: "val1", Type: influxql.String},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "field2_int", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "field1_string", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.VarRef{Val: "val2", Type: influxql.Integer},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.VarRef{Val: "val3", Type: influxql.String},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
				},
				{
					Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
					Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
				},
				{
					Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, ck.Time()[0], maxTout) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(5)) && success
				success = ast.Equal(t, ck.Column(1).StringValue(0), "test-test-test-test-4") && success
				success = ast.Equal(t, ck.Column(2).FloatValue(0), 5.5) && success
				success = ast.Equal(t, ck.Column(3).StringValue(0), "test-test-test-test-4") && success
				return success
			},
			// skip: true,
		},

		/* mean */
		// select mean[int]
		{
			name:   "select mean[int]",
			q:      fmt.Sprintf(`SELECT count(field2_int),sum(field2_int) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer},
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 2) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(55000)) && success
				success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(165000)) && success
				return success
			},
			// skip: true,
		},
		// select mean[float]
		{
			name:   "select mean[float]",
			q:      fmt.Sprintf(`SELECT count(field4_float),sum(field4_float) from cpu`),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer},
				influxql.VarRef{Val: "val2", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 2) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(55000)) && success
				success = ast.Equal(t, math.Round(ck.Column(1).FloatValue(0)), math.Round(181500.00000000105)) && success
				return success
			},
			// skip: true,
		},
		// select mean[int] with time range
		{
			name:   "select mean[int] with time range",
			q:      fmt.Sprintf(`SELECT count(field2_int),sum(field2_int) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer},
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 2) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(54520)) && success
				success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(163560)) && success
				return success
			},
			// skip: true,
		},
		// select mean[float] with time range
		{
			name:   "select mean[float] with time range",
			q:      fmt.Sprintf(`SELECT count(field4_float),sum(field4_float) from cpu WHERE time>=%v AND time<=%v`, minTOut+time.Second.Nanoseconds()*5, maxT-time.Second.Nanoseconds()*5),
			tr:     util.TimeRange{Min: minTOut + time.Second.Nanoseconds()*5, Max: maxT - time.Second.Nanoseconds()*5},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val1", Type: influxql.Integer},
				influxql.VarRef{Val: "val2", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val1", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Float},
				},
			},
			expect: func(chunks []executor.Chunk) bool {
				if len(chunks) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				ck := chunks[0]
				success := true
				success = ast.Equal(t, len(ck.Columns()), 2) && success
				success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(54520)) && success
				success = ast.Equal(t, math.Round(ck.Column(1).FloatValue(0)), math.Round(179916.0000000009)) && success
				return success
			},
			// skip: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			ctx := context.Background()
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

			cursors, _ := sh.CreateCursor(ctx, querySchema)
			var keyCursors []interface{}
			for _, cur := range cursors {
				keyCursors = append(keyCursors, cur)
			}
			chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, nil, querySchema, keyCursors, false)
			defer chunkReader.Release()

			// this is the output for this stmt
			outPutPort := executor.NewChunkPort(tt.outputRowDataType)

			agg, _ := executor.NewStreamAggregateTransform(
				[]hybridqp.RowDataType{tt.outputRowDataType},
				[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
			agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
			agg.GetOutputs()[0].Connect(outPutPort)

			chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])
			go func() {
				chunkReader.Work(ctx)
			}()
			go agg.Work(ctx)

			var closed bool
			chunks := make([]executor.Chunk, 0, 1)
			for {
				select {
				case v, ok := <-outPutPort.State:
					if !ok {
						closed = true
						break
					}
					chunks = append(chunks, v)
				}
				if closed {
					break
				}
			}
			if !tt.expect(chunks) {
				t.Errorf("`%s` failed", tt.name)
			}
		})
	}
}

var outputRowDataType_MultiCalls = hybridqp.NewRowDataTypeImpl(
	// count
	influxql.VarRef{Val: "val1", Type: influxql.Integer},
	influxql.VarRef{Val: "val2", Type: influxql.Integer},
	influxql.VarRef{Val: "val3", Type: influxql.Integer},
	influxql.VarRef{Val: "val4", Type: influxql.Integer},
	// sum min max
	influxql.VarRef{Val: "val5", Type: influxql.Integer},
	influxql.VarRef{Val: "val6", Type: influxql.Float},
	influxql.VarRef{Val: "val7", Type: influxql.Integer},
	influxql.VarRef{Val: "val8", Type: influxql.Float},
	influxql.VarRef{Val: "val9", Type: influxql.Integer},
	influxql.VarRef{Val: "val10", Type: influxql.Float},
	// first
	influxql.VarRef{Val: "val11", Type: influxql.String},
	influxql.VarRef{Val: "val12", Type: influxql.Integer},
	influxql.VarRef{Val: "val13", Type: influxql.Boolean},
	influxql.VarRef{Val: "val14", Type: influxql.Float},
	// first
	influxql.VarRef{Val: "val15", Type: influxql.String},
	influxql.VarRef{Val: "val16", Type: influxql.Integer},
	influxql.VarRef{Val: "val17", Type: influxql.Boolean},
	influxql.VarRef{Val: "val18", Type: influxql.Float},
)

var readerOps_MultiCalls = []hybridqp.ExprOptions{
	{
		Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val2", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "count", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val4", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val5", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val6", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val7", Type: influxql.String},
	}, {
		Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val8", Type: influxql.String},
	}, {
		Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val9", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val10", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val11", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val12", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val13", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val14", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field1_string", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val15", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val16", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_bool", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val17", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field4_float", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val18", Type: influxql.String},
	},
}

var aggOps_MultiCalls = []hybridqp.ExprOptions{
	{
		Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val1", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val1", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val2", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val3", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val4", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val4", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val5", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val5", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "sum", Args: []influxql.Expr{&influxql.VarRef{Val: "val6", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val6", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val7", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val7", Type: influxql.String},
	}, {
		Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val8", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val8", Type: influxql.String},
	}, {
		Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val9", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val9", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "max", Args: []influxql.Expr{&influxql.VarRef{Val: "val10", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val10", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val11", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val11", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val12", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val12", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val13", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val13", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "first", Args: []influxql.Expr{&influxql.VarRef{Val: "val14", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val14", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val15", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val15", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val16", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val16", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val17", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val17", Type: influxql.String},
	},
	{
		Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val18", Type: influxql.String}}},
		Ref:  influxql.VarRef{Val: "val18", Type: influxql.String},
	},
}

func Test_PreAggregation_FullData_MultiCalls(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z")
	// this is different from Test_PreAggregation_MissingData_MultiCalls
	pts, minT, maxT := GenDataRecord(msNames, 5, 10000, time.Millisecond*100, startTime, true, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 2)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****
	enableStates := []bool{true, false}
	for _, v := range enableStates {
		executor.EnableFileCursor(v)
		for _, tt := range []struct {
			name              string
			q                 string
			tr                util.TimeRange
			fields            map[string]influxql.DataType
			skip              bool
			outputRowDataType *hybridqp.RowDataTypeImpl
			readerOps         []hybridqp.ExprOptions
			aggOps            []hybridqp.ExprOptions
			expect            func(chunks []executor.Chunk) bool
		}{
			// select multi calls at the same time
			{
				name: "select multi calls at the same time",
				q: fmt.Sprintf(`select count(field1_string),count(field2_int),count(field3_bool),count(field4_float),
										 sum(field2_int),sum(field4_float),min(field2_int),min(field4_float),max(field2_int),max(field4_float),
										 first(field1_string),first(field2_int),first(field3_bool),first(field4_float),
 										 last(field1_string),last(field2_int),last(field3_bool),last(field4_float)
										 from cpu`),

				tr:                util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields:            fields,
				outputRowDataType: outputRowDataType_MultiCalls,
				readerOps:         readerOps_MultiCalls,
				aggOps:            aggOps_MultiCalls,
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462820030000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(50000)) && success
					success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(50000)) && success
					success = ast.Equal(t, ck.Column(2).IntegerValue(0), int64(50000)) && success
					success = ast.Equal(t, ck.Column(3).IntegerValue(0), int64(50000)) && success
					success = ast.Equal(t, ck.Column(4).IntegerValue(0), int64(150000)) && success
					success = ast.Equal(t, math.Round(ck.Column(5).FloatValue(0)), math.Round(165000.00000000105)) && success
					success = ast.Equal(t, ck.Column(6).IntegerValue(0), int64(1)) && success
					success = ast.Equal(t, ck.Column(7).FloatValue(0), 1.1) && success
					success = ast.Equal(t, ck.Column(8).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(9).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(10).StringValue(0), "test-test-test-test-0") && success
					success = ast.Equal(t, ck.Column(10).ColumnTime(0), int64(1609459200000000000)) && success
					success = ast.Equal(t, ck.Column(11).IntegerValue(0), int64(1)) && success
					success = ast.Equal(t, ck.Column(11).ColumnTime(0), int64(1609459200000000000)) && success
					//success = ast.Equal(t, ck.Column(12).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(12).ColumnTime(0), int64(1609459200000000000)) && success
					success = ast.Equal(t, ck.Column(13).FloatValue(0), 1.1) && success
					success = ast.Equal(t, ck.Column(13).ColumnTime(0), int64(1609459200000000000)) && success
					success = ast.Equal(t, ck.Column(14).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(14).ColumnTime(0), int64(1609460200300000000)) && success
					success = ast.Equal(t, ck.Column(15).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(15).ColumnTime(0), int64(1609460200300000000)) && success
					//success = ast.Equal(t, ck.Column(16).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(16).ColumnTime(0), int64(1609460200300000000)) && success
					success = ast.Equal(t, ck.Column(17).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(17).ColumnTime(0), int64(1609460200300000000)) && success
					return success
				},
			},
			// select multi calls at the same time with time range
			{
				name: "select multi calls at the same time with time range",
				q: fmt.Sprintf(`select count(field1_string),count(field2_int),count(field3_bool),count(field4_float),
										 sum(field2_int),sum(field4_float),min(field2_int),min(field4_float),max(field2_int),max(field4_float),
										 first(field1_string),first(field2_int),first(field3_bool),first(field4_float),
 										 last(field1_string),last(field2_int),last(field3_bool),last(field4_float)
										 from cpu WHERE time>=%v AND time<=%v`, minT, maxT),

				tr:                util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields:            fields,
				outputRowDataType: outputRowDataType_MultiCalls,
				readerOps:         readerOps_MultiCalls,
				aggOps:            aggOps_MultiCalls,
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609459299900000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(48520)) && success
					success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(48520)) && success
					success = ast.Equal(t, ck.Column(2).IntegerValue(0), int64(48520)) && success
					success = ast.Equal(t, ck.Column(3).IntegerValue(0), int64(48520)) && success
					success = ast.Equal(t, ck.Column(4).IntegerValue(0), int64(145560)) && success
					success = ast.Equal(t, math.Round(ck.Column(5).FloatValue(0)), math.Round(160116.00000000108)) && success
					success = ast.Equal(t, ck.Column(6).IntegerValue(0), int64(1)) && success
					success = ast.Equal(t, ck.Column(7).FloatValue(0), 1.1) && success
					success = ast.Equal(t, ck.Column(8).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(9).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(10).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(10).ColumnTime(0), int64(1609459215000000000)) && success
					success = ast.Equal(t, ck.Column(11).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(11).ColumnTime(0), int64(1609459215000000000)) && success
					//success = ast.Equal(t, ck.Column(12).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(12).ColumnTime(0), int64(1609459215000000000)) && success
					success = ast.Equal(t, ck.Column(13).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(13).ColumnTime(0), int64(1609459215000000000)) && success
					success = ast.Equal(t, ck.Column(14).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(14).ColumnTime(0), int64(1609460185300000000)) && success
					success = ast.Equal(t, ck.Column(15).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(15).ColumnTime(0), int64(1609460185300000000)) && success
					//success = ast.Equal(t, ck.Column(16).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(16).ColumnTime(0), int64(1609460185300000000)) && success
					success = ast.Equal(t, ck.Column(17).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(17).ColumnTime(0), int64(1609460185300000000)) && success
					return success
				},
				// skip: true,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, nil, querySchema, keyCursors, false)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)

				agg, _ := executor.NewStreamAggregateTransform(
					[]hybridqp.RowDataType{tt.outputRowDataType},
					[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
				agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
				agg.GetOutputs()[0].Connect(outPutPort)

				chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])
				go func() {
					chunkReader.Work(ctx)
				}()
				go agg.Work(ctx)

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				if !tt.expect(chunks) {
					t.Errorf("`%s` failed", tt.name)
				}
			})
		}
	}

}

func Test_PreAggregation_MissingData_MultiCalls(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T00:00:00Z")
	// this is different from Test_PreAggregation_FullData_MultiCalls
	pts, minT, maxT := GenDataRecord(msNames, 5, 10000, time.Millisecond*100, startTime, false, false, true)

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 2)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****
	enableStates := []bool{true, false}
	for _, v := range enableStates {
		executor.EnableFileCursor(v)
		for _, tt := range []struct {
			name              string
			q                 string
			tr                util.TimeRange
			fields            map[string]influxql.DataType
			skip              bool
			outputRowDataType *hybridqp.RowDataTypeImpl
			readerOps         []hybridqp.ExprOptions
			aggOps            []hybridqp.ExprOptions
			expect            func(chunks []executor.Chunk) bool
		}{
			// select multi calls at the same time
			{
				name: "select multi calls at the same time",
				q: fmt.Sprintf(`select count(field1_string),count(field2_int),count(field3_bool),count(field4_float),
										 sum(field2_int),sum(field4_float),min(field2_int),min(field4_float),max(field2_int),max(field4_float),
										 first(field1_string),first(field2_int),first(field3_bool),first(field4_float),
 										 last(field1_string),last(field2_int),last(field3_bool),last(field4_float),
										 mean(field2_int),mean(field4_float)
										 from cpu`),

				tr:                util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
				fields:            fields,
				outputRowDataType: outputRowDataType_MultiCalls,
				readerOps:         readerOps_MultiCalls,
				aggOps:            aggOps_MultiCalls,
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462820030000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(40000)) && success
					success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(50000)) && success
					success = ast.Equal(t, ck.Column(2).IntegerValue(0), int64(40000)) && success
					success = ast.Equal(t, ck.Column(3).IntegerValue(0), int64(40000)) && success
					success = ast.Equal(t, ck.Column(4).IntegerValue(0), int64(150000)) && success
					success = ast.Equal(t, math.Round(ck.Column(5).FloatValue(0)), math.Round(154000.000000001)) && success
					success = ast.Equal(t, ck.Column(6).IntegerValue(0), int64(1)) && success
					success = ast.Equal(t, ck.Column(7).FloatValue(0), 2.2) && success
					success = ast.Equal(t, ck.Column(8).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(9).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(10).StringValue(0), "test-test-test-test-1") && success
					success = ast.Equal(t, ck.Column(11).IntegerValue(0), int64(1)) && success
					//success = ast.Equal(t, ck.Column(12).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(13).FloatValue(0), 2.2) && success
					success = ast.Equal(t, ck.Column(14).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(15).IntegerValue(0), int64(5)) && success
					//success = ast.Equal(t, ck.Column(16).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(17).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
			// select multi calls at the same time with time range
			{
				name: "select multi calls at the same time with time range",
				q: fmt.Sprintf(`select count(field1_string),count(field2_int),count(field3_bool),count(field4_float),
										 sum(field2_int),sum(field4_float),min(field2_int),min(field4_float),max(field2_int),max(field4_float),
										 first(field1_string),first(field2_int),first(field3_bool),first(field4_float),
 										 last(field1_string),last(field2_int),last(field3_bool),last(field4_float),
										 mean(field2_int),mean(field4_float)
										 from cpu WHERE time>=%v AND time<=%v`, minT, maxT),

				tr:                util.TimeRange{Min: minT + time.Second.Nanoseconds()*15, Max: maxT - time.Second.Nanoseconds()*15},
				fields:            fields,
				outputRowDataType: outputRowDataType_MultiCalls,
				readerOps:         readerOps_MultiCalls,
				aggOps:            aggOps_MultiCalls,
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					//success = ast.Equal(t, ck.Time()[0], int64(1609462820030000000)) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(38816)) && success
					success = ast.Equal(t, ck.Column(1).IntegerValue(0), int64(48520)) && success
					success = ast.Equal(t, ck.Column(2).IntegerValue(0), int64(38816)) && success
					success = ast.Equal(t, ck.Column(3).IntegerValue(0), int64(38816)) && success
					success = ast.Equal(t, ck.Column(4).IntegerValue(0), int64(145560)) && success
					success = ast.Equal(t, math.Round(ck.Column(5).FloatValue(0)), math.Round(149441.600000001)) && success
					success = ast.Equal(t, ck.Column(6).IntegerValue(0), int64(1)) && success
					success = ast.Equal(t, ck.Column(7).FloatValue(0), 2.2) && success
					success = ast.Equal(t, ck.Column(8).IntegerValue(0), int64(5)) && success
					success = ast.Equal(t, ck.Column(9).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(10).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(11).IntegerValue(0), int64(5)) && success
					//success = ast.Equal(t, ck.Column(12).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(13).FloatValue(0), 5.5) && success
					success = ast.Equal(t, ck.Column(14).StringValue(0), "test-test-test-test-4") && success
					success = ast.Equal(t, ck.Column(15).IntegerValue(0), int64(5)) && success
					//success = ast.Equal(t, ck.Column(16).BooleanValue(0), true) && success
					success = ast.Equal(t, ck.Column(17).FloatValue(0), 5.5) && success
					return success
				},
				// skip: true,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, nil, querySchema, keyCursors, false)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)

				agg, _ := executor.NewStreamAggregateTransform(
					[]hybridqp.RowDataType{tt.outputRowDataType},
					[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
				agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
				agg.GetOutputs()[0].Connect(outPutPort)

				chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])
				go func() {
					chunkReader.Work(ctx)
				}()
				go agg.Work(ctx)

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				if !tt.expect(chunks) {
					t.Errorf("`%s` failed", tt.name)
				}
			})
		}
	}
}

func MustParseSelectStatement(s string) *influxql.SelectStatement {
	p := influxql.NewParser(strings.NewReader(s))
	defer p.Release()
	stmt, err := p.ParseStatement()
	if err != nil {
		panic(err)
	}
	return stmt.(*influxql.SelectStatement)
}

type mockShardGroup struct {
	sh         Shard
	Fields     map[string]influxql.DataType
	Dimensions []string
}

func (sg *mockShardGroup) CreateLogicalPlan(ctx context.Context, sources influxql.Sources, schema *executor.QuerySchema) (hybridqp.QueryNode, error) {
	return sg.sh.CreateLogicalPlan(ctx, sources, schema)
}

func (sg *mockShardGroup) LogicalPlanCost(m *influxql.Measurement, opt query.ProcessorOptions) (hybridqp.LogicalPlanCost, error) {
	return hybridqp.LogicalPlanCost{}, nil
}

func (sg *mockShardGroup) FieldDimensions(m *influxql.Measurement) (fields map[string]influxql.DataType, dimensions map[string]struct{}, schema *influxql.Schema, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for f, typ := range sg.Fields {
		fields[f] = typ
	}
	for _, d := range sg.Dimensions {
		dimensions[d] = struct{}{}
	}
	return fields, dimensions, schema, nil
}

func (sg *mockShardGroup) MapType(m *influxql.Measurement, field string) influxql.DataType {
	if typ, ok := sg.Fields[field]; ok {
		return typ
	}
	for _, d := range sg.Dimensions {
		if d == field {
			return influxql.Tag
		}
	}
	return influxql.Unknown
}

func (sg *mockShardGroup) MapTypeBatch(measurement *influxql.Measurement, field map[string]*influxql.FieldNameSpace, schema *influxql.Schema) error {
	for k := range field {
		field[k].DataType = sg.Fields[k]
	}
	return nil
}

func (*mockShardGroup) Close() error {
	return nil
}

func TestAppendColumnTimes(t *testing.T) {
	var columnTimes = []int64{1, 0, 9}
	var bitmap = []bool{true, false, true}
	column := executor.NewColumnImpl(influxql.Integer)
	recCol := &record.ColVal{}
	recCol.AppendInteger(1)
	recCol.AppendIntegerNull()
	recCol.AppendInteger(9)
	AppendColumnTimes(bitmap, column, columnTimes, recCol)
	if column.ColumnTimes()[0] != columnTimes[0] ||
		column.ColumnTimes()[1] != columnTimes[2] {
		t.Errorf("unexpected error")
	}
}

func TestMatchPreAgg(t *testing.T) {
	fields := []*influxql.Field{&influxql.Field{
		Expr: &influxql.Call{
			Name: "sum",
			Args: []influxql.Expr{&influxql.VarRef{Val: "a", Type: influxql.Integer}},
		},
	}}
	opt := &query.ProcessorOptions{
		Condition: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "a", Type: influxql.Integer},
			Op:  influxql.EQ,
			RHS: &influxql.StringLiteral{Val: "1"},
		},
	}
	schema := executor.NewQuerySchema(fields, []string{"a"}, opt, nil)
	if matchPreAgg(schema, &idKeyCursorContext{}) {
		t.Fatal()
	}
}

func TestRecTransToChunk(t *testing.T) {
	ck := ChunkReader{
		transColumnFun: &transColAuxFun,
	}
	schema := record.Schemas{
		record.Field{Type: influx.Field_Type_Int, Name: "int"},
		record.Field{Type: influx.Field_Type_Float, Name: "float"},
		record.Field{Type: influx.Field_Type_Boolean, Name: "boolean"},
		record.Field{Type: influx.Field_Type_String, Name: "string"},
		record.Field{Type: influx.Field_Type_Int, Name: "time"},
	}
	fields := []influxql.VarRef{
		influxql.VarRef{Val: "int", Type: influxql.Integer},
		influxql.VarRef{Val: "float", Type: influxql.Float},
		influxql.VarRef{Val: "boolean", Type: influxql.Boolean},
		influxql.VarRef{Val: "string", Type: influxql.Tag},
	}
	rt := hybridqp.NewRowDataTypeImpl(fields...)
	oldRec := genRowRec(schema,
		[]int{1}, []int64{200},
		[]int{1}, []float64{2.3},
		[]int{1}, []string{"hello"},
		[]int{1}, []bool{false},
		[]int64{1})
	chunk := executor.NewChunkBuilder(rt).NewChunk("mst")
	fieldItemIndex := make([]item, 0)
	for i := 0; i < 4; i++ {
		fieldItemIndex = append(fieldItemIndex, item{index: i})
	}
	ck.fieldItemIndex = fieldItemIndex
	if ck.transToChunk(oldRec, chunk) != nil {
		t.Error("transToChunk error")
	}
	if chunk.Len() != 1 {
		t.Error("transToChunk result error")
	}
}

func TestReadLastFromPreAgg(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &ChunkReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	pts, minT, maxT := GenDataRecord(msNames, 1, 100, time.Second, startTime, true, false, true)

	for i := range pts {
		pts[i].Fields = append([]influx.Field{}, pts[i].Fields...)
		for j := range pts[i].Fields {
			f := &pts[i].Fields[j]
			switch f.Type {
			case influx.Field_Type_Int, influx.Field_Type_Float:
				f.NumValue = float64(i + 1)
			}
		}
	}

	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)
	defer sh.Close()
	defer sh.indexBuilder.Close()
	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}
	// end ****
	enableStates := []bool{true, false}
	for _, v := range enableStates {
		executor.EnableFileCursor(v)
		for _, tt := range []struct {
			name              string
			q                 string
			tr                util.TimeRange
			fields            map[string]influxql.DataType
			skip              bool
			outputRowDataType *hybridqp.RowDataTypeImpl
			readerOps         []hybridqp.ExprOptions
			aggOps            []hybridqp.ExprOptions
			expect            func(chunks []executor.Chunk) bool
		}{
			{
				name:   "read last[int] with aux",
				q:      fmt.Sprintf(`SELECT last(field2_int),field4_float from cpu `),
				tr:     util.TimeRange{Min: minT, Max: maxT},
				fields: fields,
				outputRowDataType: hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "val2", Type: influxql.Integer},
					influxql.VarRef{Val: "val4", Type: influxql.Float},
				),
				readerOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "field4_float", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				aggOps: []hybridqp.ExprOptions{
					{
						Expr: &influxql.Call{Name: "last", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
						Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
					},
					{
						Expr: &influxql.VarRef{Val: "val4", Type: influxql.Float},
						Ref:  influxql.VarRef{Val: "val4", Type: influxql.Float},
					},
				},
				expect: func(chunks []executor.Chunk) bool {
					if len(chunks) != 1 {
						t.Errorf("The result should be 1 chunk")
					}
					ck := chunks[0]
					success := true
					success = ast.Equal(t, ck.Time()[0], maxT) && success
					success = ast.Equal(t, len(ck.Columns()), 2) && success
					success = ast.Equal(t, ck.Column(0).IntegerValue(0), int64(100)) && success
					success = ast.Equal(t, ck.Column(1).FloatValue(0), float64(100)) && success
					return success
				},
				// skip: true,
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				if tt.skip {
					t.Skipf("SKIP:: %s", tt.name)
				}
				ctx := context.Background()
				// parse stmt and opt
				stmt := MustParseSelectStatement(tt.q)
				stmt, _ = stmt.RewriteFields(shardGroup, true, false)
				stmt.OmitTime = true
				sopt := query.SelectOptions{ChunkSize: 1024}
				RemoveTimeCondition(stmt)
				opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
				source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
				opt.Name = msNames[0]
				opt.Sources = source
				opt.StartTime = tt.tr.Min
				opt.EndTime = tt.tr.Max
				querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)

				cursors, _ := sh.CreateCursor(ctx, querySchema)
				var keyCursors []interface{}
				for _, cur := range cursors {
					keyCursors = append(keyCursors, cur)
				}
				chunkReader := NewChunkReader(tt.outputRowDataType, tt.readerOps, nil, querySchema, keyCursors, false)
				defer chunkReader.Release()

				// this is the output for this stmt
				outPutPort := executor.NewChunkPort(tt.outputRowDataType)

				agg, _ := executor.NewStreamAggregateTransform(
					[]hybridqp.RowDataType{tt.outputRowDataType},
					[]hybridqp.RowDataType{tt.outputRowDataType}, tt.aggOps, &opt, false)
				agg.GetInputs()[0].Connect(chunkReader.GetOutputs()[0])
				agg.GetOutputs()[0].Connect(outPutPort)

				chunkReader.GetOutputs()[0].Connect(agg.GetInputs()[0])
				go func() {
					chunkReader.Work(ctx)
				}()
				go agg.Work(ctx)

				var closed bool
				chunks := make([]executor.Chunk, 0, 1)
				for {
					select {
					case v, ok := <-outPutPort.State:
						if !ok {
							closed = true
							break
						}
						chunks = append(chunks, v)
					}
					if closed {
						break
					}
				}
				if !tt.expect(chunks) {
					t.Fail()
				}
			})
		}
	}
}
func Test_CreateLogicalPlan(t *testing.T) {
	testDir := t.TempDir()
	executor.RegistryTransformCreator(&executor.LogicalTSSPScan{}, &TsspSequenceReader{})
	msNames := []string{"cpu"}
	startTime := mustParseTime(time.RFC3339Nano, "2021-01-01T01:00:00Z")
	//pts, minT, maxT := GenDataRecord_FullFields(msNames, 5, 10000, time.Millisecond*10, startTime)
	pts, _, _ := GenDataRecord(msNames, 5, 2000, time.Second, startTime, true, false, true)
	fields := map[string]influxql.DataType{
		"field2_int":    influxql.Integer,
		"field3_bool":   influxql.Boolean,
		"field4_float":  influxql.Float,
		"field1_string": influxql.String,
	}

	// **** start write data to the shard.
	sh, _ := createShard("db0", "rp0", 1, testDir, config.TSSTORE)

	if err := sh.WriteRows(pts, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 1)
	startTime2 := mustParseTime(time.RFC3339Nano, "2021-01-01T12:00:00Z")
	pts2, _, _ := GenDataRecord(msNames, 5, 2000, time.Millisecond*10, startTime2, true, false, true)
	if err := sh.WriteRows(pts2, nil); err != nil {
		t.Fatal(err)
	}
	sh.ForceFlush()
	time.Sleep(time.Second * 1)

	shardGroup := &mockShardGroup{
		sh:     sh,
		Fields: fields,
	}

	for _, tt := range []struct {
		name              string
		q                 string
		tr                util.TimeRange
		fields            map[string]influxql.DataType
		skip              bool
		outputRowDataType *hybridqp.RowDataTypeImpl
		readerOps         []hybridqp.ExprOptions
		aggOps            []hybridqp.ExprOptions
		expect            func(chunks []*executor.SeriesRecord) bool
	}{
		/* min */
		// select min[int]
		{
			name:   "read last[int] with aux",
			q:      fmt.Sprintf(`SELECT last(field2_int),field4_float from cpu `),
			tr:     util.TimeRange{Min: influxql.MinTime, Max: influxql.MaxTime},
			fields: fields,
			outputRowDataType: hybridqp.NewRowDataTypeImpl(
				influxql.VarRef{Val: "val2", Type: influxql.Integer},
				influxql.VarRef{Val: "val3", Type: influxql.Float},
			),
			readerOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field2_int", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "field3_float", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			aggOps: []hybridqp.ExprOptions{
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val2", Type: influxql.Integer}}},
					Ref:  influxql.VarRef{Val: "val2", Type: influxql.Integer},
				},
				{
					Expr: &influxql.Call{Name: "min", Args: []influxql.Expr{&influxql.VarRef{Val: "val3", Type: influxql.Float}}},
					Ref:  influxql.VarRef{Val: "val3", Type: influxql.Float},
				},
			},
			expect: func(rec []*executor.SeriesRecord) bool {
				if len(rec) != 1 {
					t.Errorf("The result should be 1 chunk")
				}
				success := true
				return success
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			// parse stmt and opt
			stmt := MustParseSelectStatement(tt.q)
			stmt, _ = stmt.RewriteFields(shardGroup, true, false)
			stmt.OmitTime = true
			sopt := query.SelectOptions{ChunkSize: 1024, MaxQueryParallel: 0}
			RemoveTimeCondition(stmt)
			opt, _ := query.NewProcessorOptionsStmt(stmt, sopt)
			source := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp0", Name: msNames[0]}}
			source1 := influxql.Sources{&influxql.Measurement{Database: "db0", RetentionPolicy: "rp1", Name: msNames[0]}}
			source = append(source, source1[0])

			opt.Name = msNames[0]
			opt.Sources = source
			opt.StartTime = tt.tr.Min
			opt.EndTime = tt.tr.Max
			querySchema := executor.NewQuerySchema(stmt.Fields, stmt.ColumnNames(), &opt, nil)
			ctx := context.Background()
			cursorsN := 0
			srcCursors := make([][]comm.KeyCursor, 0, len(source))
			srcCursors1 := make([][]comm.KeyCursor, 0, len(source))
			for _, source := range opt.Sources {
				mm, ok := source.(*influxql.Measurement)
				if !ok {
					panic(fmt.Sprintf("%v not a measurement", source.String()))
				}
				querySchema.Options().(*query.ProcessorOptions).Name = mm.Name

				cursors, err := sh.CreateCursor(ctx, querySchema) // source
				require.Nil(t, err)

				if len(cursors) > 0 {
					cursorsN += len(cursors)
					srcCursors = append(srcCursors, cursors)
				}
			}
			srcCursors1 = append(srcCursors1, srcCursors...)
			plan1, err1 := buildMultiSourcePlan(srcCursors, cursorsN)
			require.NotNil(t, plan1)
			require.Nil(t, err1)
			plan2, err2 := buildOneSourcePlan(srcCursors[0])
			require.NotNil(t, plan2)
			require.Nil(t, err2)
			plan3, err3 := sh.CreateLogicalPlan(ctx, source, querySchema)
			plan := plan3.(*executor.LogicalDummyShard)
			for _, r := range plan.Readers() {
				for _, rr := range r {
					cur := rr.(comm.KeyCursor)
					cur.Close()
				}
			}
			require.Nil(t, err3)
			sh.immTables.CompactionDisable()
			for _, srcCur := range srcCursors1 {
				for _, cur := range srcCur {
					cur.Close()
				}
			}
			err := closeShard(sh)
			require.Nil(t, err)
			_, err4 := sh.CreateLogicalPlan(ctx, opt.Sources, querySchema)
			require.Contains(t, err4.Error(), "shard closed 1")
		})
	}
}
