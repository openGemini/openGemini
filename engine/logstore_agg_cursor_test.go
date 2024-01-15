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
package engine

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func TestAggCursorRead(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"1", "2", "3", "4", "5", "6"}
	err := WriteSfsData(tmpDir, contents, true)
	if err != nil {
		t.Fatal("write data failed")
	}
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions: []string{},
		Ascending:  true,
		ChunkSize:  100,
		StartTime:  math.MinInt64,
		EndTime:    math.MaxInt64,
		Condition:  nil,
	}
	fields := make(influxql.Fields, 0, 2)
	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "tag",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "content",
				Type: influxql.String,
			},
			Alias: "",
		},
	)
	schema := executor.NewQuerySchema(fields, []string{"tag", "content"}, &opt, nil)
	e := &Engine{}
	ctx, err := e.InitLogStoreCtx(schema)
	cursor, _ := NewLogStoreAggCursor(nil, tmpDir, 4, ctx, nil, schema)
	_, span := tracing.NewTrace("root")
	cursor.StartSpan(span)
	a, _, _ := cursor.NextAggData()
	if a.RowNums() != 6 {
		t.Error("get error data")
	}
	a, _, _ = cursor.NextAggData()
	if a != nil {
		t.Error("get error data")
	}
	cursor.StartSpan(nil)
	s := cursor.GetSchema()
	if len(s) != 3 {
		t.Error("get error schema")
	}
	err = cursor.Close()
	if err != nil {
		t.Fatal("close err")
	}
}

func TestAggCursorGetMetaByNext(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"1", "2", "3", "4", "5", "6"}
	err := WriteSfsData(tmpDir, contents, true)
	if err != nil {
		t.Fatal("write data failed")
	}
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:     []string{},
		Ascending:      true,
		ChunkSize:      100,
		StartTime:      math.MinInt64,
		EndTime:        math.MaxInt64,
		Condition:      nil,
		IterID:         2,
		LogQueryCurrId: strconv.Itoa(int(time.Now().UnixNano())),
	}
	fields := make(influxql.Fields, 0, 2)
	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "tag",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "content",
				Type: influxql.String,
			},
			Alias: "",
		},
	)
	schema := executor.NewQuerySchema(fields, []string{"tag", "content"}, &opt, nil)

	e := &Engine{}
	ctx, err := e.InitLogStoreCtx(schema)
	cursor, _ := NewLogStoreAggCursor(nil, tmpDir, 4, ctx, nil, schema)
	_, span := tracing.NewTrace("root")
	cursor.StartSpan(span)
	if cursor == nil {
		t.Error("get cursor data")
	}
	a, _, _ := cursor.NextAggData()
	if a.RowNums() != 0 {
		t.Error("get error data")
	}
	err = cursor.Close()
	if err != nil {
		t.Fatal("close err")
	}
}

func TestAggCursorSinkPlan(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"1", "2", "3", "4", "5", "6"}
	err := WriteSfsData(tmpDir, contents, true)
	if err != nil {
		t.Fatal("write data failed")
	}
	fields := make(influxql.Fields, 0, 2)
	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "tag",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "content",
				Type: influxql.String,
			},
			Alias: "",
		},
	)
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:     []string{},
		Ascending:      true,
		ChunkSize:      100,
		StartTime:      math.MinInt64,
		EndTime:        math.MaxInt64,
		Condition:      nil,
		IterID:         2,
		LogQueryCurrId: strconv.Itoa(int(time.Now().UnixNano())),
	}
	schema := executor.NewQuerySchema(fields, []string{"tag", "content"}, &opt, nil)

	planBuilder := executor.NewLogicalPlanBuilderImpl(schema)

	var plan hybridqp.QueryNode
	if plan, err = planBuilder.CreateSeriesPlan(); err != nil {
		t.Error(err.Error())
	}

	e := &Engine{}
	ctx, err := e.InitLogStoreCtx(schema)
	cursor, _ := NewLogStoreAggCursor(nil, tmpDir, 4, ctx, nil, schema)
	if cursor == nil {
		t.Error("get cursor data")
	}
	cursor.SinkPlan(plan.Children()[0])
	err = cursor.Close()
	if err != nil {
		t.Fatal("close err")
	}
}

func TestAggCursorGetMetaByError(t *testing.T) {
	tmpDir := t.TempDir()
	config.SetSFSConfig(tmpDir)
	contents := []string{"1", "2", "3", "4", "5", "6"}
	err := WriteSfsData(tmpDir, contents, true)
	if err != nil {
		t.Fatal("write data failed")
	}
	opt := query.ProcessorOptions{
		Interval: hybridqp.Interval{
			Duration: 10 * time.Nanosecond,
		},
		Dimensions:     []string{},
		Ascending:      true,
		ChunkSize:      100,
		StartTime:      math.MinInt64,
		EndTime:        math.MaxInt64,
		Condition:      nil,
		IterID:         1,
		LogQueryCurrId: strconv.Itoa(int(time.Now().UnixNano())),
	}
	fields := make(influxql.Fields, 0, 2)
	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "tag",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "content",
				Type: influxql.String,
			},
			Alias: "",
		},
	)
	schema := executor.NewQuerySchema(fields, []string{"tag", "content"}, &opt, nil)

	e := &Engine{}
	ctx, err := e.InitLogStoreCtx(schema)
	cursor, _ := NewLogStoreAggCursor(nil, tmpDir, 4, ctx, nil, schema)
	if cursor == nil {
		t.Error("get cursor data")
	}

	cursor, err = NewLogStoreAggCursor(nil, tmpDir+"test", 4, ctx, nil, schema)
	if cursor == nil {
		t.Error("get cursor data")
	}
	_, _, err = cursor.NextAggData()
	cursor.SetSchema(ctx.schema)
	if err == nil {
		t.Error("get wrong cursor")
	}
	if cursor.Name() != "LogStoreAggCursor" {
		t.Error("get wrong name")
	}
}
