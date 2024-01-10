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

package executor_test

import (
	_ "net/http/pprof"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/services/castor"
)

func TestMockTSDBSystem(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sql           string
		ddl           func(*Catalog) error
		dml           func(*Storage) error
		validator     func([]executor.Chunk)
		intoValidator func(database, retentionPolicy string, points []influx.Row)
	}{
		{
			name: "Simple Select",
			sql:  "SELECT t,v FROM db0.rp0.mst0",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{1, 2, 3})
				assert.Equal(t, results[0].Columns()[0].StringValuesV2(make([]string, 0)), []string{"a", "a", "a"})
				assert.Equal(t, results[0].Columns()[1].IntegerValues(), []int64{0, 1, 2})
			},
		},
		{
			name: "preagg orderby time desc optimize",
			sql:  "SELECT count(a) as a from db0.rp0.mst0 order by time desc",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["a"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String}, influxql.VarRef{Val: "a", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTime(0)
				chunk1.Column(0).AppendStringValue("a")
				chunk1.Column(0).AppendManyNotNil(1)
				chunk1.Column(1).AppendIntegerValue(5)
				chunk1.Column(1).AppendManyNotNil(1)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{0})
				assert.Equal(t, results[0].Columns()[1].IntegerValues(), []int64{1})
			},
		},
		{
			name: "Multi-Table SubQuery Select",
			sql:  "SELECT t,v FROM (SELECT t,v FROM db0.rp0.mst0, db0.rp0.mst0) GROUP BY t",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)

				chunk2 := builder.NewChunk("mst0")
				chunk2.AppendTimes([]int64{1, 2, 3})
				chunk2.Column(0).AppendStringValues([]string{"b", "b", "b"})
				chunk2.Column(0).AppendManyNotNil(3)
				chunk2.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk2.Column(1).AppendManyNotNil(3)
				pts2 := influx.PointTags{influx.Tag{Key: "t", Value: "b"}}
				s.Write("db0.rp0.mst0", &pts2, chunk2)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{1, 1, 2, 2, 3, 3, 1, 1, 2, 2, 3, 3})
				assert.Equal(t, results[0].Columns()[0].StringValuesV2(make([]string, 0)),
					[]string{"a", "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "b"})
				assert.Equal(t, results[0].Columns()[1].IntegerValues(),
					[]int64{0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tsdb := NewTSDBSystem()
			if err := tsdb.DDL(tc.ddl); err != nil {
				t.Error(err)
			}
			if err := tsdb.DML(tc.dml); err != nil {
				t.Error(err)
			}
			if err := tsdb.ExecSQL(tc.sql, tc.validator, tc.intoValidator); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestSelectInto(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sql           string
		ddl           func(*Catalog) error
		dml           func(*Storage) error
		validator     func([]executor.Chunk)
		intoValidator func(database, retentionPolicy string, points []influx.Row)
	}{
		{
			name: "select into without group by",
			sql:  "SELECT t,v INTO db0.rp0.mst1 FROM db0.rp0.mst0",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Len(), 1)
				assert.Equal(t, results[0].Column(0).IntegerValue(0), int64(3))
			},
			intoValidator: func(database, retentionPolicy string, points []influx.Row) {
				assert.Equal(t, len(points), 3)
				assert.Equal(t, database, "db0")
				assert.Equal(t, retentionPolicy, "rp0")
				for _, point := range points {
					assert.Equal(t, point.Name, "mst1")
					assert.Equal(t, len(point.Fields), 2)
				}
			},
		},
		{
			name: "select into with group by",
			sql:  "SELECT v INTO db0.rp0.mst1 FROM db0.rp0.mst0 group by t",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Len(), 1)
				assert.Equal(t, results[0].Column(0).IntegerValue(0), int64(3))
			},
			intoValidator: func(database, retentionPolicy string, points []influx.Row) {
				assert.Equal(t, len(points), 3)
				assert.Equal(t, database, "db0")
				assert.Equal(t, retentionPolicy, "rp0")
				for _, point := range points {
					assert.Equal(t, point.Name, "mst1")
					assert.Equal(t, len(point.Fields), 1)
					assert.Equal(t, len(point.Tags), 1)
				}
			},
		},
		{
			name: "select into with aggregation",
			sql:  "SELECT sum(v) INTO db0.rp0.mst1 FROM db0.rp0.mst0 group by t",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Len(), 1)
				assert.Equal(t, results[0].Column(0).IntegerValue(0), int64(1))
			},
			intoValidator: func(database, retentionPolicy string, points []influx.Row) {
				assert.Equal(t, len(points), 1)
				assert.Equal(t, database, "db0")
				assert.Equal(t, retentionPolicy, "rp0")
				for _, point := range points {
					assert.Equal(t, point.Name, "mst1")
					assert.Equal(t, len(point.Fields), 1)
					assert.Equal(t, point.Fields[0].NumValue, float64(3))
					assert.Equal(t, len(point.Tags), 1)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tsdb := NewTSDBSystem()
			if err := tsdb.DDL(tc.ddl); err != nil {
				t.Error(err)
			}
			if err := tsdb.DML(tc.dml); err != nil {
				t.Error(err)
			}
			if err := tsdb.ExecSQL(tc.sql, tc.validator, tc.intoValidator); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestPercentileOGSketch(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sql           string
		ddl           func(*Catalog) error
		dml           func(*Storage) error
		validator     func([]executor.Chunk)
		intoValidator func(database, retentionPolicy string, points []influx.Row)
	}{
		{
			name: "percentile_approx(*) group by time",
			sql: "SELECT percentile_approx(v_int, 50, 5) as v_int, percentile_approx(v_float, 50, 5) as v_float " +
				"from db0.rp0.mst0 " +
				"where time >= 1000000000 and time <= 10000000000 " +
				"group by time(4s)",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v_int"] = influxql.Integer
				dataTypes["v_float"] = influxql.Float
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v_int", Type: influxql.Integer},
					influxql.VarRef{Val: "v_float", Type: influxql.Float})

				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1000000000, 2000000000, 3000000000, 4000000000, 5000000000})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(5)
				chunk1.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
				chunk1.Column(1).AppendManyNotNil(5)
				chunk1.Column(2).AppendFloatValues([]float64{5, 4, 3, 2, 1})
				chunk1.Column(2).AppendManyNotNil(5)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)

				chunk2 := builder.NewChunk("mst0")
				chunk2.AppendTimes([]int64{6000000000, 7000000000, 8000000000, 9000000000, 10000000000})
				chunk2.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk2.Column(0).AppendManyNotNil(5)
				chunk2.Column(1).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
				chunk2.Column(1).AppendManyNotNil(5)
				chunk2.Column(2).AppendFloatValues([]float64{10, 9, 8, 7, 6})
				chunk2.Column(2).AppendManyNotNil(5)
				s.Write("db0.rp0.mst0", &pts1, chunk2)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{0, 4000000000, 8000000000})
				assert.Equal(t, results[0].Columns()[0].IntegerValues(), []int64{2, 5, 9})
				assert.Equal(t, results[0].Columns()[1].FloatValues(), []float64{4, 5.5, 7})
			},
		},
		{
			name: "percentile_ogsketch(*) group by time",
			sql: "SELECT percentile_ogsketch(v_int, 50, 5) as v_int, percentile_ogsketch(v_float, 50, 5) as v_float " +
				"from db0.rp0.mst0 " +
				"where time >= 1000000000 and time <= 10000000000 " +
				"group by time(4s)",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v_int"] = influxql.Integer
				dataTypes["v_float"] = influxql.Float
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v_int", Type: influxql.Integer},
					influxql.VarRef{Val: "v_float", Type: influxql.Float})

				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1000000000, 2000000000, 3000000000, 4000000000, 5000000000})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(5)
				chunk1.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
				chunk1.Column(1).AppendManyNotNil(5)
				chunk1.Column(2).AppendFloatValues([]float64{5, 4, 3, 2, 1})
				chunk1.Column(2).AppendManyNotNil(5)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)

				chunk2 := builder.NewChunk("mst0")
				chunk2.AppendTimes([]int64{6000000000, 7000000000, 8000000000, 9000000000, 10000000000})
				chunk2.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk2.Column(0).AppendManyNotNil(5)
				chunk2.Column(1).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
				chunk2.Column(1).AppendManyNotNil(5)
				chunk2.Column(2).AppendFloatValues([]float64{10, 9, 8, 7, 6})
				chunk2.Column(2).AppendManyNotNil(5)
				s.Write("db0.rp0.mst0", &pts1, chunk2)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{0, 4000000000, 8000000000})
				assert.Equal(t, results[0].Columns()[0].IntegerValues(), []int64{2, 5, 9})
				assert.Equal(t, results[0].Columns()[1].FloatValues(), []float64{4, 5.5, 7})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tsdb := NewTSDBSystem()
			if err := tsdb.DDL(tc.ddl); err != nil {
				t.Error(err)
			}
			if err := tsdb.DML(tc.dml); err != nil {
				t.Error(err)
			}
			if err := tsdb.ExecSQL(tc.sql, tc.validator, tc.intoValidator); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestFillNull(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sql           string
		ddl           func(*Catalog) error
		dml           func(*Storage) error
		validator     func([]executor.Chunk)
		intoValidator func(database, retentionPolicy string, points []influx.Row)
	}{
		{
			name: "select count(v_int) from mst where time < 5 group by time",
			sql: "SELECT count(v_int) as v_int " +
				"from db0.rp0.mst0 " +
				"where time <= 5 " +
				"group by time(1ns)",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v_int"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v_int", Type: influxql.Integer})

				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3, 4, 5})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(5)
				chunk1.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
				chunk1.Column(1).AppendManyNotNil(5)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)

				chunk2 := builder.NewChunk("mst0")
				chunk2.AppendTimes([]int64{6, 7, 8, 9, 10})
				chunk2.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk2.Column(0).AppendManyNotNil(5)
				chunk2.Column(1).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
				chunk2.Column(1).AppendManyNotNil(5)
				s.Write("db0.rp0.mst0", &pts1, chunk2)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{1, 2, 3, 4, 5, 6})
				assert.Equal(t, results[0].Columns()[0].IntegerValues(), []int64{1, 1, 1, 1, 1, 5})
			},
		},
		{
			name: "select count(v_int) from mst where time < 1 and time > 10  group by time",
			sql: "SELECT count(v_int) as v_int " +
				"from db0.rp0.mst0 " +
				"where time < 1 and time > 10 " +
				"group by time(1ns)",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v_int"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v_int", Type: influxql.Integer})

				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3, 4, 5})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(5)
				chunk1.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
				chunk1.Column(1).AppendManyNotNil(5)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)

				chunk2 := builder.NewChunk("mst0")
				chunk2.AppendTimes([]int64{6, 7, 8, 9, 10})
				chunk2.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk2.Column(0).AppendManyNotNil(5)
				chunk2.Column(1).AppendIntegerValues([]int64{6, 7, 8, 9, 10})
				chunk2.Column(1).AppendManyNotNil(5)
				s.Write("db0.rp0.mst0", &pts1, chunk2)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{1, 6})
				assert.Equal(t, results[0].Columns()[0].IntegerValues(), []int64{5, 5})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tsdb := NewTSDBSystem()
			if err := tsdb.DDL(tc.ddl); err != nil {
				t.Error(err)
			}
			if err := tsdb.DML(tc.dml); err != nil {
				t.Error(err)
			}
			if err := tsdb.ExecSQL(tc.sql, tc.validator, tc.intoValidator); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestUDFCastor(t *testing.T) {
	for _, tc := range []struct {
		name          string
		sql           string
		ddl           func(*Catalog) error
		dml           func(*Storage) error
		validator     func([]executor.Chunk)
		intoValidator func(database, retentionPolicy string, points []influx.Row)
	}{
		{
			name: "select castor(v_int, algorithm, algoConf, processType) from mst",
			sql:  "SELECT castor(v_int, 'DIFFERENTIATEAD', 'detect_base', 'detect') as v_int from db0.rp0.mst0",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v_int"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(
					influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v_int", Type: influxql.Integer})

				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3, 4, 5})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(5)
				chunk1.Column(1).AppendIntegerValues([]int64{1, 2, 3, 4, 5})
				chunk1.Column(1).AppendManyNotNil(5)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{0})
				assert.Equal(t, results[0].Columns()[1].IntegerValues(), []int64{0})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tsdb := NewTSDBSystem()
			if err := tsdb.DDL(tc.ddl); err != nil {
				t.Error(err)
			}
			if err := tsdb.DML(tc.dml); err != nil {
				t.Error(err)
			}

			srv, _, err := castor.MockCastorService(6662)
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()

			if err := castor.MockPyWorker(srv.Config.PyWorkerAddr[0]); err != nil {
				t.Error(err)
			}
			wait := 8 * time.Second // wait for service to build connection
			time.Sleep(wait)

			if err := tsdb.ExecSQL(tc.sql, tc.validator, tc.intoValidator); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestMockTSDBSystemWhenExceedSchema(t *testing.T) {
	syscontrol.SetQuerySchemaLimit(2)
	defer syscontrol.SetQuerySchemaLimit(0)
	for _, tc := range []struct {
		name          string
		sql           string
		ddl           func(*Catalog) error
		dml           func(*Storage) error
		validator     func([]executor.Chunk)
		intoValidator func(database, retentionPolicy string, points []influx.Row)
	}{
		{
			name: "Simple Select",
			sql:  "SELECT t,v FROM db0.rp0.mst0",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{1, 2, 3})
				assert.Equal(t, results[0].Columns()[0].StringValuesV2(make([]string, 0)), []string{"a", "a", "a"})
				assert.Equal(t, results[0].Columns()[1].IntegerValues(), []int64{0, 1, 2})
			},
		},
		{
			name: "preagg orderby time desc optimize",
			sql:  "SELECT count(a) as a from db0.rp0.mst0 order by time desc",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["a"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String}, influxql.VarRef{Val: "a", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTime(0)
				chunk1.Column(0).AppendStringValue("a")
				chunk1.Column(0).AppendManyNotNil(1)
				chunk1.Column(1).AppendIntegerValue(5)
				chunk1.Column(1).AppendManyNotNil(1)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{0})
				assert.Equal(t, results[0].Columns()[1].IntegerValues(), []int64{1})
			},
		},
		{
			name: "Multi-Table SubQuery Select",
			sql:  "SELECT t,v FROM (SELECT t,v FROM db0.rp0.mst0, db0.rp0.mst0) GROUP BY t",
			ddl: func(c *Catalog) error {
				db, err := c.CreateDatabase("db0", "rp0")
				if err != nil {
					return err
				}
				mst0 := NewTable("mst0")
				dataTypes := make(map[string]influxql.DataType)
				dataTypes["t"] = influxql.Tag
				dataTypes["v"] = influxql.Integer
				mst0.AddDataTypes(dataTypes)
				db.AddTable(mst0)
				return nil
			},
			dml: func(s *Storage) error {
				rdt := hybridqp.NewRowDataTypeImpl(influxql.VarRef{Val: "t", Type: influxql.String},
					influxql.VarRef{Val: "v", Type: influxql.Integer})
				builder := executor.NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTimes([]int64{1, 2, 3})
				chunk1.Column(0).AppendStringValues([]string{"a", "a", "a"})
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)

				chunk2 := builder.NewChunk("mst0")
				chunk2.AppendTimes([]int64{1, 2, 3})
				chunk2.Column(0).AppendStringValues([]string{"b", "b", "b"})
				chunk2.Column(0).AppendManyNotNil(3)
				chunk2.Column(1).AppendIntegerValues([]int64{0, 1, 2})
				chunk2.Column(1).AppendManyNotNil(3)
				pts2 := influx.PointTags{influx.Tag{Key: "t", Value: "b"}}
				s.Write("db0.rp0.mst0", &pts2, chunk2)
				return nil
			},
			validator: func(results []executor.Chunk) {
				assert.Equal(t, len(results), 1)
				assert.Equal(t, results[0].Name(), "mst0")
				assert.Equal(t, results[0].Time(), []int64{1, 1, 2, 2, 3, 3, 1, 1, 2, 2, 3, 3})
				assert.Equal(t, results[0].Columns()[0].StringValuesV2(make([]string, 0)),
					[]string{"a", "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "b"})
				assert.Equal(t, results[0].Columns()[1].IntegerValues(),
					[]int64{0, 0, 1, 1, 2, 2, 0, 0, 1, 1, 2, 2})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tsdb := NewTSDBSystem()
			if err := tsdb.DDL(tc.ddl); err != nil {
				t.Error(err)
			}
			if err := tsdb.DML(tc.dml); err != nil {
				t.Error(err)
			}
			execErr := tsdb.ExecSQL(tc.sql, tc.validator, tc.intoValidator)
			if execErr != nil && !strings.Contains(execErr.Error(), "max-select-schema limit exceeded") {
				t.Error("expect error")
			}
		})
	}
}
