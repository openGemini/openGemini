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

package executor

import (
	_ "net/http/pprof"
	"testing"
	"time"

	"github.com/influxdata/influxdb/pkg/testing/assert"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/castor"
)

func TestMockTSDBSystem(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       string
		ddl       func(*Catalog) error
		dml       func(*Storage) error
		validator func([]Chunk)
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
				builder := NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTime(1, 2, 3)
				chunk1.Column(0).AppendStringValues("a", "a", "a")
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues(0, 1, 2)
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []Chunk) {
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
				builder := NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTime(0)
				chunk1.Column(0).AppendStringValues("a")
				chunk1.Column(0).AppendManyNotNil(1)
				chunk1.Column(1).AppendIntegerValues(5)
				chunk1.Column(1).AppendManyNotNil(1)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []Chunk) {
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
				builder := NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTime(1, 2, 3)
				chunk1.Column(0).AppendStringValues("a", "a", "a")
				chunk1.Column(0).AppendManyNotNil(3)
				chunk1.Column(1).AppendIntegerValues(0, 1, 2)
				chunk1.Column(1).AppendManyNotNil(3)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)

				chunk2 := builder.NewChunk("mst0")
				chunk2.AppendTime(1, 2, 3)
				chunk2.Column(0).AppendStringValues("b", "b", "b")
				chunk2.Column(0).AppendManyNotNil(3)
				chunk2.Column(1).AppendIntegerValues(0, 1, 2)
				chunk2.Column(1).AppendManyNotNil(3)
				pts2 := influx.PointTags{influx.Tag{Key: "t", Value: "b"}}
				s.Write("db0.rp0.mst0", &pts2, chunk2)
				return nil
			},
			validator: func(results []Chunk) {
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
			if err := tsdb.ExecSQL(tc.sql, tc.validator); err != nil {
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
		validator     func([]Chunk)
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

				builder := NewChunkBuilder(rdt)
				chunk1 := builder.NewChunk("mst0")
				chunk1.AppendTime(1, 2, 3, 4, 5)
				chunk1.Column(0).AppendStringValues("a", "a", "a", "a", "a")
				chunk1.Column(0).AppendManyNotNil(5)
				chunk1.Column(1).AppendIntegerValues(1, 2, 3, 4, 5)
				chunk1.Column(1).AppendManyNotNil(5)
				pts1 := influx.PointTags{influx.Tag{Key: "t", Value: "a"}}
				s.Write("db0.rp0.mst0", &pts1, chunk1)
				return nil
			},
			validator: func(results []Chunk) {
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

			if err := tsdb.ExecSQL(tc.sql, tc.validator); err != nil {
				t.Error(err)
			}
		})
	}
}
