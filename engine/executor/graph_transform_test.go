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

package executor_test

import (
	"context"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestGraphTransform(t *testing.T) {
	stmt := &influxql.GraphStatement{
		HopNum:      3,
		StartNodeId: "ELB",
	}
	trans, err := executor.NewGraphTransform(stmt)
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.Equal(t, len(trans.GetOutputs()), 1)
	assert.Equal(t, len(trans.GetInputs()), 0)
	assert.Equal(t, len(trans.Explain()), 0)
	assert.Equal(t, trans.GetOutputNumber(nil), 0)
	assert.Equal(t, trans.GetInputNumber(nil), 0)
	assert.Equal(t, trans.Name(), "GraphTransform")

	output := executor.NewChunkPort(hybridqp.NewRowDataTypeImpl(*influxql.DefaultGraphVarRef()))
	executor.Connect(trans.GetOutputs()[0], output)
	err = trans.Work(context.Background())
	if err != nil {
		t.Fatal(err.Error())
	}
	chunk, ok := <-output.State
	if !ok {
		t.Fatal("no result")
	}
	actGraph := chunk.GetGraph().(*executor.Graph)
	assert.Equal(t, 12, len(actGraph.Nodes))

	rows := actGraph.GraphToRows()
	assert.Equal(t, len(rows), 2)

	stmt.StartNodeId = "1"
	err = trans.Work(context.Background())
	assert.Equal(t, err.Error(), "MultiHopFilter startNodeId not found 1")
}

func TestGraphTransformAddAdditionalConfig(t *testing.T) {
	output := executor.NewChunkPort(hybridqp.NewRowDataTypeImpl(*influxql.DefaultGraphVarRef()))
	convey.Convey("with applicationId filter without time filter", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where applicationId = '68085da68514366010a73c61' return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		if err != nil {
			t.Fatal(err.Error())
		}
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err, nil)
	})
	convey.Convey("param filter condition", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where '68085da68514366010a73c61' = applicationId return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		if err != nil {
			t.Fatal(err.Error())
		}
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err, nil)
	})
	convey.Convey("with time range filter without applicationId filter", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where time > now() - 1h and time < now() - 5m return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err, nil)
	})
	convey.Convey("with both time point filter and applicationId filter", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where time = now() - 1h and applicationId = '68085da68514366010a73c61' return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		if err != nil {
			t.Fatal(err.Error())
		}
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err, nil)
	})
	convey.Convey("time range filter condition stmt has err : time > now() - 1h", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where time > now() - 1h and applicationId = '68085da68514366010a73c61' return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		if err != nil {
			t.Fatal(err.Error())
		}
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err.Error(), "time-range query: start and end time must be provided")
	})
	convey.Convey("param filter condition stmt has err : applicationId > '68085da68514366010a73c61'", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where applicationId > '68085da68514366010a73c61' return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		if err != nil {
			t.Fatal(err.Error())
		}
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err.Error(), "unsupported operator: >")
	})
	convey.Convey("param filter condition stmt has err : 'applicationId' = '68085da'", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where 'applicationId' = '68085da' return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		if err != nil {
			t.Fatal(err.Error())
		}
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err.Error(), "convert to BinaryExpr failed: expr *influxql.BooleanLiteral is not *influxql.BinaryExpr")
	})
	convey.Convey("param filter condition stmt has err : 'applicationId' = 12345", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where 'applicationId' = 12345 return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		if err != nil {
			t.Fatal(err.Error())
		}
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err.Error(), "unsupported topo filter param condition syntax")
	})
	convey.Convey("param filter condition stmt has err : applicationId = 12345", t, func() {
		sql := "match path=(startNode{uid:'ELB'})-[es*..3]-() where applicationId = 12345 return path"
		stmt := getStmt(t, sql)
		trans, err := executor.NewGraphTransform(stmt)
		if err != nil {
			t.Fatal(err.Error())
		}
		executor.Connect(trans.GetOutputs()[0], output)
		err = trans.Work(context.Background())
		assert.Equal(t, err.Error(), "unsupported topo filter param condition syntax")
	})
}

func getStmt(t *testing.T, sql string) *influxql.GraphStatement {
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	q, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal(err)
	}
	stmt := q.Statements[0].(*influxql.GraphStatement)
	return stmt
}
