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
	"fmt"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func BuildInOpInChunk1() executor.Chunk {
	rowDataType := buildInOpOutputRowDataType()
	b := executor.NewChunkBuilder(rowDataType)
	chunk := b.NewChunk("mst")
	chunk.AppendTimes([]int64{1, 1, 1})
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=1"), 0)
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=2"), 1)
	chunk.AppendTagsAndIndex(*ParseChunkTags("tag1=3"), 2)
	chunk.AppendIntervalIndex(0)
	chunk.AppendIntervalIndex(1)
	chunk.AppendIntervalIndex(2)
	chunk.Column(0).AppendIntegerValues([]int64{1, 2, 2})
	chunk.Column(0).AppendManyNotNil(3)
	return chunk
}

func buildInOpOutputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Integer},
	)
	return rowDataType
}

func buildInOpMultiOutputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Integer},
		influxql.VarRef{Val: "f2", Type: influxql.Integer},
	)
	return rowDataType
}

func buildInOpErrOutputRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "f1", Type: influxql.Float},
	)
	return rowDataType
}

func buildInnerSchema() *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NullFill,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	return schema
}

func buildOuterSchema(inCondition *influxql.InCondition, condition influxql.Expr) *executor.QuerySchema {
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Dimensions:  make([]string, 0),
		Ascending:   true,
		StartTime:   1,
		EndTime:     4,
		Fill:        influxql.NullFill,
		InConditons: []*influxql.InCondition{inCondition},
		Condition:   condition,
	}
	opt.Dimensions = append(opt.Dimensions, "tag1")
	schema := executor.NewQuerySchema(nil, nil, &opt, nil)
	schema.InConditons = append(schema.InConditons, inCondition)
	return schema
}

func buildOuterStmt(t *testing.T) *influxql.SelectStatement {
	sql := "select f1 from mst where \"f1\" > 0 and \"f1\" in (select \"f1\" from mst)"
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner(), make(map[string]interface{}))
	yaccParser.ParseTokens()
	q, err := yaccParser.GetQuery()
	if err != nil {
		t.Fatal(err)
	}

	stmt := q.Statements[0]
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	selectStmt.BinOpSource = []*influxql.BinOp{}
	if !ok {
		t.Fatal(fmt.Errorf("invalid SelectStatement"))
	}
	return selectStmt
}

func getInCondition(node influxql.Expr) *influxql.InCondition {
	switch n := node.(type) {
	case *influxql.BinaryExpr:
		lhs := getInCondition(n.LHS)
		if lhs != nil {
			return lhs
		}
		return getInCondition(n.RHS)
	case *influxql.ParenExpr:
		return getInCondition(n.Expr)
	case *influxql.InCondition:
		return n
	default:
		return nil
	}
}

type mockExecutorBuilder struct {
	build func() (hybridqp.Executor, error)
}

func (builder *mockExecutorBuilder) Analyze(span *tracing.Span) {
}

func (builder *mockExecutorBuilder) Build(node hybridqp.QueryNode) (hybridqp.Executor, error) {
	return builder.build()
}

func InTransformSingleInputTestBase(t *testing.T, chunks1 []executor.Chunk, rChunk executor.Chunk) {
	source1 := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
	var inrts []hybridqp.RowDataType
	inrts = append(inrts, source1.Output.RowDataType)
	ort := buildInOpOutputRowDataType()
	innerSchema := buildInnerSchema()

	outerStmt := buildOuterStmt(t)
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"f1": influxql.Integer, "tag1": influxql.Tag})
	creator.AddShard(table)

	inCondition := getInCondition(outerStmt.Condition)
	if inCondition == nil {
		t.Fatal(fmt.Errorf("invalid inCondition"))
	}
	inCondition.Csming = creator
	inCondition.Column = inrts[0].Field(0).Expr

	outerSchema := buildOuterSchema(inCondition, outerStmt.Condition)
	outerSchema.InConditons = append(outerSchema.InConditons, inCondition)
	var tagstmt *influxql.ShowTagValuesStatement
	trans, err := executor.NewInTransform(inrts, ort, innerSchema, outerStmt, outerSchema, creator, inCondition.Column, tagstmt)
	if err != nil {
		panic("")
	}
	trans.ExecutorBuilder = &mockExecutorBuilder{
		build: func() (hybridqp.Executor, error) {
			source := NewSourceFromMultiChunk(chunks1[0].RowDataType(), chunks1)
			dag := &executor.TransformDag{}
			root := executor.NewTransformVertex(nil, source)
			pipeline := executor.NewPipelineExecutorFromDag(dag, root)
			pipeline.SetProcessors([]executor.Processor{source})
			return pipeline, nil
		},
	}

	checkResult := func(chunk executor.Chunk) error {
		err := PromResultCompareMultiCol(chunk, rChunk)
		assert.Equal(t, err, nil)
		return nil
	}
	sink := NewSinkFromFunction(ort, checkResult)
	executor.Connect(source1.Output, trans.GetInputs()[0])
	executor.Connect(trans.GetOutputs()[0], sink.Input)
	var processors executor.Processors
	processors = append(processors, source1)
	processors = append(processors, trans)
	processors = append(processors, sink)
	executors := executor.NewPipelineExecutor(processors)
	executors.Execute(context.Background())
	executors.Release()
}

func TestInTransform1(t *testing.T) {
	chunk1 := BuildInOpInChunk1()
	InTransformSingleInputTestBase(t, []executor.Chunk{chunk1}, BuildInOpInChunk1())
}

func buildOuterCondition1() influxql.Expr {
	return &influxql.BinaryExpr{
		Op:  influxql.AND,
		LHS: &influxql.InCondition{},
		RHS: &influxql.InCondition{},
	}
}

func buildOuterCondition2(op influxql.Token) influxql.Expr {
	return &influxql.BinaryExpr{
		Op:  op,
		LHS: &influxql.InCondition{},
		RHS: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "f1", Type: influxql.Integer},
			RHS: &influxql.IntegerLiteral{Val: 1},
		},
	}
}

func buildOuterCondition3(op influxql.Token) influxql.Expr {
	return &influxql.BinaryExpr{
		Op: op,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "f1", Type: influxql.Integer},
			RHS: &influxql.IntegerLiteral{Val: 1},
		},
		RHS: &influxql.InCondition{},
	}
}

func buildOuterCondition4() influxql.Expr {
	return &influxql.ParenExpr{
		Expr: buildOuterCondition1(),
	}
}

func buildOuterCondition5() influxql.Expr {
	return &influxql.ParenExpr{
		Expr: &influxql.InCondition{},
	}
}

func buildOuterCondition6() influxql.Expr {
	return &influxql.ParenExpr{
		Expr: &influxql.VarRef{Val: "f1", Type: influxql.Integer},
	}
}

func buildOuterCondition7() influxql.Expr {
	return &influxql.InCondition{
		NotEqual: false,
	}
}

func buildOuterCondition8() influxql.Expr {
	return &influxql.InCondition{
		NotEqual: true,
	}
}

func TestInTransformRewriteInCondition(t *testing.T) {
	chunk1 := BuildInOpInChunk1()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	var inrts []hybridqp.RowDataType
	inrts = append(inrts, source1.Output.RowDataType)
	ort := buildInOpOutputRowDataType()
	innerSchema := buildInnerSchema()

	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"f1": influxql.Integer, "tag1": influxql.Tag})
	creator.AddShard(table)

	inCondition := &influxql.InCondition{}
	inCondition.Csming = creator
	inCondition.Column = inrts[0].Field(0).Expr

	outerSchema := buildOuterSchema(inCondition, buildOuterCondition1())
	outerSchema.InConditons = append(outerSchema.InConditons, inCondition)
	var tagstmt *influxql.ShowTagValuesStatement
	trans, err := executor.NewInTransform(inrts, ort, innerSchema, nil, outerSchema, creator, inCondition.Column, tagstmt)
	if err != nil {
		panic("")
	}
	err = trans.RunOuterPlan(context.Background())
	assert.Equal(t, err.Error(), "lhs and rhs is all nil in binaryExpr")

	outerSchema.Options().SetCondition(buildOuterCondition2(influxql.OR))
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err, nil)

	outerSchema.Options().SetCondition(buildOuterCondition3(influxql.OR))
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err, nil)

	outerSchema.Options().SetCondition(buildOuterCondition2(influxql.AND))
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err.Error(), "filter all points after rewriteCondition")

	outerSchema.Options().SetCondition(buildOuterCondition3(influxql.AND))
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err.Error(), "filter all points after rewriteCondition")

	outerSchema.Options().SetCondition(buildOuterCondition2(influxql.EQ))
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err.Error(), "wrong binaryOp type upper lhs inConditon")

	outerSchema.Options().SetCondition(buildOuterCondition3(influxql.EQ))
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err.Error(), "wrong binaryOp type upper rhs inConditon")

	outerSchema.Options().SetCondition(buildOuterCondition4())
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err.Error(), "lhs and rhs is all nil in binaryExpr")

	outerSchema.Options().SetCondition(buildOuterCondition5())
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err.Error(), "filter all points after rewriteCondition")

	outerSchema.Options().SetCondition(buildOuterCondition6())
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err, nil)

	trans.BufColumnMap = map[interface{}]struct{}{
		int64(1): {},
		int64(2): {},
	}
	outerSchema.Options().SetCondition(buildOuterCondition7())
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err, nil)

	outerSchema.Options().SetCondition(buildOuterCondition8())
	err = trans.RewriteOuterStmtCondition()
	assert.Equal(t, err, nil)
}

func TestInTransformGetVal(t *testing.T) {
	chunk1 := BuildInOpInChunk1()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	var inrts []hybridqp.RowDataType
	inrts = append(inrts, source1.Output.RowDataType)
	ort := buildInOpOutputRowDataType()
	innerSchema := buildInnerSchema()

	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"f1": influxql.Integer, "tag1": influxql.Tag})
	creator.AddShard(table)

	inCondition := &influxql.InCondition{}
	inCondition.Csming = creator
	inCondition.Column = inrts[0].Field(0).Expr

	outerSchema := buildOuterSchema(inCondition, buildOuterCondition1())
	outerSchema.InConditons = append(outerSchema.InConditons, inCondition)
	var tagstmt *influxql.ShowTagValuesStatement
	trans, err := executor.NewInTransform(inrts, ort, innerSchema, nil, outerSchema, creator, inCondition.Column, tagstmt)
	if err != nil {
		panic("")
	}
	c := &executor.ChunkImpl{}
	col := &executor.ColumnImpl{}
	col.AppendIntegerValues([]int64{1, 1})
	col.AppendFloatValues([]float64{1, 1})
	col.AppendBooleanValues([]bool{false, false})
	col.AppendStringBytes([]byte{'a', 'a'}, []uint32{0, 1})
	c.AddColumn(col)
	typs := []influxql.DataType{influxql.Float, influxql.Boolean, influxql.String, influxql.Tag}
	for _, typ := range typs {
		trans.OuterVarRef.Type = typ
		for k := range trans.BufColumnMap {
			delete(trans.BufColumnMap, k)
		}
		trans.AddChunkToBufColumn(c)
		assert.Equal(t, len(trans.BufColumnMap), 1)
	}

	trans.OuterVarRef.Type = influxql.AnyField
}

func TestInTransformInitErr(t *testing.T) {
	ort := buildInOpOutputRowDataType()
	multiOrt := buildInOpMultiOutputRowDataType()
	var inrts []hybridqp.RowDataType
	inrts = append(inrts, multiOrt)
	innerSchema := buildInnerSchema()

	outerStmt := buildOuterStmt(t)
	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"f1": influxql.Integer, "tag1": influxql.Tag})
	creator.AddShard(table)

	inCondition := getInCondition(outerStmt.Condition)
	if inCondition == nil {
		t.Fatal(fmt.Errorf("invalid inCondition"))
	}
	inCondition.Csming = creator
	inCondition.Column = inrts[0].Field(0).Expr

	outerSchema := buildOuterSchema(inCondition, outerStmt.Condition)
	outerSchema.InConditons = append(outerSchema.InConditons, inCondition)
	var tagstmt *influxql.ShowTagValuesStatement
	_, err := executor.NewInTransform(inrts, ort, innerSchema, outerStmt, outerSchema, creator, inCondition.Column, tagstmt)
	assert.NotEqual(t, err, nil)

	inrts[0] = buildInOpErrOutputRowDataType()
	trans, err := executor.NewInTransform(inrts, ort, innerSchema, outerStmt, outerSchema, creator, inCondition.Column, tagstmt)
	assert.NotEqual(t, err, nil)

	assert.Equal(t, trans.Name(), "InTransform")
	assert.Equal(t, len(trans.Explain()), 0)
	assert.Equal(t, trans.GetOutputNumber(nil), 0)
	assert.Equal(t, trans.GetInputNumber(nil), 0)
}

func TestShowTagValRun(t *testing.T) {
	chunk1 := BuildInOpInChunk1()
	source1 := NewSourceFromMultiChunk(chunk1.RowDataType(), []executor.Chunk{chunk1})
	var inrts []hybridqp.RowDataType
	inrts = append(inrts, source1.Output.RowDataType)
	ort := buildInOpOutputRowDataType()
	innerSchema := buildInnerSchema()

	creator := NewMockShardGroup()
	table := NewTable("mst")
	table.AddDataTypes(map[string]influxql.DataType{"f1": influxql.Integer, "tag1": influxql.Tag})
	creator.AddShard(table)

	inCondition := &influxql.InCondition{}
	inCondition.Csming = creator
	inCondition.Column = inrts[0].Field(0).Expr

	outerSchema := buildOuterSchema(inCondition, buildOuterCondition1())
	outerSchema.InConditons = append(outerSchema.InConditons, inCondition)

	// create ShowTagValuesStatement
	var tagstmt *influxql.ShowTagValuesStatement
	varRefName := &influxql.VarRef{
		Val:   "_name",
		Type:  influxql.Unknown,
		Alias: "",
	}
	stringLiteralMst2 := &influxql.StringLiteral{
		Val: "mst2",
	}
	binaryExpr1 := &influxql.BinaryExpr{
		Op:  influxql.Token(57479),
		LHS: varRefName,
		RHS: stringLiteralMst2,
	}
	varRefTagKey := &influxql.VarRef{
		Val:   "_tagKey",
		Type:  influxql.Unknown,
		Alias: "",
	}
	stringLiteralHost02 := &influxql.StringLiteral{
		Val: "host02",
	}
	binaryExpr2 := &influxql.BinaryExpr{
		Op:  influxql.EQ,
		LHS: varRefTagKey,
		RHS: stringLiteralHost02,
	}
	parenExpr1 := &influxql.ParenExpr{
		Expr: binaryExpr1,
	}
	parenExpr2 := &influxql.ParenExpr{
		Expr: binaryExpr2,
	}
	TagKeyCond := &influxql.BinaryExpr{
		Op:  influxql.Token(57496),
		LHS: parenExpr1,
		RHS: parenExpr2,
	}

	tagstmt = &influxql.ShowTagValuesStatement{
		Database: "db0",
		Sources:  nil,
		Op:       influxql.Token(57479),
		TagKeyExpr: &influxql.ListLiteral{
			Vals: []string{"host02"},
		},
		TagKeyCondition: TagKeyCond,
	}

	convey.Convey("normal condition", t, func() {
		expectTagValue := make(map[interface{}]struct{})
		expectTagValue["server02"] = struct{}{}
		expectTagValue["server03"] = struct{}{}
		expectTagValue["server10"] = struct{}{}
		expectTagValue["server01"] = struct{}{}

		trans, err := executor.NewInTransform(inrts, ort, innerSchema, nil, outerSchema, creator, inCondition.Column, tagstmt)
		assert.Equal(t, err, nil)
		_ = trans.ShowTagValRun(tagstmt)
		actualTagValues := trans.BufColumnMap
		assert.Equal(t, expectTagValue, actualTagValues, "actualTagValues and expectTagValue should be consistent")
	})

	convey.Convey("err condition", t, func() {
		tagstmt.TagKeyExpr = nil
		trans, err := executor.NewInTransform(inrts, ort, innerSchema, nil, outerSchema, creator, inCondition.Column, tagstmt)
		err = trans.ShowTagValRun(tagstmt)
		tagstmt.TagKeyExpr = &influxql.ListLiteral{Vals: []string{"host02"}}
		assert.NotEqual(t, err, nil)

		tagstmt.Database = "nodb"
		trans, err = executor.NewInTransform(inrts, ort, innerSchema, nil, outerSchema, creator, inCondition.Column, tagstmt)
		err = trans.ShowTagValRun(tagstmt)
		assert.NotEqual(t, err, nil)

		tagstmt.Database = ""
		trans, err = executor.NewInTransform(inrts, ort, innerSchema, nil, outerSchema, creator, inCondition.Column, tagstmt)
		err = trans.ShowTagValRun(tagstmt)
		assert.NotEqual(t, err, nil)
	})
}
