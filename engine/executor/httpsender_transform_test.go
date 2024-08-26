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
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockFieldsAndTags() influxql.Fields {
	fields := make(influxql.Fields, 0, 3)

	fields = append(fields,
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "f1_float",
				Type: influxql.Float,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "f2_int",
				Type: influxql.Integer,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "f3_bool",
				Type: influxql.Boolean,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "f4_string",
				Type: influxql.String,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "t1_tag",
				Type: influxql.Tag,
			},
			Alias: "",
		},
		&influxql.Field{
			Expr: &influxql.VarRef{
				Val:  "t2_tag",
				Type: influxql.Tag,
			},
			Alias: "",
		},
	)
	return fields
}

func mockColumnNames() []string {
	return []string{"f1_float", "f2_int", "f3_bool", "f4_string", "t1_tag", "t2_tag"}
}

func varRefsFromFields(fields influxql.Fields) influxql.VarRefs {
	refs := make(influxql.VarRefs, 0, len(fields))
	for _, f := range fields {
		refs = append(refs, *(f.Expr.(*influxql.VarRef)))
	}

	return refs
}

func Test_HttpSenderTransform(t *testing.T) {
	// 4 fields, 2 tags
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	ctx := context.Background()

	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
	}
	schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt, nil)
	schema.SetOpt(&opt)
	mockInput := NewMockGenDataTransform(inRowDataType)
	httpSender := executor.NewHttpSenderTransform(inRowDataType, schema)
	httpSender.GetInputs()[0].Connect(mockInput.GetOutputs()[0])

	var processors executor.Processors
	processors = append(processors, mockInput)
	processors = append(processors, httpSender)
	executors := executor.NewPipelineExecutor(processors)

	ec := make(chan error, 1)
	go func() {
		ec <- executors.Execute(context.Background())
		close(ec)
		close(opt.RowsChan)
	}()
	var closed bool
	for {
		select {
		case data, ok := <-opt.RowsChan:
			if !ok {
				closed = true
				break
			}
			_ = data
			//fmt.Println(data.Rows[0].Values[0])
		case <-ctx.Done():
			closed = true
			break
		}
		if closed {
			break
		}
	}
	executors.Release()
}

// go test -v -run none -bench BenchmarkHttpSenderTransform -benchtime=10x -count=5
/*
data: 10000*1024 rows.
commitId: 6897d42
BenchmarkHttpSenderTransform-8   	      10	9314583271 ns/op	5965853652 B/op	100020401 allocs/op
BenchmarkHttpSenderTransform-8   	      10	9390476649 ns/op	5965853056 B/op	100020399 allocs/op
BenchmarkHttpSenderTransform-8   	      10	9399963322 ns/op	5965853643 B/op	100020401 allocs/op
BenchmarkHttpSenderTransform-8   	      10	9385393478 ns/op	5965853131 B/op	100020399 allocs/op
BenchmarkHttpSenderTransform-8   	      10	9413198649 ns/op	5965852993 B/op	100020399 allocs/op

now:
BenchmarkHttpSenderTransform-8   	      10	5034056473 ns/op	3423552020 B/op	69150411 allocs/op
BenchmarkHttpSenderTransform-8   	      10	5029341181 ns/op	3423551900 B/op	69150410 allocs/op
BenchmarkHttpSenderTransform-8   	      10	5019876363 ns/op	3423551713 B/op	69150409 allocs/op
BenchmarkHttpSenderTransform-8   	      10	4994172657 ns/op	3423551564 B/op	69150409 allocs/op
BenchmarkHttpSenderTransform-8   	      10	5026938425 ns/op	3423551321 B/op	69150408 allocs/op
*/
func BenchmarkHttpSenderTransform(b *testing.B) {
	// 4 fields, 2 tags
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		outPutRowsChan := make(chan query.RowsChan)
		opt := query.ProcessorOptions{
			ChunkSize:   1024,
			ChunkedSize: 10000,
			RowsChan:    outPutRowsChan,
		}
		schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt, nil)
		schema.SetOpt(&opt)
		mockInput := NewMockGenDataTransform(inRowDataType)
		httpSender := executor.NewHttpSenderTransform(inRowDataType, schema)
		httpSender.GetInputs()[0].Connect(mockInput.GetOutputs()[0])

		var processors executor.Processors
		processors = append(processors, mockInput)
		processors = append(processors, httpSender)
		executors := executor.NewPipelineExecutor(processors)

		b.StartTimer()
		ec := make(chan error)
		go func() {
			ec <- executors.Execute(context.Background())
			close(ec)
			close(opt.RowsChan)
		}()
		var closed bool
		for {
			select {
			case data, ok := <-opt.RowsChan:
				if !ok {
					closed = true
					break
				}
				_ = data
				//fmt.Println(data.Rows[0].Values[0])
			case <-ctx.Done():
				closed = true
				break
			}
			if closed {
				break
			}
		}
		executors.Release()
	}
}

type MockGenDataTransform struct {
	executor.BaseProcessor
	output *executor.ChunkPort
	chunks []executor.Chunk
}

func NewMockGenDataTransform(outRowDataType hybridqp.RowDataType) *MockGenDataTransform {
	trans := &MockGenDataTransform{
		output: executor.NewChunkPort(outRowDataType),
	}
	trans.chunks = GetMockChunks(outRowDataType).chunks
	return trans
}

func NewMockGenDataTransformV1(outRowDataType hybridqp.RowDataType) *MockGenDataTransform {
	trans := &MockGenDataTransform{
		output: executor.NewChunkPort(outRowDataType),
	}
	trans.chunks = gen100Chunks(outRowDataType)
	return trans
}

type mockChunks struct {
	chunks []executor.Chunk
}

func NewMockChunks(outRowDataType hybridqp.RowDataType) *mockChunks {
	ins := &mockChunks{}
	ins.chunks = gen10000Chunks(outRowDataType)
	return ins
}

// 10000 * 1024 rows
func gen10000Chunks(outRowDataType hybridqp.RowDataType) []executor.Chunk {
	var chunks []executor.Chunk
	cb := executor.NewChunkBuilder(outRowDataType)
	for n := 0; n < 10000; n++ {
		ck := cb.NewChunk("cpu")
		ck.AppendIntervalIndex(0)
		ck.AppendTagsAndIndex(*executor.NewChunkTags(nil, nil), 0)
		for i := 0; i < 1024; i++ {
			ck.AppendTime(int64(i))

			ck.Column(0).AppendFloatValue(float64(i))
			ck.Column(0).AppendNotNil()
			ck.Column(1).AppendIntegerValue(int64(i))
			ck.Column(1).AppendNotNil()
			ck.Column(2).AppendBooleanValue(i%2 == 0)
			ck.Column(2).AppendNotNil()
			ck.Column(3).AppendStringValue(strconv.FormatInt(int64(i), 10))
			ck.Column(3).AppendNotNil()
			ck.Column(4).AppendStringValue("tv1")
			ck.Column(4).AppendNotNil()
			ck.Column(5).AppendStringValue("tv2")
			ck.Column(5).AppendNotNil()
		}
		chunks = append(chunks, ck)
	}
	return chunks
}

// 100 * 1 rows
func gen100Chunks(outRowDataType hybridqp.RowDataType) []executor.Chunk {
	var chunks []executor.Chunk
	cb := executor.NewChunkBuilder(outRowDataType)
	for n := 0; n < 100; n++ {
		ck := cb.NewChunk("cpu")
		ck.AppendIntervalIndex(0)
		if n <= 50 {
			ck.AppendTagsAndIndex(*executor.NewChunkTags(nil, nil), 0)
		} else {
			ck.AppendTagsAndIndex(*executor.NewChunkTagsByTagKVs([]string{"A"}, []string{"a"}), 0)
		}
		for i := 0; i < 1; i++ {
			ck.AppendTime(int64(i))
			ck.Column(0).AppendFloatValue(float64(i))
			ck.Column(0).AppendNotNil()
			ck.Column(1).AppendIntegerValue(int64(i))
			ck.Column(1).AppendNotNil()
			ck.Column(2).AppendBooleanValue(i%2 == 0)
			ck.Column(2).AppendNotNil()
			ck.Column(3).AppendStringValue(strconv.FormatInt(int64(i), 10))
			ck.Column(3).AppendNotNil()
			ck.Column(4).AppendStringValue("tv1")
			ck.Column(4).AppendNotNil()
			ck.Column(5).AppendStringValue("tv2")
			ck.Column(5).AppendNotNil()
		}
		chunks = append(chunks, ck)
	}
	return chunks
}

var instanceChunks *mockChunks
var once sync.Once

func GetMockChunks(outRowDataType hybridqp.RowDataType) *mockChunks {
	once.Do(func() {
		instanceChunks = NewMockChunks(outRowDataType)
	})
	return instanceChunks
}

func (trans *MockGenDataTransform) Work(_ context.Context) error {
	for _, ck := range trans.chunks {
		trans.output.State <- ck
	}
	trans.Close()
	return nil
}

func (trans *MockGenDataTransform) Name() string {
	return "MockGenDataTransform"
}

func (trans *MockGenDataTransform) Explain() []executor.ValuePair {
	return nil
}

func (trans *MockGenDataTransform) Close() {
	trans.output.Close()
}

func (trans *MockGenDataTransform) GetOutputs() executor.Ports {
	return executor.Ports{trans.output}
}

func (trans *MockGenDataTransform) GetInputs() executor.Ports {
	return nil
}

func (trans *MockGenDataTransform) GetOutputNumber(_ executor.Port) int {
	return 1
}

func (trans *MockGenDataTransform) GetInputNumber(_ executor.Port) int {
	return 0
}

type MockAbortProcessor struct{}

func (m *MockAbortProcessor) AbortSinkTransform() {}

func TestGenRows(t *testing.T) {
	sender := executor.NewHttpChunkSender(&query.ProcessorOptions{
		Without: true,
		Limit:   1000,
	})
	sender.SetAbortProcessor(&MockAbortProcessor{})

	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	chunk := genChunk(inRowDataType)
	sender.GenRows(chunk)

	sender = executor.NewHttpChunkSender(&query.ProcessorOptions{
		Without: true,
		Limit:   11,
	})
	sender.SetAbortProcessor(&MockAbortProcessor{})
	sender.GenRows(chunk)

	sender = executor.NewHttpChunkSender(&query.ProcessorOptions{
		Without: true,
		Limit:   11,
		Offset:  190,
	})
	sender.SetAbortProcessor(&MockAbortProcessor{})
	sender.GenRows(chunk)
}

func TestGetRows(t *testing.T) {
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	chunk := genChunk(inRowDataType)

	sender := executor.NewHttpChunkSender(&query.ProcessorOptions{})
	rows := sender.GetRows(chunk)

	g := &executor.RowsGenerator{}
	other := g.Generate(chunk, time.UTC)
	require.Equal(t, rows.Len(), len(other))
	for i := 0; i < rows.Len(); i++ {
		exp := rows[i]
		got := other[i]
		require.Equal(t, exp.Tags, got.Tags)
		require.Equal(t, exp.Columns, got.Columns)
		require.Equal(t, len(exp.Values), len(got.Values))
		for j := 0; j < len(exp.Values); j++ {
			require.Equal(t, exp.Values[j], got.Values[j])

		}
	}
}

func BenchmarkHttpChunkSender_GetRows(b *testing.B) {
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	chunk := genChunk(inRowDataType)

	sender := executor.NewHttpChunkSender(&query.ProcessorOptions{Location: time.UTC})
	_ = sender
	g := &executor.RowsGenerator{}
	_ = g
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		//BenchmarkHttpChunkSender_GetRows-12         3166            339198 ns/op
		//          245619 B/op       5015 allocs/op
		g.Reset()
		g.Generate(chunk, time.UTC)
		//BenchmarkHttpChunkSender_GetRows-12         2488            451568 ns/op
		//          236450 B/op       9050 allocs/op
		//sender.GetRows(chunk)
	}
}

func genChunk(outRowDataType hybridqp.RowDataType) executor.Chunk {
	cb := executor.NewChunkBuilder(outRowDataType)
	ck := cb.NewChunk("cpu")
	ck.AppendIntervalIndex(0)
	for n := 0; n < 10; n++ {
		ck.AppendTagsAndIndex(*executor.NewChunkTags(nil, nil), n*100)
		for i := 0; i < 100; i++ {
			ck.AppendTime(int64(i))

			ck.Column(0).AppendFloatValue(float64(i))
			ck.Column(0).AppendNotNil()

			if i%2 == 0 {
				ck.Column(1).AppendIntegerValue(int64(i))
				ck.Column(1).AppendNotNil()
			} else {
				ck.Column(1).AppendNil()
			}

			if i%3 == 0 {
				ck.Column(2).AppendNil()
			} else {
				ck.Column(2).AppendBooleanValue(i%2 == 0)
				ck.Column(2).AppendNotNil()
			}

			ck.Column(3).AppendStringValue(strconv.FormatInt(int64(i), 10))
			ck.Column(3).AppendNotNil()
			ck.Column(4).AppendStringValue("tv1")
			ck.Column(4).AppendNotNil()
			ck.Column(5).AppendStringValue("tv2")
			ck.Column(5).AppendNotNil()
		}
	}

	return ck
}

func buildRows() models.Rows {
	return models.Rows{&models.Row{
		Name:    "cpu",
		Columns: []string{"time", "f1_float", "f2_int", "f3_bool", "f4_string", "t1_tag", "t2_tag"},
		Values:  [][]interface{}{{time.Unix(0, 0), float64(0), int64(0), true, "0", "tv1", "tv2"}},
	}}
}

func Test_HttpSenderTransform_Except(t *testing.T) {
	// 4 fields, 2 tags
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	ctx := context.Background()

	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Without:     true,
		Limit:       1,
	}
	schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt, nil)
	schema.SetOpt(&opt)
	mockInput := NewMockGenDataTransform(inRowDataType)
	httpSender := executor.NewHttpSenderTransform(inRowDataType, schema)
	httpSender.SetDag(nil)
	httpSender.SetVertex(nil)
	httpSender.GetInputs()[0].Connect(mockInput.GetOutputs()[0])

	var processors executor.Processors
	processors = append(processors, mockInput)
	processors = append(processors, httpSender)
	executors := executor.NewPipelineExecutor(processors)

	ec := make(chan error, 1)
	go func() {
		ec <- executors.Execute(context.Background())
		close(ec)
		close(opt.RowsChan)
	}()
	var closed bool
	var dstRows models.Rows
	for {
		select {
		case data, ok := <-opt.RowsChan:
			if !ok {
				closed = true
				break
			}
			dstRows = append(dstRows, data.Rows...)
		case <-ctx.Done():
			closed = true
			break
		}
		if closed {
			break
		}
	}
	executors.Release()
	assert.Equal(t, len(dstRows), len(buildRows()))
}

func Test_HttpSenderHintTransform_Except(t *testing.T) {
	// 4 fields, 2 tags
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	ctx := context.Background()

	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1024,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Without:     true,
		Limit:       1,
	}
	schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt, nil)
	schema.SetOpt(&opt)
	mockInput := NewMockGenDataTransform(inRowDataType)
	httpSender := executor.NewHttpSenderHintTransform(inRowDataType, schema)
	httpSender.SetDag(nil)
	httpSender.SetVertex(nil)
	httpSender.GetInputs()[0].Connect(mockInput.GetOutputs()[0])

	var processors executor.Processors
	processors = append(processors, mockInput)
	processors = append(processors, httpSender)
	executors := executor.NewPipelineExecutor(processors)

	ec := make(chan error, 1)
	go func() {
		ec <- executors.Execute(context.Background())
		close(ec)
		close(opt.RowsChan)
	}()
	var closed bool
	var dstRows models.Rows
	for {
		select {
		case data, ok := <-opt.RowsChan:
			if !ok {
				closed = true
				break
			}
			dstRows = append(dstRows, data.Rows...)
		case <-ctx.Done():
			closed = true
			break
		}
		if closed {
			break
		}
	}
	executors.Release()
	assert.Equal(t, len(dstRows), len(buildRows()))
}

func Test_HttpSenderTransform_Except_Abort(t *testing.T) {
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	opt := query.ProcessorOptions{Without: true, Limit: 1}
	schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt, nil)
	schema.SetOpt(&opt)
	httpSender := executor.NewHttpSenderTransform(inRowDataType, schema)
	vertex := executor.NewTransformVertex(executor.NewLogicalHttpSender(nil, schema), httpSender)
	dag := executor.NewTransformDag()
	dag.AddVertex(vertex)
	httpSender.SetDag(dag)
	httpSender.SetVertex(vertex)
	httpSender.AbortSinkTransform()
	httpSender.Visit(vertex)
}

func Test_HttpSenderHintTransform_Except_Abort(t *testing.T) {
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	opt := query.ProcessorOptions{Without: true, Limit: 1}
	schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt, nil)
	schema.SetOpt(&opt)
	httpSender := executor.NewHttpSenderHintTransform(inRowDataType, schema)
	vertex := executor.NewTransformVertex(executor.NewLogicalHttpSender(nil, schema), httpSender)
	dag := executor.NewTransformDag()
	dag.AddVertex(vertex)
	httpSender.SetDag(dag)
	httpSender.SetVertex(vertex)
	httpSender.AbortSinkTransform()
	httpSender.Visit(vertex)
}

func Test_HttpSenderTransform_Except_Limit_Offset(t *testing.T) {
	// 4 fields, 2 tags
	fields := mockFieldsAndTags()
	refs := varRefsFromFields(fields)
	inRowDataType := hybridqp.NewRowDataTypeImpl(refs...)
	ctx := context.Background()
	outPutRowsChan := make(chan query.RowsChan)
	opt := query.ProcessorOptions{
		ChunkSize:   1,
		ChunkedSize: 10000,
		RowsChan:    outPutRowsChan,
		Without:     true,
		Limit:       10,
		Offset:      1,
	}
	schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt, nil)
	schema.SetOpt(&opt)
	mockInput := NewMockGenDataTransformV1(inRowDataType)
	httpSender := executor.NewHttpSenderTransform(inRowDataType, schema)
	httpSender.SetDag(nil)
	httpSender.SetVertex(nil)
	httpSender.GetInputs()[0].Connect(mockInput.GetOutputs()[0])

	var processors executor.Processors
	processors = append(processors, mockInput)
	processors = append(processors, httpSender)
	executors := executor.NewPipelineExecutor(processors)

	ec := make(chan error, 1)
	go func() {
		ec <- executors.Execute(context.Background())
		close(ec)
		close(opt.RowsChan)
	}()
	var closed bool
	var dstRows models.Rows
	for {
		select {
		case data, ok := <-opt.RowsChan:
			if !ok {
				closed = true
				break
			}
			dstRows = append(dstRows, data.Rows...)
		case <-ctx.Done():
			closed = true
			break
		}
		if closed {
			break
		}
	}
	executors.Release()
	assert.Equal(t, len(dstRows), len(buildRows()))
}

func TestRemoveCommonValues(t *testing.T) {
	now := time.Now()
	nowAdd1Min := now.Add(time.Minute)
	nowAdd2Min := now.Add(2 * time.Minute)
	type args struct {
		prev [][]interface{}
		curr [][]interface{}
	}
	tests := []struct {
		name string
		args args
		want [][]interface{}
	}{
		{
			name: "1",
			args: args{
				prev: nil,
				curr: nil,
			},
			want: nil,
		},
		{
			name: "2",
			args: args{
				prev: [][]interface{}{{now}},
				curr: [][]interface{}{{nowAdd1Min}},
			},
			want: [][]interface{}{{nowAdd1Min}},
		},
		{
			name: "3",
			args: args{
				prev: [][]interface{}{{now}},
				curr: [][]interface{}{{now}},
			},
			want: [][]interface{}{},
		},
		{
			name: "4",
			args: args{
				prev: [][]interface{}{{now}, {nowAdd1Min}},
				curr: [][]interface{}{{now}, {nowAdd2Min}},
			},
			want: [][]interface{}{{nowAdd2Min}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, executor.RemoveCommonValues(tt.args.prev, tt.args.curr), "RemoveCommonValues(%v, %v)", tt.args.prev, tt.args.curr)
		})
	}
}
