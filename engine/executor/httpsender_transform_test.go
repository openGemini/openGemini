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

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
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
	schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt)
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
		schema := executor.NewQuerySchema(fields, mockColumnNames(), &opt)
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

			ck.Column(0).AppendFloatValues(float64(i))
			ck.Column(0).AppendNilsV2(true)
			ck.Column(1).AppendIntegerValues(int64(i))
			ck.Column(1).AppendNilsV2(true)
			ck.Column(2).AppendBooleanValues(i%2 == 0)
			ck.Column(2).AppendNilsV2(true)
			ck.Column(3).AppendStringValues(strconv.FormatInt(int64(i), 10))
			ck.Column(3).AppendNilsV2(true)
			ck.Column(4).AppendStringValues("tv1")
			ck.Column(4).AppendNilsV2(true)
			ck.Column(5).AppendStringValues("tv2")
			ck.Column(5).AppendNilsV2(true)
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
