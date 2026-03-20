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
// See the Request to the specific language governing permissions and
// limitations under the License.

package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/openai"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

type mockClient struct {
}

func (m *mockClient) CreateChatCompletion(ctx context.Context, req openai.ChatCompletionRequest) (openai.ChatCompletionResponse, error) {
	response := openai.ChatCompletionResponse{
		ID:      "test-id",
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   "gpt-3.5-turbo",
		Choices: []struct {
			Index   int                          `json:"index"`
			Message openai.ChatCompletionMessage `json:"message"`
		}([]struct {
			Index   int
			Message openai.ChatCompletionMessage
		}{
			{
				Index: 0,
				Message: openai.ChatCompletionMessage{
					Role:    openai.ChatMessageRoleAssistant,
					Content: "[\"CN\", \"US\", \"DE\", \"JP\"]",
				},
			},
		}),
	}
	return response, nil
}

func (m *mockClient) CreateChatCompletionStream(ctx context.Context, req openai.ChatCompletionRequest) (openai.Stream, error) {
	return nil, nil
}

func testLLMSemanticTransformBase(
	t *testing.T,
	inChunks []executor.Chunk, dstChunks []executor.Chunk,
	inRowDataType, outRowDataType hybridqp.RowDataType,
	exprOpt []hybridqp.ExprOptions, schema hybridqp.Catalog,
	client openai.Client,
) {
	// generate each executor node node to build a dag.
	source := NewSourceFromMultiChunk(inRowDataType, inChunks)
	patch1 := gomonkey.ApplyFunc(openai.NewClient, func(endPoint, apiKey string, opts ...openai.ClientOption) openai.Client {
		return client
	})
	defer patch1.Reset()
	trans, err := executor.NewLLMSemanticTransform(
		inRowDataType,
		outRowDataType,
		exprOpt,
		schema)
	assert.NoError(t, err)
	assert.Equal(t, trans.GetInputNumber(nil), executor.INVALID_NUMBER)
	assert.Equal(t, trans.GetOutputNumber(nil), executor.INVALID_NUMBER)
	assert.Equal(t, trans.Name(), "LLMSemanticTransform")
	assert.Equal(t, len(trans.Explain()), 0)
	sink := NewNilSink(outRowDataType)
	err = executor.Connect(source.Output, trans.GetInputs()[0])
	if err != nil {
		t.Fatalf("source-trans connect error: %v", err)
	}
	err = executor.Connect(trans.GetOutputs()[0], sink.Input)
	if err != nil {
		t.Fatalf("trans-sink connect error: %v", err)
	}
	var processors executor.Processors
	processors = append(processors, source)
	processors = append(processors, trans)
	processors = append(processors, sink)

	// build the pipeline executor from the dag
	executors := executor.NewPipelineExecutor(processors)
	err = executors.Execute(context.Background())
	if err != nil {
		t.Fatalf("execute error %v", err)
	}
	executors.Release()

	// check the result
	outChunks := sink.Chunks
	if len(dstChunks) != len(outChunks) {
		t.Fatalf("the chunk number is not the same as the target: %d != %d\n", len(dstChunks), len(outChunks))
	}
	for i := range outChunks {
		assert.Equal(t, outChunks[i].Name(), dstChunks[i].Name())
		assert.Equal(t, outChunks[i].Tags(), dstChunks[i].Tags())
		assert.Equal(t, outChunks[i].Time(), dstChunks[i].Time())
		assert.Equal(t, outChunks[i].TagIndex(), dstChunks[i].TagIndex())
		assert.Equal(t, outChunks[i].IntervalIndex(), dstChunks[i].IntervalIndex())
		for j := range outChunks[i].Columns() {
			assert.Equal(t, outChunks[i].Column(j), dstChunks[i].Column(j))
		}
	}
}

func buildLLMRowDataType() hybridqp.RowDataType {
	rowDataType := hybridqp.NewRowDataTypeImpl(
		influxql.VarRef{Val: "bool", Type: influxql.Boolean},
		influxql.VarRef{Val: "int64", Type: influxql.Integer},
		influxql.VarRef{Val: "float64", Type: influxql.Float},
		influxql.VarRef{Val: "string", Type: influxql.String},
	)
	return rowDataType
}

func buildLLMSrcChunk() executor.Chunk {
	rp := buildLLMRowDataType()
	b := executor.NewChunkBuilder(rp)
	chunk := b.NewChunk("mst")
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AppendIntervalIndex(0)

	chunk.Column(0).AppendBooleanValues([]bool{true, false, true})
	chunk.Column(0).AppendNilsV2(true, true, false, true, false)

	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3})
	chunk.Column(1).AppendNilsV2(true, true, true, false, false)

	chunk.Column(2).AppendFloatValues([]float64{3.3, 4.4, 5.5})
	chunk.Column(2).AppendNilsV2(false, false, true, true, true)

	chunk.Column(3).AppendStringValues([]string{"china", "american", "germany", "japan"})
	chunk.Column(3).AppendNilsV2(true, true, false, true, true)

	return chunk
}

func buildLLMDstChunk() executor.Chunk {
	rp := buildLLMRowDataType()
	b := executor.NewChunkBuilder(rp)
	chunk := b.NewChunk("mst")
	chunk.AppendTimes([]int64{1, 2, 3, 4, 5})
	chunk.AppendTagsAndIndex(*ParseChunkTags("host=A"), 0)
	chunk.AppendIntervalIndex(0)

	chunk.Column(0).AppendBooleanValues([]bool{true, false, true})
	chunk.Column(0).AppendNilsV2(true, true, false, true, false)

	chunk.Column(1).AppendIntegerValues([]int64{1, 2, 3})
	chunk.Column(1).AppendNilsV2(true, true, true, false, false)

	chunk.Column(2).AppendFloatValues([]float64{3.3, 4.4, 5.5})
	chunk.Column(2).AppendNilsV2(false, false, true, true, true)

	chunk.Column(3).AppendStringValues([]string{"CN", "US", "DE", "JP"})
	chunk.Column(3).AppendNilsV2(true, true, false, true, true)

	return chunk
}

func createLLMOps(llmFunc string) []hybridqp.ExprOptions {
	return []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{
				Val:  "bool",
				Type: influxql.Boolean,
			},

			Ref: influxql.VarRef{
				Val:  "bool",
				Type: influxql.Boolean,
			},
		},
		{
			Expr: &influxql.VarRef{
				Val:  "int64",
				Type: influxql.Integer,
			},

			Ref: influxql.VarRef{
				Val:  "int64",
				Type: influxql.Integer,
			},
		},
		{
			Expr: &influxql.VarRef{
				Val:  "float64",
				Type: influxql.Float,
			},

			Ref: influxql.VarRef{
				Val:  "float64",
				Type: influxql.Float,
			},
		},
		{
			Expr: &influxql.Call{
				Name: llmFunc,
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "string",
						Type: influxql.String,
					},
					&influxql.StringLiteral{
						Val: "gemini",
					},
					&influxql.StringLiteral{
						Val: "Represent countries using ISO 3166 country codes:",
					},
				},
			},
			Ref: influxql.VarRef{
				Val:  "string",
				Type: influxql.String,
			},
		},
	}
}

func createLLMSchema(llmFunc string) hybridqp.Catalog {
	opt := &query.ProcessorOptions{
		Dimensions: []string{"country"},
		ChunkSize:  6,
	}
	fields := []*influxql.Field{{Expr: &influxql.Call{
		Name: llmFunc,
		Args: []influxql.Expr{
			&influxql.VarRef{
				Val:  "string",
				Type: influxql.String,
			},
			&influxql.StringLiteral{
				Val: "gemini",
			},
			&influxql.StringLiteral{
				Val: "Represent countries using ISO 3166 country codes:",
			},
		},
	}}}
	schema := executor.NewQuerySchema(fields, []string{"bool", "int64", "float64", "string"}, opt, nil)
	return schema
}

func TestLLMSemanticTransform(t *testing.T) {
	llmFunc := query.LLM_GENERATE
	client := &mockClient{}
	schema := createLLMSchema(llmFunc)
	schema.SetResource("gemini", map[string]string{
		"llm.model_name": "gemini2.5pro", "llm.endpoint": "gemini_base_url", "llm.api_key": "***",
	})
	t.Run(llmFunc, func(t *testing.T) {
		exprOpt := createLLMOps(llmFunc)
		inChunks := []executor.Chunk{buildLLMSrcChunk()}
		dstChunks := []executor.Chunk{buildLLMDstChunk()}
		testLLMSemanticTransformBase(
			t,
			inChunks, dstChunks,
			buildLLMRowDataType(), buildLLMRowDataType(),
			exprOpt, schema, client,
		)
	})
}

func TestLLMSemanticTransform_ErrHandling(t *testing.T) {
	creator := &executor.LLMSemanticTransformCreator{}
	schema := createLLMSchema(query.LLM_GENERATE)
	schema.SetResource("gemini", map[string]string{
		"llm.model_name": "gemini2.5pro", "llm.endpoint": "gemini_base_url", "llm.api_key": "***",
	})
	reader := executor.NewLogicalReader(nil, schema)
	plan := executor.NewLogicalLLMSemantic(reader, schema)
	_, err := creator.Create(plan, nil)
	assert.NoError(t, err)

	inDataType, outDataType := buildLLMRowDataType(), buildLLMRowDataType()
	ops := []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{
				Val:  "string",
				Type: influxql.String,
			},
		}}
	_, err = executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
	assert.NoError(t, err)

	ops = []hybridqp.ExprOptions{
		{
			Expr: &influxql.VarRef{
				Val:  "unknown",
				Type: influxql.Unknown,
			},
		}}
	_, err = executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
	assert.Error(t, err, "invalid datatype")

	ops = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{
				Name: query.LLM_GENERATE,
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "string",
						Type: influxql.String,
					},
				},
			},
		}}
	_, err = executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
	assert.Error(t, err, "llm args should be 3")

	ops = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{
				Name: query.LLM_GENERATE,
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "int64",
						Type: influxql.Integer,
					},
					&influxql.StringLiteral{
						Val: "gemini",
					},
					&influxql.StringLiteral{
						Val: "Represent countries using ISO 3166 country codes:",
					},
				},
			},
		}}
	_, err = executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
	assert.Error(t, err, "llm ref should be string")

	ops = []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{
				Name: query.LLM_GENERATE,
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "string",
						Type: influxql.String,
					},
					&influxql.StringLiteral{
						Val: "gemini",
					},
					&influxql.NumberLiteral{
						Val: 9,
					},
				},
			},
		}}
	_, err = executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
	assert.Error(t, err, "llm prompt should be StringLiteral")

	ops = []hybridqp.ExprOptions{
		{
			Expr: &influxql.NumberLiteral{
				Val: 9,
			},
		}}
	_, err = executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
	assert.Error(t, err, "invalid ops")
}

func TestLLMSemanticTransform_initClient(t *testing.T) {
	ops := []hybridqp.ExprOptions{
		{
			Expr: &influxql.Call{
				Name: query.LLM_GENERATE,
				Args: []influxql.Expr{
					&influxql.VarRef{
						Val:  "str",
						Type: influxql.String,
					},
					&influxql.StringLiteral{
						Val: "gemini",
					},
					&influxql.StringLiteral{
						Val: "Represent countries using ISO 3166 country codes:",
					},
				},
			},
		}}
	convey.Convey("initClient_Without_apiEndPoint", t, func() {
		schema := createLLMSchema(query.LLM_GENERATE)
		schema.SetResource("gemini", map[string]string{
			"llm.model_name": "gemini2.5pro", "llm.api_key": "***",
		})
		reader := executor.NewLogicalReader(nil, schema)
		plan := executor.NewLogicalLLMSemantic(reader, schema)
		inDataType, outDataType := buildLLMRowDataType(), buildLLMRowDataType()
		_, err := executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
		convey.So(err, convey.ShouldBeError, "llm.endpoint must be required")
	})

	convey.Convey("initClient_Without_apiKey", t, func() {
		schema := createLLMSchema(query.LLM_GENERATE)
		schema.SetResource("gemini", map[string]string{
			"llm.model_name": "gemini2.5pro", "llm.endpoint": "gemini_base_url",
		})
		reader := executor.NewLogicalReader(nil, schema)
		plan := executor.NewLogicalLLMSemantic(reader, schema)
		inDataType, outDataType := buildLLMRowDataType(), buildLLMRowDataType()
		_, err := executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
		convey.So(err, convey.ShouldBeError, "llm.api_key must be required")
	})

	convey.Convey("initClient_With_max_retries", t, func() {
		schema := createLLMSchema(query.LLM_GENERATE)
		schema.SetResource("gemini", map[string]string{
			"llm.model_name": "gemini2.5pro", "llm.endpoint": "gemini_base_url", "llm.api_key": "***",
			"llm.max_retries": "2.1",
		})
		reader := executor.NewLogicalReader(nil, schema)
		plan := executor.NewLogicalLLMSemantic(reader, schema)
		inDataType, outDataType := buildLLMRowDataType(), buildLLMRowDataType()
		_, err := executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
		convey.So(err, convey.ShouldBeError, "llm.max_retries must be int: strconv.Atoi: parsing \"2.1\": invalid syntax")
	})

	convey.Convey("initClient_With_retry_delay_ms", t, func() {
		schema := createLLMSchema(query.LLM_GENERATE)
		schema.SetResource("gemini", map[string]string{
			"llm.model_name": "gemini2.5pro", "llm.endpoint": "gemini_base_url", "llm.api_key": "***",
			"llm.retry_delay_ms": "2.1",
		})
		reader := executor.NewLogicalReader(nil, schema)
		plan := executor.NewLogicalLLMSemantic(reader, schema)
		inDataType, outDataType := buildLLMRowDataType(), buildLLMRowDataType()
		_, err := executor.NewLLMSemanticTransform(inDataType, outDataType, ops, plan.Schema())
		convey.So(err, convey.ShouldBeError, "llm.retry_delay_ms must be int: strconv.Atoi: parsing \"2.1\": invalid syntax")
	})
}
