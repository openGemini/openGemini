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

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	openai "github.com/openGemini/openGemini/lib/openai"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"go.uber.org/zap"
)

// LLMSemanticTransform is a semantic transformation operator implementing the Processor interface
type LLMSemanticTransform struct {
	BaseProcessor

	inputPort  *ChunkPort
	outputPort *ChunkPort

	inputChunk     chan Chunk
	nextSignal     chan Semaphore
	closedSignal   *bool
	nextSignalOnce sync.Once
	newChunk       Chunk
	chunkPool      *CircularChunkPool

	// Internal state
	logger *logger.Logger
	errs   errno.Errs
	ops    []hybridqp.ExprOptions
	schema hybridqp.Catalog

	// Processor
	transFuncs []func(dst Column, src Column) error

	ctx     context.Context
	span    *tracing.Span
	llmSpan *tracing.Span
}

// NewLLMSemanticTransform creates a new instance of LLMSemanticTransform
func NewLLMSemanticTransform(inRowDataType, outRowDataType hybridqp.RowDataType, ops []hybridqp.ExprOptions, schema hybridqp.Catalog) (*LLMSemanticTransform, error) {
	closedSignal := false
	st := &LLMSemanticTransform{
		logger:         logger.NewLogger(errno.ModuleQueryEngine),
		inputPort:      NewChunkPort(inRowDataType),
		outputPort:     NewChunkPort(outRowDataType),
		chunkPool:      NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		ops:            ops,
		schema:         schema,
		nextSignal:     make(chan Semaphore),
		inputChunk:     make(chan Chunk),
		closedSignal:   &closedSignal,
		nextSignalOnce: sync.Once{},
	}

	if err := st.initTransFunc(); err != nil {
		return nil, err
	}
	return st, nil
}

type LLMSemanticTransformCreator struct {
}

func (c *LLMSemanticTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	p, err := NewLLMSemanticTransform(plan.Children()[0].RowDataType(), plan.RowDataType(), plan.RowExprOptions(), plan.Schema())
	return p, err
}

var _ = RegistryTransformCreator(&LogicalLLMSemantic{}, &LLMSemanticTransformCreator{})

func (st *LLMSemanticTransform) Name() string {
	return "LLMSemanticTransform"
}

func (st *LLMSemanticTransform) Explain() []ValuePair {
	return nil
}

func (st *LLMSemanticTransform) initSpan() {
	st.span = st.StartSpan("[LLMSemanticTransform] TotalWorkCost", true)
	if st.span != nil {
		st.llmSpan = st.span.StartSpan("llm_call")
	}
}

func (st *LLMSemanticTransform) closeNextSignal() {
	st.nextSignalOnce.Do(func() {
		close(st.nextSignal)
	})
}

func (st *LLMSemanticTransform) initClient(properties map[string]string) (openai.Client, error) {
	var err error
	var ops []openai.ClientOption
	var maxRetries int
	var retryDelay time.Duration

	timeOut := openai.DefaultTimeOut
	apiEndpoint, ok := properties[influxql.LLMEndpoint]
	if !ok {
		return nil, fmt.Errorf("%s must be required", influxql.LLMEndpoint)
	}

	apiKey, ok := properties[influxql.LLMApiKey]
	if !ok {
		return nil, fmt.Errorf("%s must be required", influxql.LLMApiKey)
	}

	llmMaxRetries, ok := properties[influxql.LLMMaxRetries]
	if !ok {
		maxRetries = openai.DefaultMaxRetries
	} else {
		maxRetries, err = strconv.Atoi(llmMaxRetries)
		if err != nil {
			return nil, fmt.Errorf("%s must be int: %v", influxql.LLMMaxRetries, err)
		}
	}

	llmRetryDelayMs, ok := properties[influxql.LLMRetryDelayMs]
	if !ok {
		retryDelay = openai.DefaultRetryDelay
	} else {
		retryDelayMs, err := strconv.Atoi(llmRetryDelayMs)
		if err != nil {
			return nil, fmt.Errorf("%s must be int: %v", influxql.LLMRetryDelayMs, err)
		}
		retryDelay = time.Duration(retryDelayMs) * time.Millisecond
	}
	ops = append(ops, openai.WithTimeout(timeOut))
	ops = append(ops, openai.WithRetryPolicy(maxRetries, retryDelay))
	client := openai.NewClient(apiEndpoint, apiKey, ops...)
	return client, nil
}

func (st *LLMSemanticTransform) initFunc(client openai.Client, properties map[string]string, call, prompt string) (LLMFunc, error) {
	var err error
	modelName, ok := properties[influxql.LLMModelName]
	if !ok {
		return nil, fmt.Errorf("%s must be required", influxql.LLMModelName)
	}
	temperature := openai.DefaultTemperature
	maxTokens := openai.DefaultMaxTokens
	llmTemperature, ok := properties[influxql.LLMTemperature]
	if ok {
		temperature, err = strconv.ParseFloat(llmTemperature, 64)
		if err != nil {
			return nil, fmt.Errorf("%s must be float: %v", influxql.LLMTemperature, err)
		}
	}

	llmMaxTokens, ok := properties[influxql.LLMMaxTokens]
	if ok {
		maxTokens, err = strconv.Atoi(llmMaxTokens)
		if err != nil {
			return nil, fmt.Errorf("%s must be int: %v", influxql.LLMMaxTokens, err)
		}
	}

	callLLMApi := NewCallLLMApiFunc(context.Background(), client, modelName, temperature, maxTokens, st.logger)
	llmOp := GetLLMFunc(call).InitOp(prompt, callLLMApi)
	return llmOp, nil
}

func (st *LLMSemanticTransform) initTransFunc() error {
	st.transFuncs = make([]func(dst Column, src Column) error, len(st.ops))
	for i, opt := range st.ops {
		switch vr := opt.Expr.(type) {
		case *influxql.VarRef:
			switch vr.Type {
			case influxql.Integer:
				st.transFuncs[i] = TransForwardInteger
			case influxql.Float:
				st.transFuncs[i] = TransForwardFloat
			case influxql.Boolean:
				st.transFuncs[i] = TransForwardBoolean
			case influxql.String, influxql.Tag:
				st.transFuncs[i] = TransForwardString
			default:
				return fmt.Errorf("invalid datatype: %s", vr.Type.String())
			}
		case *influxql.Call:
			if len(vr.Args) != 3 {
				return errors.New("llm args should be 3")
			}
			ref, ok := vr.Args[0].(*influxql.VarRef)
			if !ok || (ref.Type != influxql.String && ref.Type != influxql.Tag) {
				return errors.New("llm ref should be string")
			}
			resource, ok := vr.Args[1].(*influxql.StringLiteral)
			if !ok {
				return errors.New("llm resource should be StringLiteral")
			}
			properties := st.schema.GetResource(resource.Val)
			if len(properties) == 0 {
				return errno.NewError(errno.ResourceNotFound, resource.Val)
			}
			client, err := st.initClient(properties)
			if err != nil {
				return err
			}
			prompt, ok := vr.Args[2].(*influxql.StringLiteral)
			if !ok {
				return errors.New("llm prompt should be StringLiteral")
			}
			llmOp, err := st.initFunc(client, properties, vr.Name, prompt.Val)
			if err != nil {
				return err
			}
			st.transFuncs[i] = llmOp.Process
		default:
			return fmt.Errorf("invalid ops")
		}
	}
	return nil
}

func (st *LLMSemanticTransform) Work(ctx context.Context) error {
	st.ctx = ctx
	st.initSpan()
	defer func() {
		tracing.Finish(st.llmSpan)
		st.Close()
	}()

	errs := &st.errs
	errs.Init(2, st.Close)

	go st.produce(ctx, errs)
	go st.consume(ctx, errs)
	return errs.Err()
}

func (st *LLMSemanticTransform) produce(ctx context.Context, errs *errno.Errs) {
	defer func() {
		tracing.Finish(st.span)
		close(st.inputChunk)
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			st.logger.Error(err.Error(),
				zap.String("query", "LLMSemanticTransform"),
				zap.Uint64("query_id", st.schema.Options().GetQueryID()))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()

	for {
		select {
		case c, ok := <-st.inputPort.State:
			tracing.StartPP(st.span)
			if !ok {
				return
			}
			st.inputChunk <- c
			if _, sOk := <-st.nextSignal; !sOk {
				return
			}
			tracing.EndPP(st.span)
		case <-ctx.Done():
			st.closeNextSignal()
			*st.closedSignal = true
			return
		}
	}
}

func (st *LLMSemanticTransform) consume(
	ctx context.Context, errs *errno.Errs,
) {
	var err error
	defer func() {
		st.closeNextSignal()
		if e := recover(); e != nil {
			err = errno.NewError(errno.RecoverPanic, e)
			st.logger.Error(err.Error(),
				zap.String("query", "LLMSemanticTransform"),
				zap.Uint64("query_id", st.schema.Options().GetQueryID()))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(err)
		}
	}()

	st.newChunk = st.chunkPool.GetChunk()
	if len(st.transFuncs) != st.newChunk.NumberOfCols() {
		err = fmt.Errorf("input for lllmFunc is not aligned")
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case ck, ok := <-st.inputChunk:
			// Step 1: Collect the data
			if !ok || ck == nil {
				return
			}
			if *st.closedSignal {
				return
			}

			// Step 2: Invoke the large language model for analysis
			tracing.StartPP(st.llmSpan)
			for i, transFunc := range st.transFuncs {
				if err = transFunc(st.newChunk.Column(i), ck.Column(i)); err != nil {
					return
				}
			}
			st.newChunk.SetName(ck.Name())
			st.newChunk.AppendTagsAndIndexes(ck.Tags(), ck.TagIndex())
			st.newChunk.AppendIntervalIndexes(ck.IntervalIndex())
			st.newChunk.AppendTimes(ck.Time())
			tracing.EndPP(st.llmSpan)

			// Step 3: Send the result
			st.sendChunk()
		}
	}
}

func (st *LLMSemanticTransform) sendChunk() {
	st.outputPort.State <- st.newChunk
	st.newChunk = st.chunkPool.GetChunk()
	st.nextSignal <- signal
}

func (st *LLMSemanticTransform) GetOutputs() Ports {
	return Ports{st.outputPort}
}

func (st *LLMSemanticTransform) GetInputs() Ports {
	return Ports{st.inputPort}
}

func (st *LLMSemanticTransform) GetOutputNumber(port Port) int {
	if st.outputPort == port {
		return 0
	}
	return INVALID_NUMBER
}

func (st *LLMSemanticTransform) GetInputNumber(port Port) int {
	if st.inputPort == port {
		return 0
	}
	return INVALID_NUMBER
}

func (st *LLMSemanticTransform) Close() {
	st.outputPort.Close()
	st.chunkPool.Release()
}

// NewCallLLMApiFunc used to generate callLLMAPI that calls the LLM API for processing
func NewCallLLMApiFunc(ctx context.Context, client openai.Client, modelName string, temperature float64, maxTokens int, logger *logger.Logger) func(prompt string, question string) ([]string, error) {
	return func(prompt string, question string) ([]string, error) {
		req := openai.ChatCompletionRequest{
			Model: modelName,
			Messages: []openai.ChatCompletionMessage{
				{
					Role:    openai.ChatMessageRoleSystem,
					Content: prompt,
				},
				{
					Role:    openai.ChatMessageRoleUser,
					Content: question,
				},
			},
			Temperature: temperature,
			MaxTokens:   maxTokens,
		}

		resp, err := client.CreateChatCompletion(ctx, req)
		if err != nil {
			logger.Error("Failed to create chat completion:", zap.Error(err))
			return nil, err
		}

		if len(resp.Choices) == 0 {
			logger.Error("No valid response received")
			return nil, fmt.Errorf("no valid response received")
		}

		logger.Debug("LLM Response",
			zap.Int("prompt_tokens", resp.Usage.PromptTokens),
			zap.Int("completion_tokens", resp.Usage.CompletionTokens),
			zap.Int("total_tokens", resp.Usage.TotalTokens),
		)

		result := resp.Choices[0].Message.Content
		result = strings.ReplaceAll(strings.ReplaceAll(result, "\r\n", ""), "\n", "")

		var resBody []string
		if err = json.Unmarshal(util.Str2bytes(result), &resBody); err != nil {
			return nil, err
		}
		return resBody, nil
	}
}
