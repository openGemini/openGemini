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
	"context"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/hybridqp"
)

type SourceFromSingleChunk struct {
	BaseProcessor

	Output *ChunkPort
	Chunk  Chunk
}

func NewSourceFromSingleChunk(rowDataType hybridqp.RowDataType, chunk Chunk) *SourceFromSingleChunk {
	return &SourceFromSingleChunk{
		Output: NewChunkPort(rowDataType),
		Chunk:  chunk,
	}
}

func (source *SourceFromSingleChunk) Name() string {
	return "SourceFromSingleChunk"
}

func (source *SourceFromSingleChunk) Explain() []ValuePair {
	return nil
}

func (source *SourceFromSingleChunk) Close() {
	source.Output.Close()
}

func (source *SourceFromSingleChunk) Work(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if source.Chunk == nil {
				source.Output.Close()
				return nil
			}

			source.Output.State <- source.Chunk
			source.Chunk = nil
		}
	}
}

func (source *SourceFromSingleChunk) GetOutputs() Ports {
	return Ports{source.Output}
}

func (source *SourceFromSingleChunk) GetInputs() Ports {
	return Ports{}
}

func (source *SourceFromSingleChunk) GetOutputNumber(_ Port) int {
	return 0
}

func (source *SourceFromSingleChunk) GetInputNumber(_ Port) int {
	return INVALID_NUMBER
}

type CancelOnlySource struct {
	BaseProcessor

	Output *ChunkPort
}

func NewCancelOnlySource(rowDataType hybridqp.RowDataType) *CancelOnlySource {
	return &CancelOnlySource{
		Output: NewChunkPort(rowDataType),
	}
}

func (source *CancelOnlySource) Name() string {
	return "SourceFromSingleRecord"
}

func (source *CancelOnlySource) Explain() []ValuePair {
	return nil
}

func (source *CancelOnlySource) Close() {
	source.Output.Close()
}

func (source *CancelOnlySource) Work(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (source *CancelOnlySource) GetOutputs() Ports {
	return Ports{source.Output}
}

func (source *CancelOnlySource) GetInputs() Ports {
	return Ports{}
}

func (source *CancelOnlySource) GetOutputNumber(_ Port) int {
	return 0
}

func (source *CancelOnlySource) GetInputNumber(_ Port) int {
	return INVALID_NUMBER
}

type NilSink struct {
	BaseProcessor

	Input *ChunkPort
}

func NewNilSink(rowDataType hybridqp.RowDataType) *NilSink {
	return &NilSink{
		Input: NewChunkPort(rowDataType),
	}
}

func (sink *NilSink) Name() string {
	return "NilSink"
}

func (sink *NilSink) Explain() []ValuePair {
	return nil
}

func (sink *NilSink) Close() {}

func (sink *NilSink) Work(ctx context.Context) error {
	for {
		select {
		case r, ok := <-sink.Input.State:
			if !ok {
				return nil
			}
			_ = r
		case <-ctx.Done():
			return nil
		}
	}
}

func (sink *NilSink) GetOutputs() Ports {
	return Ports{}
}

func (sink *NilSink) GetInputs() Ports {
	if sink.Input == nil {
		return Ports{}
	}

	return Ports{sink.Input}
}

func (sink *NilSink) GetOutputNumber(_ Port) int {
	return INVALID_NUMBER
}

func (sink *NilSink) GetInputNumber(_ Port) int {
	return 0
}

type SinkFromFunction struct {
	BaseProcessor

	Input    *ChunkPort
	Function func(chunk Chunk) error
}

func NewSinkFromFunction(rowDataType hybridqp.RowDataType, function func(chunk Chunk) error) *SinkFromFunction {
	return &SinkFromFunction{
		Input:    NewChunkPort(rowDataType),
		Function: function,
	}
}

func (sink *SinkFromFunction) Name() string {
	return "SinkFromFunction"
}

func (sink *SinkFromFunction) Explain() []ValuePair {
	return nil
}

func (sink *SinkFromFunction) Close() {}

func (sink *SinkFromFunction) Work(ctx context.Context) error {
	for {
		select {
		case r, ok := <-sink.Input.State:
			if !ok {
				return nil
			}
			err := sink.Function(r)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (sink *SinkFromFunction) GetOutputs() Ports {
	return Ports{}
}

func (sink *SinkFromFunction) GetInputs() Ports {
	if sink.Input == nil {
		return Ports{}
	}

	return Ports{sink.Input}
}

func (sink *SinkFromFunction) GetOutputNumber(_ Port) int {
	return INVALID_NUMBER
}

func (sink *SinkFromFunction) GetInputNumber(_ Port) int {
	return 0
}

type NilTransform struct {
	BaseProcessor

	Inputs  ChunkPorts
	Outputs ChunkPorts
}

func NewNilTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataTypes []hybridqp.RowDataType) *NilTransform {
	trans := &NilTransform{
		Inputs:  make(ChunkPorts, 0, len(inRowDataTypes)),
		Outputs: make(ChunkPorts, 0, len(outRowDataTypes)),
	}

	for _, rowDataType := range inRowDataTypes {
		input := NewChunkPort(rowDataType)
		trans.Inputs = append(trans.Inputs, input)
	}

	for _, rowDataType := range outRowDataTypes {
		output := NewChunkPort(rowDataType)
		trans.Outputs = append(trans.Outputs, output)
	}

	return trans
}

func (trans *NilTransform) Name() string {
	return "NilTransform"
}

func (trans *NilTransform) Explain() []ValuePair {
	return nil
}

func (trans *NilTransform) Close() {
	trans.Outputs.Close()
}

func (trans *NilTransform) Work(ctx context.Context) error {
	var wg sync.WaitGroup

	var gout int32 = 0

	runnable := func(in int) {
		defer wg.Done()
		for {
			select {
			case r, ok := <-trans.Inputs[in].State:
				if !ok {

					return
				}

				if len(trans.Outputs) > 0 {
					out := int(atomic.AddInt32(&gout, 1)) % len(trans.Outputs)
					trans.Outputs[out].State <- r
				}
			case <-ctx.Done():
				return
			}
		}
	}

	for i := range trans.Inputs {
		wg.Add(1)
		go runnable(i)
	}

	wg.Wait()

	trans.Close()

	return nil
}

func (trans *NilTransform) GetOutputs() Ports {
	ports := make(Ports, 0, len(trans.Outputs))

	for _, output := range trans.Outputs {
		ports = append(ports, output)
	}
	return ports
}

func (trans *NilTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.Inputs))

	for _, input := range trans.Inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *NilTransform) GetOutputNumber(port Port) int {
	for i, output := range trans.Outputs {
		if output == port {
			return i
		}
	}
	return INVALID_NUMBER
}

func (trans *NilTransform) GetInputNumber(port Port) int {
	for i, input := range trans.Inputs {
		if input == port {
			return i
		}
	}
	return INVALID_NUMBER
}
