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
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/stretchr/testify/assert"
)

// TestRegistryLLMFunc tests the registration and retrieval of LLM functions
func TestRegistryLLMFunc(t *testing.T) {
	// Test registration of new function
	RegistryLLMFunc("test_func", &LLMGenerate{})
	assert.NotNil(t, GetLLMFunc("test_func"), "Should return registered function")

	// Test registration of duplicate function
	RegistryLLMFunc("test_func", &LLMGenerate{})
	assert.NotNil(t, GetLLMFunc("test_func"), "Should not overwrite existing function")

	// Test retrieval of non-existent function
	assert.Nil(t, GetLLMFunc("nonexistent"), "Should return nil for nonexistent function")
}

// TestBaseLLMFunc_Process tests the base LLM function processing
func TestBaseLLMFunc_Process(t *testing.T) {
	// Setup test data
	src := NewColumnImpl(influxql.String)
	src.AppendStringValues([]string{"test1", "test2", "test3"})
	src.AppendManyNotNil(3)
	dst := NewColumnImpl(influxql.String)

	// Test with empty source
	srcEmpty := NewColumnImpl(influxql.String)
	err := NewBaseLLMFunc("arg", "prompt", nil).Process(dst, srcEmpty)
	assert.NoError(t, err, "Should handle empty source gracefully")

	// Test with nil values
	srcNil := NewColumnImpl(influxql.String)
	srcNil.AppendManyNil(3)
	err = NewBaseLLMFunc("arg", "prompt", nil).Process(dst, srcNil)
	assert.NoError(t, err, "Should handle all nil values gracefully")

	// Test the input and output lengths are not aligned.
	err = NewBaseLLMFunc("arg", "prompt", func(prompt, input string) ([]string, error) {
		return []string{"processed1", "processed2"}, nil
	}).Process(dst, src)
	assert.Error(t, err, "not aligned")

	// Test the output error.
	err = NewBaseLLMFunc("arg", "prompt", func(prompt, input string) ([]string, error) {
		return []string{}, fmt.Errorf("unmarshal error")
	}).Process(dst, src)
	assert.Error(t, err, "unmarshal error")

	// Test normal processing
	err = NewBaseLLMFunc("arg", "prompt", func(prompt, input string) ([]string, error) {
		return []string{"processed1", "processed2", "processed3"}, nil
	}).Process(dst, src)
	assert.NoError(t, err, "Should process normal input successfully")
	assert.Equal(t, 6, dst.Length(), "Should have correct number of processed values")
}

// TestLLMGenerate_InitOp tests the LLM generate function initialization
func TestLLMGenerate_InitOp(t *testing.T) {
	// Test initialization with sample arguments
	gen := LLMGenerate{}
	genFunc := gen.InitOp("test_arg", func(prompt, input string) ([]string, error) {
		return []string{input}, nil
	})

	assert.Equal(t, genFunc.Name(), gen.Name())
	assert.NotNil(t, genFunc, "Should return initialized function")
	assert.Equal(t, gen.GetPrompt("test_arg"), genFunc.(*LLMGenerate).BaseLLMFunc.prompt, "Should set prompt correctly")
}

// TestLLMClassify_InitOp tests the LLM classify function initialization
func TestLLMClassify_InitOp(t *testing.T) {
	// Test initialization with sample arguments
	classifier := LLMClassify{}
	classifyFunc := classifier.InitOp("test_arg", func(prompt, input string) ([]string, error) {
		return []string{input}, nil
	})

	assert.Equal(t, classifier.Name(), classifyFunc.Name())
	assert.NotNil(t, classifyFunc, "Should return initialized function")
	assert.Equal(t, classifier.GetPrompt("test_arg"), classifyFunc.(*LLMClassify).BaseLLMFunc.prompt, "Should set prompt correctly")
}

// TestLLMSummarize_InitOp tests the LLM summarize function initialization
func TestLLMSummarize_InitOp(t *testing.T) {
	// Test initialization with sample arguments
	summarizer := LLMSummarize{}
	summarizeFunc := summarizer.InitOp("test_arg", func(prompt, input string) ([]string, error) {
		return []string{input}, nil
	})

	assert.Equal(t, summarizer.Name(), summarizeFunc.Name())
	assert.NotNil(t, summarizeFunc, "Should return initialized function")
	assert.Equal(t, summarizer.GetPrompt("test_arg"), summarizeFunc.(*LLMSummarize).BaseLLMFunc.prompt, "Should set prompt correctly")
}

// TestTransForwardInteger tests integer column forwarding
func TestTransForwardInteger(t *testing.T) {
	// Setup test data
	src := NewColumnImpl(influxql.Integer)
	src.AppendIntegerValues([]int64{1, 2, 3})
	dst := NewColumnImpl(influxql.Integer)

	// Test normal forwarding
	err := TransForwardInteger(dst, src)
	assert.NoError(t, err, "Should forward integers successfully")
	assert.Equal(t, src.integerValues, dst.integerValues, "Should copy integer values correctly")
}

// TestTransForwardFloat tests float column forwarding
func TestTransForwardFloat(t *testing.T) {
	// Setup test data
	src := NewColumnImpl(influxql.Float)
	src.AppendFloatValues([]float64{1.1, 2.2, 3.3})
	dst := NewColumnImpl(influxql.Float)

	// Test normal forwarding
	err := TransForwardFloat(dst, src)
	assert.NoError(t, err, "Should forward floats successfully")
	assert.Equal(t, src.floatValues, dst.floatValues, "Should copy float values correctly")
}

// TestTransForwardBoolean tests boolean column forwarding
func TestTransForwardBoolean(t *testing.T) {
	// Setup test data
	src := NewColumnImpl(influxql.Boolean)
	src.AppendBooleanValues([]bool{true, false, true})
	dst := NewColumnImpl(influxql.Boolean)

	// Test normal forwarding
	err := TransForwardBoolean(dst, src)
	assert.NoError(t, err, "Should forward booleans successfully")
	assert.Equal(t, src.booleanValues, dst.booleanValues, "Should copy boolean values correctly")
}

// TestTransForwardString tests string column forwarding
func TestTransForwardString(t *testing.T) {
	// Setup test data
	src := NewColumnImpl(influxql.String)
	src.AppendStringValues([]string{"test1", "test2", "test3"})
	dst := NewColumnImpl(influxql.String)

	// Test normal forwarding
	err := TransForwardString(dst, src)
	assert.NoError(t, err, "Should forward strings successfully")
	assert.Equal(t, src.StringValuesRange(nil, 0, src.Length()), dst.StringValuesRange(nil, 0, src.Length()), "Should copy string values correctly")
}
