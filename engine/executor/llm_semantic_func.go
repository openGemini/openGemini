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
	"encoding/json"
	"fmt"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

func init() {
	RegistryLLMFunc(query.LLM_GENERATE, &LLMGenerate{})
	RegistryLLMFunc(query.LLM_CLASSIFY, &LLMClassify{})
	RegistryLLMFunc(query.LLM_SUMMARIZE, &LLMSummarize{})
}

type LLMFunc interface {
	Name() string
	InitOp(arg string, f func(prompt string, input string) ([]string, error)) LLMFunc
	Process(dst, src Column) error
}

var LLMFactoryInstance = map[string]LLMFunc{}

func GetLLMFunc(name string) LLMFunc {
	return LLMFactoryInstance[name]
}

func RegistryLLMFunc(name string, op LLMFunc) {
	_, ok := LLMFactoryInstance[name]
	if ok {
		return
	}
	LLMFactoryInstance[name] = op
}

type BaseLLMFunc struct {
	name    string
	prompt  string
	buf     []string
	callLLM func(prompt string, input string) ([]string, error)
}

func NewBaseLLMFunc(
	name string,
	prompt string,
	callLLM func(prompt string, input string) ([]string, error)) *BaseLLMFunc {
	return &BaseLLMFunc{name: name, prompt: prompt, callLLM: callLLM}
}

func (b *BaseLLMFunc) Process(dst, src Column) error {
	if src.Length() == src.NilCount() || src.Length() == 0 {
		AdjustNils(dst, src, 0, src.Length())
		return nil
	}

	start, end := src.GetRangeValueIndexV2(0, src.Length())
	b.buf = src.StringValuesRange(b.buf[:0], start, end)
	q, err := json.Marshal(b.buf)
	if err != nil {
		return err
	}
	dstValues, err := b.callLLM(b.prompt, util.Bytes2str(q))
	if err != nil {
		return err
	}
	if len(b.buf) != len(dstValues) {
		return fmt.Errorf("input: %d and output: %d of the %s are not aligned", len(b.buf), len(dstValues), b.name)
	}
	dst.AppendStringValues(dstValues)
	AdjustNils(dst, src, 0, src.Length())
	return nil
}

type LLMGenerate struct {
	BaseLLMFunc
}

func (o *LLMGenerate) Name() string {
	return query.LLM_GENERATE
}

func (o *LLMGenerate) GetPrompt(prompt string) string {
	return fmt.Sprintf(`
### Batch Content Generation Task  
Generate content for each parameter entry below. Output as JSON array: ["Content 1", "Content 2", ...].  
Specific requirements: %s
 
#### Parameter List:  
### {JSON array of objects, e.g., [{"theme":"Eco", "style":"Formal", "format":"Paragraph"}, ...]}  
 
#### Example:  
Parameter List:  
[
  {"theme":"AI", "style":"Concise", "format":"Keywords", "req":"3 core terms"},
  {"theme":"Smart City", "style":"Vision", "format":"Phrases", "req":"2 development directions"}
]  
Output: ["Machine Learning, NLP, Computer Vision", "Intelligent Transportation, Digital Governance"]  
`, prompt)
}

func (o *LLMGenerate) InitOp(arg string, callLLM func(prompt string, input string) ([]string, error)) LLMFunc {
	return &LLMGenerate{BaseLLMFunc: *NewBaseLLMFunc(o.Name(), o.GetPrompt(arg), callLLM)}
}

type LLMClassify struct {
	BaseLLMFunc
}

func (o *LLMClassify) Name() string {
	return query.LLM_CLASSIFY
}

func (o *LLMClassify) GetPrompt(prompt string) string {
	return fmt.Sprintf(`
### Batch Text Classification Task  
Classify each text in the list below with the most suitable label from candidates. Output as JSON array: ["label1", "label2", ...].
Specific requirements: %s
 
#### Text List:
### {JSON array, e.g., ["Text 1", "Text 2"]}
 
#### Candidate Labels:
### [Label 1, Label 2, ..., Label n]
 
#### Example:
Text List: ["AI algorithm optimizes recommendation system", "New policy requires algorithm rule disclosure"]
Candidates: [Tech Upgrade, Policy Regulation, User Service]
Output: ["Tech Upgrade", "Policy Regulation"]
`, prompt)
}

func (o *LLMClassify) InitOp(arg string, callLLM func(prompt string, input string) ([]string, error)) LLMFunc {
	return &LLMClassify{BaseLLMFunc: *NewBaseLLMFunc(o.Name(), o.GetPrompt(arg), callLLM)}
}

type LLMSummarize struct {
	BaseLLMFunc
}

func (o *LLMSummarize) Name() string {
	return query.LLM_SUMMARIZE
}

func (o *LLMSummarize) GetPrompt(prompt string) string {
	return fmt.Sprintf(`
### Batch Text Summarization Task  
Summarize each text in the list below. Output as JSON array: ["Summary 1", "Summary 2", ...], each ≤ {X} words.  
Specific requirements: %s
 
#### Text List:  
### {JSON array, e.g., ["Text 1", "Text 2"]}  
 
#### Example:  
Text List:  
["2024 EV sales up 0.42, China accounts for 0.58", "Quantum computing can crack traditional codes but faces commercial challenges"]  
Output (≤30 words): ["2024 EV sales surged with China's 0.58 share", "Quantum computing shows theoretical edge but needs tech breakthrough for commercialization"]  
`, prompt)
}

func (o *LLMSummarize) InitOp(arg string, callLLM func(prompt string, input string) ([]string, error)) LLMFunc {
	return &LLMSummarize{BaseLLMFunc: *NewBaseLLMFunc(o.Name(), o.GetPrompt(arg), callLLM)}
}

func TransForwardInteger(dst Column, src Column) error {
	dst.AppendIntegerValues(src.IntegerValues())
	AdjustNils(dst, src, 0, src.Length())
	return nil
}

func TransForwardFloat(dst Column, src Column) error {
	dst.AppendFloatValues(src.FloatValues())
	AdjustNils(dst, src, 0, src.Length())
	return nil
}

func TransForwardBoolean(dst Column, src Column) error {
	dst.AppendBooleanValues(src.BooleanValues())
	AdjustNils(dst, src, 0, src.Length())
	return nil
}

func TransForwardString(dst Column, src Column) error {
	dst.CloneStringValues(src.GetStringBytes())
	AdjustNils(dst, src, 0, src.Length())
	return nil
}
