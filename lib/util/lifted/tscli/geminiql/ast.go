// Copyright 2025 openGemini Authors
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

package geminiql

type Pair struct {
	vals [2]interface{}
}

func NewPair(first interface{}, second interface{}) *Pair {
	p := &Pair{}
	p.vals[0] = first
	p.vals[1] = second
	return p
}

func (p *Pair) First() interface{} {
	return p.vals[0]
}

func (p *Pair) Second() interface{} {
	return p.vals[1]
}

type Pairs []Pair

type QLAst struct {
	Stmt  Statement
	Error error
}

type Statement interface {
	stmt()
}

type InsertStatement struct {
	DB           string
	RP           string
	LineProtocol string
}

func (s *InsertStatement) stmt() {}

type UseStatement struct {
	DB string
	RP string
}

func (s *UseStatement) stmt() {}

type SetStatement struct {
	KVS []Pair
}

type PrecisionStatement struct {
	Precision string
}

func (s *PrecisionStatement) stmt() {}

func (s *SetStatement) stmt() {}

type ChunkedStatement struct {
}

func (s *ChunkedStatement) stmt() {}

type ChunkSizeStatement struct {
	Size int64
}

func (s *ChunkSizeStatement) stmt() {}

type AuthStatement struct {
}

func (s *AuthStatement) stmt() {}

type HelpStatement struct {
}

func (s *HelpStatement) stmt() {}

type TimerStatement struct {
}

func (s *TimerStatement) stmt() {}

type DebugStatement struct{}

func (s *DebugStatement) stmt() {}

type PromptStatement struct{}

func (s *PromptStatement) stmt() {}

type VerticalStatement struct{}

func (s *VerticalStatement) stmt() {}
