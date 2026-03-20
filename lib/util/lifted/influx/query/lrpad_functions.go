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

package query

import (
	"fmt"
	"strings"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

// Here is a sample to document the string functions in detail.
//
// Syntax: LPAD(str, length, padStr)
// Rules:
//  1. If `length` <= 0, then do nothing.
//  2. If `padStr` is empty, then do nothing.
//  3. Left padding `str` to `length` if `length` is greater than len(str).
//     `padStr` can be a single character or a string.
//     SQL standard says `padStr` is optional and default to space, but we does not implemented it.
//  4. If `length` is less than len(str), then truncate `str` to `length`.
//
// References:
//  1. SQL:2023, ISO_IEC 9075-2-2023 - SQL Foundation
//  2. SQL reference of InfluxQL and others.
//
// Our goal is not to fully compatible with SQL but to keep the implementation concise and consistent.

type lpadFunc struct {
	BaseInfo
}

func (s *lpadFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 3, Max: 3},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertIntegerLiteral}},
		&TypeCheckRule{Name: name, Index: 2, Asserts: []func(interface{}) bool{AssertStringLiteral}},
	}
}

func (s *lpadFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "lpad" {
		return fmt.Errorf("invalid name, expected %s, got %s", "lpad", expr.Name)
	}

	return compileAllStringArgs(expr, c)
}

func (s *lpadFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *lpadFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	return LpadString(args[0].(string), args[2].(string), int(args[1].(int64))), true
}

func LpadString(s, padStr string, length int) string {
	if length <= 0 || padStr == "" {
		return s
	}

	// Calculate the number of padding characters we need
	numPadChars := length - len(s)
	if numPadChars <= 0 {
		return s[:length]
	}

	// Repeat the padding string until we reach the desired length
	padding := strings.Repeat(padStr, (numPadChars+len(padStr)-1)/len(padStr))
	return padding[:numPadChars] + s
}

// Syntax: RPAD(str, length, padStr)

type rpadFunc struct {
	BaseInfo
}

func (s *rpadFunc) GetRules(name string) []CheckRule {
	return []CheckRule{
		&ArgNumberCheckRule{Name: name, Min: 3, Max: 3},
		&TypeCheckRule{Name: name, Index: 1, Asserts: []func(interface{}) bool{AssertIntegerLiteral}},
		&TypeCheckRule{Name: name, Index: 2, Asserts: []func(interface{}) bool{AssertStringLiteral}},
	}
}

func (s *rpadFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if expr.Name != "rpad" {
		return fmt.Errorf("invalid name, expected %s, got %s", "rpad", expr.Name)
	}

	return compileAllStringArgs(expr, c)
}

func (s *rpadFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	if args[0] != influxql.String {
		return influxql.Unknown, fmt.Errorf("invalid argument type for the first argument in %s(): %s", name, args[0])
	}

	return influxql.String, nil
}

func (s *rpadFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	return RpadString(args[0].(string), args[2].(string), int(args[1].(int64))), true
}

func RpadString(s, padStr string, length int) string {
	if length <= 0 || padStr == "" {
		return s
	}

	// Calculate the number of padding characters we need
	numPadChars := length - len(s)
	if numPadChars <= 0 {
		return s[:length]
	}

	// Repeat the padding string until we reach the desired length
	padding := strings.Repeat(padStr, (numPadChars+len(padStr)-1)/len(padStr))
	return s + padding[:numPadChars]
}
