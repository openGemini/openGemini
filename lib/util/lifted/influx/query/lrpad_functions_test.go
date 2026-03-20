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

package query_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/stretchr/testify/assert"
)

// runPadTestCase is a helper function for both LPAD and RPAD.
func runPadTestCase(t *testing.T, stringer query.StringValuer, funcName, testName string, args []interface{}, expected interface{}) {
	t.Helper() // Marks this as a test helper function

	// Call the function being tested (either "lpad" or "rpad")
	out, ok := stringer.Call(funcName, args)

	// Assert that the call was successful and the output is correct
	assert.True(t, ok, "stringer.Call should succeed for test: %s", testName)
	assert.Equal(t, expected, out, "output should match expected for test: %s", testName)
}

func TestStringFunctionLpad(t *testing.T) {
	stringValuer := query.StringValuer{}
	t.Run("lpad", func(t *testing.T) {
		// Define all your test cases for LPAD in a "table"
		testCases := []struct {
			name     string
			input    interface{}
			length   int64
			padStr   string
			expected interface{}
		}{
			{
				name:     "Simple padding",
				input:    "XY",
				length:   10,
				padStr:   "ha",
				expected: "hahahahaXY",
			},
			{
				name:     "Simple padding",
				input:    "XYZ",
				length:   10,
				padStr:   "ha",
				expected: "hahahahXYZ",
			},
			{
				name:     "Truncation",
				input:    "this is string",
				length:   10,
				padStr:   "xx",
				expected: "this is st",
			},
			{
				name:     "Zero Length",
				input:    "XYZ",
				length:   0,
				padStr:   "xx",
				expected: "XYZ",
			},
			{
				name:     "Negative Length",
				input:    "XYZ",
				length:   -1,
				padStr:   "xx",
				expected: "XYZ",
			},
			{
				name:     "Empty Pad String",
				input:    "XYZ",
				length:   10,
				padStr:   "",
				expected: "XYZ",
			},
		}

		// Loop over the test cases and call the generic helper
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				args := []interface{}{tc.input, tc.length, tc.padStr}
				runPadTestCase(t, stringValuer, "lpad", tc.name, args, tc.expected)
			})
		}
	})
}

func TestStringFunctionRpad(t *testing.T) {
	stringValuer := query.StringValuer{}
	t.Run("rpad", func(t *testing.T) {
		// Define all your test cases for RPAD
		testCases := []struct {
			name     string
			input    interface{}
			length   int64
			padStr   string
			expected interface{}
		}{
			{
				name:     "Simple padding",
				input:    "XY",
				length:   10,
				padStr:   "ha",
				expected: "XYhahahaha",
			},
			{
				name:     "Simple padding",
				input:    "XYZ",
				length:   10,
				padStr:   "ha",
				expected: "XYZhahahah",
			},
			{
				name:     "Truncation",
				input:    "this is abc",
				length:   10,
				padStr:   "xx",
				expected: "this is ab",
			},
			{
				name:     "Zero Length",
				input:    "XYZ",
				length:   0,
				padStr:   "xx",
				expected: "XYZ",
			},
			{
				name:     "Negative Length",
				input:    "XYZ",
				length:   -1,
				padStr:   "xx",
				expected: "XYZ",
			},
			{
				name:     "Empty Pad String",
				input:    "XYZ",
				length:   10,
				padStr:   "",
				expected: "XYZ",
			},
		}

		// Loop over the test cases and call the same generic helper
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				args := []interface{}{tc.input, tc.length, tc.padStr}
				runPadTestCase(t, stringValuer, "rpad", tc.name, args, tc.expected)
			})
		}
	})
}
