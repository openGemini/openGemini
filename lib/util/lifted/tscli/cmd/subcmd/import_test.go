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

package subcmd

import (
	"testing"

	"github.com/openGemini/openGemini-cli/core"
	"github.com/stretchr/testify/require"
)

func TestParseTimestamp(t *testing.T) {
	type testCase struct {
		timestamp string
		precision string
		expect    int64
	}

	testCases := []testCase{
		{"1234567890", "s", 1234567890000000000},
		{"1234567890", "ms", 1234567890000000},
		{"1234567890", "us", 1234567890000},
		{"1234567890", "ns", 1234567890},
		{"1234567890000000000", "", 1234567890000000000},
	}

	for _, tcase := range testCases {
		t.Run(tcase.precision, func(t *testing.T) {
			c := new(ImportCommand)
			cfg := &ImportConfig{CommandLineConfig: new(core.CommandLineConfig)}
			cfg.Precision = tcase.precision
			c.cfg = cfg
			err := c.cfg.configTimeMultiplier()
			require.NoError(t, err)
			act := c.parseTimestamp2Int64(tcase.timestamp)
			require.Equal(t, tcase.expect, act)
		})
	}
}

func TestParse2String(t *testing.T) {
	type testCase struct {
		fieldType int
		precision string
		inputAny  any
		expect    string
	}

	testCases := []testCase{
		{TypeField, "", 55, "55"},
		{TypeField, "", 66.6, "66.6"},
		{TypeField, "", true, "true"},
		{TypeField, "", false, "false"},
		{TypeField, "", "royal", "\"royal\""},
		{TypeField, "", nil, "\"\""},

		{TypeTimestamp, "", 1234567890, "1234567890"},
		{TypeTimestamp, "", 1234567890.1, "1234567890.1"},
		{TypeTimestamp, "s", "2010-07-01T18:48:00Z", "1278010080"},
		{TypeTimestamp, "ns", "2010-07-01T18:48:00Z", "1278010080000000000"},
		{TypeTimestamp, "ms", "2010-07-01T18:48:00Z", "1278010080000"},
		{TypeTimestamp, "us", "2010-07-01T18:48:00Z", "1278010080000000"},
		{TypeTimestamp, "", "2010-07-01T18:48:00ZZZ", ""},
	}

	for _, tcase := range testCases {
		t.Run(tcase.precision, func(t *testing.T) {
			c := new(ImportCommand)
			cfg := &ImportConfig{CommandLineConfig: new(core.CommandLineConfig)}
			cfg.Precision = tcase.precision
			c.cfg = cfg
			err := c.cfg.configTimeMultiplier()
			require.NoError(t, err)
			act := c.parse2String(tcase.inputAny, tcase.fieldType)
			require.Equal(t, tcase.expect, act)
		})
	}
}
