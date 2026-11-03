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

package encoding_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/encoding"
	"github.com/stretchr/testify/require"
)

func TestStdJSONConfig(t *testing.T) {
	sample := map[string]string{"b": "1", "a": "<tag>"}

	data, err := encoding.StdJSONConfig.Marshal(sample)
	require.NoError(t, err)
	require.Equal(t, `{"a":"<tag>","b":"1"}`, string(data))

	indented, err := encoding.StdJSONConfig.MarshalIndent(sample, "", "  ")
	require.NoError(t, err)
	require.Contains(t, string(indented), "\n  \"a\": \"<tag>\"")

	str, err := encoding.StdJSONConfig.MarshalToString(sample)
	require.NoError(t, err)
	require.Equal(t, `{"a":"<tag>","b":"1"}`, str)

	var out map[string]string
	require.NoError(t, encoding.StdJSONConfig.Unmarshal(data, &out))
	require.Equal(t, sample, out)

	var out2 map[string]string
	require.NoError(t, encoding.StdJSONConfig.UnmarshalFromString(str, &out2))
	require.Equal(t, sample, out2)

	var buf bytes.Buffer
	enc := encoding.StdJSONConfig.NewEncoder(&buf)
	require.NoError(t, enc.Encode(sample))

	var out3 map[string]string
	dec := encoding.StdJSONConfig.NewDecoder(strings.NewReader(buf.String()))
	require.NoError(t, dec.Decode(&out3))
	require.Equal(t, sample, out3)
}
