// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package json_test

import (
	"encoding/json"
	"math"
	"strconv"
	"testing"

	libJson "github.com/openGemini/openGemini/lib/encoding/json"
	"github.com/stretchr/testify/require"
)

// Exercise the selected implementation, including the fallback on newer Go
// versions, with values that must survive JSON encoding without precision loss.
func TestWriteNumericValues(t *testing.T) {
	for _, value := range []int64{0, -1, 1, math.MinInt64, math.MaxInt64} {
		t.Run(strconv.FormatInt(value, 10), func(t *testing.T) {
			sw := libJson.NewStreamWriter(nil)
			sw.WriteArrayStart("")
			sw.WriteValueInt64(value)
			sw.WriteArrayEnd()
			var got []int64
			require.NoError(t, json.Unmarshal(sw.Bytes(), &got))
			require.Equal(t, []int64{value}, got)
		})
	}
	for _, value := range []float64{0, math.Copysign(0, -1), 1.25, -1.25, math.SmallestNonzeroFloat64, math.MaxFloat64} {
		t.Run(strconv.FormatFloat(value, 'g', -1, 64), func(t *testing.T) {
			sw := libJson.NewStreamWriter(nil)
			sw.WriteArrayStart("")
			sw.WriteValueFloat64(value)
			sw.WriteArrayEnd()
			var got []float64
			require.NoError(t, json.Unmarshal(sw.Bytes(), &got))
			require.Len(t, got, 1)
			require.Equal(t, math.Float64bits(value), math.Float64bits(got[0]))
		})
	}
}
