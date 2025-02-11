// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package mlf_test

import (
	"testing"

	"github.com/openGemini/openGemini/lib/compress/mlf"
	"github.com/stretchr/testify/require"
)

func TestBitmap(t *testing.T) {
	n := 8
	bm := &mlf.BitMap{}
	other := &mlf.BitMap{}
	bm.Init(n)

	buf := bm.Marshal(nil)
	other.Unmarshal(buf, n)

	require.Equal(t, true, other.Empty())

	bm.SetZero(0)
	bm.SetSkip(1)
	bm.SetNegative(2)

	buf = bm.Marshal(nil)
	other.Unmarshal(buf, n)

	require.Equal(t, uint8(mlf.FlagZero), other.Get(0))
	require.Equal(t, uint8(mlf.FlagSkip), other.Get(1))
	require.Equal(t, uint8(mlf.FlagNegative), other.Get(2))
}
