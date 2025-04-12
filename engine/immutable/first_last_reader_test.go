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

package immutable_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/stretchr/testify/require"
)

func TestFirstLastReader_ReadMinMaxFromPreAgg(t *testing.T) {
	setCompressModeSelf()
	defer setCompressModeNone()
	defer beforeTest(t, 0)()

	file, release, err := createOneTsspFile()
	require.NoError(t, err)
	defer release()

	reader := &immutable.FirstLastReader{}

	iterateChunkMeta(file, func(cm *immutable.ChunkMeta) {
		cm.Validation()
		cols := cm.GetColMeta()
		_, _, ok := reader.ReadMinFromPreAgg(&cols[0])
		require.False(t, ok)

		_, _, ok = reader.ReadMaxFromPreAgg(&cols[0])
		require.False(t, ok)

		_, _, ok = reader.ReadMinFromPreAgg(&cols[1])
		require.True(t, ok)

		_, _, ok = reader.ReadMaxFromPreAgg(&cols[2])
		require.True(t, ok)

		buf := cols[2].GetPreAgg()
		buf[0], buf[1] = 100, 100
		_, _, ok = reader.ReadMaxFromPreAgg(&cols[2])
		require.False(t, ok)
	})
}
