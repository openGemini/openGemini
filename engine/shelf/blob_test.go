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

package shelf_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/shelf"
	"github.com/openGemini/openGemini/lib/spdy/transport"
	"github.com/stretchr/testify/require"
)

func TestBlob(t *testing.T) {
	req := &shelf.Blob{}
	assertCodec(t, req, true, true)

	assertCodec(t, req, true, false)
}

func assertCodec(t *testing.T, obj transport.Codec, assertSize bool, assertUnmarshalNil bool) {
	buf, err := obj.Marshal(nil)
	require.NoError(t, err)

	if assertSize {
		require.Equal(t, len(buf), obj.Size(),
			"invalid size, exp: %d, got: %d", len(buf), obj.Size())
	}

	other := obj.Instance()
	if assertUnmarshalNil {
		require.NoError(t, other.Unmarshal(nil))
	}

	require.NoError(t, other.Unmarshal(buf))
}
