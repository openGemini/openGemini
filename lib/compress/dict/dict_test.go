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

package dict_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/lib/compress/dict"
	"github.com/stretchr/testify/require"
)

func TestDict(t *testing.T) {
	file := t.TempDir() + "/dict.data"
	require.NoError(t, os.WriteFile(file, []byte("a,b,c,d"), 0600))

	d := dict.DefaultDict()
	require.NoError(t, d.LoadFromFiles(",", file))

	id := d.GetID("a")
	require.True(t, id >= 0)
	require.True(t, d.GetID("not_exists") == -1)
	require.True(t, d.GetValue(id) == "a")
	require.True(t, d.GetValue(65535) == "")
}

func TestDictLoadFail(t *testing.T) {
	file := t.TempDir() + "/dict.data"
	require.NoError(t, os.WriteFile(file, []byte("a,b,c,d"), 0600))

	patch := gomonkey.ApplyFunc(os.ReadFile, func(name string) ([]byte, error) {
		return nil, fmt.Errorf("read file error")
	})
	defer patch.Reset()

	d := dict.DefaultDict()
	require.Error(t, d.LoadFromFiles(",", file))
}
