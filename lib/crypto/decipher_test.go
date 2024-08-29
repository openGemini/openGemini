// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package crypto_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/require"
)

func TestDecipher(t *testing.T) {
	txt := "abcd1234"
	crypto.SetLogger(logger.GetLogger())
	crypto.SetDecipher(nil)
	crypto.Initialize(t.TempDir() + "/mokc.conf")
	require.Equal(t, txt, crypto.Decrypt(txt))
	crypto.Destruct()

	crypto.SetDecipher(&mockDecipher{})
	crypto.Initialize(t.TempDir() + "/mokc.conf")
	defer crypto.Destruct()

	require.Equal(t, txt, crypto.Decrypt(txt))

	encFile := t.TempDir() + "/enc.data"
	require.NoError(t, os.WriteFile(encFile, []byte(txt), 0600))

	require.Equal(t, txt, crypto.DecryptFromFile(encFile))

	// invalid input
	require.Equal(t, "", crypto.Decrypt("invalid"))
	require.Equal(t, "", crypto.DecryptFromFile(t.TempDir()+"/no_exists.data"))
	require.Equal(t, "", crypto.DecryptFromFile(""))
}

type mockDecipher struct {
}

func (d *mockDecipher) Initialize(conf string) {

}

func (d *mockDecipher) Decrypt(s string) (string, error) {
	if s == "invalid" {
		return "", fmt.Errorf("invalid")
	}
	return s, nil
}

func (d *mockDecipher) Destruct() {

}
