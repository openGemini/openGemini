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
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestDecipher(t *testing.T) {
	txt := "abcd1234"
	crypto.SetLogger(logger.GetLogger())
	crypto.SetDecipher(nil)
	crypto.Initialize(t.TempDir() + "/mokc.conf")
	require.Equal(t, txt, crypto.Decrypt(txt))
	res, _ := crypto.Encrypt(txt)
	require.Equal(t, txt, res)
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

	res, _ = crypto.Encrypt("invalid")
	require.Equal(t, "", res)
}

func newMockLogger() (*zap.Logger, *bytes.Buffer) {
	buf := new(bytes.Buffer)
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(buf), // 将输出重定向到缓冲区 便于行覆盖测试
		zapcore.DebugLevel,
	)
	l := zap.New(core)
	return l, buf
}

func TestDecrypt_DecryptError(t *testing.T) {
	l, buffer := newMockLogger()
	crypto.SetLogger(l)
	defer l.Sync()
	crypto.SetDecipher(&mockDecipher{})
	crypto.Initialize(t.TempDir() + "/mokc.conf")
	defer crypto.Destruct()
	decrypt := crypto.Decrypt("invalid")
	require.Equal(t, "", decrypt)
	require.Equal(t, true, strings.Contains(buffer.String(), "decrypt failed"))

	res, _ := crypto.Encrypt("invalid")
	require.Equal(t, "", res)

	res, _ = crypto.Encrypt("")
	require.Equal(t, "", res)
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

func (d *mockDecipher) Encrypt(s string) (string, error) {
	if s == "invalid" {
		return "", fmt.Errorf("invalid")
	}
	return s, nil
}

func (d *mockDecipher) Destruct() {

}
