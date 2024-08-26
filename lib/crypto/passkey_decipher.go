// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package crypto

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os/exec"
	"strings"
)

func InitPassKeyDecipher() {
	SetDecipher(&PassKeyDecipher{})
}

type PassKeyDecipher struct {
	Decipher

	key []byte
}

func (d *PassKeyDecipher) Initialize(command string) {
	cmd := exec.Command("/bin/bash", "-c", command)

	output, err := cmd.Output()
	if err != nil {
		_ = fmt.Errorf("execute Shell:%s failed with error:%s", command, err.Error())
	}
	d.key = []byte(string(output))
}

func (d *PassKeyDecipher) Decrypt(file string) (string, error) {
	buf := []byte(file)
	keyDERBlock, _ := pem.Decode(buf)

	if keyDERBlock.Type == "PRIVATE KEY" || strings.HasSuffix(keyDERBlock.Type, " PRIVATE KEY") {
		if x509.IsEncryptedPEMBlock(keyDERBlock) {
			if len(d.key) == 0 {
				return "", fmt.Errorf("tls: failed PEM decryption [passkey is empty]")
			}
			der, err := x509.DecryptPEMBlock(keyDERBlock, d.key)
			if err != nil {
				return "", fmt.Errorf("tls: failed PEM decryption [%s]", err)
			}
			keyDERBlock.Bytes = der

			out := bytes.NewBuffer(nil)
			err = pem.Encode(out, keyDERBlock)
			return out.String(), err
		}
	}

	return string(buf), nil
}
