// Copyright 2024 openGemini Authors.
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

package config

import "fmt"

type RecordWriteConfig struct {
	Enabled        bool      `toml:"enabled"`
	AuthEnabled    bool      `toml:"auth-enabled"`
	TLS            tlsConfig `toml:"TLS"`
	RPCAddress     string    `toml:"rpc-address"`
	MaxRecvMsgSize int       `toml:"max-message-size"`
}

type tlsConfig struct {
	Enabled    bool   `toml:"enabled"`
	KeyFile    string `toml:"key-file"`
	CertFile   string `toml:"cert-file"`
	ClientAuth bool   `toml:"mTLS-enabled"`
	CARoot     string `toml:"CA-root"`
}

func NewRecordWriteConfig() RecordWriteConfig {
	return RecordWriteConfig{
		Enabled:        false,
		AuthEnabled:    false,
		TLS:            tlsConfig{Enabled: false, KeyFile: "", CertFile: "", ClientAuth: false, CARoot: ""},
		RPCAddress:     "127.0.0.1:8305",
		MaxRecvMsgSize: 4 * 1024 * 1024,
	}
}

func (c RecordWriteConfig) Validate() error {
	err := fmt.Errorf("record writer config: ")
	if !c.Enabled {
		return nil
	}

	if c.RPCAddress == "" {
		return fmt.Errorf("%v RPC bind is not provided", err)
	}

	if c.MaxRecvMsgSize <= 0 {
		return fmt.Errorf("maximum message size must be greater than zero")
	}

	if !c.TLS.Enabled {
		if c.TLS.ClientAuth {
			return fmt.Errorf("%v TLS must be enabled first for mutal-TLS", err)
		}
	} else {
		if c.TLS.KeyFile == "" || c.TLS.CertFile == "" {
			return fmt.Errorf("%v TLS is enabled but no Key/Cert file provided", err)
		}
		if c.TLS.ClientAuth {
			if c.TLS.CARoot == "" {
				return fmt.Errorf("%v mutal-TLS is enabled but no CA root provided", err)
			}
		}
	}
	return nil
}

func (c RecordWriteConfig) ShowConfigs() map[string]interface{} {
	return map[string]interface{}{
		"record-write.enabled":          c.Enabled,
		"record-write.auth-enabled":     c.AuthEnabled,
		"record-write.rpc-address":      c.RPCAddress,
		"record-write.max-message-size": c.MaxRecvMsgSize,
		"record-write.TLS.enabled":      c.TLS.Enabled,
		"record-write.TLS.key-file":     c.TLS.KeyFile,
		"record-write.TLS.cert-file":    c.TLS.CertFile,
		"record-write.TLS.mTLS-enabled": c.TLS.ClientAuth,
		"record-write.TLS.CA-root":      c.TLS.CARoot,
	}
}
