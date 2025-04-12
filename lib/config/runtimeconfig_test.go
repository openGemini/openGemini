// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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
package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
)

func TestRuntimeConfigValidate(t *testing.T) {
	type fields struct {
		Enabled      bool
		ReloadPeriod toml.Duration
		LoadPath     string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "Enabled is false",
			fields: fields{
				Enabled:      false,
				ReloadPeriod: toml.Duration(10 * time.Second),
				LoadPath:     "/path/to/config",
			},
			wantErr: false,
		},
		{
			name: "LoadPath is empty",
			fields: fields{
				Enabled:      true,
				ReloadPeriod: toml.Duration(10 * time.Second),
				LoadPath:     "",
			},
			wantErr: true,
		},
		{
			name: "ReloadPeriod is less than 0",
			fields: fields{
				Enabled:      true,
				ReloadPeriod: toml.Duration(-10 * time.Second),
				LoadPath:     "/path/to/config",
			},
			wantErr: true,
		},
		{
			name: "Valid config",
			fields: fields{
				Enabled:      true,
				ReloadPeriod: toml.Duration(10 * time.Second),
				LoadPath:     "/path/to/config",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := RuntimeConfig{
				Enabled:      tt.fields.Enabled,
				ReloadPeriod: tt.fields.ReloadPeriod,
				LoadPath:     tt.fields.LoadPath,
			}
			if err := c.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("RuntimeConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewRuntimeConfig(t *testing.T) {
	tests := []struct {
		name string
		want RuntimeConfig
	}{
		{
			name: "Default config",
			want: RuntimeConfig{
				Enabled:      false,
				ReloadPeriod: toml.Duration(10 * time.Second),
				LoadPath:     "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewRuntimeConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewRuntimeConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
