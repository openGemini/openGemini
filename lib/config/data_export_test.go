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

package config

import (
	"errors"
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/stretchr/testify/require"
)

func TestDataExport_Validate(t *testing.T) {
	// Test cases for Validate
	tests := []struct {
		name        string
		config      *DataExport
		expectedErr error
	}{
		{
			name: "Valid configuration",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(10 * time.Second),
				MaxRetryTimes:       5,
				ExportDir:           "/tmp/export",
				MaxConcurrencyTask:  2,
				MaxConcurrencyPerPT: 1,
			},
			expectedErr: nil,
		},
		{
			name: "RunInterval is zero",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(0),
				MaxRetryTimes:       5,
				ExportDir:           "/tmp/export",
				MaxConcurrencyTask:  2,
				MaxConcurrencyPerPT: 1,
			},
			expectedErr: errors.New("run-interval must be positive"),
		},
		{
			name: "RunInterval is negative",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(-10 * time.Second),
				MaxRetryTimes:       5,
				ExportDir:           "/tmp/export",
				MaxConcurrencyTask:  2,
				MaxConcurrencyPerPT: 1,
			},
			expectedErr: errors.New("run-interval must be positive"),
		},
		{
			name: "MaxRetryTimes is zero",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(10 * time.Second),
				MaxRetryTimes:       0,
				ExportDir:           "/tmp/export",
				MaxConcurrencyTask:  2,
				MaxConcurrencyPerPT: 1,
			},
			expectedErr: errors.New("max-retry-times must be positive"),
		},
		{
			name: "MaxRetryTimes is negative",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(10 * time.Second),
				MaxRetryTimes:       -1,
				ExportDir:           "/tmp/export",
				MaxConcurrencyTask:  2,
				MaxConcurrencyPerPT: 1,
			},
			expectedErr: errors.New("max-retry-times must be positive"),
		},
		{
			name: "MaxConcurrencyTask is zero",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(10 * time.Second),
				MaxRetryTimes:       5,
				ExportDir:           "/tmp/export",
				MaxConcurrencyTask:  0,
				MaxConcurrencyPerPT: 1,
			},
			expectedErr: errors.New("max-concurrency-task must be positive"),
		},
		{
			name: "MaxConcurrencyTask is negative",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(10 * time.Second),
				MaxRetryTimes:       5,
				ExportDir:           "/tmp/export",
				MaxConcurrencyTask:  -1,
				MaxConcurrencyPerPT: 1,
			},
			expectedErr: errors.New("max-concurrency-task must be positive"),
		},
		{
			name: "ExportDir is empty",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(10 * time.Second),
				MaxRetryTimes:       5,
				ExportDir:           "",
				MaxConcurrencyTask:  2,
				MaxConcurrencyPerPT: 1,
			},
			expectedErr: errors.New("export-dir must not be empty"),
		},
		{
			name: "MaxConcurrencyPerPT is empty",
			config: &DataExport{
				Enabled:             true,
				RunInterval:         toml.Duration(10 * time.Second),
				MaxRetryTimes:       5,
				ExportDir:           "/tmp/export",
				MaxConcurrencyTask:  2,
				MaxConcurrencyPerPT: 0,
			},
			expectedErr: errors.New("max-concurrency-per-pt must be positive"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			require.Equal(t, tt.expectedErr, err)
		})
	}
}
