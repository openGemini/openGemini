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

package runtimecfg

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/influxdata/influxdb/toml"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

// Given limits are usually loaded via a config file, and that
// a configmap is limited to 1MB, we need to minimise the limits file.
// One way to do it is via YAML anchors.
func TestLoadRuntimeConfigShouldLoadAnchoredYAML(t *testing.T) {
	yamlFile := strings.NewReader(`
overrides:
  '1234': &id001
    max_label_name_length: 1022
    reject_old_samples: true
    reject_old_samples_max_age: "7d"
    creation_grace_period: "5m"
  '1235': *id001
  '1236': *id001
`)
	runtimeCfg, err := loadRuntimeConfig(yamlFile)
	require.NoError(t, err)

	limits := config.Limits{
		MaxLabelNameLength:     1022,
		RejectOldSamples:       true,
		RejectOldSamplesMaxAge: model.Duration(7 * 24 * time.Hour),
		CreationGracePeriod:    model.Duration(5 * time.Minute),
	}

	loadedLimits := runtimeCfg.TenantLimits
	require.Equal(t, 3, len(loadedLimits))
	require.Equal(t, limits, *loadedLimits["1234"])
	require.Equal(t, limits, *loadedLimits["1235"])
	require.Equal(t, limits, *loadedLimits["1236"])
}

func TestLoadRuntimeConfigShouldLoadEmptyFile(t *testing.T) {
	yamlFile := strings.NewReader(`
# This is an empty YAML.
`)
	actual, err := loadRuntimeConfig(yamlFile)
	require.NoError(t, err)
	require.Equal(t, &runtimeConfig{TenantLimits: make(map[string]*config.Limits)}, actual)
}

func TestRuntimeCfgServiceLoadLimitCfg(t *testing.T) {
	content := `
overrides:
  '1234': &id001
    max_label_name_length: 1022
    reject_old_samples: true
    reject_old_samples_max_age: "7d"
    creation_grace_period: "5m"
`

	srv := NewService(config.RuntimeConfig{Enabled: true, ReloadPeriod: toml.Duration(time.Microsecond)}, logger.NewLogger(errno.ModuleUnknown))
	require.NotNil(t, srv)
	require.Nil(t, srv.ByUserID("1234"))

	mock := gomonkey.ApplyFunc(os.ReadFile, func(filename string) ([]byte, error) {
		return []byte(content), nil
	})
	defer mock.Reset()

	srv.Open()
	defer srv.Close()
	// wait execute handle
	time.Sleep(time.Second)

	expectLimits := &config.Limits{
		MaxLabelNameLength:     1022,
		RejectOldSamples:       true,
		RejectOldSamplesMaxAge: model.Duration(7 * 24 * time.Hour),
		CreationGracePeriod:    model.Duration(5 * time.Minute),
	}
	require.Equal(t, expectLimits, srv.ByUserID("1234"))
}

// TestRuntimeConfigHandler note: yaml.v2 default marshal indent is 2, upgrade to v3, default indent is 4
func TestRuntimeConfigHandler(t *testing.T) {
	srv := NewService(config.RuntimeConfig{Enabled: true, ReloadPeriod: toml.Duration(time.Microsecond)}, logger.NewLogger(errno.ModuleUnknown))
	require.NotNil(t, srv)

	config.SetDefaultLimitsForUnmarshalling(config.NewLimits())
	handle := RuntimeConfigHandler(srv, config.NewLimits())

	yamlFile := strings.NewReader(`
overrides:
  '1234':
    max_label_name_length: 1022
    reject_old_samples: true
    reject_old_samples_max_age: "7d"
    creation_grace_period: "5m"
`)
	runtimeCfg, err := loadRuntimeConfig(yamlFile)
	require.NoError(t, err)
	srv.setConfig(runtimeCfg)

	var compare = func(exp string, got string) bool {
		expLines := strings.Split(exp, "\n")
		gotLines := strings.Split(got, "\n")

		if len(expLines) != len(gotLines) {
			return false
		}

		for i := range expLines {
			if strings.TrimSpace(expLines[i]) != strings.TrimSpace(gotLines[i]) {
				return false
			}
		}
		return true
	}

	t.Run("get current runtime config", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/runtime_config", nil)
		handle(w, req)

		expect := `overrides: &overrides
    "1234":
        prom_limit_enabled: false
        max_label_name_length: 1022
        max_label_value_length: 2048
        max_label_names_per_series: 30
        max_metadata_length: 1024
        reject_old_samples: true
        reject_old_samples_max_age: 1w
        creation_grace_period: 5m
        enforce_metadata_metric_name: true
        enforce_metric_name: true
        max_query_length: 0s
`
		require.Equal(t, http.StatusOK, w.Code)
		require.True(t, compare(expect, w.Body.String()), expect)
	})

	t.Run("get diff runtime config", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/runtime_config?mode=diff", nil)
		handle(w, req)

		expect := `overrides:
    "1234":
        creation_grace_period: 5m
        max_label_name_length: 1022
        reject_old_samples: true
        reject_old_samples_max_age: 1w
`

		require.Equal(t, http.StatusOK, w.Code)
		require.True(t, compare(expect, w.Body.String()), expect)
	})
}
