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

package config

import (
	"reflect"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLimitsLoadingFromYaml(t *testing.T) {
	SetDefaultLimitsForUnmarshalling(Limits{
		MaxLabelNameLength: 100,
	})

	inp := `max_label_value_length: 1000
reject_old_samples_max_age: 14d
creation_grace_period: 3m
max_query_length: 30d`

	l := Limits{}
	err := yaml.Unmarshal([]byte(inp), &l)
	require.NoError(t, err)

	require.Equal(t, 1000, l.MaxLabelValueLength, "from yaml")
	require.Equal(t, model.Duration(14*24*time.Hour), l.RejectOldSamplesMaxAge, "from yaml")
	require.Equal(t, model.Duration(3*time.Minute), l.CreationGracePeriod, "from yaml")
	require.Equal(t, model.Duration(30*24*time.Hour), l.MaxQueryLength, "from yaml")
	require.Equal(t, 100, l.MaxLabelNameLength, "from defaults")
}

func TestLimitsLoadingFromToml(t *testing.T) {
	inp := `
max-label-value-length = 1000
reject-old-samples-max-age = "14d"
creation-grace-period = "3m"
max-query-length = "30d"
	`

	l := Limits{}
	_, err := toml.Decode(inp, &l)
	require.NoError(t, err)

	require.Equal(t, 1000, l.MaxLabelValueLength, "from toml")
	require.Equal(t, model.Duration(14*24*time.Hour), l.RejectOldSamplesMaxAge, "from toml")
	require.Equal(t, model.Duration(3*time.Minute), l.CreationGracePeriod, "from toml")
	require.Equal(t, model.Duration(30*24*time.Hour), l.MaxQueryLength, "from toml")
}

func TestLimitsAlwaysUsesPromDuration(t *testing.T) {
	stdlibDuration := reflect.TypeOf(time.Duration(0))
	limits := reflect.TypeOf(Limits{})
	n := limits.NumField()
	var badDurationType []string

	for i := 0; i < n; i++ {
		field := limits.Field(i)
		if field.Type == stdlibDuration {
			badDurationType = append(badDurationType, field.Name)
		}
	}

	require.Empty(t, badDurationType, "some Limits fields are using stdlib time.Duration instead of model.Duration")
}
