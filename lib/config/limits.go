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
	"time"

	"github.com/prometheus/common/model"
)

// Limits describe all the limits for users; can be used to describe global default
// limits via config, or per-user limits via yaml config.
type Limits struct {
	PromLimitEnabled bool `toml:"prom-limit-enabled" yaml:"prom_limit_enabled"`

	// prom metrics write limits
	MaxLabelNameLength        int            `toml:"max-label-name-length" yaml:"max_label_name_length"`
	MaxLabelValueLength       int            `toml:"max-label-value-length" yaml:"max_label_value_length"`
	MaxLabelNamesPerSeries    int            `toml:"max-label-names-per-series" yaml:"max_label_names_per_series"`
	MaxMetadataLength         int            `toml:"max-metadata-length" yaml:"max_metadata_length"`
	RejectOldSamples          bool           `toml:"reject-old-samples" yaml:"reject_old_samples"`
	RejectOldSamplesMaxAge    model.Duration `toml:"reject-old-samples-max-age" yaml:"reject_old_samples_max_age"`
	CreationGracePeriod       model.Duration `toml:"creation-grace-period" yaml:"creation_grace_period"`
	EnforceMetadataMetricName bool           `toml:"enforce-metadata-metric-name" yaml:"enforce_metadata_metric_name"`
	EnforceMetricName         bool           `toml:"enforce-metric-name" yaml:"enforce_metric_name"`
	// query limits
	MaxQueryLength model.Duration `toml:"max-query-length" yaml:"max_query_length"`
}

func NewLimits() Limits {
	l := Limits{
		PromLimitEnabled:          false,
		MaxLabelNameLength:        1024,                                // Maximum length accepted for label names.
		MaxLabelValueLength:       2048,                                // Maximum length accepted for label value. This setting also applies to the metric name.
		MaxLabelNamesPerSeries:    30,                                  // Maximum number of label names per series.
		MaxMetadataLength:         1024,                                // Maximum length accepted for metric metadata. Metadata refers to Metric Name, HELP and UNIT.
		RejectOldSamples:          false,                               // Reject old samples.
		RejectOldSamplesMaxAge:    model.Duration(14 * 24 * time.Hour), // Maximum accepted sample age before rejecting.
		CreationGracePeriod:       model.Duration(10 * time.Minute),    // Duration which table will be created/deleted before/after it's needed; we won't accept sample from before this time.
		EnforceMetadataMetricName: true,                                // Enforce every sample has a metric name.
		EnforceMetricName:         true,                                // Enforce every metadata has a metric name.
	}
	return l
}

// Validate the limits config and returns an error if the validation doesn't pass
func (l *Limits) Validate() error {
	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (l *Limits) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// We want to set l to the defaults and then overwrite it with the input.
	// To make unmarshal fill the plain data struct rather than calling UnmarshalYAML
	// again, we have to hide it using a type indirection.  See prometheus/config.

	// During startup we wont have a default value so we don't want to overwrite them
	if defaultLimits != nil {
		*l = *defaultLimits
	}
	type plain Limits
	return unmarshal((*plain)(l))
}

// When we load YAML from disk, we want the various per-customer limits
// to default to any values specified on the command line, not default
// command line values.  This global contains those values.  I (Tom) cannot
// find a nicer way I'm afraid.
var defaultLimits *Limits

// SetDefaultLimitsForUnmarshalling sets global default limits, used when loading
// Limits from YAML files. This is used to ensure per-tenant limits are defaulted to
// those values.
func SetDefaultLimitsForUnmarshalling(defaults Limits) {
	defaultLimits = &defaults
}
