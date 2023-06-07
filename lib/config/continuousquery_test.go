/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_ContinuousQueryConfig_Disable(t *testing.T) {
	conf := NewContinuousQueryConfig()
	require.NoError(t, conf.Validate())

	conf.Enabled = false
	require.NoError(t, conf.Validate())

	_ = conf.ApplyEnvOverrides(func(s string) string { return "" })
}

func Test_ContinuousQueryConfig_Validate(t *testing.T) {
	conf := NewContinuousQueryConfig()
	conf.RunInterval = -5
	require.EqualError(t, conf.Validate(), "continuous query run interval must be greater than 0")

	conf = NewContinuousQueryConfig()
	conf.MaxProcessCQNumber = -5
	require.EqualError(t, conf.Validate(), "continuous query max process CQ number must be greater than 0")
}
