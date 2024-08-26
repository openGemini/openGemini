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

package config

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb/toml"
	"github.com/stretchr/testify/require"
)

func Test_ContinuousQueryConfig_Validate(t *testing.T) {
	c := NewContinuousQueryConfig()
	c.RunInterval = -5
	require.EqualError(t, c.Validate(), "continuous query run interval must be must be at least 1 second")
	c.RunInterval = toml.Duration(time.Second)

	c.MaxProcessCQNumber = -5
	require.EqualError(t, c.Validate(), "continuous query max process CQ number must be greater or equal than 0")

	c.MaxProcessCQNumber = 0
	require.NoError(t, c.Validate())

}
