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

package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServer_Write_InvalidMeasurement(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true)
	require.NoError(t, err)

	lines := []string{
		`c;pu,host=server01 value=1.0`,
		`cp\u,host=server01 value=1.0`,
		`cp/u,host=server01 value=1.0`,
		fmt.Sprintf(`%s,host=server01 value=1.0`, append([]byte("cpu_xxx"), 0)),
	}

	for _, line := range lines {
		_, err := s.Write("db0", "rp0", line, nil)
		require.Contains(t, err.Error(), `invalid measurement name`)
	}
}

func TestServer_Write_OutTimeRangeLimit(t *testing.T) {
	t.Skip("Skip")
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true)
	require.NoError(t, err)

	ts := time.Now().UnixNano()

	lines := []string{
		fmt.Sprintf(`cpu,host=server01 value=1.0 %d`, ts+(time.Hour.Nanoseconds())*48),
		fmt.Sprintf(`cpu,host=server01 value=2.0 %d`, ts-(time.Hour.Nanoseconds())*500),
	}

	for _, line := range lines {
		_, err := s.Write("db0", "rp0", line, nil)
		require.Contains(t, err.Error(), `point time is expired`)
	}
}
