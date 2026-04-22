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

package coordinator

import (
	"errors"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockMError struct {
	metaclient.MetaClient
}

func (m *mockMError) MatchMeasurements(database string, ms influxql.Measurements) (map[string]*meta2.MeasurementInfo, error) {
	return nil, errors.New("error")
}

func TestDropSeriesExecutor(t *testing.T) {
	e := NewDropSeriesExecutor(logger.NewLogger(errno.ModuleUnknown),
		&mockMC{}, &mockME{}, &mockNS{})
	smt := &influxql.DropSeriesStatement{
		Sources: append(influxql.Sources{}, &influxql.Measurement{}),
	}
	cond, _, er := parseTagKeyCondition("tag1 = 'a1'")
	assert.NoError(t, er)

	smt.Condition = cond

	err := e.Execute(smt, "db0")
	assert.NoError(t, err)
}

func TestDropSeriesExecutorError(t *testing.T) {
	e := NewDropSeriesExecutor(logger.NewLogger(errno.ModuleUnknown),
		&mockMError{}, &mockME{}, &mockNS{})
	smt := &influxql.DropSeriesStatement{
		Sources: append(influxql.Sources{}, &influxql.Measurement{}),
	}
	cond, _, er := parseTagKeyCondition("tag1 = 'a1'")
	assert.NoError(t, er)

	smt.Condition = cond

	err := e.Execute(smt, "db0")
	require.ErrorContains(t, err, "error")
}
