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

package series

import (
	"errors"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/require"
)

func newDropSeriesService() *Service {
	var c config.HierarchicalConfig
	c.MaxProcessN = 1
	return NewDropSeriesService()
}

type MockEngine struct {
	DropSeriesFn func() error
}

func (me *MockEngine) DropSeries() error {
	return me.DropSeriesFn()
}

func TestDropService_Open(t *testing.T) {
	s := newDropSeriesService()
	err := s.Open()
	require.NoError(t, err)

	mockEngine := &MockEngine{}
	mockEngine.DropSeriesFn = func() error {
		return nil
	}
	s.Engine = mockEngine
	s.dropSeriesHandle()

	mockEngine.DropSeriesFn = func() error {
		return errors.New("error")
	}
	s.dropSeriesHandle()
}
