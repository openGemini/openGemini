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

package consume_test

import (
	"testing"

	"github.com/openGemini/openGemini/services/consume"
	"github.com/openGemini/openGemini/services/consume/kafka/handle"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	svc := consume.NewService(nil, nil)
	require.NoError(t, svc.Open())

	factory := handle.DefaultHandlerFactory()
	factory.Create(handle.Fetch, handle.V2)
	factory.Create(handle.Metadata, handle.V1)
	factory.Create(handle.ListOffsets, handle.V1)
	factory.Create(handle.OffsetCommit, handle.V2)
	factory.Create(handle.HeartBeat, handle.V1)

	require.NoError(t, svc.Close())
}
