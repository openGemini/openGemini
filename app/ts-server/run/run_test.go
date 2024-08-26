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

package run_test

import (
	"testing"

	"github.com/openGemini/openGemini/app/ts-server/run"
	ingestserver "github.com/openGemini/openGemini/app/ts-sql/sql"
	store "github.com/openGemini/openGemini/app/ts-store/run"
	"github.com/openGemini/openGemini/coordinator"
)

func TestInitStorage(t *testing.T) {
	storeServer := &store.Server{}
	sqlServer := &ingestserver.Server{}
	sqlServer.PointsWriter = &coordinator.PointsWriter{}

	run.InitStorage(sqlServer, storeServer)
	run.InitStorage(sqlServer, sqlServer)
	run.InitStorage(storeServer, storeServer)
}
