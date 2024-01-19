/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package run

import (
	"github.com/openGemini/openGemini/app"
	ingestserver "github.com/openGemini/openGemini/app/ts-sql/sql"
	store "github.com/openGemini/openGemini/app/ts-store/run"
	"github.com/openGemini/openGemini/engine/executor"
)

func InitStorage(sqlServer app.Server, storeServer app.Server) {
	s, ok := storeServer.(*store.Server)
	if !ok {
		return
	}
	sql, ok := sqlServer.(*ingestserver.Server)
	if !ok {
		return
	}
	sql.PointsWriter.SetStore(s.GetStore())
	executor.SetLocalStorageForQuery(s.GetStore())
	executor.InitLocalStoreTemplatePlan()
}
