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

package meta

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/errno"
)

func (data *Data) CheckCanMoveDb(db string) error {
	if data.Database(db) == nil || data.Database(db).MarkDeleted {
		return errno.NewError(errno.DatabaseNotFound)
	}

	for _, rp := range data.Database(db).RetentionPolicies {
		if rp.MarkDeleted {
			return errno.NewError(errno.RpNotFound)
		}
	}

	var ptId uint32
	ptNum := data.GetEffectivePtNum(db)
	for ptId < ptNum {
		dbPtId := db + seperatorChar + fmt.Sprintf("%d", ptId)
		if data.MigrateEvents[dbPtId] != nil {
			return errno.NewError(errno.ConflictWithEvent)
		}
		ptId++
	}

	return nil
}
