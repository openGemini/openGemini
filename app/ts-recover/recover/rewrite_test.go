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

package recover

import (
	"testing"

	"github.com/openGemini/openGemini/lib/backup"
	"github.com/stretchr/testify/assert"
)

func TestRewriteError(t *testing.T) {
	n := &backup.NodeInfoMap{
		PtMap: map[string]map[string]string{
			"db1": {
				"0": "1",
				"2": "3",
				"4": "5",
			},
		},
		DbMap: map[string]map[string]*backup.RpInfoMap{
			"db1": {
				"rp1": {
					ShardMap: map[string]string{"10": "11", "12": "13", "14": "15"},
					IndexMap: map[string]string{"20": "21", "22": "23", "24": "25"},
				},
			},
			"db2": {},
		},
	}
	r := &RecoverOptions{}

	t.Run("recoverDB", func(t *testing.T) {
		err := r.recoverDB(n)("basedir", "db0", TypeData)
		assert.Error(t, err)

		err = r.recoverDB(n)("basedir", "db2", TypeData)
		assert.Error(t, err)
	})

	t.Run("recoverPt", func(t *testing.T) {
		err := r.recoverPt(n.DbMap["db1"], n.PtMap["db1"])("basedir", "6", TypeData)
		assert.Error(t, err)
	})

	t.Run("recoverRp", func(t *testing.T) {
		err := r.recoverRp(n.DbMap["db1"])("basedir", "rp2", TypeData)
		assert.Error(t, err)
	})

	t.Run("recoverShard", func(t *testing.T) {
		rpMap := n.DbMap["db1"]["rp1"]
		err := r.recoverShard(rpMap)("basedir", "wrong_shard", TypeData)
		assert.Error(t, err)

		err = r.recoverShard(rpMap)("basedir", "19_1763337600000000000_1763942400000000000_19", TypeData)
		assert.Nil(t, err)

		err = r.recoverShard(rpMap)("basedir", "10_1763337600000000000_1763942400000000000_29", TypeData)
		assert.Nil(t, err)
	})

	t.Run("recoverIndex", func(t *testing.T) {
		rpMap := n.DbMap["db1"]["rp1"]
		err := r.recoverIndex(rpMap)("basedir", "wrong_Index", TypeData)
		assert.Error(t, err)

		err = r.recoverIndex(rpMap)("basedir", "29_1763337600000000000_1763942400000000000", TypeData)
		assert.Nil(t, err)
	})

}
