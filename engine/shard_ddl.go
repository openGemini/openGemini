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

package engine

import (
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/metaclient"
)

func (s *shard) CreateShowTagValuesPlan(client metaclient.MetaClient) immutable.ShowTagValuesPlan {
	if s.indexBuilder == nil {
		return nil
	}
	idx, ok := s.indexBuilder.GetPrimaryIndex().(*tsi.MergeSetIndex)
	if !ok {
		return nil
	}
	return immutable.NewShowTagValuesPlan(s.immTables, idx, s.log, s, client)
}
