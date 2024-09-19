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

package mergeindex

import "sync"

type IndexItemsPool struct {
	p sync.Pool
}

func (iip *IndexItemsPool) Get() *indexItems {
	ii := iip.p.Get()
	if ii == nil {
		return &indexItems{}
	}
	return ii.(*indexItems)
}

func (iip *IndexItemsPool) Put(ii *indexItems) {
	ii.reset()
	iip.p.Put(ii)
}
