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

package clearevent

import (
	"github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
)

type NoClearShardAndIndexSupplier interface {
	GetNoClearShard() (uint64, error)
}

type SupplierWithMetaClient struct {
	Client metaclient.MetaClient
	Ident  *meta.ShardIdentifier
}

func (supplier *SupplierWithMetaClient) GetNoClearShard() (uint64, error) {
	return supplier.Client.GetNoClearShardId(supplier.Ident.ShardID, supplier.Ident.OwnerDb, supplier.Ident.ShardGroupID, supplier.Ident.Policy)
}

type SupplierWithId struct {
	NoClearShard uint64
}

func (supplier *SupplierWithId) GetNoClearShard() (uint64, error) {
	return supplier.NoClearShard, nil
}
