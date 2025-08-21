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
	"fmt"
	"testing"
	"time"

	meta2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
	proto2 "github.com/openGemini/openGemini/lib/util/lifted/influx/meta/proto"
)

func TestAsyncSchemaEndTimeManager(t *testing.T) {
	meta2.SchemaCleanEn = true
	InitAsyncSchemaEndtimeUpdateEn(true, 1, 1)
	client := NewMockMetaClient()
	client.UpdateSchemaByCmdFn = func(cmd *proto2.UpdateSchemaCommand) error {
		return nil
	}
	SchemaEndtimeUpdateManager.MetaClient = client
	defer func() {
		meta2.SchemaCleanEn = false
		InitAsyncSchemaEndtimeUpdateEn(false, 0, 1)
	}()

	SchemaEndtimeUpdateManager.Put("db0", "rp0", "mst0", []*proto.FieldSchema{})
	SchemaEndtimeUpdateManager.Put("db0", "rp0", "mst0", []*proto.FieldSchema{})

	client.UpdateSchemaByCmdFn = func(cmd *proto2.UpdateSchemaCommand) error {
		return fmt.Errorf("err")
	}
	SchemaEndtimeUpdateManager.Put("db0", "rp0", "mst0", []*proto.FieldSchema{})
	SchemaEndtimeUpdateManager.Put("db0", "rp0", "mst0", []*proto.FieldSchema{})
	SchemaEndtimeUpdateManager.Stop()
	time.Sleep(2 * time.Second)
}
