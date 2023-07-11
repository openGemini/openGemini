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
package netstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNetStorage_GetQueriesOnNode(t *testing.T) {
	mc := MockMetaClient{}
	mc.addDataNode(newDataNode(5, "127.0.0.1:18495"))
	ns := NewNetStorage(&mc)

	_, err := ns.GetQueriesOnNode(5)
	assert.EqualError(t, fmt.Errorf("no connections available, node: 5, 127.0.0.1:18495"), err.Error())
}
