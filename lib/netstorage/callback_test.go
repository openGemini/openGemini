// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package netstorage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRaftMsgCallback(t *testing.T) {
	c := &RaftMsgCallback{}
	m1 := c.GetCodec()
	assert.Equal(t, c.GetResponse(), nil)
	assert.Equal(t, nil, c.Handle(m1))
	m2 := &DDLMessage{}
	err := c.Handle(m2)
	assert.Equal(t, "invalid data type, exp: *netstorage.RaftMsgMessage, got: *netstorage.DDLMessage", err.Error())
}
