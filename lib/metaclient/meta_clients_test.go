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

package metaclient

import (
	"testing"
)

func TestUnImplementFunc(t *testing.T) {
	var x MetaClient
	c := &Client{}
	clients := NewClients([]*Client{c})
	x = clients
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Error("unexpect")
			}
		}()
		_, err := x.CreateDatabase("db", false, 0, nil)
		if err != nil {
			t.Error(err)
		}
	}()

}
