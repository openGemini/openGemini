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
package castor

import (
	"testing"
)

func Test_getConn(t *testing.T) {
	addr := "127.0.0.1:6663"
	if err := MockPyWorker(addr); err != nil {
		t.Fatal(err)
	}
	conn, err := getConn(addr)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
}

func Test_getConn_InvalidAddr(t *testing.T) {
	addr := "123"
	if _, err := getConn(addr); err == nil {
		t.Fatal("connect to invalid addr")
	}
}

func Test_getConn_InvalidAddr2(t *testing.T) {
	addr := "127.0.0.1:6664"
	if _, err := getConn(addr); err == nil {
		t.Fatal("connect to invalid addr")
	}
}
