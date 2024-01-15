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

package op_test

import (
	"testing"

	"github.com/openGemini/openGemini/engine/op"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/services/castor"
)

func Test_Castor_Service_NotOpen(t *testing.T) {
	call := &influxql.Call{Name: "castor"}
	heimOp := op.NewCastorOp(nil)
	if err := heimOp.Compile(call); err == nil || !errno.Equal(err, errno.ServiceNotEnable) {
		t.Fatal("compile should not pass when castor service not open")
	}
}

func Test_Castor_Service_ArgsNumNotCorrect(t *testing.T) {
	srv, _, err := castor.MockCastorService(6666)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	call := &influxql.Call{
		Name: "castor",
		Args: []influxql.Expr{},
	}
	heimOp := op.NewCastorOp(nil)
	if err := heimOp.Compile(call); err == nil || !errno.Equal(err, errno.InvalidArgsNum) {
		t.Fatal("compile should not pass when args quantity not correct")
	}
}

func Test_Castor_Service_ArgsTypeNotCorrect(t *testing.T) {
	srv, _, err := castor.MockCastorService(6666)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	call := &influxql.Call{
		Name: "castor",
		Args: []influxql.Expr{
			&influxql.IntegerLiteral{},
			&influxql.IntegerLiteral{},
			&influxql.IntegerLiteral{},
			&influxql.IntegerLiteral{},
		},
	}
	heimOp := op.NewCastorOp(nil)
	if err := heimOp.Compile(call); err == nil || !errno.Equal(err, errno.TypeAssertFail) {
		t.Fatal("compile should not pass when args quantity not correct")
	}
}

func Test_Castor_Service_AlgoTypeNotCorrect(t *testing.T) {
	srv, _, err := castor.MockCastorService(6666)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	call := &influxql.Call{
		Name: "castor",
		Args: []influxql.Expr{
			&influxql.StringLiteral{},
			&influxql.StringLiteral{},
			&influxql.StringLiteral{},
			&influxql.StringLiteral{Val: "test"},
		},
	}
	heimOp := op.NewCastorOp(nil)
	if err := heimOp.Compile(call); err == nil || !errno.Equal(err, errno.AlgoTypeNotFound) {
		t.Fatal("compile should not pass when args quantity not correct")
	}
}
