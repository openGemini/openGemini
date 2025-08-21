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

package statistics

import (
	"reflect"
	"testing"
)

func TestGetToObsCold(t *testing.T) {
	// test GetToObsCold func whether return correct toObsCold instance
	if !reflect.DeepEqual(GetToObsCold(), toObsCold) {
		t.Errorf("GetToObsCold() = %v, want %v", GetToObsCold(), toObsCold)
	}
}

func TestAddShardToColdSuccessSum(t *testing.T) {
	// test AddShardToColdSuccessSum func whether add the value of ShardToColdSuccessSum correctly
	toObsCold := GetToObsCold()
	before := reflect.ValueOf(toObsCold.ShardToColdSuccessSum.GetValue())
	toObsCold.AddShardToColdSuccessSum()
	after := reflect.ValueOf(toObsCold.ShardToColdSuccessSum.GetValue())
	if before.Int()+1 != after.Int() {
		t.Errorf("AddShardToColdSuccessSum() = %v, want %v", after, before.Int()+1)
	}
}

func TestAddShardToColdFailSum(t *testing.T) {
	// test AddShardToColdFailSum func whether add the value of ShardToColdFailSum correctly
	toObsCold := GetToObsCold()
	before := reflect.ValueOf(toObsCold.ShardToColdFailSum.GetValue())
	toObsCold.AddShardToColdFailSum()
	after := reflect.ValueOf(toObsCold.ShardToColdFailSum.GetValue())
	if before.Int()+1 != after.Int() {
		t.Errorf("AddShardToColdFailSum() = %v, want %v", after, before.Int()+1)
	}
}

func TestAddShardToColdSum(t *testing.T) {
	// test AddShardToColdSum func whether return the value of ShardToColdSum correctly
	toObsCold := GetToObsCold()
	before := reflect.ValueOf(toObsCold.ShardToColdSum.GetValue())
	toObsCold.AddShardToColdSum()
	after := reflect.ValueOf(toObsCold.ShardToColdSum.GetValue())
	if before.Int()+1 != after.Int() {
		t.Errorf("AddShardToColdSum() = %v, want %v", after, before.Int()+1)
	}
}

func TestAddIndexToColdSuccessSum(t *testing.T) {
	// test AddIndexToColdSuccessSum func whether add the value of IndexToColdSuccessSum correctly
	toObsCold := GetToObsCold()
	before := reflect.ValueOf(toObsCold.IndexToColdSuccessSum.GetValue())
	toObsCold.AddIndexToColdSuccessSum()
	after := reflect.ValueOf(toObsCold.IndexToColdSuccessSum.GetValue())
	if before.Int()+1 != after.Int() {
		t.Errorf("AddIndexToColdSuccessSum() = %v, want %v", after, before.Int()+1)
	}
}

func TestAddIndexToColdFailSum(t *testing.T) {
	// test AddIndexToColdFailSum func whether add the value of IndexToColdFailSum correctly
	toObsCold := GetToObsCold()
	before := reflect.ValueOf(toObsCold.IndexToColdFailSum.GetValue())
	toObsCold.AddIndexToColdFailSum()
	after := reflect.ValueOf(toObsCold.IndexToColdFailSum.GetValue())
	if before.Int()+1 != after.Int() {
		t.Errorf("AddIndexToColdFailSum() = %v, want %v", after, before.Int()+1)
	}
}

func TestAddIndexToColdSum(t *testing.T) {
	// test AddIndexToColdSum func whether add the value of IndexToColdSum correctly
	toObsCold := GetToObsCold()
	before := reflect.ValueOf(toObsCold.IndexToColdSum.GetValue())
	toObsCold.AddIndexToColdSum()
	after := reflect.ValueOf(toObsCold.IndexToColdSum.GetValue())
	if before.Int()+1 != after.Int() {
		t.Errorf("AddIndexToColdSum() = %v, want %v", after, before.Int()+1)
	}
}
