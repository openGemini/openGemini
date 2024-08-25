/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package stream

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/strings"
)

func Test_builderPool(t *testing.T) {
	bp := strings.NewBuilderPool()
	sb := bp.Get()
	for i := 0; i < 100; i++ {
		sb.AppendString("xx")
	}
	f := sb.NewString()
	if len(f) != 200 {
		t.Error("len fail")
	}
	sb.Reset()
	bp.Put(sb)
	sb1 := bp.Get()
	for i := 0; i < 100; i++ {
		sb1.AppendString("xx")
	}
	f = sb1.NewString()
	if len(f) != 200 {
		t.Error("len fail")
	}
}

func Test_BuilderPool_Len(t *testing.T) {
	pool := strings.NewBuilderPool()
	c1 := pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	if pool.Size() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Size()))
	}
	c2 := pool.Get()
	if pool.Len() != 0 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 0, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
	pool.Put(c1)
	if pool.Len() != 1 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 1, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
	pool.Put(c2)
	if pool.Len() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Len()))
	}
	if pool.Size() != 2 {
		t.Error(fmt.Sprintf("expect %v ,got %v", 2, pool.Size()))
	}
}

func Test_StringBuilder(t *testing.T) {
	sb := strings.StringBuilder{}
	sb.AppendString("xx")
	str := sb.String()
	strNew := sb.NewString()
	sb.Reset()
	sb.AppendString("aa")
	str1 := sb.NewString()
	if strNew != "xx" {
		t.Fatal("unexpect", strNew)
	}
	if str != str1 {
		t.Fatal("unexpect", str)
	}
	sb.Reset()
	str2 := sb.NewString()
	if str2 != "" {
		t.Fatal("unexpect", str2)
	}
}
