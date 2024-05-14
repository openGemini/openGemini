/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package logstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetAndClear(t *testing.T) {
	b := NewBlockLocks(1024)
	for i := int32(0); i < 512; i++ {
		assert.True(t, b.Set(i))
		assert.False(t, b.Empty(i))
	}
	assert.False(t, b.Set(0))
	assert.False(t, b.Set(100))
	for i := int32(0); i < 1024; i++ {
		b.Clear(i)
		assert.True(t, b.Empty(i))
	}
}

func TestNext(t *testing.T) {
	b := NewBlockLocks(1024)
	assert.Equal(t, int32(0), b.Next())
	for i := int32(0); i < 1023; i++ {
		b.Set(i)
		assert.Equal(t, i+1, b.Next())
	}
	b.Set(1023)
	assert.Equal(t, int32(-1), b.Next())
}
