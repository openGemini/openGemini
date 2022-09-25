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
package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringValidator(t *testing.T) {
	items := []stringValidatorItem{
		{"name", ""},
	}

	err := (stringValidator{}).Validate(items)
	assert.EqualError(t, err, "name must not be empty")

	items[0].val = "Sandy"
	assert.NoError(t, (stringValidator{}).Validate(items))
}

func TestIntValidator(t *testing.T) {
	iv := intValidator{0, 100}
	items := []intValidatorItem{
		{"value", -1, true},
	}

	assert.EqualError(t, iv.Validate(items), "value must be greater than 0. got: -1")

	items[0].val = 101
	assert.EqualError(t, iv.Validate(items), "value must be less than 100. got: 101")

	items[0].val = 33
	assert.NoError(t, iv.Validate(items))
}
