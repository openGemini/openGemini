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

import "fmt"

const (
	minPort = 0
	maxPort = 65536
)

type stringValidator struct {
}

type stringValidatorItem struct {
	key string
	val string
}

func (v stringValidator) Validate(items []stringValidatorItem) error {
	for i := range items {
		if items[i].val == "" {
			return fmt.Errorf("%s must not be empty", items[i].key)
		}
	}
	return nil
}

type intValidator struct {
	min int64
	max int64
}

type intValidatorItem struct {
	key   string
	val   int64
	equal bool
}

func (v intValidator) Validate(items []intValidatorItem) error {
	var item *intValidatorItem
	for i := range items {
		item = &items[i]

		if item.equal && (item.val == v.min || item.val == v.max) {
			continue
		}

		if item.val <= v.min {
			return fmt.Errorf("%s must be greater than %d. got: %d",
				item.key, v.min, item.val)
		}

		if item.val >= v.max {
			return fmt.Errorf("%s must be less than %d. got: %d",
				item.key, v.max, item.val)
		}
	}
	return nil
}
