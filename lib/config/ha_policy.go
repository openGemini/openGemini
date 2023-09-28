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
package config

import "github.com/openGemini/openGemini/lib/errno"

type HAPolicy uint8

const (
	WriteAvailableFirst HAPolicy = iota
	SharedStorage
	Replication
	PolicyEnd
)

var policies = map[string]HAPolicy{
	"write-available-first": WriteAvailableFirst,
	"shared-storage":        SharedStorage,
	"replication":           Replication,
}

var policy HAPolicy

func SetHaPolicy(haPolicy string) error {
	ok := false
	policy, ok = policies[haPolicy]
	if !ok {
		return errno.NewError(errno.InvalidHaPolicy)
	}
	return nil
}

func GetHaPolicy() HAPolicy {
	return policy
}

func IsReplication() bool {
	return policy == Replication
}
