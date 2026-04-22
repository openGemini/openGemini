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
package opsStat

import "time"

// Statistic is the representation of a statistic used by the monitoring service.
type OpsStatistic struct {
	Name   string                 `json:"name"`
	Tags   map[string]string      `json:"tags"`
	Values map[string]interface{} `json:"values"`
	Time   time.Time
}

// NewStatistic returns an initialized Statistic.
func NewStatistic(name string) OpsStatistic {
	return OpsStatistic{
		Name:   name,
		Tags:   make(map[string]string),
		Values: make(map[string]interface{}),
	}
}
