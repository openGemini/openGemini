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

package coordinator

import (
	"errors"

	"github.com/openGemini/openGemini/lib/netstorage"
)

const (
	DDLRetryInternalSecond      = 1
	DDLTimeOutSecond            = 30
	DMLRetryInternalMillisecond = 200
	DMLTimeOutSecond            = 30
)

// ErrDatabaseNameRequired is returned when executing statements that require a database,
// when a database has not been provided.
var ErrDatabaseNameRequired = errors.New("database name required")

type TagValuesSlice []netstorage.TableTagSets

func (a TagValuesSlice) Len() int           { return len(a) }
func (a TagValuesSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TagValuesSlice) Less(i, j int) bool { return a[i].Name < a[j].Name }
