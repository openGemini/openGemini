// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package netstorage

import (
	"github.com/openGemini/openGemini/lib/metaclient"
)

type ColumnKeys struct {
	Name string
	Keys []metaclient.FieldKey
}

type TableColumnKeys []ColumnKeys

func (a TableColumnKeys) Len() int           { return len(a) }
func (a TableColumnKeys) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TableColumnKeys) Less(i, j int) bool { return a[i].Name < a[j].Name }

type TagKeys struct {
	Name string
	Keys []string
}

type TableTagKeys []TagKeys

func (a TableTagKeys) Len() int           { return len(a) }
func (a TableTagKeys) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TableTagKeys) Less(i, j int) bool { return a[i].Name < a[j].Name }
