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

package colstore

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPKInfos(t *testing.T) {
	pkFiles := NewPKFiles()
	pkFiles.SetPKInfo("test", nil, nil)
	pkInfo := PKInfo{
		rec:  nil,
		mark: nil,
	}
	ret, ok := pkFiles.GetPKInfo("test")
	require.EqualValues(t, ok, true)
	require.EqualValues(t, *ret, pkInfo)
	pkInfos := pkFiles.GetPKInfos()
	require.EqualValues(t, pkInfos["test"], ret)
	pkFiles.DelPKInfo("test")
	require.EqualValues(t, PKInfos(PKInfos{}), pkFiles.pkInfos)
}
