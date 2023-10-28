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

func TestPKInfosV0(t *testing.T) {
	pkFiles := NewPKFiles()
	pkFiles.SetPKInfo("test", nil, nil, DefaultTCLocation)
	pkInfo := PKInfo{
		tcLocation: DefaultTCLocation,
		rec:        nil,
		mark:       nil,
	}
	ret, ok := pkFiles.GetPKInfo("test")
	require.EqualValues(t, ret.tcLocation, DefaultTCLocation)
	require.EqualValues(t, ok, true)
	require.EqualValues(t, *ret, pkInfo)
	pkInfos := pkFiles.GetPKInfos()
	require.EqualValues(t, pkInfos["test"], ret)
	pkFiles.DelPKInfo("test")
	require.EqualValues(t, PKInfos(PKInfos{}), pkFiles.pkInfos)
}

func TestPKInfosV1(t *testing.T) {
	pkFiles := NewPKFiles()
	pkFiles.SetPKInfo("test", nil, nil, 3)
	pkInfo := PKInfo{
		tcLocation: 3,
		rec:        nil,
		mark:       nil,
	}
	ret, ok := pkFiles.GetPKInfo("test")
	require.EqualValues(t, ret.tcLocation, 3)
	require.EqualValues(t, ok, true)
	require.EqualValues(t, *ret, pkInfo)
	pkInfos := pkFiles.GetPKInfos()
	require.EqualValues(t, pkInfos["test"], ret)
	pkFiles.DelPKInfo("test")
	require.EqualValues(t, PKInfos(PKInfos{}), pkFiles.pkInfos)
}
