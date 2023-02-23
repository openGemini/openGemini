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

package sherlock

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_collectMetrics(t *testing.T) {
	a, b, c, err := collectMetrics(8, 2)
	require.NoError(t, err)
	require.GreaterOrEqual(t, a, 0)
	require.GreaterOrEqual(t, b, 0)
	require.GreaterOrEqual(t, c, 0)
}
