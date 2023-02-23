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

package handler

import (
	"fmt"
	"testing"

	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/stretchr/testify/assert"
)

func TestProcessDDL(t *testing.T) {
	condition := "tk1=tv1"
	errStr := processDDL(&condition, func(expr influxql.Expr) error {
		return errno.NewError(errno.PtNotFound)
	})

	err := netstorage.NormalizeError(errStr)
	assert.Equal(t, true, errno.Equal(err, errno.PtNotFound))

	errStr = processDDL(&condition, func(expr influxql.Expr) error {
		return fmt.Errorf("shard not found")
	})

	err = netstorage.NormalizeError(errStr)
	assert.Equal(t, true, err.Error() == "shard not found")

	errStr = processDDL(&condition, func(expr influxql.Expr) error {
		return nil
	})

	err = netstorage.NormalizeError(errStr)
	assert.Equal(t, true, err == nil)
}
