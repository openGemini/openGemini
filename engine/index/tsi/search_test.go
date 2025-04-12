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

package tsi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortTagFilterWithCost(t *testing.T) {

	tfs := []tagFilterWithCost{}

	// filter not empty
	t1 := tagFilter{}
	t1.key = []byte("_extra_origin_metric_name")
	t1.value = []byte("disk_agt_read_bytes_rate")
	t1.isNegative = false
	t1.isEmptyValue = false
	tfs = append(tfs, tagFilterWithCost{&t1, 0, nil})

	// filter not empty
	t2 := tagFilter{}
	t2.key = []byte("project_id")
	t2.value = []byte("65c438cab60d4048982a519942a7ccc4")
	t2.isNegative = false
	t2.isEmptyValue = false
	tfs = append(tfs, tagFilterWithCost{&t2, 4673, nil})

	// filter not empty
	t3 := tagFilter{}
	t3.key = []byte("namespace")
	t3.value = []byte("AGT.ECS")
	t3.isNegative = false
	t3.isEmptyValue = false
	tfs = append(tfs, tagFilterWithCost{&t3, 43081, nil})

	// filter empty
	t4 := tagFilter{}
	t4.key = []byte("_extra_metric_type")
	t4.value = []byte("")
	t4.isNegative = false
	t4.isEmptyValue = true
	tfs = append(tfs, tagFilterWithCost{&t4, 0, nil})

	// filter empty
	t5 := tagFilter{}
	t5.key = []byte("instance_id")
	t5.value = []byte("")
	t5.isNegative = true
	t5.isEmptyValue = true
	tfs = append(tfs, tagFilterWithCost{&t5, 204862, nil})

	// filter empty
	t6 := tagFilter{}
	t6.key = []byte("_extra_metric_prefix")
	t6.value = []byte("")
	t6.isNegative = true
	t6.isEmptyValue = true
	tfs = append(tfs, tagFilterWithCost{&t6, 0, nil})

	ids := &indexSearch{}
	ids.sortTagFilterWithCost(tfs)

	assert.Equal(t, string(tfs[0].tf.key), "_extra_origin_metric_name")
	assert.Equal(t, string(tfs[1].tf.key), "project_id")
	assert.Equal(t, string(tfs[2].tf.key), "namespace")
	assert.Equal(t, string(tfs[3].tf.key), "_extra_metric_type")
	assert.Equal(t, string(tfs[4].tf.key), "_extra_metric_prefix")
	assert.Equal(t, string(tfs[5].tf.key), "instance_id")
}
