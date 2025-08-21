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

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/mergeset"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/uint64set"
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

func TestHasINExpr(t *testing.T) {
	expr := &influxql.StringLiteral{}
	chooseINPriority(expr)
	expr1 := &influxql.BinaryExpr{
		LHS: &influxql.BinaryExpr{
			Op: influxql.IN,
			LHS: &influxql.VarRef{
				Type: influxql.Tag,
			},
			RHS: &influxql.SetLiteral{Vals: map[interface{}]bool{
				"1":  true,
				"2":  true,
				"3":  true,
				"4":  true,
				"5":  true,
				"6":  true,
				"7":  true,
				"8":  true,
				"9":  true,
				"10": true,
				"11": true,
			}},
		},
		RHS: &influxql.BinaryExpr{
			Op: influxql.IN,
			LHS: &influxql.VarRef{
				Type: influxql.Tag,
			},
			RHS: &influxql.SetLiteral{Vals: map[interface{}]bool{
				"1":  true,
				"2":  true,
				"3":  true,
				"4":  true,
				"5":  true,
				"6":  true,
				"7":  true,
				"8":  true,
				"9":  true,
				"10": true,
				"11": true,
			}},
		},
	}
	chooseINPriority(expr1)
	expr1.RHS = &influxql.BinaryExpr{
		Op: influxql.IN,
		LHS: &influxql.VarRef{
			Type: influxql.Tag,
		},
		RHS: &influxql.SetLiteral{Vals: map[interface{}]bool{
			"1": true,
			"2": true,
			"3": true,
			"4": true,
			"5": true,
			"6": true,
			"7": true,
			"8": true,
			"9": true,
		}},
	}
	chooseINPriority(expr1)
	expr1.RHS = &influxql.BinaryExpr{
		Op: influxql.IN,
		LHS: &influxql.VarRef{
			Type: influxql.Tag,
		},
		RHS: &influxql.SetLiteral{Vals: map[interface{}]bool{
			"1":  true,
			"2":  true,
			"3":  true,
			"4":  true,
			"5":  true,
			"6":  true,
			"7":  true,
			"8":  true,
			"9":  true,
			"10": true,
			"11": true,
			"12": true,
		}},
	}
	chooseINPriority(expr1)
}

func TestSeriesByINExprIterator(t *testing.T) {
	is := &indexSearch{}
	name := []byte("name")
	expr := &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op: influxql.IN,
			LHS: &influxql.VarRef{
				Type: influxql.Tag,
			},
			RHS: &influxql.SetLiteral{Vals: map[interface{}]bool{
				"1":  true,
				"2":  true,
				"3":  true,
				"4":  true,
				"5":  true,
				"6":  true,
				"7":  true,
				"8":  true,
				"9":  true,
				"10": true,
				"11": true,
			}},
		},
		RHS: &influxql.BinaryExpr{
			Op: influxql.IN,
			LHS: &influxql.VarRef{
				Type: influxql.Tag,
			},
			RHS: &influxql.SetLiteral{Vals: map[interface{}]bool{
				"1":  true,
				"2":  true,
				"3":  true,
				"4":  true,
				"5":  true,
				"6":  true,
				"7":  true,
				"8":  true,
				"9":  true,
				"10": true,
				"11": true,
			}},
		},
	}
	tsids := &uint64set.Set{}
	is.seriesByINExprIterator(name, expr, &tsids, true, true, true)
	is.seriesByINExprIterator(name, expr, &tsids, true, false, true)
}

func TestSeriesByBinaryExprSetLiteral(t *testing.T) {
	is := &indexSearch{}
	name := []byte("name")
	key := []byte("key")
	vals := map[interface{}]bool{
		"key": true,
		"1":   true,
	}
	is.seriesByBinaryExprSetLiteral(name, key, vals, false)
}

func TestDoPruneWithSet(t *testing.T) {
	mem := 1 * 1024 * 1024
	path := t.TempDir()
	is := &indexSearch{
		idx: &MergeSetIndex{
			cache: newIndexCache(mem/32, mem/32, mem/16, mem/128, path, false, false),
			tb:    &mergeset.Table{},
		},
		ts: mergeset.TableSearch{},
	}
	set := &uint64set.Set{}
	set.Add(1)
	set.Add(2)
	set.Add(3)
	set.Add(4)
	vals := map[interface{}]bool{
		"1": true,
		"2": true,
		"3": true,
		"4": true,
	}
	tagKey := "1"
	is.doPruneWithSet(set, vals, tagKey, true)
}

func TestMatchSeriesKeyWithSet(t *testing.T) {
	src := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x02,
		0x00, 0x03, 'a', 'b', 'c',
		0x00, 0x05, '1', '2', '3', '4', '5',
	}
	ctx := &PruneContext{
		SeriesKey: src,
	}
	vals := map[interface{}]bool{
		"12345": true,
		"abc":   true,
	}
	matchSeriesKeyWithSet(ctx, vals, "abc")

	src = []byte{
		0x00, 0x00, 0x00, 0x01,
		'a', 'b', 'c',
	}
	ctx.SeriesKey = src
	matchSeriesKeyWithSet(ctx, vals, "abc")
}

func TestMatchSeriesKeyWithSetTag(t *testing.T) {
	tags := influx.PointTags{
		influx.Tag{
			Key:   "1",
			Value: "1",
		},
		influx.Tag{
			Key:   "2",
			Value: "2",
		},
	}
	tagKey := "1"
	vals := map[interface{}]bool{
		"1": false,
		"2": false,
	}
	matchSeriesKeyWithSetTag(tags, vals, tagKey)
	vals = map[interface{}]bool{}
	matchSeriesKeyWithSetTag(tags, vals, tagKey)
}
