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

package encoding

import (
	"reflect"
	"testing"

	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/jwilder/encoding/simple8b"
)

func TestTimeEncoder_simple8b(t *testing.T) {
	coder := GetTimeCoder()

	inArr := []int64{
		1655189096973640419, 1655189096982730400, 1655189097038068410, 1655189097051959505,
		1655189097056706333, 1655189097074400007, 1655189097148478701, 1655189097259744774,
		1655189097264054355, 1655189097279860920, 1655189097286808049, 1655189097302007108,
		1655189097326103397, 1655189097377999133, 1655189097394988856, 1655189097404915359,
		1655189097412727467, 1655189097435317900, 1655189097466196081, 1655189097483507485,
		1655189097491189289, 1655189097515426521, 1655189097604188511, 1655189097657017788,
		1655189097665961192, 1655189097672852815, 1655189097681771335, 1655189097724614744,
		1655189097764193543, 1655189097770989969, 1655189097889670365, 1655189097899350215,
	}

	var err error
	ib := util.Int64Slice2byte(inArr)
	ob := make([]byte, 0, len(ib))
	ob, err = coder.Encoding(ib, ob)
	if err != nil {
		t.Fatal(err)
	}

	db := make([]byte, 0, len(ib))
	db, err = coder.Decoding(ob, db)
	if err != nil {
		t.Fatal(err)
	}
	dArr := util.Bytes2Int64Slice(db)
	if !reflect.DeepEqual(inArr, dArr) {
		t.Fatalf("exp:%v, get:%v", inArr, dArr)
	}
}

func TestTimeEncoder_Uncompressed(t *testing.T) {
	coder := GetTimeCoder()

	inArr := []int64{
		1655189096973640419, 1655189096982730400, 1655189097038068410, 1655189097051959505,
		1655189097056706333, 1655189097074400007, 1655189097148478701, 1655189097259744774,
		1655189097264054355, 1655189097279860920, 1655189097286808049, 1655189097302007108,
		1655189097326103397, 1655189097377999133, 1655189097394988856, 1655189097404915359,
		1655189097412727467, 1655189097435317900, 1655189097466196081, 1655189097483507485,
		1655189097491189289, 1655189097515426521, 1655189097604188511, 1655189097657017788,
		1655189097665961192, 1655189097672852815, 1655189097681771335, 1655189097724614744,
		1655189097764193543, 1655189097770989969, 1655189097889670365, 1655189097899350215,
	}

	inArr[len(inArr)-1] += simple8b.MaxValue + 1

	var err error
	ib := util.Int64Slice2byte(inArr)
	ob := make([]byte, 0, len(ib))
	ob, err = coder.Encoding(ib, ob)
	if err != nil {
		t.Fatal(err)
	}

	db := make([]byte, 0, len(ib))
	db, err = coder.Decoding(ob, db)
	if err != nil {
		t.Fatal(err)
	}
	dArr := util.Bytes2Int64Slice(db)
	if !reflect.DeepEqual(inArr, dArr) {
		t.Fatalf("exp:%v, get:%v", inArr, dArr)
	}
}

func TestTimeEncoder_Snappy(t *testing.T) {
	coder := GetTimeCoder()

	inArr := []int64{
		1655190265000000000, 1655190266000000000, 1655190267000000000, 1655190268000000000,
		1655190269000000000, 1655190270000000000, 1655190271000000000, 1655190272000000000,
		1655190273000000000, 1655190274000000000, 1655190275000000000, 1655190276000000000,
		1655190277000000000, 1655190278000000000, 1655190279000000000, 1655190280000000000,
		1655190281000000000, 1655190282000000000, 1655190283000000000, 1655190284000000000,
		1655190285000000000, 1655190286000000000, 1655190287000000000, 1655190288000000000,
		1655190289000000000, 1655190290000000000, 1655190291000000000, 1655190292000000000,
		1655190293000000000, 1655190294000000000, 1655190295000000000, 2808111800606846976,
	}

	var err error
	ib := util.Int64Slice2byte(inArr)
	ob := make([]byte, 0, len(ib))
	ob, err = coder.Encoding(ib, ob)
	if err != nil {
		t.Fatal(err)
	}

	db := make([]byte, 0, len(ib))
	db, err = coder.Decoding(ob, db)
	if err != nil {
		t.Fatal(err)
	}
	dArr := util.Bytes2Int64Slice(db)
	if !reflect.DeepEqual(inArr, dArr) {
		t.Fatalf("exp:%v, get:%v", inArr, dArr)
	}
}
