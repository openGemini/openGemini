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

package executor

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
)

func TestLinearFillProcessor_fillHelperFunc(t *testing.T) {
	convey.Convey("test fill helper func prev and input equals", t, func() {
		fakeChunkImpl := &ColumnImpl{times: []int64{1, 2, 3}}
		p := gomonkey.ApplyMethod(fakeChunkImpl, "IsNilV2", func(_ *ColumnImpl, _ int) bool {
			return false
		})
		defer p.Reset()
		resultCol := &ColumnImpl{}
		p2 := gomonkey.ApplyMethod(resultCol, "AppendNil", func(_ *ColumnImpl) {})
		defer p2.Reset()

		input, output, prev := ChunkImpl{columns: []Column{fakeChunkImpl}, time: []int64{1, 2, 3}}, ChunkImpl{columns: []Column{resultCol}}, ChunkImpl{columns: []Column{fakeChunkImpl}, time: []int64{1, 2, 3}}
		fillItem, preWin := FillItem{interval: 1}, prevWindow{}

		convey.Convey("test integer linear", func() {
			f := IntegerLinearFillProcessor{}
			convey.So(func() { f.fillHelperFunc(&input, &output, &prev, &fillItem, &preWin) }, convey.ShouldNotPanic)
		})

		convey.Convey("test float linear", func() {
			f := FloatLinearFillProcessor{}
			convey.So(func() { f.fillHelperFunc(&input, &output, &prev, &fillItem, &preWin) }, convey.ShouldNotPanic)
		})
	})

}
