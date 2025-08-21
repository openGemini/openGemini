// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this File except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fence_test

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/geo/s2"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/fence"
	"github.com/stretchr/testify/require"
)

func TestMatchFence(t *testing.T) {
	data := []string{
		"circle,2.112838,5.140505,1000.10 tag_00000001",
		"circle,2.716669,7.867259,1000.10 tag_00000011",
		"circle,3.034971,2.690358,1000.10 tag_00000006",
		"circle,3.690891,2.934575,1000.10 tag_00000016",
		"circle,4.348002,6.143535,1000.10 tag_00000003",
		"circle,8.035435,1.896889,1000.10 tag_00000008",
		"circle,6.633910,1.944029,1000.10 tag_00000015",
		"circle,7.042273,9.159096,1000.10 tag_00000000",
		"circle,5.552798,9.761163,1000.10 tag_00000004",
		"circle,5.865473,8.294107,1000.10 tag_00000013",
	}

	mg := fence.ManagerIns()
	for i := range data {
		idx := strings.Index(data[i], " ")
		geometry, id := data[i][:idx], data[i][idx+1:]
		f, err := fence.ParseFence(id, geometry)
		require.NoError(t, err)
		mg.Add(f)
	}

	require.NotEmpty(t, mg.MatchPoint(&fence.PointMatchCtx{}, 2.112838, 5.140505))
	require.NotEmpty(t, mg.MatchPoint(&fence.PointMatchCtx{}, 6.633910, 1.944029))
	require.NotEmpty(t, mg.MatchPoint(&fence.PointMatchCtx{}, 5.865473, 8.294107))
}

func TestMakeCellID(t *testing.T) {
	circle := &fence.Circle{}
	err := circle.Build([]float64{10.11, 20.11, 1000})
	require.NoError(t, err)

	cells := circle.Cells()
	for _, c := range cells {
		require.Equal(t, fence.MaxLevel, c.Level())
	}

	point := s2.LatLngFromDegrees(10.111, 20.111)
	pc := fence.GetLatLonCell(s2.LatLngFromDegrees(10.111, 20.111))
	require.Equal(t, fence.MaxLevel, pc.Level())
	require.True(t, circle.Contains(point))
}

func TestRewriteRows_Fence(t *testing.T) {
	row := influx.Row{
		Name: "fence",
		Tags: influx.PointTags{
			influx.Tag{
				Key:   "id",
				Value: "001",
			},
		},
		Fields: influx.Fields{
			influx.Field{
				Key:      "geometry",
				StrValue: "circle,10.112233,20.334455,1000.1",
				Type:     influx.Field_Type_String,
			},
		},
	}

	rows := influx.Rows{row}
	fence.ManagerIns().Open()
	fence.RewriteRows(rows)
	f, ok := fence.ManagerIns().Get("001")
	require.True(t, ok)
	require.Equal(t, "001", f.ID)
}

func TestParseFloats(t *testing.T) {
	cases := []string{
		"10.112233,20.334455,1000.1",
		"Inf,20.334455,1000.1",
		"NaN,20.334455,1000.1",
		"10.112233a,20.334455,1000.1",
		"1e9,20.334455,1000.1",
	}

	var assert = func(exp, got []float64) bool {
		if len(exp) != len(got) {
			return false
		}

		for i := range exp {
			if math.IsNaN(exp[i]) && math.IsNaN(got[i]) ||
				math.IsInf(exp[i], 1) && math.IsInf(got[i], 1) ||
				math.IsInf(exp[i], -1) && math.IsInf(got[i], -1) {
				continue
			}

			if exp[i] != got[i] {
				return false
			}
		}
		return true
	}

	for i := range cases {
		exp, errExp := parseFloats(cases[i])
		got, errGot := fence.ParseFloats(cases[i])

		if errExp != nil {
			require.Error(t, errGot)
			continue
		}

		require.True(t, assert(exp, got), "exp: %v, got: %v", exp, got)
	}
}

func parseFloats(s string) ([]float64, error) {
	items := strings.Split(s, ",")
	ret := make([]float64, len(items))

	for i := range items {
		f, err := strconv.ParseFloat(items[i], 64)
		if err != nil {
			return nil, err
		}
		ret[i] = f
	}
	return ret, nil
}

func BenchmarkParseFloats(b *testing.B) {
	s := "10.112233,20.334455,1000.1, , 2"
	b.Run("v1", func(b *testing.B) {
		for range b.N {
			_, _ = parseFloats(s)
		}
	})

	b.Run("v2", func(b *testing.B) {
		for range b.N {
			_, _ = fence.ParseFloats(s)
		}
	})
}

func BenchmarkCoverCells(b *testing.B) {
	b.Run("circle", func(b *testing.B) {
		circle := &fence.Circle{}
		_ = circle.Build([]float64{10.11, 20.11, 1000})
		for range b.N {
			circle.Cells()
		}
	})
}

func BenchmarkFenceCheck(b *testing.B) {
	runtime.GOMAXPROCS(1)
	mg := fence.ManagerIns()
	for i := range 100000 {
		circle := &fence.Circle{}
		_ = circle.Build([]float64{randLat(), randLat(), 400})

		f := &fence.Fence{
			ID:       fmt.Sprintf("f%08d", i),
			Typ:      fence.GeometryCircle,
			Geometry: circle,
		}
		mg.Add(f)
	}

	ctx := &fence.PointMatchCtx{}
	var lat, lon float64
	for range 100000 {
		lat = randLat()
		lon = randLat()

		items := mg.MatchPoint(ctx, lat, lon)
		if len(items) > 0 {
			break
		}
	}

	runtime.GC()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mg.MatchPoint(ctx, lat, lon)
	}
}

func TestFenceCheck(t *testing.T) {
	mg := fence.ManagerIns()
	for i := range 100000 {
		circle := &fence.Circle{}
		_ = circle.Build([]float64{randLat(), randLat(), 400})

		f := &fence.Fence{
			ID:       fmt.Sprintf("f%08d", i),
			Typ:      fence.GeometryCircle,
			Geometry: circle,
		}
		mg.Add(f)
	}

	ctx := &fence.PointMatchCtx{}
	var lat, lon float64
	for range 100000 {
		lat = randLat()
		lon = randLat()

		items := mg.MatchPoint(ctx, lat, lon)
		if len(items) > 0 {
			break
		}
	}

	runtime.GC()

	for range 1000000 {
		mg.MatchPoint(ctx, lat, lon)
	}

}

func randLat() float64 {
	return math.Floor(rand.Float64()*1e6*100) / 1e6
}

func TestSerialize(t *testing.T) {

	circle := &fence.Circle{}
	_ = circle.Build([]float64{randLat(), randLat(), 400})

	f := &fence.Fence{
		ID:       fmt.Sprintf("f%08d", 1),
		Typ:      "circle",
		Geometry: circle,
	}
	serialize := f.Serialize()
	fe, err := f.UnSerialize(serialize)
	require.NoError(t, err)
	require.Equal(t, fe.ID, f.ID)

	f1 := &fence.Fence{
		ID:  fmt.Sprintf("f%08d", 1),
		Typ: "",
	}
	_ = f1.Serialize()
	require.NoError(t, nil, err)
}
