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
	"errors"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/services/fence"
	"github.com/stretchr/testify/require"
)

func TestReadFile1(t *testing.T) {
	row := influx.Row{Name: "fence",
		Tags: influx.PointTags{
			influx.Tag{
				Key:   "id",
				Value: "001",
			},
		},
		Fields: influx.Fields{
			influx.Field{
				Key:      "geometry",
				StrValue: ",",
				Type:     influx.Field_Type_String,
			},
		}}
	rows := influx.Rows{row}
	fence.ManagerIns().Open()
	fence.RewriteRows(rows)
	if fence.ManagerIns().Fd != nil {
		fence.ManagerIns().Close()
	}

}

func TestReadFile2(t *testing.T) {
	row := influx.Row{Name: "fence",
		Tags: influx.PointTags{
			influx.Tag{
				Key:   "id",
				Value: "001",
			},
		},
		Fields: influx.Fields{
			influx.Field{
				Key:      "geometry",
				StrValue: "circle,10.112233,20.334455,张三",
				Type:     influx.Field_Type_String,
			},
		}}
	rows := influx.Rows{row}
	fence.ManagerIns().Open()
	fence.RewriteRows(rows)
	if fence.ManagerIns().Fd != nil {
		fence.ManagerIns().Close()
	}
}

func TestReadFile4(t *testing.T) {
	row := influx.Row{Name: "fence",
		Tags: influx.PointTags{
			influx.Tag{
				Key:   "id",
				Value: "001",
			},
		},
		Fields: influx.Fields{
			influx.Field{
				Key:      "geometry",
				StrValue: "rect,10.112233,20.334455,1000.1",
				Type:     influx.Field_Type_String,
			},
		}}
	rows := influx.Rows{row}
	fence.ManagerIns().Open()
	fence.RewriteRows(rows)
	if fence.ManagerIns().Fd != nil {
		fence.ManagerIns().Close()
	}

}
func TestReadFile(t *testing.T) {
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
	row1 := influx.Row{
		Name: "fence",
		Tags: influx.PointTags{
			influx.Tag{
				Key:   "id",
				Value: "",
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
	row2 := influx.Row{
		Name: "fence",
		Tags: influx.PointTags{
			influx.Tag{
				Key:   "id",
				Value: "003",
			},
		},
		Fields: influx.Fields{
			influx.Field{
				Key:      "geometry",
				StrValue: "",
				Type:     influx.Field_Type_String,
			},
		},
	}

	config.GetStoreConfig().Fence.FenceEnable = true
	rows := influx.Rows{row}
	rows = append(rows, row1)
	rows = append(rows, row2)

	fence.ManagerIns().Open()
	fence.RewriteRows(rows)

	_, _ = fence.ReadFencedFile()

	if fence.ManagerIns().Fd != nil {
		fence.ManagerIns().Close()
	}

}

func TestDeleteFenceByID(t *testing.T) {
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

	row2 := influx.Row{
		Name: "fence",
		Tags: influx.PointTags{
			influx.Tag{
				Key:   "id",
				Value: "002",
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
	rows = append(rows, row2)
	config.GetStoreConfig().Fence.FenceEnable = true
	fence.ManagerIns().Open()
	fence.RewriteRows(rows)

	mg := fence.ManagerIns()
	err := mg.DeleteFenceByID("002")
	require.NoError(t, err)
	fence.ReadFencedFile()
	err = mg.DeleteFenceByID("009")
	require.NoError(t, err)
	if fence.ManagerIns().Fd != nil {
		fence.ManagerIns().Close()
	}
}

func TestRemoveFenceMap(t *testing.T) {

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
	config.GetStoreConfig().Fence.FenceEnable = true
	rows := influx.Rows{row}
	fence.ManagerIns().Open()
	fence.RewriteRows(rows)

	mg := fence.ManagerIns()
	mg.RemoveFenceMap("009")

	mg.RemoveFenceMap("001")

	value, ok := mg.Get("001")
	require.True(t, !ok)
	require.Equal(t, (*fence.Fence)(nil), value)
	if fence.ManagerIns().Fd != nil {
		fence.ManagerIns().Close()
	}
}

func TestReadFencedFile(t *testing.T) {
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
	config.GetStoreConfig().Fence.FenceEnable = true
	rows := influx.Rows{row}
	fence.ManagerIns().Open()
	fence.RewriteRows(rows)
	err := fence.ReloadFence()
	require.NoError(t, err)
	if fence.ManagerIns().Fd != nil {
		fence.ManagerIns().Close()
	}
}

func TestParseFenceById(t *testing.T) {

	data := make([]byte, 1, 1)
	_, err := fence.ParseFenceById("001", data)
	require.Equal(t, errors.New("invalid data format"), err)

	data1 := make([]byte, 3, 10)
	_, err = fence.ParseFenceById("001", data1)
	require.Equal(t, errors.New("invalid data format"), err)
}

func TestParseId(t *testing.T) {
	data := make([]byte, 1, 1)
	_, _, err := fence.ParseId(data)
	require.Equal(t, errors.New("invalid data format"), err)

	data1 := make([]byte, 3, 10)
	_, _, err = fence.ParseId(data1)
	require.Equal(t, nil, err)
}
