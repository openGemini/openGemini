// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func defaultMstInfo(timeClusterDuration time.Duration) *meta.MeasurementInfo {
	mstsInfo := &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:             []string{},
			PrimaryKey:          []string{"field1_string", "field2_int"},
			TimeClusterDuration: timeClusterDuration,
		},
		Schema: &meta.CleanSchema{
			"field1_string": meta.SchemaVal{Typ: influx.Field_Type_String},
			"field2_int":    meta.SchemaVal{Typ: influx.Field_Type_Int},
		},
	}

	return mstsInfo
}

func simpleMstInfo() *meta.MeasurementInfo {
	return &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		Schema:     &meta.CleanSchema{},
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:    []string{},
			PrimaryKey: []string{},
		}}
}

func pkTimeMstInfo() *meta.MeasurementInfo {
	return &meta.MeasurementInfo{
		Name:       defaultMeasurementName,
		Schema:     &meta.CleanSchema{},
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:    []string{"time"},
			PrimaryKey: []string{"time"},
		}}
}

func defaultBFIndexRelation() influxql.IndexRelation {
	list := make([]*influxql.IndexList, 1)
	list[0] = &influxql.IndexList{IList: []string{"field1_string"}}

	return influxql.IndexRelation{
		IndexNames: []string{"bloomfilter"},
		Oids:       []uint32{uint32(index.BloomFilter)},
		IndexList:  list,
	}
}

func fullTxtBFIndexRelation() influxql.IndexRelation {
	list := make([]*influxql.IndexList, 1)
	list[0] = &influxql.IndexList{IList: []string{"field1_string"}}

	return influxql.IndexRelation{
		IndexNames: []string{index.BloomFilterFullTextIndex},
		Oids:       []uint32{uint32(index.BloomFilterFullText)},
		IndexList:  list,
	}
}

func defaultIdent() colstore.MeasurementIdent {
	ident := colstore.NewMeasurementIdent(defaultDb, defaultRp)
	ident.SetName(defaultMeasurementName)
	return ident
}

func initColumnStoreMstInfo(mi *meta.MeasurementInfo) ([]record.Field, bool) {
	ident := defaultIdent()
	ident.SetName(mi.Name)
	colstore.MstManagerIns().Add(ident, mi)

	mst, ok := colstore.MstManagerIns().GetByIdent(ident)
	if !ok {
		return nil, false
	}

	return mst.PrimaryKey(), true
}

func clearMstInfo() {
	colstore.MstManagerIns().Clear()
}

func TestWriteRowsToColumnStoreError(t *testing.T) {
	testDir := t.TempDir()
	colstore.MstManagerIns().Clear()

	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	require.NoError(t, err)

	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{defaultMeasurementName}, 4, 100, time.Second, st, true, true, false, 1)
	ctx := getMstWriteCtx(time.Millisecond*10, config.TSSTORE)
	putMstWriteCtx(ctx)

	mapRows(rows, ctx)
	mmPoints := ctx.getMstMap()
	ctx.writeRowsCtx.SetSeriesRowCountFunc(sh.addRowCountsBySid)

	err = sh.activeTbl.MTable.WriteRows(sh.activeTbl, mmPoints, ctx.writeRowsCtx)
	require.Equal(t, errors.New("measurement info is not found"), err)
	err = closeShard(sh)
	require.NoError(t, err)
}
