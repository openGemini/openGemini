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
	"os"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/immutable"
	"github.com/openGemini/openGemini/engine/immutable/colstore"
	"github.com/openGemini/openGemini/engine/index/sparseindex"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/smartystreets/goconvey/convey"
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
			Property:            meta.MeasurementProperty{PrimaryKeyType: meta.PrimaryKeyTypeCluster},
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
			Property:   meta.MeasurementProperty{PrimaryKeyType: meta.PrimaryKeyTypeCluster},
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
			Property:   meta.MeasurementProperty{PrimaryKeyType: meta.PrimaryKeyTypeCluster},
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

func defaultIdent() util.MeasurementIdent {
	ident := util.NewMeasurementIdent(defaultDb, defaultRp)
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

func enableClusterIndex() {
	mst, ok := colstore.MstManagerIns().GetByIdent(defaultIdent())
	if !ok {
		return
	}

	csInfo := mst.ColStoreInfo()
	csInfo.PropertyKey = append(csInfo.PropertyKey, meta.PropertyKeyPKType)
	csInfo.PropertyValue = append(csInfo.PropertyValue, meta.PrimaryKeyTypeCluster)
	csInfo.BuildProperty()
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

func TestColumnStoreScanPrimaryIndex(t *testing.T) {
	testDir := t.TempDir()
	colstore.MstManagerIns().Clear()

	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	require.NoError(t, err)

	sh.skIndexReader = sparseindex.NewSKIndexReader(util.RowsNumPerFragment, colstore.CoarseIndexFragment, colstore.MinRowsForSeek)

	opt := &query.ProcessorOptions{Sources: influxql.Sources{&influxql.Measurement{Name: defaultMeasurementName}}}
	schema := executor.NewQuerySchema(nil, nil, opt, nil)

	convey.Convey("scanWithSparseIndex with err", t, func() {
		dataFile := &MocTsspFile{}
		patches := gomonkey.ApplyMethodFunc(dataFile, "ContainsByTime", func(tr util.TimeRange) (contains bool, err error) {
			return false, errors.New("invalid")
		})
		defer patches.Reset()
		dataFiles := []immutable.TSSPFile{dataFile}
		_, _, err = sh.scanWithSparseIndex(dataFiles, schema, defaultMeasurementName)
		require.EqualError(t, err, "data file contain by time error: invalid")
	})

	convey.Convey("scanWithSparseIndex without err", t, func() {
		dataFile := &MocTsspFile{}
		patches := gomonkey.ApplyMethodFunc(dataFile, "ContainsByTime", func(tr util.TimeRange) (contains bool, err error) {
			return false, nil
		})
		defer patches.Reset()

		dataFiles := []immutable.TSSPFile{dataFile}
		_, _, err = sh.scanWithSparseIndex(dataFiles, schema, defaultMeasurementName)
		require.NoError(t, err)
	})

	convey.Convey("scanWithClusterIndex with err", t, func() {
		dataFile := &MocTsspFile{}
		patches := gomonkey.ApplyMethodFunc(dataFile, "ContainsByTime", func(tr util.TimeRange) (contains bool, err error) {
			return false, errors.New("invalid")
		})
		defer patches.Reset()

		dataFiles := []immutable.TSSPFile{dataFile}
		_, _, err = sh.scanWithClusterIndex(dataFiles, schema, defaultMeasurementName)
		require.EqualError(t, err, "data file contain by time error: invalid")
	})

	convey.Convey("scanWithClusterIndex without err", t, func() {
		dataFile := &MocTsspFile{}
		patches := gomonkey.ApplyMethodFunc(dataFile, "ContainsByTime", func(tr util.TimeRange) (contains bool, err error) {
			return false, nil
		})
		defer patches.Reset()

		dataFiles := []immutable.TSSPFile{dataFile}
		_, _, err = sh.scanWithClusterIndex(dataFiles, schema, defaultMeasurementName)
		require.NoError(t, err)
	})

	err = closeShard(sh)
	require.NoError(t, err)
}

func pkClusterMstInfo(mst string) *meta.MeasurementInfo {
	return &meta.MeasurementInfo{
		Name:       mst,
		Schema:     &meta.CleanSchema{"tagkey1": meta.SchemaVal{Typ: influx.Field_Type_Tag}},
		EngineType: config.COLUMNSTORE,
		ColStoreInfo: &meta.ColStoreInfo{
			SortKey:    []string{"time"},
			PrimaryKey: []string{"tagkey1"},
			Property:   meta.MeasurementProperty{PrimaryKeyType: meta.PrimaryKeyTypeCluster},
		}}
}

func TestWriteColumnStoreWithRowCount(t *testing.T) {
	testDir := t.TempDir()
	mst := "cs_mst"
	colstore.MstManagerIns().Clear()
	_ = os.RemoveAll(testDir)
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, testDir, config.COLUMNSTORE)
	if err != nil {
		t.Fatal(err)
	}
	conf := immutable.GetColStoreConfig()
	conf.SetMaxRowsPerSegment(20)
	conf.SetExpectedSegmentSize(102)
	conf.SetMaxSegmentLimit(65535)
	st := time.Now().Truncate(time.Second)
	rows, _, _ := GenDataRecord([]string{mst}, 4, 100, time.Second, st, true, true, false, 1)

	mstInfo := pkClusterMstInfo(mst)
	ident := util.NewMeasurementIdent(defaultDb, defaultRp)
	ident.SetName(mstInfo.Name)
	colstore.MstManagerIns().Add(ident, mstInfo)
	defer clearMstInfo()

	// write rows
	err = sh.WriteRows(rows, nil)
	require.NoError(t, err)
	sh.ForceFlush()
	sh.waitSnapshot()

	// case1: normal query
	opt := &query.ProcessorOptions{
		Sources: influxql.Sources{&influxql.Measurement{Name: mst}},
		Condition: &influxql.BinaryExpr{
			Op:  influxql.EQ,
			LHS: &influxql.VarRef{Val: "tagkey1", Type: influxql.Tag},
			RHS: &influxql.StringLiteral{Val: "tagvalue1_1"}},
	}
	schema := executor.NewQuerySchema(nil, []string{"count"}, opt, nil)
	rowCount, err := sh.GetRowCountClusterIndex(schema)
	require.NoError(t, err)
	require.Equal(t, int64(100), rowCount)

	// case2: multi measurement
	opt.Sources = influxql.Sources{&influxql.Measurement{Name: mst},
		&influxql.Measurement{Name: mst + "1"}}
	_, err = sh.GetRowCountClusterIndex(schema)
	require.EqualError(t, err, "currently, Only a single table is supported")

	// case3: unknown measurement
	opt.Sources = influxql.Sources{&influxql.Measurement{Name: mst + "1"}}
	_, err = sh.GetRowCountClusterIndex(schema)
	require.NoError(t, err)
	err = closeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
}
