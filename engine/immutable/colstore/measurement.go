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

package colstore

import (
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var mstManagerIns *MstManager

func init() {
	mstManagerIns = &MstManager{
		mst: make(map[util.MeasurementIdent]*Measurement),
	}
}

type Measurement struct {
	mi         *meta.MeasurementInfo
	pkSchema   []record.Field
	skSchema   []record.Field
	tcDuration time.Duration
}

func (m *Measurement) ColStoreInfo() *meta.ColStoreInfo {
	return m.mi.ColStoreInfo
}

func (m *Measurement) IndexRelation() *influxql.IndexRelation {
	return &m.mi.IndexRelation
}

func (m *Measurement) PrimaryKey() []record.Field {
	return m.pkSchema
}

func (m *Measurement) BuildPKMap() map[string]struct{} {
	pk := m.PrimaryKey()
	pkMap := make(map[string]struct{})
	for i := range pk {
		if pk[i].Name == record.TimeField {
			continue
		}
		pkMap[pk[i].Name] = struct{}{}
	}
	return pkMap
}

func (m *Measurement) SortKey() []record.Field {
	return m.skSchema
}

func (m *Measurement) MeasurementInfo() *meta.MeasurementInfo {
	return m.mi
}

func (m *Measurement) TCDuration() int64 {
	return int64(m.tcDuration)
}

func (m *Measurement) TCLocation() int8 {
	if m.TCDuration() > 0 {
		return TCLocationUsed
	}
	return DefaultTCLocation
}

func (m *Measurement) IsUniqueEnabled() bool {
	return m.ColStoreInfo().UniqueEnabled()
}

func (m *Measurement) IsClusterPKIndex() bool {
	return m.ColStoreInfo().IsClusterPKIndex()
}

func MstManagerIns() *MstManager {
	return mstManagerIns
}

type MstManager struct {
	mu  sync.RWMutex
	mst map[util.MeasurementIdent]*Measurement
}

func (m *MstManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.mst)
}

func (m *MstManager) Exists(ident util.MeasurementIdent) bool {
	m.mu.RLock()
	_, ok := m.mst[ident]
	m.mu.RUnlock()
	return ok
}

func (m *MstManager) Del(ident util.MeasurementIdent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.mst, ident)
}

func (m *MstManager) DelAll(db, rp string) {
	var idents []util.MeasurementIdent
	m.mu.RLock()
	for trip := range m.mst {
		if trip.DB == db && (rp == "" || trip.RP == rp) {
			idents = append(idents, trip)
		}
	}
	m.mu.RUnlock()

	if len(idents) == 0 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range idents {
		delete(m.mst, idents[i])
	}
}

func (m *MstManager) Get(db, rp, name string) (*Measurement, bool) {
	ident := util.MeasurementIdent{
		DB:   db,
		RP:   rp,
		Name: name,
	}
	return m.GetByIdent(ident)
}

func (m *MstManager) GetByIdent(ident util.MeasurementIdent) (*Measurement, bool) {
	m.mu.RLock()
	mst, ok := m.mst[ident]
	m.mu.RUnlock()

	return mst, ok
}

func (m *MstManager) Add(ident util.MeasurementIdent, mi *meta.MeasurementInfo) {
	if m.Exists(ident) {
		return
	}

	mst := m.buildMeasurement(mi)

	m.mu.Lock()
	defer m.mu.Unlock()
	m.mst[ident] = mst
}

func (m *MstManager) buildMeasurement(mi *meta.MeasurementInfo) *Measurement {
	mi.SchemaLock.RLock()
	defer mi.SchemaLock.RUnlock()

	pk := mi.ColStoreInfo.PrimaryKey
	sk := mi.ColStoreInfo.SortKey
	mst := &Measurement{mi: mi}

	mst.pkSchema, mst.tcDuration = m.buildPrimaryKeySchema(mi)
	mst.skSchema = m.buildSortKeySchema(mi.Schema, pk, sk, mi.ColStoreInfo.IsClusterPKIndex())

	return mst
}

func (m *MstManager) buildPrimaryKeySchema(mi *meta.MeasurementInfo) (record.Schemas, time.Duration) {
	pk := mi.ColStoreInfo.PrimaryKey
	if config.IsLogKeeper() || config.GetStoreConfig().MemTable.CsDetachedFlushEnabled {
		return m.buildKeySchema(mi.Schema, pk), 0
	}

	tcd := mi.ColStoreInfo.TimeClusterDuration
	for i := range pk {
		if pk[i] == record.TimeField && tcd == 0 {
			// The primary key includes a time column, but since TimeCluster is not set,
			// the default Duration will be used.
			tcd = DefaultTCDuration
			break
		}
	}

	if tcd > 0 {
		tcd = max(tcd, MinTCDuration)
	}

	return m.buildKeySchema(mi.Schema, pk), tcd
}

func (m *MstManager) buildSortKeySchema(schema *meta.CleanSchema, pk, sk []string, isCluster bool) record.Schemas {
	pkMap := make(map[string]struct{}, len(pk))
	for i := range pk {
		pkMap[pk[i]] = struct{}{}
	}

	newSk := make([]string, 0, 1)
	hasTime := false
	for i := range sk {
		if sk[i] == record.TimeField {
			hasTime = true
		}
		if !isCluster {
			newSk = append(newSk, sk[i])
			continue
		}
		if _, ok := pkMap[sk[i]]; !ok {
			newSk = append(newSk, sk[i])
		}
	}
	if !hasTime {
		newSk = append(newSk, record.TimeField)
	}

	return m.buildKeySchema(schema, newSk)
}

func (m *MstManager) buildKeySchema(schema *meta.CleanSchema, keys []string) record.Schemas {
	dst := make([]record.Field, len(keys))
	for i, key := range keys {
		f := &dst[i]
		f.Name = key

		if key == record.TimeField {
			f.Type = influx.Field_Type_Int
		} else {
			v, _ := schema.GetTyp(key)
			f.Type = record.ToPrimitiveType(v)
		}
	}
	return dst
}
