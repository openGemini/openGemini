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
	"fmt"
	"sync"

	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

var mstManagerIns *MstManager

func init() {
	mstManagerIns = &MstManager{
		mst: make(map[MeasurementIdent]*Measurement),
	}
}

type MeasurementIdent struct {
	DB   string
	RP   string
	Name string
}

func NewMeasurementIdent(db, rp string) MeasurementIdent {
	return MeasurementIdent{
		DB: db,
		RP: rp,
	}
}

func (t *MeasurementIdent) SetSafeName(name string) {
	t.Name = stringinterner.InternSafe(name)
}

func (t *MeasurementIdent) SetName(name string) {
	t.Name = name
}

func (t *MeasurementIdent) String() string {
	return fmt.Sprintf("%s.%s.%s", t.DB, t.RP, t.Name)
}

type Measurement struct {
	mi       *meta.MeasurementInfo
	pkSchema []record.Field
	skSchema []record.Field
}

func (m *Measurement) ColStoreInfo() *meta.ColStoreInfo {
	return m.mi.ColStoreInfo
}

func (m *Measurement) IndexRelation() influxql.IndexRelation {
	return m.mi.IndexRelation
}

func (m *Measurement) PrimaryKey() []record.Field {
	return m.pkSchema
}

func (m *Measurement) SortKey() []record.Field {
	return m.skSchema
}

func (m *Measurement) MeasurementInfo() *meta.MeasurementInfo {
	return m.mi
}

func MstManagerIns() *MstManager {
	return mstManagerIns
}

type MstManager struct {
	mu  sync.RWMutex
	mst map[MeasurementIdent]*Measurement
}

func (m *MstManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.mst)
}

func (m *MstManager) Exists(ident MeasurementIdent) bool {
	m.mu.RLock()
	_, ok := m.mst[ident]
	m.mu.RUnlock()
	return ok
}

func (m *MstManager) Del(ident MeasurementIdent) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.mst, ident)
}

func (m *MstManager) DelAll(db, rp string) {
	var idents []MeasurementIdent
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
	ident := MeasurementIdent{
		DB:   db,
		RP:   rp,
		Name: name,
	}
	return m.GetByIdent(ident)
}

func (m *MstManager) GetByIdent(ident MeasurementIdent) (*Measurement, bool) {
	m.mu.RLock()
	mst, ok := m.mst[ident]
	m.mu.RUnlock()

	return mst, ok
}

func (m *MstManager) Add(ident MeasurementIdent, mi *meta.MeasurementInfo) {
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

	mst.pkSchema = m.buildKeySchema(mi.Schema, pk)
	mst.skSchema = m.buildSortKeySchema(mi.Schema, pk, sk)

	return mst
}

func (m *MstManager) buildSortKeySchema(schema *meta.CleanSchema, pk, sk []string) record.Schemas {
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
