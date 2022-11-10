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

package tsi

import (
	"fmt"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/kvstorage"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/stringinterner"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/github.com/savsgio/dictpool"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

const (
	// Avoid stepping to slow path in marshalTagValue
	minVersion    = uint16(0x33)
	versionPrefix = "v_"
)

// IndexBuilder is a collection of all indexes
type IndexBuilder struct {
	path      string                    // eg, data/db/pt/rp/index/indexid
	Relations map[uint32]*IndexRelation // <oid, indexRelation>
	ident     *meta.IndexIdentifier
	endTime   time.Time
	duration  time.Duration
	kvStorage kvstorage.KVStorage
	// Measurement version
	mVersion    map[string]uint16
	versionLock sync.RWMutex

	mu sync.RWMutex
}

func NewIndexBuilder(opt *Options) *IndexBuilder {
	iBuilder := &IndexBuilder{
		path:      opt.path,
		ident:     opt.ident,
		duration:  opt.duration,
		endTime:   opt.endTime,
		kvStorage: opt.kvStorage,
		mVersion:  make(map[string]uint16),
	}
	return iBuilder
}

func (iBuilder *IndexBuilder) Flush() {
	idx := iBuilder.GetPrimaryIndex().(*MergeSetIndex)
	idx.DebugFlush()
}

func (iBuilder *IndexBuilder) Open() error {
	if iBuilder.kvStorage == nil || iBuilder.kvStorage.Closed() {
		path := iBuilder.path + "/" + KVDirName
		kv, err := kvstorage.NewStorage(&kvstorage.Config{
			KVType: kvstorage.PEBBLEDB,
			Path:   path,
			Pebble: &kvstorage.PebbleOptions{},
		})
		if err != nil {
			return err
		}
		iBuilder.kvStorage = kv
	}

	if err := iBuilder.loadMeasurementVersion(); err != nil {
		return err
	}

	// Open all indexes
	for _, relation := range iBuilder.Relations {
		if err := relation.IndexOpen(); err != nil {
			logger.GetLogger().Error("Index open fail", zap.Error(err))
			return err
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) Path() string {
	return iBuilder.path
}

func (iBuilder *IndexBuilder) GetPrimaryIndex() PrimaryIndex {
	return iBuilder.Relations[0].indexAmRoutine.index.(PrimaryIndex)
}

func (iBuilder *IndexBuilder) Ident() *meta.IndexIdentifier {
	return iBuilder.ident
}

func (iBuilder *IndexBuilder) RPName() string {
	return iBuilder.ident.Policy
}

func (iBuilder *IndexBuilder) GetIndexID() uint64 {
	return iBuilder.ident.Index.IndexID
}

func (iBuilder *IndexBuilder) SetDuration(duration time.Duration) {
	iBuilder.duration = duration
}

func (iBuilder *IndexBuilder) GetDuration() time.Duration {
	return iBuilder.duration
}

func (iBuilder *IndexBuilder) Expired() bool {
	// duration == 0 means INF.
	now := time.Now().UTC()
	if iBuilder.duration != 0 && iBuilder.endTime.Add(iBuilder.duration).Before(now) {
		return true
	}

	return false
}

func (iBuilder *IndexBuilder) GetEndTime() time.Time {
	return iBuilder.endTime
}

func (iBuilder *IndexBuilder) saveVersion(name []byte, version uint16) error {
	key := make([]byte, 0, len(versionPrefix)+len(name))
	key = append(key, versionPrefix...)
	key = append(key, name...)
	if err := iBuilder.kvStorage.Set(key, encoding.MarshalUint16(nil, version)); err != nil {
		return err
	}
	return nil
}

func (iBuilder *IndexBuilder) loadMeasurementVersion() error {
	iBuilder.kvStorage.GetByPrefixWithFunc([]byte(versionPrefix), func(k, v []byte) bool {
		value := encoding.UnmarshalUint16(v)
		iBuilder.storeVersion(string(k[len(versionPrefix):]), value)
		return false
	})

	return nil
}

func (iBuilder *IndexBuilder) loadOrStore(name string) (uint16, bool) {
	iBuilder.versionLock.RLock()
	version, ok := iBuilder.mVersion[name]
	if ok {
		iBuilder.versionLock.RUnlock()
		return version, true
	}
	iBuilder.versionLock.RUnlock()

	iBuilder.versionLock.Lock()
	defer iBuilder.versionLock.Unlock()
	version, ok = iBuilder.mVersion[name]
	if ok {
		return version, true
	}
	iBuilder.mVersion[name] = minVersion
	return minVersion, false
}

func (iBuilder *IndexBuilder) getVersion(name string) (uint16, bool) {
	iBuilder.versionLock.RLock()
	defer iBuilder.versionLock.RUnlock()
	version, ok := iBuilder.mVersion[name]
	return version, ok
}

func (iBuilder *IndexBuilder) storeVersion(name string, version uint16) {
	iBuilder.versionLock.Lock()
	defer iBuilder.versionLock.Unlock()
	iBuilder.mVersion[name] = version
}

func (iBuilder *IndexBuilder) walkVersions(fn func(key string, value uint16) error) error {
	iBuilder.versionLock.RLock()
	defer iBuilder.versionLock.RUnlock()
	var err error
	for k := range iBuilder.mVersion {
		err = fn(k, iBuilder.mVersion[k])
		if err != nil {
			return err
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) DropMeasurement(name []byte) error {
	curVersion, ok := iBuilder.getVersion(record.Bytes2str(name))
	if !ok {
		// Measurement not found, ignore it
		return nil
	}
	newVersion := curVersion + 1
	iBuilder.storeVersion(stringinterner.InternSafe(record.Bytes2str(name)), newVersion)
	if err := iBuilder.saveVersion(name, newVersion); err != nil {
		return err
	}
	return nil
}

func (iBuilder *IndexBuilder) CreateIndexIfNotExists(mmRows *dictpool.Dict) error {
	primaryIndex := iBuilder.GetPrimaryIndex()
	iBuilder.mu.Lock()
	defer iBuilder.mu.Unlock()
	for mmIdx := range mmRows.D {
		rows, ok := mmRows.D[mmIdx].Value.(*[]influx.Row)
		if !ok {
			return errno.NewError(errno.CreateIndexFailPointRowType)
		}
		version, loaded := iBuilder.loadOrStore(stringinterner.InternSafe(mmRows.D[mmIdx].Key))
		if !loaded {
			if err := iBuilder.saveVersion([]byte(mmRows.D[mmIdx].Key), version); err != nil {
				return err
			}
		}

		for rowIdx := range *rows {
			row := &(*rows)[rowIdx]
			if row.SeriesId != 0 {
				row.PrimaryId = row.SeriesId
				if err := iBuilder.createSecondaryIndex(row, primaryIndex, version); err != nil {
					return err
				}
				continue
			}

			// use primary index to create index
			mergetIndex := primaryIndex.(*MergeSetIndex)
			var err error
			sid, err := mergetIndex.GetSeriesIdBySeriesKey(row.IndexKey, record.Str2bytes(row.Name))
			if err != nil {
				return err
			}
			if sid == 0 {
				row.SeriesId, err = mergetIndex.CreateIndexIfNotExistsByRow(row, version)
				if err != nil {
					return err
				}
				row.SeriesId = (*rows)[rowIdx].SeriesId
			} else {
				(*rows)[rowIdx].SeriesId = sid
				row.SeriesId = sid
			}

			// PrimaryId is the same as SeriesId by default.
			row.PrimaryId = row.SeriesId
			if err = iBuilder.createSecondaryIndex(row, primaryIndex, version); err != nil {
				return err
			}
		}
	}

	return nil
}

func (iBuilder *IndexBuilder) CreateIndexIfPrimaryKeyExists(mmRows *dictpool.Dict, openIndexOption bool) error {
	if !openIndexOption {
		return nil
	}
	primaryIndex := iBuilder.GetPrimaryIndex()
	for mmIdx := range mmRows.D {
		rows, ok := mmRows.D[mmIdx].Value.(*[]influx.Row)
		if !ok {
			return errno.NewError(errno.CreateIndexFailPointRowType)
		}
		version, loaded := iBuilder.loadOrStore(stringinterner.InternSafe(mmRows.D[mmIdx].Key))
		if !loaded {
			if err := iBuilder.saveVersion([]byte(mmRows.D[mmIdx].Key), version); err != nil {
				return err
			}
		}

		for rowIdx := range *rows {
			row := &(*rows)[rowIdx]
			row.PrimaryId = row.SeriesId
			if err := iBuilder.createSecondaryIndex(row, primaryIndex, version); err != nil {
				return err
			}
		}
	}

	return nil
}

func (iBuilder *IndexBuilder) createSecondaryIndex(row *influx.Row, primaryIndex PrimaryIndex, version uint16) error {
	for _, indexOpt := range row.IndexOptions {
		relation := iBuilder.Relations[indexOpt.Oid]
		if relation == nil {
			opt := &Options{
				indexType: GetIndexTypeById(indexOpt.Oid),
				path:      primaryIndex.Path(),
			}
			var err error
			relation, err = NewIndexRelation(opt, primaryIndex, iBuilder)
			if err != nil {
				return err
			}
			if err = relation.IndexOpen(); err != nil {
				return err
			}
			iBuilder.Relations[indexOpt.Oid] = relation
		}
		err := relation.IndexInsert([]byte(row.Name), row, version)
		if err != nil {
			return err
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) Scan(span *tracing.Span, name []byte, opt *query.ProcessorOptions, idxType IndexType) (interface{}, error) {
	oid := GetIndexIdByType(idxType)
	relation := iBuilder.Relations[oid]
	if relation == nil {
		relation = iBuilder.Relations[uint32(MergeSet)]
		if relation == nil {
			return nil, fmt.Errorf("not exist index for %s", name)
		}
	}
	return relation.IndexScan(span, name, opt)
}

func (iBuilder *IndexBuilder) Delete(name []byte, condition influxql.Expr, tr TimeRange) error {
	var err error
	var index uint32
	for i, relation := range iBuilder.Relations {
		if relation.oid == uint32(MergeSet) {
			index = i
			continue
		}
		err = relation.IndexDelete(name, condition, tr)
		if err != nil {
			return err
		}
	}

	//delete primary index last
	return iBuilder.Relations[index].IndexDelete(name, condition, tr)
}

func (iBuilder *IndexBuilder) Close() error {
	if err := iBuilder.kvStorage.Close(); err != nil {
		return err
	}
	for _, relation := range iBuilder.Relations {
		if err := relation.IndexClose(); err != nil {
			return err
		}
	}
	return nil
}
