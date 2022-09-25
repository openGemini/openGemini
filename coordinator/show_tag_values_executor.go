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

package coordinator

import (
	"sort"
	"sync"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"go.uber.org/zap"
)

type ShowTagValuesExecutor struct {
	logger      *logger.Logger
	mc          meta.MetaClient
	me          IMetaExecutor
	store       netstorage.Storage
	cardinality bool
	dimensions  influxql.Dimensions
}

func NewShowTagValuesExecutor(logger *logger.Logger, mc meta.MetaClient, me IMetaExecutor, store netstorage.Storage) *ShowTagValuesExecutor {
	return &ShowTagValuesExecutor{
		logger: logger,
		mc:     mc,
		me:     me,
		store:  store,
	}
}

func (e *ShowTagValuesExecutor) Cardinality(dimensions influxql.Dimensions) {
	e.dimensions = dimensions
	e.cardinality = true
}

func (e *ShowTagValuesExecutor) Execute(stmt *influxql.ShowTagValuesStatement) (models.Rows, error) {
	if stmt.Database == "" {
		return nil, ErrDatabaseNameRequired
	}

	tagValues, err := e.queryTagValues(stmt)
	if err != nil {
		return nil, err
	}
	tagValues = e.mergeTagValuesSlice(tagValues)

	if e.cardinality {
		return e.emitCardinality(tagValues)
	}
	return e.emit(tagValues, stmt.Offset, stmt.Limit)
}

func (e *ShowTagValuesExecutor) emit(tagValues TagValuesSlice, offset, limit int) (models.Rows, error) {
	rows := make(models.Rows, 0, len(tagValues))

	for _, m := range tagValues {
		values := e.applyLimit(offset, limit, m.Values)
		if len(values) == 0 {
			continue
		}

		row := &models.Row{
			Name:    m.Name,
			Columns: []string{"key", "value"},
			Values:  make([][]interface{}, len(values)),
		}
		for i := range values {
			row.Values[i] = []interface{}{values[i].Key, values[i].Value}
		}

		rows = append(rows, row)
	}

	return rows, nil
}

func (e *ShowTagValuesExecutor) emitCardinality(tagValues TagValuesSlice) (models.Rows, error) {
	rows := make(models.Rows, 0, len(tagValues))

	for _, m := range tagValues {
		values := e.applyLimit(0, 0, m.Values)
		if len(values) == 0 {
			continue
		}

		rows = append(rows, &models.Row{
			Name:    m.Name,
			Columns: []string{"count"},
			Values: [][]interface{}{
				{len(values)},
			},
		})
	}

	return rows, nil
}

func (e *ShowTagValuesExecutor) applyLimit(offset, limit int, values netstorage.TagSets) netstorage.TagSets {
	size := len(values)
	if offset >= size {
		return nil
	}
	sort.Sort(values)

	// The same tag key-value may be distributed on different nodes
	cursor := 0
	for i := 1; i < size; i++ {
		if values[cursor] == values[i] {
			continue
		}
		cursor++
		if cursor != i {
			values[cursor] = values[i]
		}
	}
	size = cursor + 1

	if offset >= size {
		return nil
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = size
	}

	limit += offset
	if limit > size {
		limit = size
	}
	return values[offset:limit]
}

// mergeTagValuesSlice
func (e *ShowTagValuesExecutor) mergeTagValuesSlice(s TagValuesSlice) TagValuesSlice {
	l := len(s)
	if l == 0 {
		return nil
	}
	sort.Sort(s)
	ret := TagValuesSlice{s[0]}
	if l == 1 {
		return ret
	}

	k := 0
	for i := 1; i < l; i++ {
		if ret[k].Name == s[i].Name {
			ret[k].Values = append(ret[k].Values, s[i].Values...)
			continue
		}

		ret = append(ret, netstorage.TableTagSets{
			Name:   s[i].Name,
			Values: s[i].Values,
		})
		k++
	}

	return ret
}

func (e *ShowTagValuesExecutor) queryTagValues(q *influxql.ShowTagValuesStatement) (TagValuesSlice, error) {
	tagKeys, err := e.mc.QueryTagKeys(q.Database, q.Sources.Measurements(), q.TagKeyCondition)
	if err != nil {
		return nil, err
	}
	if len(tagKeys) == 0 {
		e.logger.Info("no matching tag key found", zap.String("pos", "ShowTagValuesExecutor.queryTagValues"))
		return nil, nil
	}

	var tagValuesSlice TagValuesSlice

	lock := new(sync.Mutex)
	err = e.me.EachDBNodes(q.Database, func(nodeID uint64, pts []uint32) {
		s, err := e.store.TagValues(nodeID, q.Database, pts, tagKeys, q.Condition)
		if err != nil {
			e.logger.Error("failed to query tag values", zap.Error(err))
			return
		}
		lock.Lock()
		defer lock.Unlock()
		tagValuesSlice = append(tagValuesSlice, s...)
	})
	if err != nil {
		return nil, err
	}

	return tagValuesSlice, nil
}
