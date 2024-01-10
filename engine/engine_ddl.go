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

package engine

import (
	"context"
	"path"
	"sort"
	"strconv"
	sysStrings "strings"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/netstorage"
	stat "github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
	"github.com/openGemini/openGemini/lib/strings"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
	"go.uber.org/zap"
)

func (e *Engine) DeleteDatabase(db string, ptId uint32) (err error) {
	traceId := tsi.GenerateUUID()
	begin := time.Now()
	e.log.Info("drop database begin", zap.String("db", db),
		zap.Uint32("pt", ptId), zap.Uint64("trace_id", traceId))
	defer func() {
		e.log.Info("drop database finish",
			zap.Error(err),
			zap.String("time use", time.Since(begin).String()),
			zap.String("db", db), zap.Uint32("pt", ptId),
			zap.Uint64("trace_id", traceId))
	}()

	if err := e.startDrop(db, e.droppingDB); err != nil {
		return err
	}
	defer e.endDrop(db, e.droppingDB)

	atomic.AddInt64(&stat.EngineStat.DropDatabaseCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DropDatabaseDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
	}(begin)

	dataPath := path.Join(e.dataPath, config.DataDirectory, db, strconv.Itoa(int(ptId)))
	walPath := path.Join(e.walPath, config.WalDirectory, db, strconv.Itoa(int(ptId)))
	lockPath := ""

	e.mu.RLock()
	dbInfo, ok := e.DBPartitions[db]
	if !ok {
		e.mu.RUnlock()
		return deleteDataAndWalPath(dataPath, walPath, &lockPath)
	}

	dbPTInfo, exist := dbInfo[ptId]
	if !exist {
		e.mu.RUnlock()
		return deleteDataAndWalPath(dataPath, walPath, &lockPath)
	}

	done := make(chan bool, 1)
	if ok = dbPTInfo.markOffload(done); !ok {
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			log.Warn("offload dbPt timeout", zap.String("db", db), zap.Uint32("pt id", ptId))
			dbPTInfo.unMarkOffload()
			e.mu.RUnlock()
			atomic.AddInt64(&stat.EngineStat.DropDatabaseErrs, 1)
			return meta2.ErrConflictWithIo
		}
	}

	select {
	case <-dbPTInfo.unload:
	default:
		close(dbPTInfo.unload)
	}

	dbPTInfo.wg.Wait()
	err = e.deleteShardsAndIndexes(dbPTInfo)
	if err == nil {
		err = deleteDataAndWalPath(dataPath, walPath, dbInfo[ptId].lockPath)
	}
	if err != nil {
		dbPTInfo.unMarkOffload()
		e.mu.RUnlock()
		atomic.AddInt64(&stat.EngineStat.DropDatabaseErrs, 1)
		return err
	}

	e.mu.RUnlock()
	e.mu.Lock()
	e.dropDBPTInfo(db, ptId)
	e.mu.Unlock()
	return nil
}

func (e *Engine) DropRetentionPolicy(db string, rp string, ptId uint32) error {
	rpName := db + "." + rp
	if err := e.startDrop(rpName, e.droppingRP); err != nil {
		return err
	}
	defer e.endDrop(rpName, e.droppingRP)

	atomic.AddInt64(&stat.EngineStat.DropRPCount, 1)
	start := time.Now()
	e.log.Info("start drop retention policy...", zap.String("db", db), zap.String("rp", rp), zap.Uint32("pt", ptId))
	defer func(st time.Time) {
		d := time.Since(st)
		atomic.AddInt64(&stat.EngineStat.DropRPDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("drop retention policy done",
			zap.String("db", db), zap.String("rp", rp), zap.Duration("duration", d), zap.Uint32("pt", ptId))
	}(start)

	deleteDirFunc := func() error {
		dataPath := path.Join(e.dataPath, config.DataDirectory, db, strconv.Itoa(int(ptId)), rp)
		walPath := path.Join(e.walPath, config.WalDirectory, db, strconv.Itoa(int(ptId)), rp)
		lockPath := ""
		if err := deleteDataAndWalPath(dataPath, walPath, &lockPath); err != nil {
			atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
			return err
		}
		return nil
	}

	if err := e.DbPTRef(db, ptId); err != nil {
		atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
		return err
	}
	defer e.DbPTUnref(db, ptId)

	if err := e.deleteIndexes(db, ptId, rp, func(dbPTInfo *DBPTInfo, shardID uint64, sh Shard) error {
		if err := sh.Close(); err != nil {
			return err
		}
		dbPTInfo.mu.Lock()
		delete(dbPTInfo.shards, shardID)
		delete(dbPTInfo.newestRpShard, rp)
		dbPTInfo.mu.Unlock()
		return nil
	}); err != nil {
		atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
		return err
	}

	return deleteDirFunc()
}

func (e *Engine) DropMeasurement(db string, rp string, name string, shardIds []uint64) error {
	e.log.Info("start delete measurement...", zap.String("db", db), zap.String("name", name))
	start := time.Now()
	atomic.AddInt64(&stat.EngineStat.DropMstCount, 1)
	defer func(tm time.Time) {
		d := time.Since(tm)
		atomic.AddInt64(&stat.EngineStat.DropMstDurations, d.Nanoseconds())
		stat.UpdateEngineStatS()
		e.log.Info("delete measurement done", zap.String("db", db), zap.String("name", name),
			zap.Duration("time used", d))
	}(start)

	mstName := db + "." + rp + "." + name
	if err := e.startDrop(mstName, e.droppingMst); err != nil {
		return err
	}
	defer e.endDrop(mstName, e.droppingMst)

	e.mu.RLock()
	pts, ok := e.DBPartitions[db]
	if !ok || len(pts) == 0 {
		e.mu.RUnlock()
		return nil
	}

	ptIds, err := e.refDBPTsNoLock(pts, db)
	if err != nil {
		atomic.AddInt64(&stat.EngineStat.DropRPErrs, 1)
		e.mu.RUnlock()
		return err
	}
	e.mu.RUnlock()
	defer e.unrefDBPTs(db, ptIds)

	for ptID, pt := range pts {
		pt.mu.RLock()
		// drop measurement from shard
		for _, id := range shardIds {
			sh, ok := pt.shards[id]
			if !ok {
				continue
			}
			if err := sh.DropMeasurement(context.TODO(), name); err != nil {
				e.log.Error("drop measurement fail", zap.Uint32("ptid", ptID),
					zap.Uint64("shard", id), zap.Error(err))
				pt.mu.RUnlock()
				atomic.AddInt64(&stat.EngineStat.DropMstErrs, 1)
				return err
			}
		}
		pt.mu.RUnlock()
	}

	return nil
}

func (e *Engine) DropSeries(database string, sources []influxql.Source, ptId []uint32, condition influxql.Expr) (int, error) {
	panic("implement me")
}

func (e *Engine) TagKeys(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr, tr influxql.TimeRange) ([]string, error) {
	keysMap, err := e.searchIndex(db, ptIDs, measurements, condition, tr, e.handleTagKeys)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(keysMap))
	for k, v := range keysMap {
		var builder sysStrings.Builder
		builder.WriteString(k)
		hastag := false
		for tag := range v {
			builder.WriteString(",")
			builder.WriteString(tag)
			hastag = true
		}
		if !hastag {
			continue
		}
		result = append(result, builder.String())
	}
	return result, nil
}

func (e *Engine) SeriesKeys(db string, ptIDs []uint32, measurements [][]byte, condition influxql.Expr, tr influxql.TimeRange) ([]string, error) {
	keysMap, err := e.searchIndex(db, ptIDs, measurements, condition, tr, e.handleSeries)
	if err != nil {
		return nil, err
	}

	seriesNum := 0
	for _, v := range keysMap {
		seriesNum += len(v)
	}

	result := make([]string, 0, seriesNum)
	for _, v := range keysMap {
		for series := range v {
			result = append(result, series)
		}
	}
	sort.Strings(result)

	return result, nil
}

func (e *Engine) SeriesCardinality(db string, ptIDs []uint32, namesWithVer [][]byte, condition influxql.Expr, tr influxql.TimeRange) ([]meta2.MeasurementCardinalityInfo, error) {
	e.mu.RLock()
	var err error
	if ptIDs, err = e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	var measurementCardinalityInfos []meta2.MeasurementCardinalityInfo
	for i := range ptIDs {
		pt, ok := pts[ptIDs[i]]
		if !ok {
			continue
		}
		pt.mu.RLock()
		if condition != nil {
			measurementCardinalityInfos, err = pt.seriesCardinalityWithCondition(namesWithVer, condition, measurementCardinalityInfos, tr)
		} else {
			measurementCardinalityInfos, err = pt.seriesCardinality(namesWithVer, measurementCardinalityInfos, tr)
		}

		if err != nil {
			pt.mu.RUnlock()
			return nil, err
		}
		pt.mu.RUnlock()
	}
	return measurementCardinalityInfos, nil
}

func (e *Engine) TagValuesCardinality(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr, tr influxql.TimeRange) (map[string]uint64, error) {
	e.mu.RLock()
	var err error
	if ptIDs, err = e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}
	tvMap := make(map[string]map[string]struct{}, len(tagKeys))
	for name := range tagKeys {
		tvMap[name] = make(map[string]struct{}, 64)
	}
	for _, ptID := range ptIDs {
		pt, ok := pts[ptID]
		if !ok {
			continue
		}
		pt.mu.RLock()
		for _, iBuild := range pt.indexBuilder {
			if !iBuild.Overlaps(tr) {
				continue
			}
			for name, tks := range tagKeys {
				idx := iBuild.GetPrimaryIndex().(*tsi.MergeSetIndex)
				values, err := idx.SearchTagValues([]byte(name), tks, condition)
				if err != nil {
					pt.mu.RUnlock()
					return nil, err
				}
				if values == nil {
					// Measurement name not found
					continue
				}
				for _, vs := range values {
					for _, v := range vs {
						tvMap[name][v] = struct{}{}
					}
				}
			}
		}
		pt.mu.RUnlock()
	}

	result := make(map[string]uint64, len(tagKeys))
	for nameWithVer := range tagKeys {
		name := influx.GetOriginMstName(nameWithVer)
		result[name] = uint64(len(tvMap[nameWithVer]))
	}
	return result, nil
}

func (e *Engine) TagValues(db string, ptIDs []uint32, tagKeys map[string][][]byte, condition influxql.Expr, tr influxql.TimeRange) (netstorage.TablesTagSets, error) {
	e.mu.RLock()
	var err error
	if ptIDs, err = e.checkAndAddRefPTSNoLock(db, ptIDs); err != nil {
		e.mu.RUnlock()
		return nil, err
	}
	defer e.unrefDBPTs(db, ptIDs)
	pts, ok := e.DBPartitions[db]
	e.mu.RUnlock()
	if !ok {
		return nil, nil
	}

	tagValuess := make(netstorage.TablesTagSets, 0)
	results := make(map[string][][]string, len(tagKeys))
	for _, ptID := range ptIDs {
		pt, ok := pts[ptID]
		if !ok {
			continue
		}
		pt.mu.RLock()
		for _, iBuild := range pt.indexBuilder {
			if !iBuild.Overlaps(tr) {
				continue
			}
			for name, tks := range tagKeys {
				idx := iBuild.GetPrimaryIndex().(*tsi.MergeSetIndex)
				values, err := idx.SearchTagValues([]byte(name), tks, condition)
				if err != nil {
					pt.mu.RUnlock()
					return nil, err
				}
				if values == nil {
					// Measurement name not found
					continue
				}

				appendValuesToMap(results, name, values)
			}
		}
		pt.mu.RUnlock()
	}

	// transform to tagvalues
	for name, tvs := range results {
		tv := netstorage.TableTagSets{
			Name:   influx.GetOriginMstName(name),
			Values: make(netstorage.TagSets, 0, len(tvs[0])),
		}
		for i, tk := range tagKeys[name] {
			for _, v := range tvs[i] {
				tv.Values = append(tv.Values, netstorage.TagSet{Key: util.Bytes2str(tk), Value: v})
			}
		}
		tagValuess = append(tagValuess, tv)
	}

	return tagValuess, nil
}

func appendValuesToMap(results map[string][][]string, name string, values [][]string) {
	if _, ok := results[name]; !ok {
		results[name] = make([][]string, len(values))
	}
	for i := 0; i < len(values); i++ {
		results[name][i] = strings.UnionSlice(append(results[name][i], values[i]...))
	}
}

func (e *Engine) StatisticsOps() []opsStat.OpsStatistic {
	databases := e.Databases()
	statistics := make([]opsStat.OpsStatistic, 0, len(databases))
	for _, database := range databases {
		msts := e.getAllMst(database)
		if len(msts) == 0 {
			continue
		}
		ptIDs := e.getDBPtIds(database)
		mstCardinality, err := e.SeriesCardinality(database, ptIDs, msts, nil, influxql.TimeRange{
			Min: time.Unix(0, influxql.MinTime).UTC(),
			Max: time.Unix(0, influxql.MaxTime).UTC(),
		})
		if err != nil {
			return nil
		}

		var ret meta2.CardinalityInfos
		for i := range mstCardinality {
			ret = append(ret, mstCardinality[i].CardinalityInfos...)
		}

		var totalCount uint64
		for i := range ret {
			totalCount += ret[i].Cardinality
		}
		valueMap := map[string]interface{}{
			"numSeries": int64(totalCount),
		}

		statistics = append(statistics, opsStat.OpsStatistic{
			Name:   stat.DatabaseStatisticsName,
			Tags:   stat.StatisticTags{"database": database}.Merge(stat.DatabaseTagMap),
			Values: valueMap,
		})
	}

	return statistics
}

func (e *Engine) Databases() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	databases := make([]string, 0, len(e.DBPartitions))
	for dbName := range e.DBPartitions {
		databases = append(databases, dbName)
	}

	return databases
}

func (e *Engine) getDBPtIds(dbName string) []uint32 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ids := make([]uint32, 0, len(e.DBPartitions[dbName]))
	for ptId := range e.DBPartitions[dbName] {
		ids = append(ids, ptId)
	}
	return ids
}

func (e *Engine) getAllMst(dbName string) [][]byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.metaClient == nil {
		return nil
	}
	msts := e.metaClient.GetAllMst(dbName)
	mstNames := make([][]byte, len(msts))
	for i := range msts {
		mstNames[i] = []byte(msts[i])
	}
	return mstNames
}
