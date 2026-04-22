// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package tsi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/obs"
	"github.com/openGemini/openGemini/lib/record"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/mergeset"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/pingcap/failpoint"
	"github.com/savsgio/dictpool"
	"go.uber.org/zap"
)

// IndexBuilder is a collection of all indexes
type IndexBuilder struct {
	opId uint64 // assign task id

	path          string           // eg, data/db/pt/rp/index/indexid
	Relations     []*IndexRelation // <oid, indexRelation>
	ident         *meta.IndexIdentifier
	startTime     time.Time
	endTime       time.Time
	duration      time.Duration
	cacheDuration time.Duration

	mu             sync.RWMutex
	logicalClock   uint64
	sequenceID     *uint64
	lock           *string
	EnableTagArray bool

	// wait group for running index move
	moveWG   *sync.WaitGroup
	moveStop chan struct{}
	ObsOpt   *obs.ObsOptions

	MoveIndexStartTime time.Time
	Tier               uint64
	IndexColdDuration  time.Duration

	seriesLimiter func() error
	mergeDuration time.Duration
}

const (
	Mergeset = "mergeset"
	Tmp      = "tmp"
	Txn      = "txn"
)

func NewIndexBuilder(opt *Options) *IndexBuilder {
	iBuilder := &IndexBuilder{
		opId:          opt.opId,
		path:          opt.path,
		ident:         opt.ident,
		duration:      opt.duration,
		cacheDuration: opt.ident.Index.TimeRange.EndTime.Sub(opt.ident.Index.TimeRange.StartTime),
		startTime:     opt.startTime,
		endTime:       opt.endTime,
		logicalClock:  opt.logicalClock,
		sequenceID:    opt.sequenceID,
		lock:          opt.lock,
		Relations:     make([]*IndexRelation, index.IndexTypeAll),
		mergeDuration: opt.mergeDuration,
		ObsOpt:        opt.obsOpt,
	}
	return iBuilder
}

type indexRow struct {
	Row *influx.Row
	Wg  *sync.WaitGroup
	Err error
}

type indexRows []indexRow

func (rows *indexRows) reset() {
	for i := range *rows {
		(*rows)[i].Wg = nil
		(*rows)[i].Row = nil
		(*rows)[i].Err = nil
	}
}

var indexRowsPool sync.Pool

func getIndexRows() *indexRows {
	rows := indexRowsPool.Get()
	if rows == nil {
		return &indexRows{}
	}
	return rows.(*indexRows)
}

func putIndexRows(rows *indexRows) {
	rows.reset()
	*rows = (*rows)[:0]
	indexRowsPool.Put(rows)
}

func (iBuilder *IndexBuilder) SetSeriesLimiter(limiter func() error) {
	iBuilder.seriesLimiter = limiter
}

func (iBuilder *IndexBuilder) SeriesLimited() error {
	if iBuilder.seriesLimiter == nil {
		return nil
	}

	return iBuilder.seriesLimiter()
}

func (iBuilder *IndexBuilder) GenerateUUID() uint64 {
	b := kbPool.Get()
	// first three bytes is big endian of logicClock
	b.B = append(b.B, byte(iBuilder.logicalClock>>16))
	b.B = append(b.B, byte(iBuilder.logicalClock>>8))
	b.B = append(b.B, byte(iBuilder.logicalClock))

	// last five bytes is big endian of sequenceID
	id := atomic.AddUint64(iBuilder.sequenceID, 1)
	b.B = append(b.B, byte(id>>32))
	b.B = append(b.B, byte(id>>24))
	b.B = append(b.B, byte(id>>16))
	b.B = append(b.B, byte(id>>8))
	b.B = append(b.B, byte(id))

	pid := binary.BigEndian.Uint64(b.B)
	kbPool.Put(b)

	return pid
}

func (iBuilder *IndexBuilder) Flush() {
	if config.IsLogKeeper() {
		return
	}
	if iBuilder.Tier >= util.Cold {
		return
	}
	for i := range iBuilder.Relations {
		if iBuilder.isRelationInited(uint32(i)) {
			iBuilder.Relations[i].IndexFlush()
		}
	}
}

func (iBuilder *IndexBuilder) Open() error {
	start := time.Now()

	// Open all indexes
	for i := range iBuilder.Relations {
		if iBuilder.isRelationInited(uint32(i)) {
			if err := iBuilder.Relations[i].IndexOpen(); err != nil {
				logger.GetLogger().Error("Index open fail", zap.Error(err))
				return err
			}
		}
	}
	statistics.IndexTaskInit(iBuilder.GetIndexID(), iBuilder.opId, iBuilder.ident.OwnerDb, iBuilder.ident.OwnerPt, iBuilder.RPName())
	statistics.IndexStepDuration(iBuilder.GetIndexID(), iBuilder.opId, "OpenIndexDone", time.Since(start).Nanoseconds(), true)
	return nil
}

func (iBuilder *IndexBuilder) Path() string {
	return iBuilder.path
}

func (iBuilder *IndexBuilder) SetPath(path string) {
	iBuilder.path = path
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

func (iBuilder *IndexBuilder) SetMergeDuration(duration time.Duration) {
	iBuilder.mergeDuration = duration
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

func (iBuilder *IndexBuilder) Overlaps(tr influxql.TimeRange) bool {
	return !iBuilder.startTime.After(tr.Max) && iBuilder.endTime.After(tr.Min)
}

func (iBuilder *IndexBuilder) CreateIndexIfNotExists(mmRows *dictpool.Dict, needSecondaryIndex bool) error {
	if iBuilder.Tier >= util.Cold {
		return errno.NewError(errno.ForbidIndexWrite, iBuilder.GetIndexID(), iBuilder.Tier)
	}
	primaryIndex := iBuilder.GetPrimaryIndex()
	var wg sync.WaitGroup
	// 1st, create primary index.
	iRows := getIndexRows()
	for mmIdx := range mmRows.D {
		rows, ok := mmRows.D[mmIdx].Value.(*[]influx.Row)
		if !ok {
			putIndexRows(iRows)
			return errno.NewError(errno.CreateIndexFailPointRowType)
		}

		for rowIdx := range *rows {
			row := &(*rows)[rowIdx]
			if row.SeriesId != 0 {
				continue
			}
			if cap(*iRows) > len(*iRows) {
				*iRows = (*iRows)[:len(*iRows)+1]
			} else {
				*iRows = append(*iRows, indexRow{})
			}
			iRow := &(*iRows)[len(*iRows)-1]
			iRow.Row = row
			iRow.Wg = &wg

			wg.Add(1)
			idx := primaryIndex.(*MergeSetIndex)
			idx.WriteRow(iRow)
		}
	}
	// Wait all rows in the batch finished.
	wg.Wait()

	// Check Err.
	for _, row := range *iRows {
		if row.Err != nil {
			putIndexRows(iRows)
			return row.Err
		}
	}
	putIndexRows(iRows)

	// 2nd, create secondary index.
	if !needSecondaryIndex {
		return nil
	}

	for mmIdx := range mmRows.D {
		rows, _ := mmRows.D[mmIdx].Value.(*[]influx.Row)
		for rowIdx := range *rows {
			row := &(*rows)[rowIdx]
			if err := iBuilder.createSecondaryIndex(row, primaryIndex); err != nil {
				return err
			}
		}
	}

	return nil
}

func (iBuilder *IndexBuilder) CreateIndexIfNotExistsByCol(rec *record.Record, tagIndex []int, mst string) error {
	primaryIndex := iBuilder.GetPrimaryIndex()
	idx, ok := primaryIndex.(*MergeSetIndex)
	if !ok {
		return errors.New("get mergeSetIndex failed")
	}
	tagCols := GetIndexTagCols(rec.RowNums()) // preset one colVal.Len cap
	buf := mergeset.GetIndexBuffer()
	curVal := GetIndexKeyCache()

	defer func() {
		PutIndexTagCols(tagCols)
		mergeset.PutIndexBuffer(buf)
		PutIndexKeyCache(curVal)
	}()

	mstBinary := []byte(mst)
	tagsByteSlice := make([][]byte, len(tagIndex))
	for i := range tagsByteSlice {
		tagsByteSlice[i] = []byte(rec.Schema[tagIndex[i]].Name)
	}
	var wg sync.WaitGroup
	cache := make(map[string]struct{})
	var cacheKey string
	for i := range tagIndex {
		for index := 0; index < rec.RowNums(); index++ {
			*curVal, _ = rec.ColVals[tagIndex[i]].StringValue(index)
			buf.Write(tagsByteSlice[i])
			buf.Write(*curVal)
			cacheKey = buf.String()
			buf.Reset()
			if _, ok = cache[cacheKey]; !ok {
				cache[cacheKey] = struct{}{}
			} else {
				continue
			}

			if cap(*tagCols) > len(*tagCols) {
				*tagCols = (*tagCols)[:len(*tagCols)+1]
			} else {
				*tagCols = append(*tagCols, TagCol{})
			}
			tagCol := &(*tagCols)[len(*tagCols)-1]
			tagCol.Mst = mstBinary
			tagCol.Key = tagsByteSlice[i]
			tagCol.Val = *curVal
			tagCol.Wg = &wg
			wg.Add(1)
			idx.WriteTagCols(tagCol)
		}
	}
	wg.Wait()
	// Check Err.
	for _, tagCol := range *tagCols {
		if tagCol.Err != nil {
			return tagCol.Err
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) CreateSecondaryIndexIfNotExist(mmRows *dictpool.Dict) error {
	primaryIndex := iBuilder.GetPrimaryIndex()
	for mmIdx := range mmRows.D {
		rows, _ := mmRows.D[mmIdx].Value.(*[]influx.Row)
		for rowIdx := range *rows {
			row := &(*rows)[rowIdx]
			if err := iBuilder.createSecondaryIndex(row, primaryIndex); err != nil {
				return err
			}
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) createSecondaryIndex(row *influx.Row, primaryIndex PrimaryIndex) error {
	for _, indexOpt := range row.IndexOptions {
		relation := iBuilder.Relations[indexOpt.Oid]
		if relation == nil {
			opt := &Options{
				indexType: index.IndexType(indexOpt.Oid),
				path:      primaryIndex.Path(),
				lock:      iBuilder.lock,
			}
			if err := iBuilder.initRelation(indexOpt.Oid, opt, primaryIndex); err != nil {
				return err
			}

			relation = iBuilder.Relations[indexOpt.Oid]
		}
		if err := relation.IndexInsert([]byte(row.Name), row); err != nil {
			return err
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) isRelationInited(oid uint32) bool {
	return iBuilder.Relations[oid] != nil
}

func (iBuilder *IndexBuilder) initRelation(oid uint32, opt *Options, primaryIndex PrimaryIndex) error {
	if iBuilder.isRelationInited(oid) {
		return nil
	}

	iBuilder.mu.Lock()
	defer iBuilder.mu.Unlock()
	if iBuilder.isRelationInited(oid) {
		return nil
	}

	var err error
	relation, err := NewIndexRelation(opt, primaryIndex, iBuilder)
	if err != nil {
		return err
	}
	if err = relation.IndexOpen(); err != nil {
		return err
	}

	iBuilder.Relations[oid] = relation
	return nil
}

func (iBuilder *IndexBuilder) Scan(span *tracing.Span, name []byte, opt *query.ProcessorOptions, callBack func(num int64) error) (interface{}, int64, error) {
	// 1st, use primary index to scan.
	relation := iBuilder.Relations[uint32(index.MergeSet)]
	if relation == nil {
		return nil, 0, fmt.Errorf("not exist index for %s", name)
	}
	groups, seriesNum, err := relation.IndexScan(span, name, opt, nil, callBack)
	if err != nil {
		return nil, seriesNum, err
	}

	// 2nd, use secondary index to scan.
	for i := range iBuilder.Relations {
		indexSeriesNum := int64(0)
		if !iBuilder.isRelationInited(uint32(i)) {
			continue
		}

		if iBuilder.Relations[i].oid == uint32(index.MergeSet) {
			continue
		}
		groups, indexSeriesNum, err = iBuilder.Relations[i].IndexScan(span, name, opt, groups, callBack)
		if err != nil {
			return nil, seriesNum, err
		}
		seriesNum += indexSeriesNum
	}

	return groups, seriesNum, nil
}

func (iBuilder *IndexBuilder) Delete(name []byte, condition influxql.Expr, tr TimeRange) error {
	var err error
	var idx int
	for i := range iBuilder.Relations {
		if !iBuilder.isRelationInited(uint32(i)) {
			continue
		}

		if iBuilder.Relations[i].oid == uint32(index.MergeSet) {
			idx = i
			continue
		}
		err = iBuilder.Relations[i].IndexDelete(name, condition, tr)
		if err != nil {
			return err
		}
	}

	//delete primary index last
	return iBuilder.Relations[idx].IndexDelete(name, condition, tr)
}

func (iBuilder *IndexBuilder) Close() error {
	for i := range iBuilder.Relations {
		if !iBuilder.isRelationInited(uint32(i)) {
			continue
		}

		if err := iBuilder.Relations[i].IndexClose(); err != nil {
			return err
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) ExpiredCache() bool {
	// duration == 0 means INF.
	now := time.Now().UTC()
	if iBuilder.duration != 0 && iBuilder.endTime.Add(iBuilder.cacheDuration).Before(now) {
		return true
	}

	return false
}

func (iBuilder *IndexBuilder) ClearCache() error {
	for i := range iBuilder.Relations {
		if !iBuilder.isRelationInited(uint32(i)) {
			continue
		}

		if err := iBuilder.Relations[i].IndexCacheClear(); err != nil {
			return err
		}
	}
	return nil
}

func (iBuilder *IndexBuilder) ExecIndexMove() error {
	logger.GetLogger().Info("[hierarchical storage ExecIndexMove] index info",
		zap.Uint64("indexID", iBuilder.GetIndexID()), zap.Uint64("Tier", iBuilder.Tier))
	iBuilder.mu.Lock()
	iBuilder.Tier = util.Moving

	if iBuilder.MoveIndexStartTime.IsZero() {
		iBuilder.MoveIndexStartTime = time.Now()
	}

	if iBuilder.moveStop == nil {
		iBuilder.moveStop = make(chan struct{})
	}
	wg := new(sync.WaitGroup)
	wg.Add(1)
	iBuilder.moveWG = wg
	iBuilder.mu.Unlock()

	defer wg.Done()
	return iBuilder.doIndexMove()
}

func (iBuilder *IndexBuilder) getAllIndexFilesPath() ([]string, error) {
	var filePaths []string
	for _, relation := range iBuilder.Relations {
		if relation == nil {
			continue
		}
		indexAmRoutine := relation.indexAmRoutine
		if indexAmRoutine.amKeyType == index.MergeSet {
			mergeSetPath := filepath.Join(iBuilder.path, Mergeset)
			mergeSetDirs, err := fileops.ReadDir(mergeSetPath)
			if err != nil {
				logger.GetLogger().Error("read mergeSet directory failed", zap.Error(err))
				continue
			}
			for _, dir := range mergeSetDirs {
				if dir.IsDir() {
					if dir.Name() == Tmp || dir.Name() == Txn {
						filePaths = append(filePaths, filepath.Join(mergeSetPath, dir.Name()))
						continue
					}
					mergeSetInnerDir := filepath.Join(mergeSetPath, dir.Name())
					innerDirs, err := fileops.ReadDir(mergeSetInnerDir)
					if err != nil {
						logger.GetLogger().Error("read mergeSet inner directory failed", zap.Error(err))
						return nil, err
					}
					for j := range innerDirs {
						filePaths = append(filePaths, filepath.Join(mergeSetInnerDir, innerDirs[j].Name()))
					}
				} else {
					filePaths = append(filePaths, filepath.Join(mergeSetPath, dir.Name()))
				}
			}
		}
	}
	return filePaths, nil
}

func (iBuilder *IndexBuilder) getAllIndexFilesColdPath(indexFilesPath []string) []string {
	var coldTmpFilesPath []string
	for i := range indexFilesPath {
		coldTmpFilesPath = append(coldTmpFilesPath, fileops.GetOBSTmpIndexFileName(indexFilesPath[i], iBuilder.ObsOpt))
	}
	return coldTmpFilesPath
}

func (iBuilder *IndexBuilder) doIndexMove() error {
	logger.GetLogger().Info("[hierarchical index storage] begin to move index", zap.Uint64("tier", iBuilder.Tier), zap.String("path", iBuilder.path))
	if iBuilder.stopMove() {
		logger.GetLogger().Info("[hierarchical index storage] received stop signal", zap.Uint64("index id", iBuilder.GetIndexID()))
		return errno.NewError(errno.IndexIsMoving, iBuilder.GetIndexID())
	}
	iBuilder.stopIndexMergeAndFlush()

	indexFiles, err := iBuilder.getAllIndexFilesPath()
	if err != nil {
		return err
	}
	coldIndexFilesPath := iBuilder.getAllIndexFilesColdPath(indexFiles)
	if len(indexFiles) == 0 {
		logger.GetLogger().Error("There is no index files to move")
		return errors.New("no index files to move")
	}
	err = iBuilder.startFilesMove(indexFiles, coldIndexFilesPath)
	if err != nil {
		return err
	}

	// all file copy success, update tier status
	iBuilder.Tier = util.Cold
	return nil
}

func (iBuilder *IndexBuilder) stopIndexMergeAndFlush() {
	for _, relation := range iBuilder.Relations {
		if relation == nil {
			continue
		}
		if relation.indexAmRoutine.amKeyType == index.MergeSet {
			relation.indexAmRoutine.index.(*MergeSetIndex).tb.StopMergeAndFlusher()
		}
	}
}

func (iBuilder *IndexBuilder) stopMove() bool {
	select {
	case <-iBuilder.moveStop:
		return true
	default:
		return false
	}
}

func (iBuilder *IndexBuilder) startFilesMove(localIndexFiles []string, coldTmpIndexFilesPath []string) error {
	var err error
	lock := fileops.FileLockOption(*iBuilder.lock)
	for i := range localIndexFiles {
		if iBuilder.stopMove() {
			return errno.NewError(errno.ShardMovingStopped)
		}

		logger.GetLogger().Info("[hierarchical index storage] start move", zap.Uint64("index id", iBuilder.GetIndexID()), zap.String("index file", localIndexFiles[i]))
		if err = fileops.CopyFileFromDFVToOBS(localIndexFiles[i], coldTmpIndexFilesPath[i], lock); err != nil {
			if !strings.HasSuffix(localIndexFiles[i], "/tmp") && !strings.HasSuffix(localIndexFiles[i], "/txn") {
				iBuilder.copyFileRollBack(coldTmpIndexFilesPath[i])
				logger.GetLogger().Error("copy index file to obs error", zap.Error(err))
				return err
			}
		}
		failpoint.Inject("copy-file-delay", nil)
	}
	indexPath := iBuilder.path
	iBuilder.renameIBuilderAndRelationPath()
	if err = fileops.RemoveAll(indexPath, lock); err != nil {
		logger.GetLogger().Error("remove local file index files error", zap.Error(err))
		return err
	}
	logger.GetLogger().Info("[hierarchical index storage] remove index file success")
	return nil
}
func (iBuilder *IndexBuilder) renameIBuilderAndRelationPath() {
	iBuilder.mu.Lock()
	defer iBuilder.mu.Unlock()
	if iBuilder.ObsOpt == nil {
		return
	}
	newPath := iBuilder.path[len(obs.GetPrefixDataPath()):]
	newPath = filepath.Join(iBuilder.ObsOpt.BasePath, newPath)

	newPath = fmt.Sprintf("%s%s/%s/%s/%s/%s", fileops.ObsPrefix, iBuilder.ObsOpt.Endpoint, iBuilder.ObsOpt.Ak, iBuilder.ObsOpt.Sk, iBuilder.ObsOpt.BucketName, newPath)
	iBuilder.path = newPath

	for _, relation := range iBuilder.Relations {
		if relation == nil {
			continue
		}
		if relation.indexAmRoutine.amKeyType == index.MergeSet {
			relation.indexAmRoutine.index.(*MergeSetIndex).path = newPath
			tbPath := fmt.Sprintf("%s/%s", newPath, index.MergeSetIndex)
			relation.indexAmRoutine.index.(*MergeSetIndex).tb.ResetPath(tbPath)
			prefixPath := fmt.Sprintf("%s%s/%s/%s/%s/%s", fileops.ObsPrefix, iBuilder.ObsOpt.Endpoint, iBuilder.ObsOpt.Ak, iBuilder.ObsOpt.Sk, iBuilder.ObsOpt.BucketName, iBuilder.ObsOpt.BasePath)
			relation.indexAmRoutine.index.(*MergeSetIndex).tb.UpdatePartsObsPath(prefixPath)
		}
	}
}

func (iBuilder *IndexBuilder) copyFileRollBack(fileName string) {
	lock := fileops.FileLockOption(*iBuilder.lock)
	if err := fileops.Remove(fileName, lock); err != nil {
		logger.GetLogger().Error("[run hierarchical storage RollBack]: remove file err",
			zap.String("file", fileName), zap.Error(err))
	}
}

func (iBuilder *IndexBuilder) GetTier() (tier uint64) {
	return iBuilder.Tier
}

func (iBuilder *IndexBuilder) IsTierExpired() bool {
	now := time.Now().UTC()
	if iBuilder.IndexColdDuration != 0 && iBuilder.endTime.Add(iBuilder.IndexColdDuration).Before(now) {
		return true
	}
	return false
}

/*
updateIndexPath: update local path to other node's obs remote path
completePath: fmt.Sprintf("%s%s/%s/%s/%s/%s", fileops.ObsPrefix, iBuilder.obsOpt.Endpoint, iBuilder.obsOpt.Ak, iBuilder.obsOpt.Sk, iBuilder.obsOpt.BucketName, iBuilder.obsOpt.BasePath, indexPath)
*/
func (iBuilder *IndexBuilder) updateIndexPath(completePath string) {
	iBuilder.mu.Lock()
	defer iBuilder.mu.Unlock()

	iBuilder.path = completePath

	for _, relation := range iBuilder.Relations {
		if relation == nil {
			continue
		}
		if relation.indexAmRoutine.amKeyType == index.MergeSet {
			relation.indexAmRoutine.index.(*MergeSetIndex).path = completePath
			tbPath := fmt.Sprintf("%s/%s", completePath, index.MergeSetIndex)
			relation.indexAmRoutine.index.(*MergeSetIndex).tb.ResetPath(tbPath)
		}
	}
}

func (iBuilder *IndexBuilder) ReplaceByNoClearIndexId(noClearIndex uint64) (string, *string, error) {
	oldTier := iBuilder.Tier
	iBuilder.Tier = util.Clearing
	re := regexp.MustCompile(`(\d+)_\d+_\d+`)
	matches := re.FindStringSubmatch(iBuilder.path)
	if len(matches) < 2 {
		iBuilder.Tier = oldTier
		return "", iBuilder.lock, errors.New("find string submatch error")
	}
	oldPath := iBuilder.path

	oldPart := matches[0]
	newPart := strings.Replace(oldPart, matches[1], strconv.FormatUint(noClearIndex, 10), 1)
	path := strings.Replace(iBuilder.path, oldPart, newPart, 1)
	iBuilder.updateIndexPath(path)
	iBuilder.Tier = util.Cleared
	return oldPath, iBuilder.lock, nil
}

func (iBuilder *IndexBuilder) DropSeries() error {
	iBuilder.mu.Lock()
	defer iBuilder.mu.Unlock()
	e := errors.New("idx is nil or not be *MergeSetIndex")
	if idx, ok := iBuilder.GetPrimaryIndex().(*MergeSetIndex); ok {
		deleteMergeSet := idx.DeleteMergeSet()
		if deleteMergeSet == nil {
			logger.GetLogger().Info("new db and didn't execute drop, no need to delete")
			return nil
		}
		deleteMergeSet.tb.SetLabelForDeletePart()

		delTsids := idx.GetDeletedTSIDs()
		if delTsids == nil || delTsids.Len() <= 0 {
			return nil
		}

		if e = idx.tb.RemoveItemsByDelTsidsFromParts(delTsids); e == nil {
			deleteMergeSet.tb.RemoveDeletedPart()
		}
	}
	return e
}

func (iBuilder *IndexBuilder) DeleteMsts(msts []string, onlyUseDiskThreshold uint64) error {
	var errs []error
	for _, relation := range iBuilder.Relations {
		if relation == nil {
			continue
		}
		indexAmRoutine := relation.indexAmRoutine
		if indexAmRoutine.amKeyType == index.MergeSet {
			mergeSetPath := filepath.Join(iBuilder.path, Mergeset)
			mergeSetDirs, err := fileops.ReadDir(mergeSetPath)
			if err != nil {
				logger.GetLogger().Error("read mergeSet directory failed", zap.Error(err))
				return err
			}
			for _, dir := range mergeSetDirs {
				if dir.IsDir() {
					if dir.Name() == Tmp || dir.Name() == Txn {
						continue
					}
					iBuilder.mu.Lock()
					if err = indexAmRoutine.index.(*MergeSetIndex).tb.DeleteMstsInIndex(dir.Name(), msts, onlyUseDiskThreshold, ParseItem); err != nil {
						errs = append(errs, err)
					}
					iBuilder.mu.Unlock()
				}
			}
		}
	}
	if len(errs) > 0 {
		err := errors.Join(errs...)
		return err
	}
	return nil
}
