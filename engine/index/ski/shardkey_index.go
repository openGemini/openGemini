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

package ski

import (
	"bytes"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/workingsetcache"
	"github.com/VictoriaMetrics/fastcache"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/lib/fileops"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
	"go.uber.org/zap"
)

const (
	nsPrefixShardKey = iota
	nsPrefixShardKeyToSid
)
const ShardKeyDirectory = "shard_key_index"
const ShardKeyCache = "shard_key_cache"

type ShardKeyIndex struct {
	tb       *mergeset.Table
	logger   *zap.Logger
	path     string
	lock     *string
	cache    *workingsetcache.Cache
	sidCount uint64
}

func NewShardKeyIndex(dataPath string, lockPath *string) (*ShardKeyIndex, error) {
	index := &ShardKeyIndex{path: dataPath, lock: lockPath}
	err := index.Open()
	if err != nil {
		return nil, err
	}
	mem := memory.Allowed()
	cachePath := path.Join(dataPath, ShardKeyCache)
	index.cache = workingsetcache.Load(cachePath, mem/32, time.Hour)
	var cs fastcache.Stats
	index.cache.UpdateStats(&cs)
	return index, nil
}

func mergeIndexRows(data []byte, items []mergeset.Item) ([]byte, []mergeset.Item) {
	srm := getShardKeyToTSIDsRowsMerger()
	defer putShardKeyToTSIDsRowsMerger(srm)
	data, items = mergeindex.MergeItems(data, items, nsPrefixShardKeyToSid, srm)
	return data, items
}

func (idx *ShardKeyIndex) Open() error {
	shardKeyPath := path.Join(idx.path, ShardKeyDirectory)
	if err := fileops.MkdirAll(shardKeyPath, 0750); err != nil {
		panic(err)
	}
	tb, err := mergeset.OpenTable(shardKeyPath, nil, mergeIndexRows, idx.lock)
	if err != nil {
		return fmt.Errorf("cannot open index:%s, err: %+v", idx.path, err)
	}
	idx.tb = tb
	if logger.GetLogger() == nil {
		idx.logger = zap.NewNop()
	} else {
		idx.logger = logger.GetLogger().With(zap.String("shard key index", "mergeset"))
	}
	atomic.StoreUint64(&idx.sidCount, 0)
	return idx.loadSeriesCount()
}

var kbPool bytesutil.ByteBufferPool

var idxItemsPool mergeindex.IndexItemsPool

func (idx *ShardKeyIndex) ForceFlush() {
	idx.tb.DebugFlush()
}

func (idx *ShardKeyIndex) CreateIndex(name []byte, shardKey []byte, sid uint64) error {
	ii := idxItemsPool.Get()
	defer idxItemsPool.Put(ii)

	exist, err := idx.hasShardKey(shardKey)
	if err != nil {
		return err
	}

	if !exist {
		ii.B = append(ii.B, nsPrefixShardKey)
		ii.B = append(ii.B, shardKey...)
		ii.Next()
	}

	ii.B = append(ii.B, nsPrefixShardKeyToSid)
	kb := kbPool.Get()
	kb.B = marshalShardKey(kb.B, name, shardKey)
	ii.B = append(ii.B, kb.B...)
	ii.B = encoding.MarshalUint64(ii.B, sid)
	kbPool.Put(kb)
	ii.Next()

	if err := idx.tb.AddItems(ii.Items); err != nil {
		return err
	}
	atomic.AddUint64(&idx.sidCount, 1)
	return nil
}

func (idx *ShardKeyIndex) hasShardKey(key []byte) (bool, error) {
	if idx.getShardKeyFromCache(key) {
		return true, nil
	}

	exist := false
	defer func() {
		if exist {
			idx.putShardKeyToCache(key)
		}
	}()

	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	var err error
	exist, err = is.hasItem(nsPrefixShardKey, key)
	return exist, err
}

func (idx *ShardKeyIndex) getShardKeyFromCache(key []byte) bool {
	if idx.cache == nil {
		return false
	}
	return idx.cache.Has(key)
}

func (idx *ShardKeyIndex) putShardKeyToCache(key []byte) {
	idx.cache.Set(key, nil)
}

var indexSearchPool sync.Pool

func (idx *ShardKeyIndex) getIndexSearch() *indexSearch {
	v := indexSearchPool.Get()
	if v == nil {
		v = &indexSearch{
			idx: idx,
		}
	}

	is := v.(*indexSearch)
	is.ts.Init(idx.tb)
	is.idx = idx

	return is
}

func (idx *ShardKeyIndex) putIndexSearch(is *indexSearch) {
	is.kb.Reset()
	is.ts.MustClose()
	is.mp.Reset()
	is.idx = nil
	indexSearchPool.Put(is)
}

func (idx *ShardKeyIndex) GetSplitPointsWithSeriesCount(poses []int64) ([]string, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	ts := &is.ts
	kb := &is.kb

	kb.B = append(kb.B[:0], nsPrefixShardKey)
	ts.Seek(kb.B)
	i := 0
	splitPoints := make([]string, 0, len(poses))
	var sidCount int64
	var err error
	valid := false
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			break
		}
		shardKey := ts.Item[1:]
		valid, sidCount, err = idx.validSplitKey(sidCount, poses[i], shardKey)
		if err != nil {
			return nil, err
		}
		if valid {
			splitPoints = append(splitPoints, string(shardKey))
			i++
		}
		if i >= len(poses) {
			break
		}
	}
	if i != len(poses) {
		return nil, fmt.Errorf("can not find enough split keys, find split key len %d, need %d", len(splitPoints), len(poses))
	}
	return splitPoints, nil
}

func (idx *ShardKeyIndex) validSplitKey(sidCount, pos int64, shardKey []byte) (bool, int64, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp

	kb.B = append(kb.B[:0], nsPrefixShardKeyToSid)
	kb.B = encoding.MarshalVarUint64(kb.B, uint64(len(shardKey)))
	kb.B = append(kb.B, shardKey...)
	ts.Seek(kb.B)
	var err error
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			break
		}
		mp.Reset()
		err = mp.Init(ts.Item, nsPrefixShardKeyToSid)
		if err != nil {
			return false, sidCount, err
		}
		sidCount += int64(mp.TSIDsLen())
		if sidCount >= pos {
			return true, sidCount, nil
		}
	}
	return false, sidCount, nil
}

func (idx *ShardKeyIndex) GetSplitPointsByRowCount(poses []int64, f func(name string, sid uint64) (int64, error)) ([]string, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)

	ts := &is.ts
	kb := &is.kb

	kb.B = append(kb.B[:0], nsPrefixShardKey)
	ts.Seek(kb.B)
	var rowCount int64
	var err error
	valid := false
	i := 0
	splitPoints := make([]string, 0, len(poses))
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			break
		}
		shardKey := ts.Item[1:]
		valid, rowCount, err = idx.isSplitKey(rowCount, poses[i], shardKey, f)
		if err != nil {
			return nil, err
		}

		if valid {
			splitPoints = append(splitPoints, string(shardKey))
			i++
		}
		if i >= len(poses) {
			break
		}
	}

	if i != len(poses) {
		return nil, fmt.Errorf("can not find enough split keys poses %v", poses)
	}
	return splitPoints, nil
}

func (idx *ShardKeyIndex) isSplitKey(rowCount, pos int64, shardKey []byte, f func(name string, sid uint64) (int64, error)) (bool, int64, error) {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp

	kb.B = append(kb.B[:0], nsPrefixShardKeyToSid)
	kb.B = encoding.MarshalVarUint64(kb.B, uint64(len(shardKey)))
	kb.B = append(kb.B, shardKey...)
	ts.Seek(kb.B)

	var err error
	var preSid uint64
	var count int64
	res := false
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			break
		}
		mp.Reset()
		err = mp.Init(ts.Item, nsPrefixShardKeyToSid)
		if err != nil {
			return false, rowCount, err
		}
		mp.ParseTSIDs()
		for _, tsid := range mp.TSIDs {
			if tsid == preSid {
				continue
			}

			count, err = f(util.Bytes2str(mp.Name), tsid)
			if err != nil {
				return false, rowCount, err
			}
			rowCount += count
			if rowCount >= pos {
				res = true
			}
			preSid = tsid
		}
	}
	return res, rowCount, nil
}

func (idx *ShardKeyIndex) Close() error {
	idx.tb.MustClose()
	return idx.cache.Save(path.Join(idx.path, ShardKeyCache))
}

func (idx *ShardKeyIndex) loadSeriesCount() error {
	is := idx.getIndexSearch()
	defer idx.putIndexSearch(is)
	ts := &is.ts
	kb := &is.kb
	mp := &is.mp

	kb.B = append(kb.B[:0], nsPrefixShardKeyToSid)
	ts.Seek(kb.B)
	var err error
	for ts.NextItem() {
		if !bytes.HasPrefix(ts.Item, kb.B) {
			break
		}
		mp.Reset()
		err = mp.Init(ts.Item, nsPrefixShardKeyToSid)
		if err != nil {
			return err
		}
		atomic.AddUint64(&idx.sidCount, uint64(mp.TSIDsLen()))
	}
	return nil
}

func (idx *ShardKeyIndex) GetShardSeriesCount() int {
	return int(atomic.LoadUint64(&idx.sidCount))
}
