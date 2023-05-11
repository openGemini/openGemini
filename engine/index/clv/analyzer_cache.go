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

package clv

import (
	"bytes"
	"fmt"
	"path"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/openGemini/openGemini/open_src/github.com/VictoriaMetrics/VictoriaMetrics/lib/mergeset"
)

// dictonary version
const (
	Unknown uint32 = 0
	Default uint32 = 1
)

var mergeSetLockFile = "dicitonary.lock"
var mergeSetLock sync.RWMutex

type analyzerInfo struct {
	analyzers []*Analyzer
	collector *collector
}

func newAnalyzerInfo(path, measurement, field string) *analyzerInfo {
	ai := &analyzerInfo{
		analyzers: make([]*Analyzer, 0),
		collector: newCollector(path, measurement, field),
	}
	return ai
}

func getLockFilePath(path string) *string {
	lockPath := path + "/" + mergeSetLockFile
	return &lockPath
}

type analyzerCache struct {
	analyzers map[string]*analyzerInfo // 'dicPath/mstName/fieldName' is the key
	lock      sync.RWMutex
}

func (c *analyzerCache) init() {
	c.analyzers = make(map[string]*analyzerInfo)
}

func (c *analyzerCache) getCollector(dicPath, name, field string) *collector {
	c.lock.RLock()
	defer c.lock.RUnlock()

	key := path.Join(dicPath, name, field)

	analyzerInfo := c.analyzers[key]
	if _, ok := c.analyzers[key]; !ok {
		analyzerInfo = newAnalyzerInfo(dicPath, name, field)
		c.analyzers[key] = analyzerInfo
	}

	return analyzerInfo.collector
}

func (c *analyzerCache) getAnalyzer(dicPath, name, field string, version uint32) *Analyzer {
	c.lock.RLock()
	defer c.lock.RUnlock()

	key := path.Join(dicPath, name, field)

	analyzerInfo, ok := c.analyzers[key]
	if !ok {
		return nil
	}

	for _, a := range analyzerInfo.analyzers {
		if a.Version() == version {
			return a
		}
	}

	return nil
}

func (c *analyzerCache) saveAnalyzer(a *Analyzer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if a == nil {
		return
	}

	dicPath, name, field := a.path, a.measurement, a.field
	key := path.Join(dicPath, name, field)

	analyzerInfo, ok := c.analyzers[key]
	if !ok {
		analyzerInfo = newAnalyzerInfo(dicPath, name, field)
		c.analyzers[key] = analyzerInfo
	}

	for _, a := range analyzerInfo.analyzers {
		if a.Version() == a.version {
			return
		}
	}

	analyzerInfo.analyzers = append(analyzerInfo.analyzers, a)
}

var cache analyzerCache

func init() {
	cache.init()
}

// get lastest version of dictonary from mergeset
func getLatestVersion(dicPath, name, field string) (uint32, error) {
	mergeSetLock.Lock()
	defer mergeSetLock.Unlock()
	tb, err := mergeset.OpenTable(path.Join(dicPath, name, field), nil, nil, getLockFilePath(dicPath))
	if err != nil {
		return 0, err
	}
	defer tb.MustClose()

	ts := &mergeset.TableSearch{}
	ts.Init(tb)
	defer ts.MustClose()

	latestVersion := Default
	b := make([]byte, 0)
	b = append(b, txPrefixDicVersion)
	ts.Seek(b)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, b) {
			break
		}
		if latestVersion < unmarshaDiclVersion(item) {
			latestVersion = unmarshaDiclVersion(item)
		}
	}

	return latestVersion, nil
}

func getNextValidVersion(dicPath, name, field string) (uint32, error) {
	latestVersion, err := getLatestVersion(dicPath, name, field)
	if err != nil {
		return 0, err
	}

	mergeSetLock.Lock()
	defer mergeSetLock.Unlock()
	tb, err := mergeset.OpenTable(path.Join(dicPath, name, field), nil, nil, getLockFilePath(dicPath))
	if err != nil {
		return 0, err
	}
	defer tb.MustClose()

	ts := &mergeset.TableSearch{}
	ts.Init(tb)
	defer ts.MustClose()

	nextVersion := latestVersion + 1
	for {
		b := genPrefixForDic(nextVersion)
		ts.Seek(b)
		if ts.NextItem() {
			if !bytes.HasPrefix(ts.Item, b) {
				break
			}
			nextVersion++
		} else {
			break
		}
	}
	return nextVersion, nil
}

func genPrefixForDic(version uint32) []byte {
	prefix := make([]byte, 0, 8)
	prefix = append(prefix, txPrefixDic)
	prefix = encoding.MarshalUint32(prefix, version)
	prefix = append(prefix, txSuffix)
	return prefix
}

func saveAnalyzerToMergeSet(dicPath, name, field string, items [][]byte) error {
	mergeSetLock.Lock()
	defer mergeSetLock.Unlock()
	tb, err := mergeset.OpenTable(path.Join(dicPath, name, field), nil, nil, getLockFilePath(dicPath))
	if err != nil {
		return err
	}
	defer tb.MustClose()
	return tb.AddItems(items)
}

func loadAnalyzer(dicPath, name, field string, version uint32) (*Analyzer, error) {
	mergeSetLock.Lock()
	defer mergeSetLock.Unlock()
	tb, err := mergeset.OpenTable(path.Join(dicPath, name, field), nil, nil, getLockFilePath(dicPath))
	if err != nil {
		return nil, err
	}
	defer tb.MustClose()

	ts := &mergeset.TableSearch{}
	ts.Init(tb)
	defer ts.MustClose()

	a := newAnalyzer(dicPath, name, field, version)

	prefix := genPrefixForDic(version)
	ts.Seek(prefix)
	for ts.NextItem() {
		item := ts.Item
		if !bytes.HasPrefix(item, prefix) {
			break
		}
		// insert tree
		a.InsertToDictionary(string(item[len(prefix):]))
	}

	err = a.AssignId()
	if err != nil {
		return nil, err
	}

	return a, nil
}

// find from the cache, if not existed, load from mergeset table.
func getAnalyzer(path, name, field string, version uint32) (*Analyzer, error) {
	if version < Default {
		return nil, fmt.Errorf("incorrect dictionary version: %d", version)
	}

	a := cache.getAnalyzer(path, name, field, version)
	if a != nil {
		return a, nil
	}

	if version == Default {
		a = newAnalyzer("", name, field, Default)
	} else {
		var err error
		a, err = loadAnalyzer(path, name, field, version)
		if err != nil {
			return nil, err
		}
	}
	cache.saveAnalyzer(a)

	return a, nil
}

/*
 * return a analyzer by version.
 * The analyzer's location: path/name/field/MergeSetTable
 */
func GetAnalyzer(path, name, field string, version uint32) (*Analyzer, error) {
	needCollect := false
	// if the verison is Unknown, need to get the latest version of dictionary from cache or MergeSet.
	if version == Unknown {
		latestVersion, err := getLatestVersion(path, name, field)
		if err != nil {
			return nil, err
		}
		if latestVersion == Default {
			needCollect = true
		}
		version = latestVersion
	}

	a, err := getAnalyzer(path, name, field, version)
	if err != nil {
		return nil, err
	}

	collector := cache.getCollector(path, name, field)
	if needCollect && collector.IsStopped() {
		collector.StartCollect()
	}

	a.RegisterCollector(collector)

	return a, nil
}
