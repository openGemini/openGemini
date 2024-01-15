/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
	"strings"
	"sync"

	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"go.uber.org/zap"
)

type ShowTagKeysExecutor struct {
	logger *logger.Logger
	mc     meta.MetaClient
	me     IMetaExecutor
	store  netstorage.Storage
}

func NewShowTagKeysExecutor(logger *logger.Logger, mc meta.MetaClient, me IMetaExecutor, store netstorage.Storage) *ShowTagKeysExecutor {
	return &ShowTagKeysExecutor{
		logger: logger,
		mc:     mc,
		me:     me,
		store:  store,
	}
}

func (e *ShowTagKeysExecutor) Execute(stmt *influxql.ShowTagKeysStatement) (netstorage.TableTagKeys, error) {
	mis, err := e.mc.MatchMeasurements(stmt.Database, stmt.Sources.Measurements())
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(mis))
	for _, m := range mis {
		names = append(names, m.Name)
	}

	lock := new(sync.Mutex)

	mapMstMap := make(map[string]map[string]struct{})
	err = e.me.EachDBNodes(stmt.Database, func(nodeID uint64, pts []uint32, hasErr *bool) error {
		if *hasErr {
			return nil
		}
		/*
			ShowTagKeys return
				{"mst,tag1,tag2,tag3","mst,tag2,tag3"}
			or
				{"mst,tag1,tag2,tag3","mst2,tag1,tag2","mst3,tag1,tag2"}
		*/
		arr, err := e.store.ShowTagKeys(nodeID, stmt.Database, pts, names, stmt.Condition)
		lock.Lock()
		defer lock.Unlock()
		if err != nil {
			return err
		}
		var measurement string
		for _, item := range arr {
			ks := strings.Split(item, ",")
			measurement = ks[0]
			_, ok := mapMstMap[measurement]
			if !ok {
				mapMstMap[measurement] = make(map[string]struct{})
			}
			mapTagKeys := mapMstMap[measurement]
			for _, tag := range ks[1:] {
				_, ok := mapTagKeys[tag]
				if !ok {
					mapTagKeys[tag] = struct{}{}
				}
			}
		}
		return err
	})
	if err != nil {
		e.logger.Error("failed to show tag keys", zap.Error(err))
		return nil, err
	}
	tagKeys := make(netstorage.TableTagKeys, 0, len(mapMstMap))
	for k, v := range mapMstMap {
		tk := &netstorage.TagKeys{Name: k}
		tk.Keys = make([]string, 0, len(v))
		for k1 := range v {
			tk.Keys = append(tk.Keys, k1)
		}
		sort.Strings(tk.Keys)
		tagKeys = append(tagKeys, *tk)
	}

	if len(tagKeys) == 0 {
		return nil, nil
	}
	return tagKeys, nil
}
