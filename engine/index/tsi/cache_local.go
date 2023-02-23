//go:build !streamfs
// +build !streamfs

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

func NewIndexCache(tsidCacheSize, skeyCacheSize, tagCacheSize int, path string) *IndexCache {
	return newIndexCache(tsidCacheSize, skeyCacheSize, tagCacheSize, path, true)
}

func (ic *IndexCache) close() error {
	if err := ic.SeriesKeyToTSIDCache.Save(ic.path + "/" + SeriesKeyToTSIDCacheName); err != nil {
		return err
	}
	ic.SeriesKeyToTSIDCache.Stop()

	if err := ic.TSIDToSeriesKeyCache.Save(ic.path + "/" + TSIDToSeriesKeyCacheName); err != nil {
		return err
	}
	ic.TSIDToSeriesKeyCache.Stop()

	ic.tagCache.Stop()

	return nil
}
