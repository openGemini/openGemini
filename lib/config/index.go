/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package config

type Index struct {
	TSIDCacheSize          int `toml:"tsid-cache-size"`
	SKeyCacheSize          int `toml:"skey-cache-size"`
	TagCacheSize           int `toml:"tag-cache-size"`
	TagFilterCostCacheSize int `toml:"tag-filter-cost-cache-size"`

	CacheCompressEnable bool `toml:"cache-compress-enable"`
	BloomFilterEnable   bool `toml:"bloom-filter-enable"`
}

func NewIndex() *Index {
	return &Index{}
}

var indexConfig *Index

func SetIndexConfig(conf *Index) {
	indexConfig = conf
}

func GetIndexConfig() *Index {
	if indexConfig == nil {
		return &Index{
			CacheCompressEnable: true,
		}
	}
	return indexConfig
}
