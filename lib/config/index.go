// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package config

import (
	"flag"
	"log"
	"strconv"
)

const (
	defaultTagScanPruneThreshold = 20000
)

type Index struct {
	TSIDCacheSize          int `toml:"tsid-cache-size"`
	SKeyCacheSize          int `toml:"skey-cache-size"`
	TagCacheSize           int `toml:"tag-cache-size"`
	TagFilterCostCacheSize int `toml:"tag-filter-cost-cache-size"`

	TagScanPruneThreshold int `toml:"tag-scan-prune-threshold"`
	MemoryAllowedPercent  int `toml:"memory-allowed-percent"`

	CacheCompressEnable bool `toml:"cache-compress-enable"`
	BloomFilterEnabled  bool `toml:"bloom-filter-enable"`
}

func NewIndex() *Index {
	return &Index{}
}

var indexConfig *Index

func SetIndexConfig(conf *Index) {
	indexConfig = conf
	if indexConfig.TagScanPruneThreshold == 0 {
		indexConfig.TagScanPruneThreshold = defaultTagScanPruneThreshold
	}

	if conf.MemoryAllowedPercent > 0 {
		// See: github.com/VictoriaMetrics/VictoriaMetrics/lib/memory/memory.go memory.allowedPercent
		err := flag.Set("memory.allowedPercent", strconv.Itoa(conf.MemoryAllowedPercent))
		log.Printf("set memory.allowedPercent=%d; err=%v \n", conf.MemoryAllowedPercent, err)
	}
}

func GetIndexConfig() *Index {
	if indexConfig == nil {
		return &Index{
			CacheCompressEnable:   true,
			TagScanPruneThreshold: defaultTagScanPruneThreshold,
		}
	}
	return indexConfig
}
