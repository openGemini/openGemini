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

package statistics_test

import (
	"testing"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/stretchr/testify/require"
)

func TestHitRatio(t *testing.T) {
	stat := statistics.NewHitRatioStatistics()
	tags := map[string]string{"hostname": "127.0.0.1:8866", "mst": "hitRatio"}
	stat.Init(tags)
	stat.AddIndexWriterGetTotal(2)
	stat.AddIndexWriterHitTotal(2)
	stat.AddMergeColValGetTotal(2)
	stat.AddMergeColValHitTotal(2)
	stat.AddFileOpenTotal(2)
	stat.AddQueryFileUnHitTotal(2)
	stat.AddSeriesKeyToTSIDCacheGetTotal(2)
	stat.AddSeriesKeyToTSIDCacheGetMissTotal(2)
	stat.AddSeriesKeyToTSIDCacheGetNewSeriesTotal(2)

	fields := map[string]interface{}{
		"IndexWriterGetTotal":                   int64(2),
		"IndexWriterHitTotal":                   int64(2),
		"MergeColValGetTotal":                   int64(2),
		"MergeColValHitTotal":                   int64(2),
		"FileOpenTotal":                         int64(2),
		"QueryFileUnHitTotal":                   int64(2),
		"SeriesKeyToTSIDCacheGetTotal":          int64(2),
		"SeriesKeyToTSIDCacheGetMissTotal":      int64(2),
		"SeriesKeyToTSIDCacheGetNewSeriesTotal": int64(2),
	}
	statistics.NewTimestamp().Init(time.Second)
	buf, err := stat.Collect(nil)
	require.NoError(t, err)
	require.NoError(t, compareBuffer("hitRatio", tags, fields, buf))
}

func TestHitRatioItem(t *testing.T) {
	tags := map[string]string{"hostname": "127.0.0.1:8866", "mst": "hitRatio"}
	stat := statistics.NewHitRatioStatistics()
	stat.Init(tags)

	item := stat.Register("foo")
	item.Stat(true)
	item.Stat(false)

	require.Equal(t, int64(1), item.HitTotal())
	require.Equal(t, int64(2), item.GetTotal())
}
