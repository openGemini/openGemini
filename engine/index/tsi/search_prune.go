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

package tsi

import (
	"regexp"

	"github.com/openGemini/openGemini/lib/pool"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/uint64set"
)

var pruneThreshold = 10
var pruneContextPool = pool.NewDefaultUnionPool[PruneContext](func() *PruneContext {
	return &PruneContext{}
})

type PruneContext struct {
	SeriesKey  []byte
	SeriesKeys [][]byte
	Ids        []uint64
	tags       influx.PointTags
}

func (ctx *PruneContext) MemSize() int {
	return len(ctx.SeriesKey) + len(ctx.Ids)*util.Uint64SizeBytes
}

func (is *indexSearch) doPrune(set *uint64set.Set, tfs []*tagFilter) (*uint64set.Set, error) {
	ctx := pruneContextPool.Get()
	defer pruneContextPool.Put(ctx)

	ctx.Ids = ctx.Ids[:0]
	set.ForEach(func(part []uint64) bool {
		ctx.Ids = append(ctx.Ids, part...)
		return true
	})

	var err error
	var match bool
	for _, sid := range ctx.Ids {
		ctx.SeriesKey, err = is.idx.searchSeriesKey(ctx.SeriesKey[:0], sid)
		if err != nil {
			return nil, err
		}
		match, err = matchSeriesKeyTagFilters(ctx, tfs)
		if err != nil {
			return nil, err
		}
		if !match {
			set.Del(sid)
		}
	}

	return set, err
}

func matchSeriesKeyTagFilters(ctx *PruneContext, tfs []*tagFilter) (bool, error) {
	var err error
	ctx.SeriesKeys, _, err = unmarshalCombineIndexKeys(ctx.SeriesKeys, ctx.SeriesKey)
	if err != nil {
		return false, err
	}

	var tagArray bool
	if len(ctx.SeriesKeys) > 1 {
		for i := range ctx.SeriesKeys {
			var tmpTags influx.PointTags
			_, err := influx.IndexKeyToTags(ctx.SeriesKeys[i], true, &tmpTags)
			if err != nil {
				return false, err
			}
			ctx.tags = append(ctx.tags, tmpTags...)
		}
		tagArray = true
	} else {
		var tags = &ctx.tags
		tags, err = influx.IndexKeyToTags(ctx.SeriesKey, false, tags)
		if err != nil {
			return false, err
		}
		ctx.tags = *tags
	}
	for _, tf := range tfs {
		if !matchSeriesKeyTagFilter(ctx.tags, tf, tagArray) {
			return false, nil
		}
	}
	return true, nil
}

func matchSeriesKeyTagFilter(tags influx.PointTags, tf *tagFilter, tagArray bool) bool {
	var match bool
	var exist bool
	matchKey := util.Bytes2str(tf.key)
	matchValue := util.Bytes2str(tf.value)

	var re *regexp.Regexp
	if tf.isRegexp {
		re = regexp.MustCompile(matchValue)
	}

	for _, tag := range tags {
		if tag.Key == matchKey {
			exist = true
			if tf.isRegexp {
				match = re.MatchString(tag.Value)
			} else {
				match = matchWithNoRegex(matchValue, tag.Value)
			}
			// not match then continue find when seriesKey of tags has tagArray
			if tagArray && (!match && !tf.isNegative || match && tf.isNegative) {
				continue
			}
			if tf.isNegative {
				return !match
			}
			return match
		}
	}
	// if find matchKey but matchValue != tag.Value, then return false
	if exist {
		return false
	}
	// if matchKey is not exsit in tags, compare matchValue with empty string
	if tf.isRegexp {
		match = re.MatchString("")
	} else {
		match = matchWithNoRegex(matchValue, "")
	}
	if tf.isNegative {
		return !match
	}
	return match
}
