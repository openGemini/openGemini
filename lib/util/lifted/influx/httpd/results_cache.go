// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package httpd

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/resultcache"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/httpd/config"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/zap"
)

const (
	cacheControlHeader = "Cache-Control"
	noStoreValue       = "no-store"
	MaxRequestCount    = 3
)

type ResultsCache struct {
	SplitQueriesByInterval time.Duration
	MaxCacheFreshness      time.Duration
	minCacheExtent         int64
	Logger                 *logger.Logger
	cache                  resultcache.ResultCache
}

func NewResultCache(logger *logger.Logger, conf config.ResultCacheConfig) *ResultsCache {
	c := resultcache.NewResultCache(conf)
	return &ResultsCache{
		Logger:                 logger,
		SplitQueriesByInterval: time.Duration(conf.SplitQueriesByInterval),
		MaxCacheFreshness:      time.Duration(conf.MaxCacheFreshness),
		minCacheExtent:         (5 * time.Minute).Milliseconds(),
		cache:                  c,
	}
}

func (rc *ResultsCache) Do(reqInfo *RequestInfo, command *promql2influxql.PromCommand, key string) (*promql2influxql.PromQueryResponse, bool, error) {
	rc.Logger.Debug("result cache key", zap.String("key", key))
	r := reqInfo.r
	if !shouldCache(r) {
		rc.Logger.Info("not enable cache", zap.String(cacheControlHeader, noStoreValue))
		return nil, false, nil
	}

	response := &promql2influxql.PromQueryResponse{}
	var extents []promql2influxql.Extent
	var err error

	maxCacheFreshness := rc.MaxCacheFreshness
	maxCacheTime := model.Now().Add(-maxCacheFreshness).UnixNano()
	if command.Start.UnixNano() > maxCacheTime {
		rc.Logger.Debug("cache miss", zap.Int64("start time", command.Start.UnixNano()))
		return nil, false, nil
	}

	fullHit := false
	cached, ok := rc.get(key)
	if ok {
		rc.Logger.Debug("hit result cache", zap.String("key", key))
		response, extents, err = rc.handleHit(reqInfo, command, cached, maxCacheTime, &fullHit)
	} else {
		rc.Logger.Debug("miss result cache", zap.String("key", key))
		response, extents, err = rc.handleMiss(reqInfo, command, maxCacheTime)
	}
	if err != nil {
		return nil, true, err
	}
	if fullHit {
		return response, true, nil
	}

	extents = rc.filterRecentExtents(extents, command.Step.Nanoseconds(), maxCacheFreshness)
	if len(extents) > 0 {
		rc.put(key, extents)
	}

	return response, true, nil
}

func (rc *ResultsCache) handleHit(reqInfo *RequestInfo, command *promql2influxql.PromCommand, extents []promql2influxql.Extent, maxCacheTime int64, fullHit *bool) (*promql2influxql.PromQueryResponse, []promql2influxql.Extent, error) {
	statistics.NewResultCache().CacheTotal.Add(1)
	cmds, responses := rc.partition(command, extents)
	statistics.NewResultCache().CacheRequestTotal.Add(int64(len(cmds)))
	if len(responses) > 0 {
		statistics.NewResultCache().HitCacheTotal.Add(1)
	}

	if len(cmds) == 0 {
		*fullHit = true
		response, err := MergeResponse(command, responses...)
		return response, extents, err
	}

	var reqResps []promql2influxql.Extent
	var err error
	// TODO based on a series of rules,decision to use cache or request data directly
	if len(cmds) > MaxRequestCount {
		res, err := DoRequests(reqInfo, []*promql2influxql.PromCommand{command})
		if err != nil {
			return nil, nil, err
		}
		return res[0].Response, extents, nil
	} else {
		reqResps, err = DoRequests(reqInfo, cmds)
		if err != nil {
			return nil, nil, err
		}
	}

	for _, res := range reqResps {
		responses = append(responses, res.Response)
		if !rc.shouldCacheResponse(maxCacheTime, command) {
			continue
		}
		extents = append(extents, res)
	}

	response, err := MergeResponse(command, responses...)
	if err != nil {
		return nil, nil, err
	}

	if len(extents) == 0 {
		return response, extents, nil
	}
	sort.Slice(extents, func(i, j int) bool {
		if extents[i].Start == extents[j].Start {
			return extents[i].End > extents[j].End
		}
		return extents[i].Start < extents[j].Start
	})

	mergeExtents := make([]promql2influxql.Extent, 0, len(extents))
	accumulator := extents[0]
	// extents merge
	for i := 1; i < len(extents); i++ {
		if accumulator.End+command.Step.Nanoseconds() < extents[i].Start {
			mergeExtents = append(mergeExtents, accumulator)
			accumulator = extents[i]
			continue
		}
		if accumulator.End >= extents[i].End {
			continue
		}

		accumulator.End = extents[i].End
		merged, err := MergeResponse(command, accumulator.Response, extents[i].Response)
		if err != nil {
			return nil, nil, err
		}
		accumulator.Response = merged
	}
	mergeExtents = append(mergeExtents, accumulator)

	return response, mergeExtents, nil
}

func (rc *ResultsCache) handleMiss(reqInfo *RequestInfo, command *promql2influxql.PromCommand, maxCacheTime int64) (*promql2influxql.PromQueryResponse, []promql2influxql.Extent, error) {
	res, err := DoRequests(reqInfo, []*promql2influxql.PromCommand{command})
	if err != nil {
		return nil, nil, err
	}

	if !rc.shouldCacheResponse(maxCacheTime, command) {
		return res[0].Response, []promql2influxql.Extent{}, nil
	}
	return res[0].Response, []promql2influxql.Extent{res[0]}, nil
}

func (rc *ResultsCache) partition(command *promql2influxql.PromCommand, extents []promql2influxql.Extent) ([]*promql2influxql.PromCommand, []*promql2influxql.PromQueryResponse) {
	var cmds []*promql2influxql.PromCommand
	var cachedResponses []*promql2influxql.PromQueryResponse
	reqStart := command.Start.UnixMilli()
	reqEnd := command.End.UnixMilli()
	start := reqStart

	for _, extent := range extents {
		// no overlap, ignore this extent
		if extent.End < start || extent.Start > reqEnd {
			continue
		}
		// if the extent is tiny but request is not tiny,discard it
		if reqStart != reqEnd && reqEnd-reqStart > rc.minCacheExtent && extent.End-extent.Start < rc.minCacheExtent {
			continue
		}

		if start < extent.Start {
			c := command.WithStartEnd(start, extent.Start)
			cmds = append(cmds, c)
		}
		cachedResponses = append(cachedResponses, ExtractResponse(extent.Response, start, reqEnd))
		start = extent.End
	}

	if start < reqEnd {
		r := command.WithStartEnd(start, reqEnd)
		cmds = append(cmds, r)
	}

	if reqStart == reqEnd && len(cachedResponses) == 0 {
		cmds = append(cmds, command)
	}

	return cmds, cachedResponses
}

func (rc *ResultsCache) get(key string) ([]promql2influxql.Extent, bool) {
	bufs, ok := rc.cache.Get(key)
	if !ok {
		return nil, false
	}
	var resp promql2influxql.CachedResponse
	if _, err := resp.UnmarshalMsg(bufs); err != nil {
		rc.Logger.Error("error unmarshalling cached value", zap.Error(err))
		return nil, false
	}

	if resp.Key != key {
		return nil, false
	}

	for _, e := range resp.Extents {
		if e.Response == nil {
			return nil, false
		}
	}
	return resp.Extents, true
}

func (rc *ResultsCache) put(key string, extents []promql2influxql.Extent) {
	cache := &promql2influxql.CachedResponse{
		Key:     key,
		Extents: extents,
	}
	bufs, err := cache.MarshalMsg(nil)
	if err != nil {
		rc.Logger.Error("marshalling cached value error", zap.Error(err))
		return
	}
	rc.cache.Put(key, bufs)
}

func (rc *ResultsCache) filterRecentExtents(extents []promql2influxql.Extent, step int64, maxCacheFreshness time.Duration) []promql2influxql.Extent {
	maxCacheTime := (time.Now().Add(-maxCacheFreshness).UnixNano() / step) * step
	filterExtents := make([]promql2influxql.Extent, 0, len(extents))
	for _, extent := range extents {
		if extent.Start > maxCacheTime {
			continue
		}
		if extent.Response == nil || extent.Response.Data == nil {
			continue
		}
		if extent.Response.Data.IsEmptyData() {
			continue
		}
		if extent.End > maxCacheTime {
			extent.End = maxCacheTime
			extent.Response = ExtractResponse(extent.Response, extent.Start, maxCacheTime)
		}
		filterExtents = append(filterExtents, extent)
	}
	return filterExtents
}

func (rc *ResultsCache) shouldCacheResponse(maxCacheTime int64, command *promql2influxql.PromCommand) bool {
	if !rc.isAtModifierCacheable(maxCacheTime, command) {
		return false
	}
	if !rc.isOffsetCacheable(command) {
		return false
	}
	return true
}

func (rc *ResultsCache) isAtModifierCacheable(maxCacheTime int64, command *promql2influxql.PromCommand) bool {
	// There are 2 cases when @ modifier is not safe to cache:
	//   1. When @ modifier points to time beyond the maxCacheTime.
	//   2. If the @ modifier time is > the query range end while being
	//      below maxCacheTime. In such cases if any tenant is intentionally
	//      playing with old data, we could cache empty result if we look
	//      beyond query end.
	var errAtModifierAfterEnd = errors.New("at modifier after end")
	if !strings.Contains(command.Cmd, "@") {
		return true
	}
	expr, err := parser.ParseExpr(command.Cmd)
	if err != nil {
		rc.Logger.Error("failed to parse query,considering @ modifier as not cacheable", zap.Error(err))
		return false
	}

	expr = promql.PreprocessExpr(expr, *command.Start, *command.End)
	end := command.End.UnixNano()
	shouldCache := true
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		switch e := n.(type) {
		case *parser.VectorSelector:
			if e.Timestamp != nil && (*e.Timestamp > end || *e.Timestamp > maxCacheTime) {
				shouldCache = false
				return errAtModifierAfterEnd
			}
		case *parser.MatrixSelector:
			ts := e.VectorSelector.(*parser.VectorSelector).Timestamp
			if ts != nil && (*ts > end || *ts > maxCacheTime) {
				shouldCache = false
				return errAtModifierAfterEnd
			}
		case *parser.SubqueryExpr:
			if e.Timestamp != nil && (*e.Timestamp > end || *e.Timestamp > maxCacheTime) {
				shouldCache = false
				return errAtModifierAfterEnd
			}
		}

		return nil
	})

	return shouldCache
}

func (rc *ResultsCache) isOffsetCacheable(command *promql2influxql.PromCommand) bool {
	var errNegativeOffset = errors.New("negative offset")
	if !strings.Contains(command.Cmd, "offset") {
		return true
	}
	expr, err := parser.ParseExpr(command.Cmd)
	if err != nil {
		rc.Logger.Error("failed to parse query,considering offset as not cacheable", zap.Error(err))
		return false
	}
	shouldCache := true
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		switch e := n.(type) {
		case *parser.VectorSelector:
			if e.OriginalOffset < 0 {
				shouldCache = false
				return errNegativeOffset
			}
		case *parser.MatrixSelector:
			offset := e.VectorSelector.(*parser.VectorSelector).OriginalOffset
			if offset < 0 {
				shouldCache = false
				return errNegativeOffset
			}
		case *parser.SubqueryExpr:
			if e.OriginalOffset < 0 {
				shouldCache = false
				return errNegativeOffset
			}
		}
		return nil
	})

	return shouldCache
}

func shouldCache(r *http.Request) bool {
	for _, v := range r.Header.Values(cacheControlHeader) {
		if strings.Contains(v, noStoreValue) {
			return false
		}
	}
	return true
}

func generateCacheKey(command *promql2influxql.PromCommand, mst string, interval time.Duration) string {
	currentInterval := command.Start.UnixNano() / int64(interval)
	var builder strings.Builder
	builder.WriteString(mst)
	builder.WriteString(":")
	builder.WriteString(command.Database)
	builder.WriteString(":")
	builder.WriteString(command.RetentionPolicy)
	builder.WriteString(":")
	builder.WriteString(command.Cmd)
	builder.WriteString(":")
	builder.WriteString(command.Step.String())
	builder.WriteString(":")
	builder.WriteString(strconv.FormatInt(currentInterval, 10))

	return builder.String()
}

func DoRequests(reqInfo *RequestInfo, cmds []*promql2influxql.PromCommand) ([]promql2influxql.Extent, error) {
	rw, ok := reqInfo.w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(reqInfo.w, reqInfo.r)
	}
	resps := make([]promql2influxql.Extent, len(cmds))
	start := time.Now()
	var queryErr error
	mu := sync.RWMutex{}
	wg := sync.WaitGroup{}
	wg.Add(len(cmds))
	for i, cmd := range cmds {
		go func(cmd *promql2influxql.PromCommand, i int) {
			defer wg.Done()
			res, apiErr, _ := reqInfo.h.execQuery(rw, reqInfo.r, reqInfo.u, start, *cmd, false, false)
			if apiErr != nil {
				mu.Lock()
				if queryErr == nil {
					queryErr = apiErr
				}
				mu.Unlock()
				return
			}
			resps[i] = promql2influxql.Extent{
				Start:    cmd.Start.UnixMilli(),
				End:      cmd.End.UnixMilli(),
				Response: res,
			}
		}(cmd, i)
	}
	wg.Wait()

	return resps, queryErr
}
