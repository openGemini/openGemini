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

package executor

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/prometheus/prometheus/promql/parser"
	"go.uber.org/zap"
)

type GroupLocs struct {
	Locs               []*Loc
	Loc                int
	tagKeys, tagValues []string
}

func newLocs() *GroupLocs {
	return &GroupLocs{Locs: make([]*Loc, 0, 1)}
}

func (ls *GroupLocs) add(chunkLen int) {
	loc := ls.Locs[ls.Loc]
	loc.add()
	if loc.RowLoc == chunkLen {
		ls.Loc++
	}
}

func (ls *GroupLocs) reset() {
	ls.Loc = 0
	for _, loc := range ls.Locs {
		loc.RowLoc = loc.startRowLoc
	}
}

type Loc struct {
	ChunkLoc    int
	GroupLoc    int
	RowLoc      int
	startRowLoc int
}

func newLoc(chunkLoc int, groupLoc int, startRow int) *Loc {
	return &Loc{ChunkLoc: chunkLoc, GroupLoc: groupLoc, RowLoc: startRow, startRowLoc: startRow}
}

func (l *Loc) add() {
	l.RowLoc++
}

type BinOpTransform struct {
	BaseProcessor
	inputs              []*ChunkPort
	bufChunks           []Chunk
	output              *ChunkPort
	outputChunk         Chunk
	chunkPool           *CircularChunkPool
	workTracing         *tracing.Span
	nextChunks          []chan Semaphore
	inputSignals        []chan Semaphore
	schema              *QuerySchema
	streamBinOpLogger   *logger.Logger
	opt                 *query.ProcessorOptions
	nextChunksCloseOnce []sync.Once
	errs                errno.Errs

	OpType                   int
	On                       bool     // true: on; false: ignore
	MatchKeysForMatchCompute []string // on(MatchKeys)/ ignore(MatchKeys)
	MatchKeys                []string
	matchType                influxql.MatchCardinality
	IncludeKeys              []string // group_left/group_right(IncludeKeys)
	ReturnBool               bool
	NilMst                   influxql.NilMstState

	primaryLoc        int
	secondaryLoc      int
	primaryChunks     []Chunk
	primaryMap        map[string]*GroupLocs // <matchGroupKey, tagsGroupsPerMatchGroup>
	resultMap         map[string]interface{}
	preTags           string
	primaryChunkNum   int
	shouldDropMstName bool
	resultTagKeys     []string
	resultTagValues   []string
	reserveLoc        []int
	delLoc            []int

	computeResultTags func(pTagKeys, pTagValues, sTagKeys, sTagValues []string) ([]string, []string, string)
	BinOpHelper       func(ctx context.Context, errs *errno.Errs)
	skipFlag          bool

	computeValue func(lVal, rVal float64) (float64, bool)
}

const (
	BinOpTransformName = "BinOpTransform"
)

type BinOpTransformCreator struct {
}

func (c *BinOpTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))
	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}
	binOp, ok := plan.(*LogicalBinOp)
	if !ok {
		return nil, fmt.Errorf("logicalplan isnot binOp")
	}
	p, err := NewBinOpTransform(inRowDataTypes, plan.RowDataType(), plan.Schema().(*QuerySchema), binOp.Para)
	return p, err
}

var _ = RegistryTransformCreator(&LogicalBinOp{}, &BinOpTransformCreator{})

func NewBinOpTransform(inRowDataTypes []hybridqp.RowDataType, outRowDataType hybridqp.RowDataType, schema *QuerySchema, para *influxql.BinOp) (*BinOpTransform, error) {
	trans := &BinOpTransform{
		output:            NewChunkPort(outRowDataType),
		chunkPool:         NewCircularChunkPool(CircularChunkNum, NewChunkBuilder(outRowDataType)),
		schema:            schema,
		opt:               schema.opt.(*query.ProcessorOptions),
		streamBinOpLogger: logger.NewLogger(errno.ModuleQueryEngine),
		primaryMap:        make(map[string]*GroupLocs),
		resultMap:         make(map[string]interface{}),
		resultTagKeys:     make([]string, 0),
		resultTagValues:   make([]string, 0),
		reserveLoc:        make([]int, 0),
		delLoc:            make([]int, 0),
		NilMst:            para.NilMst,
	}
	for i := range inRowDataTypes {
		trans.inputs = append(trans.inputs, NewChunkPort(inRowDataTypes[i]))
		trans.nextChunks = append(trans.nextChunks, make(chan Semaphore))
		trans.inputSignals = append(trans.inputSignals, make(chan Semaphore))
		trans.bufChunks = append(trans.bufChunks, nil)
		trans.nextChunksCloseOnce = append(trans.nextChunksCloseOnce, sync.Once{})
	}
	trans.outputChunk = trans.chunkPool.GetChunk()
	trans.initMatchType(para)
	return trans, nil
}

func (trans *BinOpTransform) initMatchType(para *influxql.BinOp) {
	trans.matchType = para.MatchCard
	trans.OpType = para.OpType
	trans.On = para.On
	trans.MatchKeys = para.MatchKeys
	trans.MatchKeysForMatchCompute = para.MatchKeys
	trans.IncludeKeys = para.IncludeKeys
	trans.ReturnBool = para.ReturnBool
	if trans.matchType == influxql.OneToMany {
		trans.secondaryLoc = 1
	} else {
		trans.primaryLoc = 1
	}
	if !trans.On {
		trans.MatchKeysForMatchCompute = append(trans.MatchKeysForMatchCompute, promql2influxql.DefaultMetricKeyLabel)
	}
	sort.Strings(trans.MatchKeys)
	sort.Strings(trans.MatchKeysForMatchCompute)

	if promql2influxql.ShouldDropMetricName(parser.ItemType(trans.OpType)) || trans.ReturnBool {
		trans.shouldDropMstName = true
	}
	if trans.matchType == influxql.OneToOne {
		trans.computeResultTags = trans.oneComputeResultTags
	} else {
		trans.computeResultTags = trans.manyComputeResultTags
	}
	if trans.OpType == parser.LAND {
		trans.BinOpHelper = trans.BinOpHelperConditionSingle
		trans.skipFlag = false
	} else if trans.OpType == parser.LUNLESS {
		trans.BinOpHelper = trans.BinOpHelperConditionSingle
		trans.skipFlag = true
	} else if trans.OpType == parser.LOR {
		trans.BinOpHelper = trans.BinOpHelperConditionBoth
		trans.skipFlag = true
	} else {
		trans.BinOpHelper = trans.BinOpHelperOperator
		trans.initComputeFn()
	}
}

func (trans *BinOpTransform) initComputeFn() {
	switch trans.OpType {
	case parser.ADD:
		trans.computeValue = BinOpADD
	case parser.SUB:
		trans.computeValue = BinOpSUB
	case parser.MUL:
		trans.computeValue = BinOpMUL
	case parser.DIV:
		trans.computeValue = BinOpDIV
	case parser.POW:
		trans.computeValue = BinOpPOW
	case parser.MOD:
		trans.computeValue = BinOpMOD
	case parser.EQLC:
		trans.computeValue = BinOpEQLC
	case parser.NEQ:
		trans.computeValue = BinOpNEQ
	case parser.GTR:
		trans.computeValue = BinOpGTR
	case parser.LSS:
		trans.computeValue = BinOpLSS
	case parser.GTE:
		trans.computeValue = BinOpGTE
	case parser.LTE:
		trans.computeValue = BinOpLTE
	default:
		panic(fmt.Errorf("operator %q not allowed", trans.OpType))
	}
}

func (trans *BinOpTransform) Name() string {
	return BinOpTransformName
}

func (trans *BinOpTransform) Explain() []ValuePair {
	return nil
}

func (trans *BinOpTransform) Close() {
	trans.output.Close()
}

func (trans *BinOpTransform) addChunk(c Chunk, i int) {
	trans.bufChunks[i] = c
	trans.inputSignals[i] <- signal
}

func (trans *BinOpTransform) runnable(ctx context.Context, errs *errno.Errs, i int) {
	defer func() {
		close(trans.inputSignals[i])
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.streamBinOpLogger.Error(err.Error(), zap.String("query", "BinOpTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	for {
		select {
		case c, ok := <-trans.inputs[i].State:
			if !ok {
				trans.addChunk(nil, i)
				return
			}
			trans.addChunk(c, i)
			_, iok := <-trans.nextChunks[i]
			if !iok {
				return
			}
		case <-ctx.Done():
			trans.closeNextChunk(i)
			return
		}
	}
}

func (trans *BinOpTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[BinOpTransform] TotalWorkCost", false)
	trans.workTracing = tracing.Start(span, "cost_for_BinOp", false)
	defer func() {
		trans.Close()
		tracing.Finish(span, trans.workTracing)
	}()

	errs := &trans.errs

	if trans.NilMst == influxql.NoNilMst {
		errs.Init(3, trans.Close)
		go trans.runnable(ctx, errs, 0)
		go trans.runnable(ctx, errs, 1)
	} else {
		errs.Init(2, trans.Close)
		go trans.runnable(ctx, errs, 0)
	}
	go trans.BinOpHelper(ctx, errs)

	return errs.Err()
}

func (trans *BinOpTransform) SendChunk() {
	if trans.outputChunk.Len() >= trans.opt.ChunkSize {
		trans.output.State <- trans.outputChunk
		trans.outputChunk = trans.chunkPool.GetChunk()
	}
}

func (trans *BinOpTransform) closeNextChunks() {
	if trans.NilMst == influxql.NoNilMst {
		trans.closeNextChunk(0)
		trans.closeNextChunk(1)
	} else {
		trans.closeNextChunk(0)
	}
}

func (trans *BinOpTransform) closeNextChunk(i int) {
	trans.nextChunksCloseOnce[i].Do(func() {
		close(trans.nextChunks[i])
	})
}

func (trans *BinOpTransform) notSameGroup(tags ChunkTags) bool {
	return string(tags.subset) != trans.preTags
}

var seps = []byte{'\xff'}

func (trans *BinOpTransform) computeMatchTags(tags ChunkTags) (string, []string, []string) {
	tagkeys, tagValues := tags.GetChunkTagAndValues()
	var i, j int
	var retMatchTags []byte
	if trans.On {
		for i < len(trans.MatchKeysForMatchCompute) && j < len(tagkeys) {
			if trans.MatchKeysForMatchCompute[i] < tagkeys[j] {
				i++
			} else if tagkeys[j] < trans.MatchKeysForMatchCompute[i] {
				j++
			} else {
				retMatchTags = append(retMatchTags, seps[0])
				retMatchTags = append(retMatchTags, []byte(tagkeys[j])...)
				retMatchTags = append(retMatchTags, seps[0])
				retMatchTags = append(retMatchTags, []byte(tagValues[j])...)
				i++
				j++
			}
		}
		return string(retMatchTags), tagkeys, tagValues
	} else {
		for i < len(trans.MatchKeysForMatchCompute) && j < len(tagkeys) {
			if trans.MatchKeysForMatchCompute[i] < tagkeys[j] {
				i++
			} else if tagkeys[j] < trans.MatchKeysForMatchCompute[i] {
				retMatchTags = append(retMatchTags, seps[0])
				retMatchTags = append(retMatchTags, []byte(tagkeys[j])...)
				retMatchTags = append(retMatchTags, seps[0])
				retMatchTags = append(retMatchTags, []byte(tagValues[j])...)
				j++
			} else {
				j++
			}
		}
		for ; j < len(tagkeys); j++ {
			retMatchTags = append(retMatchTags, seps[0])
			retMatchTags = append(retMatchTags, []byte(tagkeys[j])...)
			retMatchTags = append(retMatchTags, seps[0])
			retMatchTags = append(retMatchTags, []byte(tagValues[j])...)
		}
		return string(retMatchTags), tagkeys, tagValues
	}
}

func (trans *BinOpTransform) addPrimaryMatchTags(tags ChunkTags, loc, startRow int) error {
	matchTags, tagKeys, tagValues := trans.computeMatchTags(tags)
	var ok bool
	var primaryGroups *GroupLocs
	if primaryGroups, ok = trans.primaryMap[matchTags]; ok {
		if trans.notSameGroup(tags) {
			return fmt.Errorf("duplicate matchTags of primary map building:%s", matchTags)
		} else {
			primaryGroups.Locs = append(primaryGroups.Locs, newLoc(trans.primaryChunkNum, loc, startRow))
		}
	} else {
		newLocs := newLocs()
		newLocs.Locs = append(newLocs.Locs, newLoc(trans.primaryChunkNum, loc, startRow))
		if trans.matchType != influxql.OneToOne {
			newLocs.tagKeys, newLocs.tagValues = tagKeys, tagValues
		}
		trans.primaryMap[matchTags] = newLocs
	}
	return nil
}

func (trans *BinOpTransform) addPrimaryMatchTagsSimple(tags ChunkTags, loc, startRow int) {
	matchTags, _, _ := trans.computeMatchTags(tags)
	trans.primaryMap[matchTags] = nil
}

func (trans *BinOpTransform) addPrimaryMap(c Chunk) error {
	trans.primaryChunks = append(trans.primaryChunks, c.Clone())
	tags := trans.primaryChunks[len(trans.primaryChunks)-1].Tags()
	for i := range tags {
		startRow := trans.primaryChunks[len(trans.primaryChunks)-1].TagIndex()[i]
		if err := trans.addPrimaryMatchTags(tags[i], i, startRow); err != nil {
			return err
		}
		trans.preTags = string(tags[i].subset)
	}
	trans.primaryChunkNum++
	return nil
}

// 1.no copy; 2.no primayMap dup check 3.no primaryGroupLocs to mark
func (trans *BinOpTransform) addPrimaryMapSimple(c Chunk) {
	for i := range c.Tags() {
		startRow := c.TagIndex()[i]
		trans.addPrimaryMatchTagsSimple(c.Tags()[i], i, startRow)
	}
}

func (trans *BinOpTransform) addPrimaryMapAndResultSimple(c Chunk) {
	for i := range c.Tags() {
		startRow := c.TagIndex()[i]
		trans.addPrimaryMatchTagsSimple(c.Tags()[i], i, startRow)
		trans.computeMatchResultSimple(c, i, &c.Tags()[i])
	}
}

func (trans *BinOpTransform) AddResult(secondaryChunk Chunk) error {
	var ok bool
	var primaryGroups *GroupLocs
	var err error
	trans.preTags = ""
	tags := secondaryChunk.Tags()
	for i := range tags {
		matchTags, tagsKeys, tagValues := trans.computeMatchTags(tags[i])
		if primaryGroups, ok = trans.primaryMap[matchTags]; !ok {
			continue
		}
		if err = trans.addResulMap(matchTags, tagsKeys, tagValues, tags[i], primaryGroups); err != nil {
			return err
		}
		if trans.notSameGroup(tags[i]) {
			primaryGroups.reset()
		}
		if err := trans.computeMatchResult(primaryGroups, secondaryChunk, i); err != nil {
			return err
		}
		trans.preTags = string(tags[i].subset)
	}
	return nil
}

// 1.no compute resultTags; 2.no computeMatchResult by both side input; 3.no resultMap dup err check
func (trans *BinOpTransform) AddResultSimple(secondaryChunk Chunk) {
	var ok bool
	tags := secondaryChunk.Tags()
	for i := range tags {
		matchTags, _, _ := trans.computeMatchTags(tags[i])
		if _, ok = trans.primaryMap[matchTags]; ok == trans.skipFlag {
			continue
		}
		trans.computeMatchResultSimple(secondaryChunk, i, &tags[i])
	}
}

func (trans *BinOpTransform) computeMatchResult(primaryGroups *GroupLocs, secondaryChunk Chunk, secondaryGroupLoc int) error {
	var start, end int
	var rVal float64
	var keep bool
	var pTime, sTime int64
	preOutSize := trans.outputChunk.Len()
	start = secondaryChunk.TagIndex()[secondaryGroupLoc]
	if secondaryGroupLoc == secondaryChunk.TagLen()-1 {
		end = secondaryChunk.Len()
	} else {
		end = secondaryChunk.TagIndex()[secondaryGroupLoc+1]
	}
	for start < end {
		if primaryGroups.Loc >= len(primaryGroups.Locs) {
			break
		}
		pLoc := primaryGroups.Locs[primaryGroups.Loc]
		pChunk := trans.primaryChunks[pLoc.ChunkLoc]
		pTime = pChunk.Time()[pLoc.RowLoc]
		sTime = secondaryChunk.Time()[start]
		if pTime < sTime {
			primaryGroups.add(pChunk.Len())
			continue
		} else if sTime < pTime {
			start++
			continue
		}
		pVal := pChunk.Columns()[0].FloatValues()[pLoc.RowLoc]
		sVal := secondaryChunk.Columns()[0].FloatValues()[start]
		if trans.matchType == influxql.OneToMany {
			rVal, keep = trans.computeValue(pVal, sVal)
		} else {
			rVal, keep = trans.computeValue(sVal, pVal)
		}
		if trans.ReturnBool {
			if keep {
				rVal = 1
			} else {
				rVal = 0
			}
		} else if !keep {
			primaryGroups.add(pChunk.Len())
			start++
			continue
		}
		trans.addOutputVal(pTime, rVal)
		primaryGroups.add(pChunk.Len())
		start++
	}
	if trans.outputChunk.Len() > preOutSize {
		trans.addOutPutTags(preOutSize)
	}
	trans.SendChunk()
	return nil
}

func (trans *BinOpTransform) computeMatchResultSimple(secondaryChunk Chunk, secondaryGroupLoc int, tags *ChunkTags) {
	trans.addOutPutTagsSimple(tags)
	var start, end int
	start = secondaryChunk.TagIndex()[secondaryGroupLoc]
	if secondaryGroupLoc == secondaryChunk.TagLen()-1 {
		end = secondaryChunk.Len()
	} else {
		end = secondaryChunk.TagIndex()[secondaryGroupLoc+1]
	}
	trans.addOutputVals(secondaryChunk.Time()[start:end], secondaryChunk.Columns()[0].FloatValues()[start:end])
	trans.SendChunk()
}

func (trans *BinOpTransform) addOutPutTags(preOutSize int) {
	tagSet := NewChunkTagsByTagKVs(trans.resultTagKeys, trans.resultTagValues)
	if len(trans.outputChunk.Tags()) > 0 && bytes.Equal(trans.outputChunk.Tags()[len(trans.outputChunk.Tags())-1].subset, tagSet.subset) {
		return
	}
	trans.outputChunk.AddTagAndIndex(*tagSet, preOutSize)
	trans.outputChunk.AddIntervalIndex(preOutSize)
}

func (trans *BinOpTransform) addOutPutTagsSimple(tags *ChunkTags) {
	tagSet := NewChunkTagsDeepCopy(tags.subset, tags.offsets)
	trans.outputChunk.AddTagAndIndex(*tagSet, trans.outputChunk.Len())
	trans.outputChunk.AddIntervalIndex(trans.outputChunk.Len())
}

func BinOpADD(lVal, rVal float64) (float64, bool) {
	return lVal + rVal, true
}

func BinOpSUB(lVal, rVal float64) (float64, bool) {
	return lVal - rVal, true
}

func BinOpMUL(lVal, rVal float64) (float64, bool) {
	return lVal * rVal, true
}

func BinOpDIV(lVal, rVal float64) (float64, bool) {
	return lVal / rVal, true
}

func BinOpPOW(lVal, rVal float64) (float64, bool) {
	return math.Pow(lVal, rVal), true
}

func BinOpMOD(lVal, rVal float64) (float64, bool) {
	return math.Pow(lVal, rVal), true
}

func BinOpEQLC(lVal, rVal float64) (float64, bool) {
	return lVal, lVal == rVal
}

func BinOpNEQ(lVal, rVal float64) (float64, bool) {
	return lVal, lVal != rVal
}

func BinOpGTR(lVal, rVal float64) (float64, bool) {
	return lVal, lVal > rVal
}

func BinOpLSS(lVal, rVal float64) (float64, bool) {
	return lVal, lVal < rVal
}

func BinOpGTE(lVal, rVal float64) (float64, bool) {
	return lVal, lVal >= rVal
}

func BinOpLTE(lVal, rVal float64) (float64, bool) {
	return lVal, lVal <= rVal
}

func (trans *BinOpTransform) addOutputVal(t int64, v float64) {
	trans.outputChunk.AppendTime(t)
	trans.outputChunk.Columns()[0].AppendFloatValue(v)
	trans.outputChunk.Columns()[0].AppendManyNotNil(1)
}

func (trans *BinOpTransform) addOutputVals(t []int64, v []float64) {
	trans.outputChunk.AppendTimes(t)
	trans.outputChunk.Columns()[0].AppendFloatValues(v)
	trans.outputChunk.Columns()[0].AppendManyNotNil(len(t))
}

func (trans *BinOpTransform) addResulMap(sMatchTags string, sTagKeys []string, sTagValues []string, tags ChunkTags, pLocs *GroupLocs) error {
	if trans.matchType == influxql.OneToOne {
		if _, ok := trans.resultMap[sMatchTags]; ok {
			if trans.notSameGroup(tags) {
				return fmt.Errorf("one-to-one duplicate result matchkeys:%s", sMatchTags)
			}
		} else {
			trans.resultMap[sMatchTags] = nil
		}
		trans.resultTagKeys, trans.resultTagValues, _ = trans.ComputeResultTags(pLocs.tagKeys, pLocs.tagValues, sTagKeys, sTagValues)
		return nil
	} else {
		var resultTags string
		trans.resultTagKeys, trans.resultTagValues, resultTags = trans.ComputeResultTags(pLocs.tagKeys, pLocs.tagValues, sTagKeys, sTagValues)
		if _, ok := trans.resultMap[resultTags]; ok {
			if trans.notSameGroup(tags) {
				return fmt.Errorf("one-to-many/many-to-one duplicate resultkeys:%s", resultTags)
			}
		} else {
			trans.resultMap[resultTags] = nil
		}
		return nil
	}
}

func (trans *BinOpTransform) oneComputeResultTags(pTagKeys, pTagValues, sTagKeys, sTagValues []string) ([]string, []string, string) {
	trans.dropMstName(&sTagKeys, &sTagValues)
	if trans.On {
		for i := range trans.MatchKeys {
			for j := range sTagKeys {
				if sTagKeys[j] == trans.MatchKeys[i] {
					trans.reserveLoc = append(trans.reserveLoc, j)
				}
			}
		}
	} else {
		for i := range sTagKeys {
			var find bool
			for j := range trans.MatchKeys {
				if sTagKeys[i] == trans.MatchKeys[j] {
					find = true
					break
				}
			}
			if find {
				continue
			}
			trans.reserveLoc = append(trans.reserveLoc, i)
		}
	}
	for i := range trans.reserveLoc {
		trans.resultTagKeys = append(trans.resultTagKeys, sTagKeys[trans.reserveLoc[i]])
		trans.resultTagValues = append(trans.resultTagValues, sTagValues[trans.reserveLoc[i]])
	}
	resultTags := trans.encodeTags()
	return trans.resultTagKeys, trans.resultTagValues, resultTags
}

func (trans *BinOpTransform) manyComputeResultTags(pTagKeys, pTagValues, sTagKeys, sTagValues []string) ([]string, []string, string) {
	trans.dropMstName(&sTagKeys, &sTagValues)
	for i := range trans.IncludeKeys {
		var find bool
		for j := range pTagKeys {
			if pTagKeys[j] == trans.IncludeKeys[i] && pTagValues[j] != "" {
				trans.reserveLoc = append(trans.reserveLoc, j)
				find = true
				break
			}
		}
		if find {
			continue
		}
		for j := range sTagKeys {
			if sTagKeys[j] == trans.IncludeKeys[i] {
				trans.delLoc = append(trans.delLoc, j)
			}
		}
	}
	i := 0
	j := 0
	k := 0
	for i < len(sTagKeys) {
		if j < len(trans.delLoc) && i == trans.delLoc[j] {
			i++
			j++
			continue
		}
		if k < len(trans.reserveLoc) && sTagKeys[i] > pTagKeys[trans.reserveLoc[k]] {
			trans.resultTagKeys = append(trans.resultTagKeys, pTagKeys[trans.reserveLoc[k]])
			trans.resultTagValues = append(trans.resultTagValues, pTagValues[trans.reserveLoc[k]])
			k++
			continue
		} else if k < len(trans.reserveLoc) && sTagKeys[i] == pTagKeys[trans.reserveLoc[k]] {
			trans.resultTagKeys = append(trans.resultTagKeys, pTagKeys[trans.reserveLoc[k]])
			trans.resultTagValues = append(trans.resultTagValues, pTagValues[trans.reserveLoc[k]])
			i++
			k++
			continue
		}
		trans.resultTagKeys = append(trans.resultTagKeys, sTagKeys[i])
		trans.resultTagValues = append(trans.resultTagValues, sTagValues[i])
		i++
	}
	// ptagkeys has but stagkeys not has: need reserve
	for ; k < len(trans.reserveLoc); k++ {
		trans.resultTagKeys = append(trans.resultTagKeys, pTagKeys[trans.reserveLoc[k]])
		trans.resultTagValues = append(trans.resultTagValues, pTagValues[trans.reserveLoc[k]])
	}
	resultTags := trans.encodeTags()
	return trans.resultTagKeys, trans.resultTagValues, resultTags
}

func (trans *BinOpTransform) ComputeResultTags(pTagKeys, pTagValues, sTagKeys, sTagValues []string) ([]string, []string, string) {
	trans.reserveLoc = trans.reserveLoc[:0]
	trans.resultTagKeys = trans.resultTagKeys[:0]
	trans.resultTagValues = trans.resultTagValues[:0]
	trans.delLoc = trans.delLoc[:0]
	return trans.computeResultTags(pTagKeys, pTagValues, sTagKeys, sTagValues)
}

func (trans *BinOpTransform) dropMstName(tagKeys *[]string, tagValues *[]string) {
	if trans.shouldDropMstName {
		for i := range *tagKeys {
			if (*tagKeys)[i] == promql2influxql.DefaultMetricKeyLabel {
				*tagKeys = append((*tagKeys)[:i], (*tagKeys)[i+1:]...)
				*tagValues = append((*tagValues)[:i], (*tagValues)[i+1:]...)
				return
			}
		}
	}
}

func (trans *BinOpTransform) encodeTags() string {
	var resultTags []byte
	for i := range trans.resultTagKeys {
		resultTags = append(resultTags, []byte(trans.resultTagKeys[i])...)
		resultTags = append(resultTags, seps[0])
		resultTags = append(resultTags, []byte(trans.resultTagValues[i])...)
		resultTags = append(resultTags, seps[0])
	}
	return string(resultTags)
}

func (trans *BinOpTransform) BinOpHelperOperator(ctx context.Context, errs *errno.Errs) {
	defer func() {
		trans.closeNextChunks()
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.streamBinOpLogger.Error(err.Error(), zap.String("query", "BinOpTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	var err error
	// 1. build primaryMap
	for {
		<-trans.inputSignals[trans.primaryLoc]
		if trans.bufChunks[trans.primaryLoc] == nil {
			break
		}
		err = trans.addPrimaryMap(trans.bufChunks[trans.primaryLoc])
		if err != nil {
			errs.Dispatch(err)
			return
		}
		trans.nextChunks[trans.primaryLoc] <- signal
	}

	// 2. add result
	for {
		<-trans.inputSignals[trans.secondaryLoc]
		if trans.bufChunks[trans.secondaryLoc] == nil {
			break
		}
		err = trans.AddResult(trans.bufChunks[trans.secondaryLoc])
		if err != nil {
			errs.Dispatch(err)
			return
		}
		trans.nextChunks[trans.secondaryLoc] <- signal
	}
	if trans.outputChunk.Len() > 0 {
		trans.output.State <- trans.outputChunk
	}
}

func (trans *BinOpTransform) BinOpHelperConditionSingle(ctx context.Context, errs *errno.Errs) {
	defer func() {
		trans.closeNextChunks()
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.streamBinOpLogger.Error(err.Error(), zap.String("query", "BinOpTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	// 0. one side input is nilMst
	if trans.NilMst == influxql.LNilMst {
		<-trans.inputSignals[0]
		return
	} else if trans.NilMst == influxql.RNilMst {
		for {
			<-trans.inputSignals[0]
			if trans.bufChunks[0] == nil {
				return
			}
			trans.bufChunks[0].CopyTo(trans.outputChunk)
			trans.output.State <- trans.outputChunk
			trans.outputChunk = trans.chunkPool.GetChunk()
			trans.nextChunks[0] <- signal
		}
	}

	// 1. build PrimaryMapSimple by right
	for {
		<-trans.inputSignals[1]
		if trans.bufChunks[1] == nil {
			break
		}
		trans.addPrimaryMapSimple(trans.bufChunks[1])
		trans.nextChunks[1] <- signal
	}

	// 2. add result by left
	for {
		<-trans.inputSignals[0]
		if trans.bufChunks[0] == nil {
			break
		}
		trans.AddResultSimple(trans.bufChunks[0])
		trans.nextChunks[0] <- signal
	}
	if trans.outputChunk.Len() > 0 {
		trans.output.State <- trans.outputChunk
	}
}

func (trans *BinOpTransform) BinOpHelperConditionBoth(ctx context.Context, errs *errno.Errs) {
	defer func() {
		trans.closeNextChunks()
		if e := recover(); e != nil {
			err := errno.NewError(errno.RecoverPanic, e)
			trans.streamBinOpLogger.Error(err.Error(), zap.String("query", "BinOpTransform"),
				zap.Uint64("query_id", trans.opt.QueryId))
			errs.Dispatch(err)
		} else {
			errs.Dispatch(nil)
		}
	}()
	// 0. one side input is nilMst
	if trans.NilMst != influxql.NoNilMst {
		for {
			<-trans.inputSignals[0]
			if trans.bufChunks[0] == nil {
				return
			}
			trans.bufChunks[0].CopyTo(trans.outputChunk)
			trans.output.State <- trans.outputChunk
			trans.outputChunk = trans.chunkPool.GetChunk()
			trans.nextChunks[0] <- signal
		}
	}

	// 1. build PrimaryMapSimple and add result by left
	for {
		<-trans.inputSignals[0]
		if trans.bufChunks[0] == nil {
			break
		}
		trans.addPrimaryMapAndResultSimple(trans.bufChunks[0])
		trans.nextChunks[0] <- signal
	}

	// 2. add result by right
	for {
		<-trans.inputSignals[1]
		if trans.bufChunks[1] == nil {
			break
		}
		trans.AddResultSimple(trans.bufChunks[1])
		trans.nextChunks[1] <- signal
	}
	if trans.outputChunk.Len() > 0 {
		trans.output.State <- trans.outputChunk
	}
}

func (trans *BinOpTransform) GetOutputs() Ports {
	return Ports{trans.output}
}

func (trans *BinOpTransform) GetInputs() Ports {
	ports := make(Ports, 0, len(trans.inputs))
	for _, input := range trans.inputs {
		ports = append(ports, input)
	}
	return ports
}

func (trans *BinOpTransform) GetOutputNumber(_ Port) int {
	return 0
}

func (trans *BinOpTransform) GetInputNumber(_ Port) int {
	return 0
}
