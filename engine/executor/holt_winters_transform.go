/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.9/query/functions.go

2023.01.18 add HoltWintersTransform.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

package executor

import (
	"container/list"
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/query/neldermead"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

const (
	// Arbitrary weight for initializing some intial guesses.
	// This should be in the  range [0,1]
	hwWeight = 0.5
	// Epsilon value for the minimization process
	hwDefaultEpsilon = 1.0e-4
	// Define a grid of initial guesses for the parameters: alpha, beta, gamma, and phi.
	// Keep in mind that this grid is N^4 so we should keep N small
	// The starting lower guess
	hwGuessLower = 0.3
	//  The upper bound on the grid
	hwGuessUpper = 1.0
	// The step between guesses
	hwGuessStep = 0.4
)

type holtWintersReducer struct {
	// Season period
	m        int
	seasonal bool

	// Horizon
	h int

	// Interval between points
	interval int64
	// interval / 2 -- used to perform rounding
	halfInterval int64

	// Whether to include all data or only future values
	includeFitData bool

	// NelderMead optimizer
	optim *neldermead.Optimizer
	// Small difference bound for the optimizer
	epsilon float64

	parameters []float64

	y          []float64
	pointsVal  []float64
	pointsTime []int64

	forecasted    []float64
	startTime     int64
	forecastedLoc int
}

func NewHoltWintersReducer(h, m int, includeFitData bool, interval time.Duration) *holtWintersReducer {
	seasonal := true
	if m < 2 {
		seasonal = false
	}
	return &holtWintersReducer{
		h:              h,
		m:              m,
		seasonal:       seasonal,
		includeFitData: includeFitData,
		interval:       int64(interval),
		halfInterval:   int64(interval) / 2,
		optim:          neldermead.New(),
		epsilon:        hwDefaultEpsilon,
		parameters:     make([]float64, 0),
		y:              make([]float64, 0),
		pointsVal:      make([]float64, 0),
		pointsTime:     make([]int64, 0),
		forecasted:     make([]float64, 0),
		startTime:      0,
		forecastedLoc:  0,
	}
}

func (r *holtWintersReducer) ClearForcasted() {
	r.forecastedLoc = 0
	r.forecasted = r.forecasted[0:0]
	r.startTime = 0
}

func (r *holtWintersReducer) constrain(x []float64) {
	// alpha
	if x[0] > 1 {
		x[0] = 1
	}
	if x[0] < 0 {
		x[0] = 0
	}
	// beta
	if x[1] > 1 {
		x[1] = 1
	}
	if x[1] < 0 {
		x[1] = 0
	}
	// gamma
	if x[2] > 1 {
		x[2] = 1
	}
	if x[2] < 0 {
		x[2] = 0
	}
	// phi
	if x[3] > 1 {
		x[3] = 1
	}
	if x[3] < 0 {
		x[3] = 0
	}
}

// Using the recursive relations compute the next values
func (r *holtWintersReducer) next(alpha, beta, gamma, phi, phiH, yT, lTp, bTp, sTm, sTmh float64) (yTh, lT, bT, sT float64) {
	lT = alpha*(yT/sTm) + (1-alpha)*(lTp+phi*bTp)
	bT = beta*(lT-lTp) + (1-beta)*phi*bTp
	sT = gamma*(yT/(lTp+phi*bTp)) + (1-gamma)*sTm
	yTh = (lT + phiH*bT) * sTmh
	return
}

func (r *holtWintersReducer) setForecated(i int) {
	if i == 0 {
		return
	}
	for j, v := range r.forecasted {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			r.forecasted[j] = 0.0
		}
	}
	if r.includeFitData {
		r.startTime = r.pointsTime[0]
	} else {
		r.forecasted = r.forecasted[(len(r.forecasted) - r.h):len(r.forecasted)]
		r.startTime = r.pointsTime[len(r.pointsTime)-1] + r.interval
	}
	r.y = r.y[0:0]
	r.pointsTime = r.pointsTime[0:0]
	r.pointsVal = r.pointsVal[0:0]
}

func (r *holtWintersReducer) forecast(i int) {
	r.constrain(r.parameters) // Constrain parameters
	yT := r.y[0]
	phi := r.parameters[3]
	lT := r.parameters[4]
	bT := r.parameters[5]
	phiH := phi
	var seasonals []float64
	var m, so int
	if r.seasonal {
		seasonals = r.parameters[6:]
		m = len(r.parameters[6:])
		if m == 1 {
			seasonals[0] = 1
		}
		so = m - 1
	}
	if i == 0 {
		r.forecasted = make([]float64, len(r.y))
	} else {
		r.forecasted = make([]float64, r.h+len(r.y))
	}
	l := len(r.forecasted)
	r.forecasted[0] = yT
	var hm int
	stm, stmh := 1.0, 1.0
	for t := 1; t < l; t++ {
		if r.seasonal {
			hm = (t) % m
			stm = seasonals[((t)-m+so)%m]
			stmh = seasonals[((t)-m+hm+so)%m]
		}
		var sT float64
		yT, lT, bT, sT = r.next(r.parameters[0], r.parameters[1], r.parameters[2], phi, phiH, yT, lT, bT, stm, stmh)
		phiH += math.Pow(phi, float64(t))
		if r.seasonal {
			seasonals[(t+so)%m] = sT
			so++
		}
		r.forecasted[t] = yT
	}
	r.setForecated(i)
}

// Compute sum squared error for the given parameters.
func (r *holtWintersReducer) sse(params []float64) float64 {
	sse := 0.0
	r.parameters = params
	r.forecast(0)
	for i := range r.forecasted {
		// Skip missing values since we cannot use them to compute an error.
		if !math.IsNaN(r.y[i]) {
			// Compute error
			if math.IsNaN(r.forecasted[i]) {
				// Penalize forecasted NaNs
				return math.Inf(1)
			}
			diff := r.forecasted[i] - r.y[i]
			sse += diff * diff
		}
	}
	r.forecasted = r.forecasted[0:0]
	return sse
}

func (r *holtWintersReducer) roundTime(t int64) int64 {
	// Overflow safe round function
	remainder := t % r.interval
	if remainder > r.halfInterval {
		// Round up
		return (t/r.interval + 1) * r.interval
	}
	// Round down
	return (t / r.interval) * r.interval
}

func (r *holtWintersReducer) initPoints() {
	// First fill in r.y with values and NaNs for missing values
	r.y = append(r.y, r.pointsVal[0])
	t := r.roundTime(r.pointsTime[0])
	i := 1
	for _, p := range r.pointsVal[1:] {
		rounded := r.roundTime(r.pointsTime[i])
		if rounded <= t {
			// Drop values that occur for the same time bucket
			i++
			continue
		}
		t += r.interval
		// Add any missing values before the next point
		for rounded != t {
			// Add in a NaN so we can skip it later.
			r.y = append(r.y, math.NaN())
			t += r.interval
		}
		r.y = append(r.y, p)
		i++
	}
}

func (r *holtWintersReducer) Emit() {
	if r.m > len(r.pointsTime) || len(r.pointsTime) < 2 {
		r.pointsTime = r.pointsTime[0:0]
		r.pointsVal = r.pointsVal[0:0]
		return
	}
	r.initParameters()
	r.forecast(1)
}

func (r *holtWintersReducer) initStartPara() (float64, float64) {
	l0 := 0.0
	if r.seasonal {
		for i := 0; i < r.m; i++ {
			if !math.IsNaN(r.y[i]) {
				l0 += (1 / float64(r.m)) * r.y[i]
			}
		}
	} else {
		l0 += hwWeight * r.y[0]
	}
	b0 := 0.0
	if r.seasonal {
		for i := 0; i < r.m && r.m+i < len(r.y); i++ {
			if !math.IsNaN(r.y[i]) && !math.IsNaN(r.y[r.m+i]) {
				b0 += 1 / float64(r.m*r.m) * (r.y[r.m+i] - r.y[i])
			}
		}
	} else {
		if !math.IsNaN(r.y[1]) {
			b0 = hwWeight * (r.y[1] - r.y[0])
		}
	}
	return l0, b0
}

func (r *holtWintersReducer) initParameters() {
	r.initPoints()
	l0, b0 := r.initStartPara()
	var s []float64
	if r.seasonal {
		s = make([]float64, r.m)
		for i := 0; i < r.m; i++ {
			if !math.IsNaN(r.y[i]) {
				s[i] = r.y[i] / l0
			} else {
				s[i] = 0
			}
		}
	}
	r.parameters = make([]float64, 6+len(s))
	r.parameters[4] = l0
	r.parameters[5] = b0
	o := len(r.parameters) - len(s)
	for i := range s {
		r.parameters[i+o] = s[i]
	}
	minSSE := math.Inf(1)
	var bestParams []float64
	for alpha := hwGuessLower; alpha < hwGuessUpper; alpha += hwGuessStep {
		for beta := hwGuessLower; beta < hwGuessUpper; beta += hwGuessStep {
			for gamma := hwGuessLower; gamma < hwGuessUpper; gamma += hwGuessStep {
				for phi := hwGuessLower; phi < hwGuessUpper; phi += hwGuessStep {
					r.parameters[0] = alpha
					r.parameters[1] = beta
					r.parameters[2] = gamma
					r.parameters[3] = phi
					r.parameters[4] = l0
					r.parameters[5] = b0
					for i := range s {
						r.parameters[i+o] = s[i]
					}
					sse, params := r.optim.Optimize(r.sse, r.parameters, r.epsilon, 1)
					if sse < minSSE || len(bestParams) == 0 {
						minSSE = sse
						bestParams = params
					}
				}
			}
		}
	}
	r.parameters = bestParams
}

type holtWinters struct {
	index int
	hwr   *holtWintersReducer
}

func newHoltWinters(index int, h int, m int, withFit bool, interval time.Duration) *holtWinters {
	return &holtWinters{
		index: index,
		hwr:   NewHoltWintersReducer(h, m, withFit, interval),
	}
}

func (hw *holtWinters) setColumnRange(inEle *holtWintersChunk, start int, end int) {
	col := inEle.chunk.Column(hw.index)
	nstart, nend := col.GetRangeValueIndexV2(start, end)
	var time []int64
	if col.BitMap().array != nil {
		for _, index := range col.BitMap().array {
			time = append(time, inEle.chunk.TimeByIndex((int)(index)))
		}
	} else {
		time = inEle.chunk.Time()[start:end]
	}
	pointsLen := nend - nstart
	if col.DataType() == influxql.Integer {
		for ; nstart < nend; nstart++ {
			hw.hwr.pointsVal = append(hw.hwr.pointsVal, float64(col.IntegerValue(nstart)))
		}
	} else if col.DataType() == influxql.Float {
		hw.hwr.pointsVal = append(hw.hwr.pointsVal, col.FloatValues()[nstart:nend]...)
	}
	if len(time) == pointsLen {
		hw.hwr.pointsTime = append(hw.hwr.pointsTime, time...)
	} else {
		hw.hwr.pointsVal = hw.hwr.pointsVal[0:0]
	}
}

func (hw *holtWinters) appendValue(ocol Column, time int64) {
	if time == hw.hwr.startTime && hw.hwr.forecastedLoc < len(hw.hwr.forecasted) {
		ocol.AppendFloatValues(hw.hwr.forecasted[hw.hwr.forecastedLoc])
		ocol.AppendManyNotNil(1)
		hw.hwr.forecastedLoc++
		hw.hwr.startTime += hw.hwr.interval
	} else {
		ocol.AppendManyNil(1)
	}
}

type holtWintersChunk struct {
	chunk      Chunk
	tagsLoc    int
	endTagsLoc int
}

func newHoltWintersChunk(c Chunk) *holtWintersChunk {
	re := &holtWintersChunk{
		chunk:      c.Clone(),
		tagsLoc:    0,
		endTagsLoc: 0,
	}
	return re
}

func (hwc *holtWintersChunk) setEndTagsLoc(endLoc int) {
	hwc.endTagsLoc = endLoc
}

type HoltWintersTransform struct {
	BaseProcessor
	hwsMap       map[int]*holtWinters
	bufChunk     Chunk
	inputChunks  *list.List
	outputChunks []Chunk
	withTags     int
	chunkBuilder *ChunkBuilder

	nextChunk   chan struct{}
	reduceChunk chan struct{}

	Input             ChunkPort
	Output            ChunkPort
	opt               query.ProcessorOptions
	holtWintersLogger *logger.Logger
	schema            hybridqp.Catalog
	holtWintersCost   *tracing.Span
	startTime         int64
	endTime           int64
	tagKeys           []string
	tagVals           []string
}

func NewHoltWintersTransform(
	inRowDataType, outRowDataType hybridqp.RowDataType, opt query.ProcessorOptions, schema hybridqp.Catalog,
) (*HoltWintersTransform, error) {
	trans := &HoltWintersTransform{
		opt:               opt,
		Input:             *NewChunkPort(inRowDataType),
		Output:            *NewChunkPort(outRowDataType),
		holtWintersLogger: logger.NewLogger(errno.ModuleQueryEngine),
		bufChunk:          nil,
		schema:            schema,
		nextChunk:         make(chan struct{}),
		reduceChunk:       make(chan struct{}),
		inputChunks:       list.New(),
		outputChunks:      make([]Chunk, 0),
		hwsMap:            make(map[int]*holtWinters, 0),
		withTags:          1,
		tagKeys:           make([]string, 0),
		tagVals:           make([]string, 0),
	}
	trans.startTime, _ = trans.opt.Window(trans.opt.StartTime)
	trans.endTime, _ = trans.opt.Window(trans.opt.EndTime)
	if err := trans.initHwsMap(); err != nil {
		return nil, err
	}
	if err := trans.checkInputDatatype(); err != nil {
		return nil, err
	}
	trans.initOutputDataType()
	trans.chunkBuilder = NewChunkBuilder(trans.Output.RowDataType)
	return trans, nil
}

type HoltWintersTransformCreator struct {
}

func (c *HoltWintersTransformCreator) Create(plan LogicalPlan, opt query.ProcessorOptions) (Processor, error) {
	p, err := NewHoltWintersTransform(plan.Children()[0].RowDataType(), plan.RowDataType(), opt, plan.Schema())
	return p, err
}

var _ = RegistryTransformCreator(&LogicalHoltWinters{}, &HoltWintersTransformCreator{})

func (trans *HoltWintersTransform) checkInputDatatype() error {
	for _, f := range trans.Input.RowDataType.Fields() {
		val := f.Name()
		index := trans.Input.RowDataType.IndexByName()[val]
		if _, ok := trans.hwsMap[index]; ok {
			if f.Expr.(*influxql.VarRef).Type != influxql.Integer && f.Expr.(*influxql.VarRef).Type != influxql.Float {
				return errno.NewError(errno.UnsupportedHoltWinterInit)
			}
		}
	}
	return nil
}

func (trans *HoltWintersTransform) addHw(index int, h int, m int, hwWithFit bool) {
	hw := newHoltWinters(index, h, m, hwWithFit, time.Duration(trans.schema.Options().GetInterval().Nanoseconds()))
	trans.hwsMap[index] = hw
}

func (trans *HoltWintersTransform) initOutputDataType() {
	for _, hw := range trans.hwsMap {
		loc := hw.index
		expr := &(trans.Output.RowDataType.Fields()[loc].Expr)
		if e, ok := (*expr).(*influxql.VarRef); ok {
			e.Type = influxql.Float
		}
	}
}

func (trans *HoltWintersTransform) initHwsMap() error {
	if err := trans.initSingleHwsMap(); err != nil {
		return err
	}
	if err := trans.initSubHwsMap(); err != nil {
		return err
	}
	return nil
}

func (trans *HoltWintersTransform) initSingleHwsMap() error {
	hwWithFit := false
	fields := trans.Input.RowDataType.Fields()
	indexs := trans.Input.RowDataType.IndexByName()
	for _, f := range fields {
		val := f.Expr.(*influxql.VarRef).Val
		funcName := ""
		hwNum := 0
		var err error
		if strings.HasPrefix(val, "holt_winters") {
			hwWithFit = false
			funcName = "holt_winters"
			if strings.HasPrefix(val, "holt_winters_with_fit") {
				funcName = "holt_winters_with_fit"
				hwWithFit = true
			}
			if len(funcName) == len(val) {
				hwNum = 0
			} else {
				hwNum, err = strconv.Atoi(val[len(funcName)+1:])
				if err != nil {
					return errno.NewError(errno.UnsupportedHoltWinterInit)
				}
			}
		} else {
			continue
		}
		i := 0
		for _, call := range trans.schema.HoltWinters() {
			if call.Alias != "" {
				continue
			}
			c, _ := call.Expr.(*influxql.Call)
			if hwNum == i && c.Name == funcName {
				h := int(c.Args[1].(*influxql.IntegerLiteral).Val)
				seasonal := int(c.Args[2].(*influxql.IntegerLiteral).Val)
				index, ok := indexs[val]
				if !ok {
					return errno.NewError(errno.UnsupportedHoltWinterInit)
				}
				trans.addHw(index, h, seasonal, hwWithFit)
			}
			if c.Name == funcName {
				i++
			}
		}
	}
	return nil
}

func (trans *HoltWintersTransform) initSubHwsMap() error {
	qfields := trans.schema.GetQueryFields()
	indexs := trans.Input.RowDataType.IndexByName()
	aliasMap := make(map[string]*influxql.Expr, 0)
	for _, f := range qfields {
		alias := f.Alias
		if alias == "" {
			continue
		}
		var ok bool
		if _, ok = aliasMap[alias]; ok {
			return errno.NewError(errno.UnsupportedHoltWinterInit)
		}
		aliasMap[alias] = &f.Expr
		var call *influxql.Call
		if call, ok = f.Expr.(*influxql.Call); !ok {
			continue
		}
		hwWithFit := false
		if call.Name == "holt_winters" {
			hwWithFit = false
		} else if call.Name == "holt_winters_with_fit" {
			hwWithFit = true
		} else {
			continue
		}
		h := int(call.Args[1].(*influxql.IntegerLiteral).Val)
		seasonal := int(call.Args[2].(*influxql.IntegerLiteral).Val)
		index, ok := indexs[alias]
		if !ok {
			return errno.NewError(errno.UnsupportedHoltWinterInit)
		}
		trans.addHw(index, h, seasonal, hwWithFit)
	}
	return nil
}

func (trans *HoltWintersTransform) addInputChunk() {
	hwChunk := newHoltWintersChunk(trans.bufChunk)
	trans.inputChunks.PushBack(hwChunk)
}

func (trans *HoltWintersTransform) setTags(keys []string, vals []string) {
	trans.tagKeys = keys
	trans.tagVals = vals
}

func (trans *HoltWintersTransform) tagsEqu(keys []string, vals []string) bool {
	if len(trans.tagKeys) != len(keys) || len(trans.tagVals) != len(vals) {
		return false
	}
	for i, k := range keys {
		if k != trans.tagKeys[i] {
			return false
		}
	}
	for i, v := range vals {
		if v != trans.tagVals[i] {
			return false
		}
	}
	return true
}

func (trans *HoltWintersTransform) appendValue(ocol Column, icol Column, start int, iHasTime bool) {
	if !iHasTime || icol.IsNilV2(start) {
		ocol.AppendNil()
		return
	}
	datatype := icol.DataType()
	start = icol.GetValueIndexV2(start)
	switch datatype {
	case influxql.Integer:
		ocol.AppendIntegerValues(icol.IntegerValue(start))
	case influxql.Float:
		ocol.AppendFloatValues(icol.FloatValue(start))
	case influxql.Boolean:
		ocol.AppendBooleanValues(icol.BooleanValue(start))
	case influxql.String:
		ocol.AppendStringValues(icol.StringValue(start))
	case influxql.Tag:
		ocol.AppendStringValues(icol.StringValue(start))
	}
	ocol.AppendNilsV2(true)
}

func (trans *HoltWintersTransform) getMinHwsStartTime() int64 {
	var startTime int64 = math.MaxInt64
	noForecatedNum := 0
	for _, hw := range trans.hwsMap {
		if len(hw.hwr.forecasted) == 0 {
			noForecatedNum++
		} else {
			if hw.hwr.startTime < startTime {
				startTime = hw.hwr.startTime
			}
		}
	}
	if noForecatedNum == len(trans.hwsMap) {
		return -1
	} else {
		return startTime
	}
}

func (trans *HoltWintersTransform) getMaxHwsEndTime() int64 {
	var endTime int64 = math.MinInt64
	noForecatedNum := 0
	for _, hw := range trans.hwsMap {
		if len(hw.hwr.forecasted) == 0 {
			noForecatedNum++
		} else {
			tempEndTime := int64(len(hw.hwr.forecasted)-1)*hw.hwr.interval + hw.hwr.startTime
			if tempEndTime > endTime {
				endTime = tempEndTime
			}
		}
	}
	if noForecatedNum == len(trans.hwsMap) {
		return -1
	} else {
		return endTime
	}
}

func (trans *HoltWintersTransform) appendSeries(ichunkEle *holtWintersChunk, oStartTime *int64, oEndTime int64, ochunk Chunk) {
	ichunk := ichunkEle.chunk
	for tagsLoc := &ichunkEle.tagsLoc; *tagsLoc <= ichunkEle.endTagsLoc; (*tagsLoc)++ {
		ochunk.AddTagAndIndex(ichunk.Tags()[*tagsLoc], ochunk.Len())
		start := ichunk.TagIndex()[*tagsLoc]
		end := 0
		if *tagsLoc == ichunk.TagLen()-1 {
			end = ichunk.NumberOfRows()
		} else {
			end = ichunk.TagIndex()[*tagsLoc+1]
		}
		for ; *oStartTime <= oEndTime; *oStartTime += trans.opt.Interval.Duration.Nanoseconds() {
			iHasTime := false
			if start < end {
				iTime := ichunk.TimeByIndex(start)
				if iTime == *oStartTime {
					iHasTime = true
					start++
				}
			} else {
				break
			}
			ochunk.AppendTime(*oStartTime)
			for i, icol := range ichunk.Columns() {
				ocol := ochunk.Column(i)
				if hw, ok := trans.hwsMap[i]; ok {
					hw.appendValue(ocol, *oStartTime)
				} else {
					trans.appendValue(ocol, icol, start-1, iHasTime)
				}
			}

		}
	}
}

func (trans *HoltWintersTransform) appendLastForecated(ochunk Chunk) Chunk {
	for _, hw := range trans.hwsMap {
		j := hw.hwr.forecastedLoc
		for {
			if j >= len(hw.hwr.forecasted) {
				break
			}
			time := hw.hwr.startTime
			if ochunk.TimeByIndex(ochunk.Len()-1) < time || ochunk.Len() == 0 {
				if ochunk.Len() >= trans.opt.ChunkSize {
					newOchunk := trans.chunkBuilder.NewChunk(ochunk.Name())
					if ochunk.TagLen() > 0 {
						newOchunk.AddTagAndIndex(ochunk.Tags()[ochunk.TagLen()-1], newOchunk.Len())
					}
					trans.outputChunks = append(trans.outputChunks, newOchunk)
					ochunk = newOchunk
				}
				ochunk.AppendTime(time)
				for i, ocol := range ochunk.Columns() {
					if ihw, ok := trans.hwsMap[i]; ok {
						ihw.appendValue(ocol, time)
					} else {
						ocol.AppendNil()
					}
				}
			}
			j++
		}
	}
	return ochunk
}

func (trans *HoltWintersTransform) appendLastPoints(oStartTime *int64, oEndTime int64, ochunk Chunk) {
	for ; *oStartTime <= oEndTime; *oStartTime += trans.opt.Interval.Duration.Nanoseconds() {
		ochunk.AppendTime(*oStartTime)
		for _, ocol := range ochunk.Columns() {
			ocol.AppendNil()
		}
	}
}

func (trans *HoltWintersTransform) generateOutPut() {
	defer func() {
		trans.outputChunks = trans.outputChunks[0:0]
	}()
	if trans.inputChunks.Len() == 0 {
		return
	}
	oStartTime := trans.startTime
	oEndTime := trans.endTime
	if len(trans.hwsMap) == trans.Output.RowDataType.NumColumn() {
		startTime, endTime := trans.allHwsRange()
		if startTime != -1 {
			oStartTime = startTime
			oEndTime = endTime
		}
	}
	for ; trans.inputChunks.Len() > 0; trans.inputChunks.Remove(trans.inputChunks.Front()) {
		var ichunkEle *holtWintersChunk
		var ok bool
		if ichunkEle, ok = trans.inputChunks.Front().Value.(*holtWintersChunk); !ok {
			continue
		}
		ochunk := trans.chunkBuilder.NewChunk(ichunkEle.chunk.Name())
		trans.outputChunks = append(trans.outputChunks, ochunk)
		trans.appendSeries(ichunkEle, &oStartTime, oEndTime, ochunk)
		if ichunkEle.endTagsLoc < ichunkEle.chunk.TagLen()-1 {
			ichunkEle.endTagsLoc++
			break
		}
	}
	ochunk := trans.lastOutputChunk()
	if ochunk.Len() == 0 || ochunk == nil {
		return
	}
	ochunk = trans.appendLastForecated(ochunk)
	oStartTime = ochunk.TimeByIndex(ochunk.Len()-1) + trans.opt.Interval.Duration.Nanoseconds()
	trans.appendLastPoints(&oStartTime, oEndTime, ochunk)
	for _, c := range trans.outputChunks {
		trans.sendChunk(c)
	}
	for _, hw := range trans.hwsMap {
		hw.hwr.ClearForcasted()
	}
}

func (trans *HoltWintersTransform) lastOutputChunk() Chunk {
	var ochunk Chunk = nil
	i := len(trans.outputChunks) - 1
	for ; i >= 0; i-- {
		ochunk = trans.outputChunks[i]
		if ochunk.Len() > 0 {
			break
		}
	}
	trans.outputChunks = trans.outputChunks[0 : i+1]
	return ochunk
}

func (trans *HoltWintersTransform) allHwsRange() (int64, int64) {
	startTime := trans.getMinHwsStartTime()
	endTime := trans.getMaxHwsEndTime()
	return startTime, endTime
}

func (trans *HoltWintersTransform) computeHoltWinters() {
	inEle := newHoltWintersChunk(trans.bufChunk)
	if trans.bufChunk.TagLen() == 0 || (trans.bufChunk.TagLen() == 1 && trans.bufChunk.Tags()[0].subset == nil) {
		for _, hw := range trans.hwsMap {
			hw.setColumnRange(inEle, 0, trans.bufChunk.NumberOfRows())
		}
		inEle.setEndTagsLoc(0)
		trans.inputChunks.PushBack(inEle)
		trans.withTags = 2
	} else {
		if trans.withTags == 2 {
			panic("holt_winter runing error")
		}
		for i, tags := range trans.bufChunk.Tags() {
			start := trans.bufChunk.TagIndex()[i]
			var end int
			if i == trans.bufChunk.TagLen()-1 {
				end = trans.bufChunk.NumberOfRows()
			} else {
				end = trans.bufChunk.TagIndex()[i+1]
			}
			tagKeys, tagVals := tags.GetChunkTagAndValues()
			if len(trans.tagKeys) == 0 {
				trans.setTags(tagKeys, tagVals)
				for _, hw := range trans.hwsMap {
					hw.setColumnRange(inEle, start, end)
				}
				trans.inputChunks.PushBack(inEle)
				continue
			}
			if !trans.tagsEqu(tagKeys, tagVals) {
				for _, hw := range trans.hwsMap {
					hw.hwr.Emit()
				}
				trans.generateOutPut()
			}
			for _, hw := range trans.hwsMap {
				hw.setColumnRange(inEle, start, end)
			}
			inEle.setEndTagsLoc(i)
			trans.inputChunks.PushBack(inEle)
			trans.setTags(tagKeys, tagVals)
		}
	}
}

func (trans *HoltWintersTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[HoltWinters]TotalWorkCost", false)
	trans.holtWintersCost = tracing.Start(span, "limit_cost", false)
	defer func() {
		tracing.Finish(span, trans.holtWintersCost)
	}()
	runnable := func() {
		for {
			select {
			case c, ok := <-trans.Input.State:
				tracing.StartPP(span)
				if !ok {
					trans.doForLast()
					return
				}
				trans.bufChunk = c
				trans.computeHoltWinters()
				tracing.EndPP(span)
			case <-ctx.Done():
				return
			}
		}
	}
	runnable()
	trans.Close()
	return nil
}

func (trans *HoltWintersTransform) doForLast() {
	for _, hw := range trans.hwsMap {
		hw.hwr.Emit()
	}
	trans.generateOutPut()
}

func (trans *HoltWintersTransform) sendChunk(chunk Chunk) {
	if chunk.Len() > 0 {
		trans.Output.State <- chunk
	}
}

func (trans *HoltWintersTransform) Name() string {
	return "HoltWintersTransform"
}

func (trans *HoltWintersTransform) Explain() []ValuePair {
	return nil
}

func (trans *HoltWintersTransform) Close() {
	trans.Output.Close()
}

func (trans *HoltWintersTransform) GetOutputs() Ports {
	ports := make(Ports, 0, 1)
	ports = append(ports, &trans.Output)
	return ports
}

func (trans *HoltWintersTransform) GetInputs() Ports {
	ports := make(Ports, 0, 1)
	ports = append(ports, &trans.Input)
	return ports
}

func (trans *HoltWintersTransform) GetOutputNumber(port Port) int {
	if &trans.Output == port {
		return 0
	}
	return INVALID_NUMBER
}

func (trans *HoltWintersTransform) GetInputNumber(port Port) int {
	if &trans.Input == port {
		return 0
	}
	return INVALID_NUMBER
}
