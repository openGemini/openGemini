// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// nolint
package executor

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"github.com/openGemini/openGemini/lib/tracing"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

const (
	LenOfValuers  = 2 // MathValuer + MapValuer
	ZeroTimeStamp = 0
)

type AbortProcessor interface {
	AbortSinkTransform()
}

type HttpChunkSender struct {
	buffRows models.Rows
	RowChunk RowChunk
	opt      *query.ProcessorOptions

	rowsGenerator *RowsGenerator

	except    bool
	count     int
	offsetPos int
	limit     int
	offset    int
	prevRow   *models.Row
	trans     AbortProcessor
}

func NewHttpChunkSender(opt *query.ProcessorOptions) *HttpChunkSender {
	h := &HttpChunkSender{
		opt:           opt,
		rowsGenerator: NewRowsGenerator(),
		except:        opt.IsExcept(),
		limit:         opt.GetLimit(),
		offset:        opt.GetOffset(),
	}

	if h.opt.Location == nil {
		h.opt.Location = time.UTC
	}
	return h
}

func (w *HttpChunkSender) Write(chunk Chunk, lastChunk bool) bool {
	w.GenRows(chunk)

	var chunkedRow models.Rows
	var partial bool

	var EmitPartialRow = func() {
		// return partial rows for this series
		chunkedRow = models.Rows{
			&models.Row{
				Name:    w.buffRows[0].Name,
				Tags:    w.buffRows[0].Tags,
				Columns: w.buffRows[0].Columns,
				Values:  w.buffRows[0].Values[:w.opt.ChunkedSize],
				Partial: true,
			},
		}
		leftSeriesRow := &models.Row{
			Name:    w.buffRows[0].Name,
			Tags:    w.buffRows[0].Tags,
			Columns: w.buffRows[0].Columns,
			Values:  w.buffRows[0].Values[w.opt.ChunkedSize:],
		}
		w.buffRows = append(models.Rows{leftSeriesRow}, w.buffRows[1:]...)

		w.sendRows(chunkedRow, true)
		partial = true
	}

	if w.buffRows.Len() == 0 {
		return false
	} else if w.buffRows.Len() == 1 {
		if len(w.buffRows[0].Values) > w.opt.ChunkedSize {
			EmitPartialRow()
		}
		// May next Chunk has the same tag as this buffRow
	} else {
		if len(w.buffRows[0].Values) <= w.opt.ChunkedSize {
			// return left rows for this series
			chunkedRow = append(models.Rows{}, w.buffRows[0])
			w.buffRows = w.buffRows[1:]
			w.sendRows(chunkedRow, true)
			partial = true
		} else if len(w.buffRows[0].Values) > w.opt.ChunkedSize {
			EmitPartialRow()
		}
	}

	if !partial && lastChunk {
		// return the last rows
		w.sendRows(w.buffRows, false)
		w.buffRows = nil
		partial = false
	}
	return partial
}

func (w *HttpChunkSender) GenRows(chunk Chunk) {
	if chunk == nil || (w.except && 0 < w.limit+w.offset && w.limit+w.offset <= w.count) {
		return
	}

	statistics.ExecutorStat.SinkRows.Push(int64(chunk.NumberOfRows()))
	rows := w.rowsGenerator.Generate(chunk, w.opt.Location)
	if w.except {
		for i := 0; i < len(rows); i++ {
			rows[i].Values = removeDuplicationValues(rows[i].Values)
		}
	}

	// May next Chunk has the same tag as this buffRow
	if rows.Len() > 0 && w.buffRows.Len() > 0 {
		firstRow := rows[0]
		lastRow := w.buffRows[len(w.buffRows)-1]
		if lastRow.Name == firstRow.Name && hybridqp.EqualMap(lastRow.Tags, firstRow.Tags) {
			lastRow.Values = append(lastRow.Values, firstRow.Values...)
			if w.except {
				lastRow.Values = removeDuplicationValues(lastRow.Values)
			}
			rows = rows[1:]
		}
	}

	if !w.except || (w.limit+w.offset) == 0 {
		w.buffRows = append(w.buffRows, rows...)
		return
	}
	if w.prevRow != nil && rows.Len() > 0 {
		firstRow, lastRow := w.prevRow, rows[0]
		if lastRow.Name == firstRow.Name && hybridqp.EqualMap(lastRow.Tags, firstRow.Tags) {
			lastRow.Values = RemoveCommonValues(firstRow.Values, lastRow.Values)
			if len(lastRow.Values) == 0 {
				rows = rows[1:]
			}
		}
	}
	if rows.Len() == 0 {
		return
	}
	w.prevRow = rows[rows.Len()-1]
	w.exceptLimit(rows)
}

func (w *HttpChunkSender) exceptLimit(rows models.Rows) {
	for i := range rows {
		count := len(rows[i].Values)
		w.count += count
		if w.count >= w.limit+w.offset {
			start := w.offset - w.offsetPos
			end := w.limit + w.offset - (w.count - count)
			if start < end {
				rows[i].Values = rows[i].Values[start:end]
				w.buffRows = append(w.buffRows, rows[i])
			}
			w.trans.AbortSinkTransform()
			break
		}

		if w.count < w.offset {
			w.offsetPos += count
			continue
		}
		if w.offset == w.offsetPos {
			w.buffRows = append(w.buffRows, rows[i])
		} else {
			if remain := w.offset - w.offsetPos; remain < count {
				rows[i].Values = rows[i].Values[remain:]
				w.buffRows = append(w.buffRows, rows[i])
			}
			w.offsetPos = w.offset
		}
	}
}

func (w *HttpChunkSender) SetAbortProcessor(trans AbortProcessor) {
	w.trans = trans
}

func removeDuplicationValues(values [][]interface{}) [][]interface{} {
	length := len(values)
	if length == 0 {
		return values
	}

	j := 0
	for i := 1; i < length; i++ {
		if values[i][0] != values[j][0] {
			j++
			if j < i {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
	return values[:j+1]
}

func RemoveCommonValues(prev, curr [][]interface{}) [][]interface{} {
	if len(prev) == 0 || len(curr) == 0 {
		return curr
	}
	if prev[len(prev)-1][0].(time.Time).Before(curr[0][0].(time.Time)) {
		return curr
	}

	i, j, k := 0, 0, 0
	for i < len(prev) && j < len(curr) {
		if prev[i][0] == curr[j][0] {
			// Skip the element in curr since it is common to both
			i++
			j++
		} else if prev[i][0].(time.Time).Before(curr[j][0].(time.Time)) {
			// Move pointer in prev
			i++
		} else {
			// Element only exists in curr, place it at position k and move pointers
			curr[k] = curr[j]
			k++
			j++
		}
	}

	// Append remaining elements from curr
	for j < len(curr) {
		curr[k] = curr[j]
		k++
		j++
	}

	// Truncate curr to the new length
	curr = curr[:k]
	return curr
}

// GetRows transfer Chunk to models.Rows
func (w *HttpChunkSender) GetRows(chunk Chunk) models.Rows {
	if chunk == nil {
		return w.buffRows
	}

	statistics.ExecutorStat.SinkRows.Push(int64(chunk.NumberOfRows()))
	w.RowChunk.RowsInit(chunk)
	w.RowChunk.Series = w.RowChunk.RowsGen(chunk)
	s := w.RowChunk.Series
	rows := make([]*Rows, len(chunk.Tags()))
	for i := 0; i < len(chunk.Tags()); i++ {
		rows[i] = &Rows{}
		rows[i].Series = Series{
			Name: chunk.Name(),
			Tags: chunk.Tags()[i],
			id:   uint64(i + 1),
		}
		rows[i].invalidSize = len(s[i].time)
		rows[i].Time = s[i].time
		rows[i].Values = make([][]interface{}, len(s[i].time))
		for j := 0; j < len(s[i].time); j++ {
			for k := 0; k < len(s[i].values); k++ {
				if k == 0 {
					rows[i].Values[j] = make([]interface{}, len(s[i].values)+1)
					rows[i].Values[j][0] = time.Unix(0, rows[i].Time[j]).In(w.opt.Location)
				}
				rows[i].Values[j][k+1] = s[i].values[k][j]
			}
		}
	}
	if len(rows) == 0 {
		rows = nil
		return w.buffRows
	}
	if w.buffRows.Len() == 0 {
		w.buffRows = make([]*models.Row, 0, len(rows))
	}

	firstRows := rows[0]
	if w.buffRows.Len() > 0 {
		lastBuffRow := w.buffRows[len(w.buffRows)-1]
		if lastBuffRow.Name == firstRows.Series.Name {
			buffTags := lastBuffRow.Tags
			tags := firstRows.Series.Tags.KeyValues()
			if hybridqp.EqualMap(buffTags, tags) {
				lastBuffRow.Values = append(lastBuffRow.Values, firstRows.Values...)
				rows = rows[1:]
			}
		}
	}
	for _, r := range rows {
		row := &models.Row{
			Name:    r.Series.Name,
			Tags:    r.Series.Tags.KeyValues(),
			Columns: append([]string{"time"}, w.RowChunk.ColumnName...),
			Values:  r.Values,
		}
		w.buffRows = append(w.buffRows, row)
	}

	return w.buffRows
}

func (w *HttpChunkSender) sendRows(rows models.Rows, partial bool) {
	rc := query.RowsChan{
		Rows:    rows,
		Partial: partial,
	}

	if w.opt.AbortChan == nil {
		w.opt.RowsChan <- rc
		return
	}

	select {
	case w.opt.RowsChan <- rc:
	case <-w.opt.AbortChan:
	}
}

func (w *HttpChunkSender) Release() {
	if w.rowsGenerator != nil {
		w.rowsGenerator.Release()
		w.rowsGenerator = nil
	}
}

type RowChunk struct {
	Name string
	Tags []ChunkTags

	Series     []*Row
	ColumnName []string
}

type Row struct {
	time   []int64
	values [][]interface{}
}

func (r *RowChunk) RowsInit(c Chunk) {
	if c.Name() != r.Name {
		r.ColumnName = make([]string, c.NumberOfCols())
		for i, f := range c.RowDataType().Fields() {
			r.ColumnName[i] = f.Name() // TODO....
		}
	}
	r.Name = c.Name()
	r.Tags = c.Tags()
	if cap(r.Series) < len(r.Tags) {
		r.Series = make([]*Row, len(r.Tags))
		for i := range r.Tags {
			r.Series[i] = &Row{}
		}
	} else {
		r.Series = r.Series[:len(r.Tags)]
	}
}

func (r *RowChunk) RowsGen(c Chunk) []*Row {
	var start, end int
	index := 0
	for index < len(c.TagIndex()) {
		series := r.Series[index]
		seriesValues := series.values
		if cap(seriesValues) < c.NumberOfCols() {
			seriesValues = make([][]interface{}, c.NumberOfCols())
		} else {
			seriesValues = seriesValues[:c.NumberOfCols()]
		}
		for i := range seriesValues {
			if cap(seriesValues[i]) < c.NumberOfRows() {
				seriesValues[i] = make([]interface{}, 0, c.NumberOfRows())
			} else {
				seriesValues[i] = seriesValues[i][:0]
			}
		}
		start = c.TagIndex()[index]
		if start == c.TagIndex()[len(c.TagIndex())-1] {
			end = len(c.Time())
		} else {
			end = c.TagIndex()[index+1]
		}
		for i, col := range c.Columns() {
			length := len(seriesValues[i])
			seriesValues[i] = seriesValues[i][:length+end-start]
			// seriesValues[i][length:][:0] : reference this memory
			seriesValues[i] = GetColValsFn[col.DataType()](col, start, end, c.Len(), seriesValues[i][length:][:0])
		}
		r.Series[index].time = c.Time()[start:end]
		r.Series[index].values = seriesValues
		index++
	}
	return r.Series
}

type RowsGenerator struct {
	name        string
	columnNames []string
	values      []interface{}
	buf         []byte
	rows        []models.Row
}

var rowsGeneratorPool sync.Pool

func NewRowsGenerator() *RowsGenerator {
	rg, ok := rowsGeneratorPool.Get().(*RowsGenerator)
	if !ok || rg == nil {
		rg = &RowsGenerator{}
	}
	rg.Reset()
	return rg
}

func (g *RowsGenerator) Release() {
	rowsGeneratorPool.Put(g)
}

func (g *RowsGenerator) Reset() {
	g.name = ""
	// pre-allocated memory
	g.columnNames = make([]string, 0, cap(g.columnNames))
	g.buf = make([]byte, 0, cap(g.buf))
	g.values = make([]interface{}, 0, cap(g.values))
	g.rows = make([]models.Row, 0, cap(g.rows))
}

func (g *RowsGenerator) allocValues(size int) []interface{} {
	var items []interface{}
	g.values, items = util.AllocSlice(g.values, size)
	return items
}

func (g *RowsGenerator) allocBytes(size int) []byte {
	var buf []byte
	g.buf, buf = util.AllocSlice(g.buf, size)
	return buf
}

func (g *RowsGenerator) allocRows(size int) []models.Row {
	var rows []models.Row
	g.rows, rows = util.AllocSlice(g.rows, size)
	return rows
}

func (g *RowsGenerator) buildColumnNames(name string, rdt hybridqp.RowDataType) []string {
	if g.name == name {
		return g.columnNames
	}
	if g.name != "" {
		g.columnNames = make([]string, 0, len(rdt.Fields())+1)
	}

	g.name = name
	g.columnNames = append(g.columnNames[:0], "time")
	for _, f := range rdt.Fields() {
		g.columnNames = append(g.columnNames, f.Name())
	}
	return g.columnNames
}

func (g *RowsGenerator) Generate(chunk Chunk, loc *time.Location) models.Rows {
	chunkTags := chunk.Tags()
	tagIndex := chunk.TagIndex()
	times := chunk.Time()
	columns := chunk.Columns()
	name := chunk.Name()
	columnNames := g.buildColumnNames(name, chunk.RowDataType())

	rows := make(models.Rows, 0, len(tagIndex))
	tmpRows := g.allocRows(len(tagIndex))
	var start, end int
	for index := 0; index < len(tagIndex); index++ {
		start = tagIndex[index]
		if start == tagIndex[len(tagIndex)-1] {
			end = len(times)
		} else {
			end = tagIndex[index+1]
		}

		row := &tmpRows[index]
		row.Name = name
		row.Tags = chunkTags[index].KeyValues()
		row.Columns = columnNames
		row.Values = g.buildValues(end, start, times, loc, columns)
		rows = append(rows, row)
	}
	return rows
}

func (g *RowsGenerator) buildValues(end int, start int, times []int64, loc *time.Location, columns []Column) [][]interface{} {
	var values = make([][]interface{}, end-start)
	for i := 0; i < end-start; i++ {
		values[i] = g.allocValues(len(columns) + 1)
		values[i][0] = time.Unix(0, times[start+i]).In(loc)
		for j, col := range columns {
			values[i][j+1] = g.GetColValue(col, start+i)
		}
	}
	return values
}

func (g *RowsGenerator) GetColValue(col2 Column, idx int) interface{} {
	col, ok := col2.(*ColumnImpl)
	if !ok {
		return nil
	}

	if col.NilCount() > 0 {
		if col.IsNilV2(idx) {
			return nil
		}
		idx = col.GetValueIndexV2(idx)
	}

	switch col.DataType() {
	case influxql.Float:
		return col.FloatValue(idx)
	case influxql.Integer:
		return col.IntegerValue(idx)
	case influxql.Boolean:
		return col.BooleanValue(idx)
	case influxql.String, influxql.Tag:
		oriStr := col.StringValue(idx)
		newStr := g.allocBytes(len(oriStr))
		copy(newStr, oriStr)
		return util.Bytes2str(newStr)
	}
	return nil
}

type Rows struct {
	Time        []int64
	Values      [][]interface{}
	Series      Series
	invalidSize int
}

type Series struct {
	// Name is the measurement name.
	Name string

	// Tags for the series.
	Tags ChunkTags

	// This is an internal id used to easily compare if a series is the
	// same as another series. Whenever the internal cursor changes
	// to a new series, this id gets incremented. It is not exposed to
	// the user so we can implement this in whatever way we want.
	// If a series is not generated by a cursor, this id is zero and
	// it will instead attempt to compare the name and tags.
	id uint64
}

var aggregationCall = map[string]bool{
	"count": true, "distinct": true, "sum": true,
	"mean": true, "median": true, "spread": true,
	"mode": true, "stddev": true, "integral": true,
}

var transformationCall = map[string]bool{
	"difference": true, "non_negative_difference": true,
	"derivative": true, "non_negative_derivative": true,
	"elapsed": true, "histogram": true, "moving_average": true,
	"cumulative_sum": true,
}

func SetTimeZero(schema *QuerySchema) bool {
	if schema.Options().HasInterval() {
		return false
	}
	if schema.PromResetTime() {
		return true
	}
	calls := schema.calls
	if len(calls) == 0 {
		return false
	}
	if len(calls) == 1 {
		for i := range calls {
			if aggregationCall[calls[i].Name] {
				return true
			}
		}
		return false
	}
	for i := range calls {
		if transformationCall[calls[i].Name] {
			return false
		}
	}
	return true
}

type HttpSenderTransformCreator struct {
}

func (c *HttpSenderTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))

	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}

	p := NewHttpSenderTransform(inRowDataTypes[0], plan.Schema().(*QuerySchema))
	return p, nil
}

var _ = RegistryTransformCreator(&LogicalHttpSender{}, &HttpSenderTransformCreator{})

type HttpSenderTransform struct {
	BaseProcessor

	input  *ChunkPort
	schema *QuerySchema
	Sender *http.ResponseWriter
	Writer *HttpChunkSender

	dag    *TransformDag
	vertex *TransformVertex
}

func NewHttpSenderTransform(inRowDataType hybridqp.RowDataType, schema *QuerySchema) *HttpSenderTransform {
	trans := &HttpSenderTransform{
		input:  NewChunkPort(inRowDataType),
		Writer: NewHttpChunkSender(schema.Options().(*query.ProcessorOptions)),
		schema: schema,
	}

	if schema.Options().IsExcept() && schema.Options().GetLimit() > 0 {
		trans.Writer.SetAbortProcessor(trans)
	}
	return trans
}

func (trans *HttpSenderTransform) Name() string {
	return "HttpSenderTransform"
}

func (trans *HttpSenderTransform) Explain() []ValuePair {
	return nil
}

func (trans *HttpSenderTransform) Close() {

}

func (trans *HttpSenderTransform) Release() error {
	if trans.Writer != nil {
		trans.Writer.Release()
		trans.Writer = nil
	}
	return nil
}

func (trans *HttpSenderTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[HttpSender]TotalWorkCost", false)
	defer func() {
		trans.Close()
		tracing.Finish(span)
	}()

	statistics.ExecutorStat.SinkWidth.Push(int64(trans.input.RowDataType.NumColumn()))

	for {
		select {
		case chunk, ok := <-trans.input.State:
			tracing.StartPP(span)
			if !ok {
				partial := trans.Writer.Write(chunk, true)
				for partial {
					partial = trans.Writer.Write(nil, true)
				}
				return nil
			}

			partial := trans.Writer.Write(chunk, false)
			for partial {
				partial = trans.Writer.Write(nil, false)
			}
			tracing.EndPP(span)
		case <-ctx.Done():
			return nil
		}
	}
}

func (trans *HttpSenderTransform) GetOutputs() Ports {
	return Ports{}
}

func (trans *HttpSenderTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *HttpSenderTransform) GetOutputNumber(_ Port) int {
	return INVALID_NUMBER
}

func (trans *HttpSenderTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *HttpSenderTransform) SetDag(dag *TransformDag) {
	trans.dag = dag
}

func (trans *HttpSenderTransform) SetVertex(vertex *TransformVertex) {
	trans.vertex = vertex
}

func (trans *HttpSenderTransform) Visit(vertex *TransformVertex) TransformVertexVisitor {
	if !vertex.transform.IsSink() {
		return trans
	}

	if rpc, ok := vertex.transform.(*RPCReaderTransform); ok {
		rpc.Abort()
	}
	return trans
}

func (trans *HttpSenderTransform) AbortSinkTransform() {
	if trans.dag == nil || trans.vertex == nil {
		return
	}

	go func() {
		trans.dag.DepthFirstWalkVertex(trans, trans.vertex)
	}()
}

type HttpSenderHintTransformCreator struct {
}

func (c *HttpSenderHintTransformCreator) Create(plan LogicalPlan, _ *query.ProcessorOptions) (Processor, error) {
	inRowDataTypes := make([]hybridqp.RowDataType, 0, len(plan.Children()))

	for _, inPlan := range plan.Children() {
		inRowDataTypes = append(inRowDataTypes, inPlan.RowDataType())
	}

	p := NewHttpSenderHintTransform(inRowDataTypes[0], plan.Schema().(*QuerySchema))
	return p, nil
}

var (
	_ = RegistryTransformCreator(&LogicalHttpSenderHint{}, &HttpSenderHintTransformCreator{})
)

type HttpSenderHintTransform struct {
	BaseProcessor

	schema    *QuerySchema
	input     *ChunkPort
	Writer    *HttpChunkSender
	init      bool
	chunks    []Chunk
	ridIdxMap map[int]struct{} // null column index

	dag    *TransformDag
	vertex *TransformVertex
}

func NewHttpSenderHintTransform(inRowDataType hybridqp.RowDataType, schema *QuerySchema) *HttpSenderHintTransform {
	trans := &HttpSenderHintTransform{
		input:  NewChunkPort(inRowDataType),
		Writer: NewHttpChunkSender(schema.Options().(*query.ProcessorOptions)),
		schema: schema,
	}
	if schema.Options().IsExcept() && schema.Options().GetLimit() > 0 {
		trans.Writer.SetAbortProcessor(trans)
	}
	return trans
}

func (trans *HttpSenderHintTransform) Name() string {
	return "HttpSenderHintTransform"
}

func (trans *HttpSenderHintTransform) Explain() []ValuePair {
	return nil
}

func (trans *HttpSenderHintTransform) Close() {

}

func (trans *HttpSenderHintTransform) Work(ctx context.Context) error {
	span := trans.StartSpan("[HttpSenderHint]TotalWorkCost", false)
	defer func() {
		trans.Close()
		tracing.Finish(span)
	}()

	statistics.ExecutorStat.SinkWidth.Push(int64(trans.input.RowDataType.NumColumn()))

	for {
		select {
		case chunk, ok := <-trans.input.State:
			tracing.StartPP(span)
			if !ok {
				if len(trans.ridIdxMap) > 0 {
					var ridIdx []int
					for i := range trans.ridIdxMap {
						ridIdx = append(ridIdx, i)
					}
					sort.Ints(ridIdx)
					for i, ck := range trans.chunks {
						ck = ck.SlimChunk(ridIdx)
						partial := trans.Writer.Write(ck, i == len(trans.chunks)-1)
						for partial {
							partial = trans.Writer.Write(nil, i == len(trans.chunks)-1)
						}
					}
					trans.chunks = nil
				} else {
					partial := trans.Writer.Write(nil, true)
					for partial {
						partial = trans.Writer.Write(nil, true)
					}
				}
				return nil
			}

			if trans.init && len(trans.ridIdxMap) == 0 && chunk != nil {
				partial := trans.Writer.Write(chunk, false)
				for partial {
					partial = trans.Writer.Write(nil, false)
				}
				continue
			}

			trans.pushChunkAndFixRidIdx(chunk, trans.init)
			if !trans.init {
				trans.init = true
			}
			if len(trans.ridIdxMap) == 0 {
				for _, ck := range trans.chunks {
					partial := trans.Writer.Write(ck, false)
					for partial {
						partial = trans.Writer.Write(nil, false)
					}
				}
				trans.chunks = nil
			}
			tracing.EndPP(span)
		case <-ctx.Done():
			return nil
		}
	}
}

func (trans *HttpSenderHintTransform) initRidIdxMap(chunk Chunk) {
	trans.ridIdxMap = make(map[int]struct{})
	for i, column := range chunk.Columns() {
		if column.IsEmpty() {
			trans.ridIdxMap[i] = struct{}{}
		}
	}
}

func (trans *HttpSenderHintTransform) pushChunkAndFixRidIdx(chunk Chunk, init bool) {
	if !init {
		trans.initRidIdxMap(chunk)
	}
	if len(trans.ridIdxMap) != 0 {
		for i := range trans.ridIdxMap {
			if !chunk.Column(i).IsEmpty() {
				delete(trans.ridIdxMap, i)
			}
		}
	}
	trans.chunks = append(trans.chunks, chunk.Clone())
}

func (trans *HttpSenderHintTransform) GetOutputs() Ports {
	return Ports{}
}

func (trans *HttpSenderHintTransform) GetInputs() Ports {
	return Ports{trans.input}
}

func (trans *HttpSenderHintTransform) GetOutputNumber(_ Port) int {
	return INVALID_NUMBER
}

func (trans *HttpSenderHintTransform) GetInputNumber(_ Port) int {
	return 0
}

func (trans *HttpSenderHintTransform) SetDag(dag *TransformDag) {
	trans.dag = dag
}

func (trans *HttpSenderHintTransform) SetVertex(vertex *TransformVertex) {
	trans.vertex = vertex
}

func (trans *HttpSenderHintTransform) Visit(vertex *TransformVertex) TransformVertexVisitor {
	if !vertex.transform.IsSink() {
		return trans
	}

	if rpc, ok := vertex.transform.(*RPCReaderTransform); ok {
		rpc.Abort()
	}
	return trans
}

func (trans *HttpSenderHintTransform) AbortSinkTransform() {
	if trans.dag == nil || trans.vertex == nil {
		return
	}

	go func() {
		trans.dag.DepthFirstWalkVertex(trans, trans.vertex)
	}()
}
