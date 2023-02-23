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

package executor_test

import (
	"bytes"
	"context"
	"fmt"
	_ "net/http/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/openGemini/openGemini/engine/executor"
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	qry "github.com/openGemini/openGemini/open_src/influx/query"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

func forwardIntegerColumn(dst executor.Column, src executor.Column) {
	dst.AppendIntegerValues(src.IntegerValues()...)
	executor.AdjustNils(dst, src)
}

func forwardFloatColumn(dst executor.Column, src executor.Column) {
	dst.AppendFloatValues(src.FloatValues()...)
	executor.AdjustNils(dst, src)
}

func forwardBooleanColumn(dst executor.Column, src executor.Column) {
	dst.AppendBooleanValues(src.BooleanValues()...)
	executor.AdjustNils(dst, src)
}

func forwardStringColumn(dst executor.Column, src executor.Column) {
	dst.CloneStringValues(src.GetStringBytes())
	executor.AdjustNils(dst, src)
}

func createTransforms(ops []hybridqp.ExprOptions) []func(dst executor.Column, src executor.Column) {
	transforms := make([]func(dst executor.Column, src executor.Column), len(ops))

	for i, op := range ops {
		if vr, ok := op.Expr.(*influxql.VarRef); ok {
			switch vr.Type {
			case influxql.Integer:
				transforms[i] = forwardIntegerColumn
			case influxql.Float:
				transforms[i] = forwardFloatColumn
			case influxql.Boolean:
				transforms[i] = forwardBooleanColumn
			case influxql.String, influxql.Tag:
				transforms[i] = forwardStringColumn
			}
		}
	}

	return transforms
}

type MockPointWriter struct {
	Validator func(database, retentionPolicy string, points []influx.Row)
}

func (w *MockPointWriter) RetryWritePointRows(database, retentionPolicy string, points []influx.Row) error {
	if w.Validator != nil {
		w.Validator(database, retentionPolicy, points)
	}
	return nil
}

type MockSeries struct {
	executor.BaseProcessor

	output        *executor.ChunkPort
	ops           []hybridqp.ExprOptions
	opt           qry.ProcessorOptions
	chunkBuilder  *executor.ChunkBuilder
	segment       *Segment
	columnBinding []int
	transforms    []func(dst executor.Column, src executor.Column)
}

func NewMockSeries(rowDataType hybridqp.RowDataType, segment *Segment, ops []hybridqp.ExprOptions, opt qry.ProcessorOptions, schema *executor.QuerySchema) *MockSeries {
	series := &MockSeries{
		output:        executor.NewChunkPort(rowDataType),
		ops:           ops,
		opt:           opt,
		chunkBuilder:  executor.NewChunkBuilder(rowDataType),
		segment:       segment,
		columnBinding: make([]int, len(rowDataType.Fields())),
		transforms:    nil,
	}

	series.transforms = createTransforms(series.ops)

	series.bindingColumn()

	return series
}

type MockSeriesCreator struct {
}

func (mock *MockSeriesCreator) Create(plan executor.LogicalPlan, opt qry.ProcessorOptions) (executor.Processor, error) {
	segment, ok := plan.Trait().(*Segment)
	if !ok {
		return nil, fmt.Errorf("no segment for mock series, trait is %v", plan.Trait())
	}
	p := NewMockSeries(plan.RowDataType(), segment, plan.RowExprOptions(), opt, plan.Schema().(*executor.QuerySchema))
	return p, nil
}

func (mock *MockSeries) bindingColumn() {
	for i := range mock.ops {
		if val, ok := mock.ops[i].Expr.(*influxql.VarRef); ok {
			mock.columnBinding[i] = mock.segment.Data().RowDataType().FieldIndex(val.Val)
		}
	}
}

func (mock *MockSeries) Work(ctx context.Context) error {
	defer func() {
		mock.Close()
	}()

	chunk := (*mock.segment.Data().(*executor.ChunkImpl))
	chunkTags := executor.NewChunkTags(*mock.segment.PointTags(), mock.opt.Dimensions)
	chunk.ResetTagsAndIndexes([]executor.ChunkTags{*chunkTags}, []int{0})
	executor.IntervalIndexGen(&chunk, mock.opt)

	out := mock.chunkBuilder.NewChunk(chunk.Name())
	out.AppendTime(chunk.Time()...)
	out.AppendTagsAndIndexes(chunk.Tags(), chunk.TagIndex())
	out.AppendIntervalIndex(chunk.IntervalIndex()...)

	for i, t := range mock.transforms {
		dst := out.Column(i)
		src := chunk.Column(mock.columnBinding[i])
		t(dst, src)
	}
	mock.output.State <- out
	return nil
}

func (mock *MockSeries) Close() {
	mock.output.Close()
}

func (mock *MockSeries) Release() error {
	return nil
}

func (mock *MockSeries) Name() string {
	return "MockSeries"
}

func (mock *MockSeries) GetOutputs() executor.Ports {
	return executor.Ports{mock.output}
}

func (mock *MockSeries) GetInputs() executor.Ports {
	return nil
}

func (mock *MockSeries) GetOutputNumber(port executor.Port) int {
	return 0
}

func (mock *MockSeries) GetInputNumber(port executor.Port) int {
	return executor.INVALID_NUMBER
}

func (mock *MockSeries) IsSink() bool {
	return false
}

func (mock *MockSeries) Explain() []executor.ValuePair {
	pairs := make([]executor.ValuePair, 0, len(mock.ops))
	for _, option := range mock.ops {
		pairs = append(pairs, executor.ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

type MockScanner struct {
	executor.BaseProcessor

	output       *executor.ChunkPort
	ops          []hybridqp.ExprOptions
	opt          qry.ProcessorOptions
	chunkBuilder *executor.ChunkBuilder
	exchange     *executor.LogicalExchange
	path         string
}

func NewMockScanner(rowDataType hybridqp.RowDataType, innerPlan hybridqp.QueryNode, path string, ops []hybridqp.ExprOptions, opt qry.ProcessorOptions, schema *executor.QuerySchema) *MockScanner {
	exchange, ok := innerPlan.(*executor.LogicalExchange)
	if !ok {
		return nil
	}

	scanner := &MockScanner{
		output:       executor.NewChunkPort(rowDataType),
		ops:          ops,
		opt:          opt,
		chunkBuilder: executor.NewChunkBuilder(rowDataType),
		exchange:     exchange,
		path:         path,
	}

	return scanner
}

type MockScannerCreator struct {
}

func (mock *MockScannerCreator) Create(plan executor.LogicalPlan, opt qry.ProcessorOptions) (executor.Processor, error) {
	p := NewMockScanner(plan.RowDataType(), plan.Children()[0].Clone(), plan.(*executor.LogicalReader).MstName(), plan.RowExprOptions(), opt, plan.Schema().(*executor.QuerySchema))
	if p == nil {
		return nil, fmt.Errorf("inner plan is not exchange, it is %v", plan.Children()[0])
	}
	return p, nil
}

func (mock *MockScanner) Work(ctx context.Context) error {
	defer func() {
		mock.Close()
	}()

	storage, ok := ctx.Value(MOCK_SCANNER_STORAGE).(*Storage)

	if !ok {
		return fmt.Errorf("no storage found")
	}

	memstore, err := storage.MemStore(mock.path)
	if err != nil {
		return err
	}
	reader := NewMemStoreReader(memstore, mock.opt.Dimensions)
	for reader.HasMore() {
		exchange := mock.exchange.Clone().(*executor.LogicalExchange)
		segments := reader.Next()
		for _, segment := range segments {
			exchange.AddTrait(segment)
		}
		builder := executor.NewQueryExecutorBuilder(0)
		e, _ := builder.Build(exchange)
		exec := e.(*executor.PipelineExecutor)
		exec.GetRoot().GetTransform().GetOutputs()[0].Redirect(mock.output)
		if err := exec.ExecuteExecutor(context.Background()); err != nil {
			return err
		}
	}

	return nil
}

func (mock *MockScanner) Close() {
	mock.output.Close()
}

func (mock *MockScanner) Release() error {
	return nil
}

func (mock *MockScanner) Name() string {
	return "MockScanner"
}

func (mock *MockScanner) GetOutputs() executor.Ports {
	return executor.Ports{mock.output}
}

func (mock *MockScanner) GetInputs() executor.Ports {
	return nil
}

func (mock *MockScanner) GetOutputNumber(port executor.Port) int {
	return 0
}

func (mock *MockScanner) GetInputNumber(port executor.Port) int {
	return executor.INVALID_NUMBER
}

func (mock *MockScanner) IsSink() bool {
	return false
}

func (mock *MockScanner) Explain() []executor.ValuePair {
	pairs := make([]executor.ValuePair, 0, len(mock.ops))
	for _, option := range mock.ops {
		pairs = append(pairs, executor.ValuePair{First: option.Expr.String(), Second: option.Ref.String()})
	}
	return pairs
}

type MockSender struct {
	executor.BaseProcessor

	input  *executor.ChunkPort
	schema *executor.QuerySchema

	results []executor.Chunk
}

func NewMockSender(rowDataType hybridqp.RowDataType, schema *executor.QuerySchema) *MockSender {
	return &MockSender{
		input:   executor.NewChunkPort(rowDataType),
		schema:  schema,
		results: nil,
	}
}

type SQL_TEST_IDENTIFIER int

var (
	MOCK_TESTING_DESC    SQL_TEST_IDENTIFIER = 0xf1
	MOCK_SENDER_HANDLER  SQL_TEST_IDENTIFIER = 0xf2
	MOCK_SCANNER_STORAGE SQL_TEST_IDENTIFIER = 0xf3
)

type MockSenderCreator struct {
}

func (mock *MockSenderCreator) Create(plan executor.LogicalPlan, opt qry.ProcessorOptions) (executor.Processor, error) {
	p := NewMockSender(plan.Children()[0].RowDataType(), plan.Schema().(*executor.QuerySchema))
	return p, nil
}

func (mock *MockSender) Work(ctx context.Context) error {
	defer func() {
		mock.Close()

		handler := ctx.Value(MOCK_SENDER_HANDLER)

		if handler != nil {
			if h, ok := handler.(func([]executor.Chunk)); ok {
				h(mock.results)
			}
		}
	}()

	for {
		select {
		case chunk, ok := <-mock.input.State:
			if !ok {
				return nil
			}

			mock.results = append(mock.results, chunk)

		case <-ctx.Done():
			return nil
		}
	}
}

func (mock *MockSender) Close() {
}

func (mock *MockSender) Release() error {
	return nil
}

func (mock *MockSender) Name() string {
	return "MockSender"
}

func (mock *MockSender) GetOutputs() executor.Ports {
	return nil
}

func (mock *MockSender) GetInputs() executor.Ports {
	return executor.Ports{mock.input}
}

func (mock *MockSender) GetOutputNumber(port executor.Port) int {
	return executor.INVALID_NUMBER
}

func (mock *MockSender) GetInputNumber(port executor.Port) int {
	return 0
}

func (mock *MockSender) IsSink() bool {
	return true
}

func (mock *MockSender) Explain() []executor.ValuePair {
	return nil
}

func RegistryMockTransformCreator() {
	executor.RegistryTransformCreator(&executor.LogicalReader{}, &MockScannerCreator{})
	executor.RegistryTransformCreator(&executor.LogicalHttpSender{}, &MockSenderCreator{})
	executor.RegistryTransformCreator(&executor.LogicalSeries{}, &MockSeriesCreator{})
}

type MockShardGroup struct {
	shards map[string]*Table
}

func (mock *MockShardGroup) FieldDimensions(
	m *influxql.Measurement) (map[string]influxql.DataType, map[string]struct{}, *influxql.Schema, error) {
	panic("FieldDimensions is not implements yet")
}

func (mock *MockShardGroup) MapType(m *influxql.Measurement, field string) influxql.DataType {
	if tbl, ok := mock.shards[m.Name]; ok {
		return tbl.DataType(field)
	}
	return influxql.Unknown
}

func (mock *MockShardGroup) MapTypeBatch(m *influxql.Measurement, fields map[string]*influxql.FieldNameSpace, schema *influxql.Schema) error {
	for k := range fields {
		if tbl, ok := mock.shards[m.Name]; ok {
			fields[k].DataType = tbl.DataType(k)
			continue
		}

		return fmt.Errorf("column(%s) is not a field or tag in %s", k, m.Name)
	}
	return nil
}

func (mock *MockShardGroup) CreateLogicalPlan(
	ctx context.Context,
	sources influxql.Sources,
	schema hybridqp.Catalog) (hybridqp.QueryNode, error) {
	builder := executor.NewLogicalPlanBuilderImpl(schema)

	builder.Series()
	builder.Exchange(executor.SERIES_EXCHANGE, nil)
	builder.Reader()
	builder.Exchange(executor.READER_EXCHANGE, nil)
	builder.Exchange(executor.SINGLE_SHARD_EXCHANGE, nil)
	return builder.Build()
}

func (mock *MockShardGroup) Close() error {
	return nil
}

func (mock *MockShardGroup) GetSources(sources influxql.Sources) influxql.Sources {
	return sources
}

func (mock *MockShardGroup) LogicalPlanCost(source *influxql.Measurement, opt qry.ProcessorOptions) (hybridqp.LogicalPlanCost, error) {
	panic("GetSources is not implements")
}

func (mock *MockShardGroup) AddShard(table *Table) {
	mock.shards[table.Name()] = table
}

func NewMockShardGroup() *MockShardGroup {
	return &MockShardGroup{
		shards: make(map[string]*Table),
	}
}

type Table struct {
	dbPath    string
	name      string
	dataTypes map[string]influxql.DataType
}

func NewTable(name string) *Table {
	return &Table{
		dbPath:    "",
		name:      name,
		dataTypes: make(map[string]influxql.DataType),
	}
}

func (t *Table) AddDataType(col string, typ influxql.DataType) {
	t.dataTypes[col] = typ
}

func (t *Table) AddDataTypes(typs map[string]influxql.DataType) {
	for col, typ := range typs {
		t.dataTypes[col] = typ
	}
}

func (t *Table) DataType(col string) influxql.DataType {
	if typ, ok := t.dataTypes[col]; ok {
		return typ
	}
	return influxql.Unknown
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) DbPath() string {
	return t.dbPath
}

func (t *Table) SetDbPath(dbPath string) {
	t.dbPath = dbPath
}

func (t *Table) Path() string {
	return t.dbPath + "." + t.name
}

func AccessPath(first string, second string) string {
	return first + "." + second
}

type Database struct {
	name   string
	rp     string
	tables map[string]*Table
}

func NewDatabase(name string, rp string) *Database {
	return &Database{
		name:   name,
		rp:     rp,
		tables: make(map[string]*Table),
	}
}

func (d *Database) Table(name string) (*Table, error) {
	if tbl, ok := d.tables[name]; ok {
		return tbl, nil
	}
	return nil, fmt.Errorf("table(%s) is not existing in database(%s)", name, d.name)
}

func (d *Database) AddTable(tbl *Table) error {
	if _, ok := d.tables[tbl.Name()]; ok {
		return fmt.Errorf("table(%s) has already in database(%s)", tbl.Name(), d.name)
	}

	d.tables[tbl.Name()] = tbl

	return nil
}

func (d *Database) Name() string {
	return d.name
}

func (d *Database) Path() string {
	return AccessPath(d.name, d.rp)
}

type Catalog struct {
	databases map[string]*Database
}

func NewCatalog() *Catalog {
	return &Catalog{
		databases: make(map[string]*Database),
	}
}

func (c *Catalog) Database(name string, rp string) (*Database, error) {
	path := AccessPath(name, rp)
	if database, ok := c.databases[path]; ok {
		return database, nil
	}
	return nil, fmt.Errorf("database(%s) is not existing", path)
}

func (c *Catalog) Table(name string, rp string, tbl string) (*Table, error) {
	if database, err := c.Database(name, rp); err != nil {
		return nil, err
	} else {
		if table, err := database.Table(tbl); err != nil {
			return nil, err
		} else {
			return table, nil
		}
	}
}

func (c *Catalog) CreateDatabase(name string, rp string) (*Database, error) {
	path := AccessPath(name, rp)
	if _, ok := c.databases[path]; !ok {
		c.databases[path] = NewDatabase(name, rp)
		return c.databases[path], nil
	}

	return nil, fmt.Errorf("create database(%s) failed, it already exist", path)
}

type MockShardMapper struct {
	catalog *Catalog
}

func (mock *MockShardMapper) MapShards(
	sources influxql.Sources,
	t influxql.TimeRange,
	opt qry.SelectOptions,
	condition influxql.Expr) (qry.ShardGroup, error) {
	shardGroup := NewMockShardGroup()
	for _, s := range sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			if table, err := mock.catalog.Table(s.Database, s.RetentionPolicy, s.Name); err != nil {
				return nil, err
			} else {
				shardGroup.AddShard(table)
				continue
			}
		case *influxql.SubQuery:
			return mock.MapShards(s.Statement.Sources, t, opt, condition)
		default:
			panic("unsupport source")
		}
	}

	return shardGroup, nil
}

func (mock *MockShardMapper) Close() error {
	return nil
}

func (mock *MockShardMapper) GetSeriesKey() []byte {
	return nil
}

// create a new mock shard mapper.
func NewMockShardMapper(catalog *Catalog) *MockShardMapper {
	return &MockShardMapper{
		catalog: catalog,
	}
}

type Segment struct {
	data executor.Chunk
	pts  *influx.PointTags
}

func NewSegment(pts *influx.PointTags, data executor.Chunk) *Segment {
	seg := &Segment{
		data: data,
		pts:  pts,
	}

	sort.Sort(seg.pts)

	return seg
}

func (seg *Segment) Data() executor.Chunk {
	return seg.data
}

func (seg *Segment) PointTags() *influx.PointTags {
	return seg.pts
}

func (seg *Segment) GroupValue(keys ...string) string {
	buf := new(bytes.Buffer)
	for _, key := range keys {
		buf.WriteString(seg.pts.FindPointTag(key).Value)
	}
	return buf.String()
}

type MemStore struct {
	store map[uint64]*Segment
	id    uint64
	rwm   sync.RWMutex
}

func NewMemStore() *MemStore {
	return &MemStore{
		store: make(map[uint64]*Segment),
		id:    0,
	}
}

func (ms *MemStore) WriteSegment(seg *Segment) error {
	id := atomic.AddUint64(&ms.id, 1)

	func() {
		defer ms.rwm.Unlock()
		ms.rwm.Lock()
		ms.store[id] = seg
	}()

	return nil
}

func (ms *MemStore) Segments() map[uint64]*Segment {
	return ms.store
}

type MemStoreReader struct {
	memstore *MemStore
	dims     []string
	tagset   [][]*Segment
	cursor   int32
}

func NewMemStoreReader(memstore *MemStore, dims []string) *MemStoreReader {
	reader := &MemStoreReader{
		memstore: memstore,
		dims:     dims,
		tagset:   nil,
		cursor:   0,
	}
	reader.init()
	return reader
}

func (r *MemStoreReader) init() {
	segments := r.memstore.Segments()
	group := make(map[string][]*Segment)
	for _, segment := range segments {
		value := segment.GroupValue(r.dims...)
		grpSegs := group[value]
		grpSegs = append(grpSegs, segment)
		group[value] = grpSegs
	}

	values := make([]string, 0, len(group))
	for value := range group {
		values = append(values, value)
	}

	sort.Strings(values)

	r.tagset = make([][]*Segment, len(values))
	for i, value := range values {
		r.tagset[i] = group[value]
	}
}

func (r *MemStoreReader) HasMore() bool {
	return r.cursor < int32(len(r.tagset))
}

func (r *MemStoreReader) Next() []*Segment {
	seg := r.tagset[r.cursor]
	atomic.AddInt32(&r.cursor, 1)
	return seg
}

type Storage struct {
	catalog   *Catalog
	directory map[string]*MemStore
	rwm       sync.RWMutex
}

func NewStorage(catalog *Catalog) *Storage {
	return &Storage{
		catalog:   catalog,
		directory: make(map[string]*MemStore),
	}
}

func (s *Storage) MemStore(path string) (*MemStore, error) {
	defer s.rwm.RUnlock()
	s.rwm.RLock()
	if memstore, ok := s.directory[path]; ok {
		return memstore, nil
	} else {
		return nil, fmt.Errorf("memstore at path(%v) is not exist", path)
	}
}

func (s *Storage) Write(path string, pts *influx.PointTags, data executor.Chunk) error {
	memstore, ok := func() (m *MemStore, ok bool) {
		defer s.rwm.RUnlock()
		s.rwm.RLock()
		m, ok = s.directory[path]
		return
	}()

	if !ok {
		func() {
			defer s.rwm.Unlock()
			s.rwm.Lock()
			memstore = NewMemStore()
			s.directory[path] = memstore
		}()
	}

	if err := memstore.WriteSegment(NewSegment(pts, data)); err != nil {
		return err
	}

	if err := s.updateCatalog(path, pts, data.RowDataType()); err != nil {
		return err
	}

	return nil
}

func (s *Storage) updateCatalog(path string, pts *influx.PointTags, dt hybridqp.RowDataType) error {
	strs := DecomposePath(path)
	if database, err := s.catalog.Database(strs[0], strs[1]); err != nil {
		return err
	} else {
		table, err := database.Table(strs[2])
		if err != nil {
			table = NewTable(strs[2])
			if err := database.AddTable(table); err != nil {
				return err
			}
		}
		for _, ref := range dt.MakeRefs() {
			table.AddDataType(ref.Val, ref.Type)
		}
		for _, tag := range *pts {
			table.AddDataType(tag.Key, influxql.Tag)
		}
		return nil
	}
}

func DecomposePath(path string) []string {
	return strings.Split(path, ".")
}

func MeasurementsFromSelectStmt(stmt *influxql.SelectStatement) []*influxql.Measurement {
	return DFSSources(stmt.Sources)
}

func DFSSources(sources influxql.Sources) []*influxql.Measurement {
	msts := make([]*influxql.Measurement, 0)
	for _, s := range sources {
		switch source := s.(type) {
		case *influxql.Measurement:
			msts = append(msts, source)
		case *influxql.SubQuery:
			msts = append(msts, DFSSources(source.Statement.Sources)...)
		default:
		}
	}
	return msts
}

type TSDBSystem struct {
	catalog *Catalog
	storage *Storage
}

func NewTSDBSystem() *TSDBSystem {
	RegistryMockTransformCreator()
	system := &TSDBSystem{
		catalog: NewCatalog(),
	}
	system.storage = NewStorage(system.catalog)
	return system
}

func (s *TSDBSystem) DDL(handler func(*Catalog) error) error {
	return handler(s.catalog)
}

func (s *TSDBSystem) DML(handler func(*Storage) error) error {
	return handler(s.storage)
}

func (s *TSDBSystem) ExecSQL(sql string,
	validator func([]executor.Chunk),
	intoValidator func(database, retentionPolicy string, points []influx.Row)) error {
	sqlReader := strings.NewReader(sql)
	parser := influxql.NewParser(sqlReader)
	yaccParser := influxql.NewYyParser(parser.GetScanner())
	yaccParser.ParseTokens()
	query, err := yaccParser.GetQuery()
	if err != nil {
		return err
	}

	opts := qry.ExecutionOptions{
		Database:        "",
		RetentionPolicy: "",
		ChunkSize:       10000,
		Chunked:         true,
		ReadOnly:        true,
		NodeID:          0,
		InnerChunkSize:  1024,
		Quiet:           true,
		Traceid:         1,
	}

	stmt := query.Statements[0]

	stmt, err = qry.RewriteStatement(stmt)
	if err != nil {
		return err
	}

	sopts := qry.SelectOptions{
		NodeID:                  opts.NodeID,
		MaxSeriesN:              0,
		MaxFieldsN:              0,
		MaxPointN:               0,
		MaxBucketsN:             0,
		Authorizer:              opts.Authorizer,
		MaxQueryMem:             0,
		MaxQueryParallel:        0,
		QueryTimeCompareEnabled: true,
		Chunked:                 opts.Chunked,
		ChunkedSize:             opts.ChunkSize,
		QueryLimitEn:            opts.QueryLimitEn,
		RowsChan:                opts.RowsChan,
		ChunkSize:               opts.InnerChunkSize,
		Traceid:                 opts.Traceid,
		AbortChan:               opts.AbortCh,
	}

	shardMapper := NewMockShardMapper(s.catalog)
	selectStmt, ok := stmt.(*influxql.SelectStatement)
	if !ok {
		return fmt.Errorf("not select statement(%v)", stmt)
	}

	selectStmt.OmitTime = true
	preparedStmt, err := qry.Prepare(selectStmt, shardMapper, sopts)
	if err != nil {
		return err
	}
	preparedStmt.ChangeCreator(func() hybridqp.PipelineExecutorBuilder {
		msts := MeasurementsFromSelectStmt(selectStmt)
		mapShard2Reader := make(map[uint64][][]interface{})
		for i := range msts {
			mapShard2Reader[uint64(i)] = [][]interface{}{nil}
		}
		traits := executor.NewStoreExchangeTraits(nil, mapShard2Reader)
		executorBuilder := executor.NewMocStoreExecutorBuilder(traits, 0)
		return executorBuilder
	})
	preparedStmt.ChangeOptimizer(func() hybridqp.Planner {
		pb := NewHeuProgramBuilder()
		pb.InitDefaultRules()
		planner := executor.NewHeuPlannerImpl(pb.Build())

		planner.AddRule(executor.NewLimitPushdownToExchangeRule(""))
		planner.AddRule(executor.NewLimitPushdownToReaderRule(""))
		planner.AddRule(executor.NewLimitPushdownToSeriesRule(""))
		planner.AddRule(executor.NewAggPushdownToExchangeRule(""))
		planner.AddRule(executor.NewAggPushdownToReaderRule(""))
		planner.AddRule(executor.NewAggPushdownToSeriesRule(""))

		planner.AddRule(executor.NewCastorAggCutRule(""))

		planner.AddRule(executor.NewAggSpreadToSortAppendRule(""))
		planner.AddRule(executor.NewAggSpreadToExchangeRule(""))
		planner.AddRule(executor.NewAggSpreadToReaderRule(""))
		planner.AddRule(executor.NewSlideWindowSpreadRule(""))
		return planner
	})
	exec, err := preparedStmt.Select(context.Background())
	if err != nil {
		return err
	}
	piplineExecutor := exec.(*executor.PipelineExecutor)
	ctx := context.WithValue(context.Background(), MOCK_SENDER_HANDLER, validator)
	ctx = context.WithValue(ctx, MOCK_SCANNER_STORAGE, s.storage)
	ctx = context.WithValue(ctx, executor.WRITER_CONTEXT, &MockPointWriter{Validator: intoValidator})
	if err := piplineExecutor.ExecuteExecutor(ctx); err != nil {
		return err
	}
	return nil
}
