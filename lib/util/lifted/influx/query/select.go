package query

/*
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/query/select.go

2022.01.23 The ProcessOption struct take reference from IteratorOption struct from iterator.go in influxql/query/iterator.go
Add StmtBuilderCreatorFactory feature to register creator for statement.
Add LogicalPlanCreator etc.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"context"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

// SelectOptions are options that customize the select call.
type SelectOptions struct {
	// Authorizer is used to limit access to data
	Authorizer FineAuthorizer

	// Node to exclusively read from.
	// If zero, all nodes are used.
	NodeID uint64

	// Maximum number of concurrent series.
	MaxSeriesN int

	// Maximum number of concurrent fileds.
	MaxFieldsN int

	// Maximum number of points to read from the query.
	// This requires the passed in context to have a Monitor that is
	// created using WithMonitor.
	MaxPointN int

	// Maximum number of buckets for a statement.
	MaxBucketsN int

	// Maximum number of memory a query can use
	MaxQueryMem int64

	// Maximum parallelism a query can use
	MaxQueryParallel int

	// The number of point for chunk
	ChunkSize int

	// The requested maximum number of points to return in each result.
	ChunkedSize int

	Chunked bool

	QueryLimitEn bool

	// IsPromQuery indicates whether the query is a promql query.
	IsPromQuery bool

	QueryTimeCompareEnabled bool

	AbortChan <-chan struct{}
	RowsChan  chan RowsChan

	HintType hybridqp.HintType

	IncQuery bool
	QueryID  string
	IterID   int32
}

type LogicalPlanCreator interface {
	// Creates a simple iterator for use in an InfluxQL Logical.
	CreateLogicalPlan(ctx context.Context, sources influxql.Sources, schema hybridqp.Catalog) (hybridqp.QueryNode, error)

	// Determines the potential cost for creating an iterator.
	LogicalPlanCost(source *influxql.Measurement, opt ProcessorOptions) (hybridqp.LogicalPlanCost, error)

	GetSources(sources influxql.Sources) influxql.Sources
	GetETraits(ctx context.Context, sources influxql.Sources, schema hybridqp.Catalog) ([]hybridqp.Trait, error)
	GetSeriesKey() []byte
}

// ShardMapper retrieves and maps shards into an IteratorCreator that can later be
// used for executing queries.
type ShardMapper interface {
	MapShards(sources influxql.Sources, t influxql.TimeRange, opt SelectOptions, condition influxql.Expr) (ShardGroup, error)
	Close() error
}

// ShardGroup represents a shard or a collection of shards that can be accessed
// for creating iterators.
// When creating iterators, the resource used for reading the iterators should be
// separate from the resource used to map the shards. When the ShardGroup is closed,
// it should not close any resources associated with the created Iterator. Those
// resources belong to the Iterator and will be closed when the Iterator itself is
// closed.
// The query engine operates under this assumption and will close the shard group
// after creating the iterators, but before the iterators are actually read.
type ShardGroup interface {
	LogicalPlanCreator
	influxql.FieldMapper
	io.Closer
}

// PreparedStatement is a prepared statement that is ready to be executed.
type PreparedStatement interface {
	BuildLogicalPlan(ctx context.Context) (hybridqp.QueryNode, hybridqp.Trait, error)
	Select(ctx context.Context) (hybridqp.Executor, error)

	ChangeCreator(hybridqp.ExecutorBuilderCreator)
	ChangeOptimizer(hybridqp.ExecutorBuilderOptimizer)
	Statement() *influxql.SelectStatement

	// Explain outputs the explain plan for this statement.
	Explain() (string, error)

	// Close closes the resources associated with this prepared statement.
	// This must be called as the mapped shards may hold open resources such
	// as network connections.
	Close() error
}

// Prepare will compile the statement with the default compile options and
// then prepare the query.
func Prepare(stmt *influxql.SelectStatement, shardMapper ShardMapper, opt SelectOptions) (PreparedStatement, error) {
	c, err := Compile(stmt, CompileOptions{})
	if err != nil {
		return nil, err
	}
	return c.Prepare(shardMapper, opt)
}

// ProcessorOptions is an object passed to CreateIterator to specify creation options.
type ProcessorOptions struct {
	Name string
	Expr influxql.Expr
	// Expression to iterate for.
	// This can be VarRef or a Call.
	Exprs []influxql.Expr

	// Auxiliary tags or values to also retrieve for the point.
	Aux []influxql.VarRef

	FieldAux []influxql.VarRef
	TagAux   []influxql.VarRef

	// Data sources from which to receive data. This is only used for encoding
	// measurements over RPC and is no longer used in the open source version.
	Sources []influxql.Source

	// Group by interval and tags.
	Interval   hybridqp.Interval
	Dimensions []string // The final dimensions of the query (stays the same even in subqueries).
	Except     bool
	GroupBy    map[string]struct{} // Dimensions to group points by in intermediate iterators.
	Location   *time.Location

	// Fill options.
	Fill      influxql.FillOption
	FillValue interface{}

	// Condition to filter by.
	Condition influxql.Expr

	// Time range for the iterator.
	StartTime int64
	EndTime   int64

	// Limits the number of points per series.
	Limit, Offset int

	// Limits the number of series.
	SLimit, SOffset int

	// Sorted in time ascending order if true.
	Ascending bool

	// Removes the measurement name. Useful for meta queries.
	StripName bool

	// Removes duplicate rows from raw queries.
	Dedupe bool

	// Determines if this is a query for raw data or an aggregate/selector.
	Ordered bool

	Parallel bool

	// Limits on the creation of iterators.
	MaxSeriesN int

	// If this channel is set and is closed, the iterator should try to exit
	// and close as soon as possible.
	InterruptCh <-chan struct{}

	// Authorizer can limit access to data
	Authorizer FineAuthorizer

	// The requested maximum number of points to return in each result.
	ChunkedSize int

	// If this query return chunk once by once
	Chunked bool

	ChunkSize int

	MaxParallel int
	AbortChan   <-chan struct{}
	RowsChan    chan RowsChan
	Query       string

	EnableBinaryTreeMerge int64

	QueryId uint64

	// hint supported (need to marshal)
	HintType hybridqp.HintType

	// SeriesKey is assigned only the query is single time series, and it's used in the index.
	SeriesKey []byte

	GroupByAllDims bool

	isTimeFirstKey bool

	SortFields influxql.SortFields

	HasFieldWildcard bool
	// useful for topQuery/subQuery, modify by out-query opt.stmtId in newSubOpt
	StmtId int

	LogQueryCurrId string

	// IncQuery indicates whether the query is a incremental query.
	IncQuery bool

	// IterID indicates the number of iteration in incremental query, starting from 0.
	IterID int32

	// PromQuery indicates whether the query is a promql query.
	PromQuery bool

	// Step is query resolution step width in duration format or float number of seconds for Prom.
	Step time.Duration

	// Range is used to specify how far back in time values should be fetched for each resulting
	// range vector element. The range is a closed interval for Prom.
	Range time.Duration

	// LookBackDelta determines the time since the last sample after which a time series is considered
	// stale for Prom.
	LookBackDelta time.Duration

	// QueryOffset is the offset used during the query execution fo promql
	// which is calculated using the original offset, at modifier time,
	// eval time, and subquery offsets in the AST tree.
	QueryOffset time.Duration
}

// NewProcessorOptionsStmt creates the iterator options from stmt.
func NewProcessorOptionsStmt(stmt *influxql.SelectStatement, sopt SelectOptions) (opt ProcessorOptions, err error) {
	// valuer := &influxql.NowValuer{Location: stmt.Location}
	// make sure call influxql.ConditionExpr before to remove time condition
	// condition, timeRange, err := influxql.ConditionExpr(stmt.Condition, valuer)
	opt.Condition = stmt.Condition
	opt.StartTime = influxql.MinTime
	opt.EndTime = influxql.MaxTime

	opt.Location = stmt.Location

	// Determine group by interval.
	interval, err := stmt.GroupByInterval()
	if err != nil {
		return opt, err
	}
	// Set duration to zero if a negative interval has been used.
	if interval < 0 {
		interval = 0
	} else if interval > 0 {
		opt.Interval.Offset, err = stmt.GroupByOffset()
		if err != nil {
			return opt, err
		}
	}
	opt.Interval.Duration = interval

	// Always request an ordered output for the top level iterators.
	// The emitter will always emit points as ordered.
	opt.Ordered = true

	exceptDimensions := make(map[string]struct{})
	for _, dim := range stmt.ExceptDimensions {
		if d, ok := dim.Expr.(*influxql.VarRef); ok {
			exceptDimensions[d.Val] = struct{}{}
		}
	}

	if len(exceptDimensions) > 0 {
		opt.Except = true
	}

	// Determine dimensions.
	opt.GroupBy = make(map[string]struct{}, len(opt.Dimensions))
	for _, d := range stmt.Dimensions {
		if d, ok := d.Expr.(*influxql.VarRef); ok {
			if _, ok = exceptDimensions[d.Val]; ok {
				continue
			}
			if ContainDim(opt.Dimensions, d.Val) {
				opt.Dimensions = append(opt.Dimensions, d.Val)
				opt.GroupBy[d.Val] = struct{}{}
			}
		}
	}
	if sopt.IsPromQuery {
		sort.Strings(opt.Dimensions)
	}
	opt.Ascending = stmt.TimeAscending()
	opt.Dedupe = stmt.Dedupe
	opt.StripName = stmt.StripName

	opt.Fill, opt.FillValue = stmt.Fill, stmt.FillValue
	opt.Limit, opt.Offset = stmt.Limit, stmt.Offset
	opt.SLimit, opt.SOffset = stmt.SLimit, stmt.SOffset
	opt.MaxSeriesN = sopt.MaxSeriesN
	opt.Authorizer = sopt.Authorizer

	opt.ChunkedSize = sopt.ChunkedSize
	opt.Chunked = sopt.Chunked

	opt.ChunkSize = sopt.ChunkSize

	opt.MaxParallel = sopt.MaxQueryParallel
	opt.AbortChan = sopt.AbortChan
	opt.RowsChan = sopt.RowsChan
	opt.GroupByAllDims = stmt.GroupByAllDims
	opt.HasFieldWildcard = stmt.HasWildcardField
	opt.StmtId = stmt.StmtId
	opt.Step = stmt.Step
	opt.Range = stmt.Range
	opt.LookBackDelta = stmt.LookBackDelta
	opt.QueryOffset = stmt.QueryOffset
	return opt, nil
}

func (opt *ProcessorOptions) GetStmtId() int {
	return opt.StmtId
}

func (opt *ProcessorOptions) UpdateSources(sources influxql.Sources) {
	opt.Sources = sources
}

func (opt *ProcessorOptions) Clone() *ProcessorOptions {
	popt := ProcessorOptions{}
	popt = *opt
	return &popt
}

// MergeSorted returns true if the options require a sorted merge.
func (opt *ProcessorOptions) MergeSorted() bool {
	return opt.Ordered
}

// SeekTime returns the time the iterator should start from.
// For ascending iterators this is the start time, for descending iterators it's the end time.
func (opt *ProcessorOptions) SeekTime() int64 {
	if opt.Ascending {
		return opt.StartTime
	}
	return opt.EndTime
}

// StopTime returns the time the iterator should end at.
// For ascending iterators this is the end time, for descending iterators it's the start time.
func (opt *ProcessorOptions) StopTime() int64 {
	if opt.Ascending {
		if !opt.HasInterval() {
			return opt.EndTime
		}
		_, stopTime := opt.Window(opt.EndTime)
		return stopTime
	}
	if !opt.HasInterval() {
		return opt.StartTime
	}
	stopTime, _ := opt.Window(opt.StartTime)
	return stopTime
}

func (opt *ProcessorOptions) GetMaxParallel() int {
	return opt.MaxParallel
}

func (opt *ProcessorOptions) OptionsName() string {
	return opt.Name
}

func (opt *ProcessorOptions) GetStartTime() int64 {
	return opt.StartTime
}

func (opt *ProcessorOptions) GetEndTime() int64 {
	return opt.EndTime
}

func (opt *ProcessorOptions) ChunkSizeNum() int {
	return opt.ChunkSize
}

func (opt *ProcessorOptions) IsAscending() bool {
	return opt.Ascending
}

func (opt *ProcessorOptions) SetAscending(a bool) {
	opt.Ascending = a
}

func (opt *ProcessorOptions) SetPromQuery(isPromQuery bool) {
	opt.PromQuery = isPromQuery
}

func (opt *ProcessorOptions) IsPromInstantQuery() bool {
	return opt.PromQuery && opt.Step == 0
}

func (opt *ProcessorOptions) IsPromRangeQuery() bool {
	return opt.PromQuery && opt.Step > 0
}

func (opt *ProcessorOptions) IsPromQuery() bool {
	return opt.PromQuery
}

func (opt *ProcessorOptions) GetPromStep() time.Duration {
	return opt.Step
}

func (opt *ProcessorOptions) GetPromRange() time.Duration {
	return opt.Range
}

func (opt *ProcessorOptions) GetPromLookBackDelta() time.Duration {
	return opt.LookBackDelta
}

func (opt *ProcessorOptions) GetPromQueryOffset() time.Duration {
	return opt.QueryOffset
}

func (opt *ProcessorOptions) IsRangeVectorSelector() bool {
	return opt.PromQuery && opt.Range > 0
}

func (opt *ProcessorOptions) IsInstantVectorSelector() bool {
	return opt.PromQuery && opt.Range == 0
}

// Window returns the time window [start,end) that t falls within.
func (opt *ProcessorOptions) Window(t int64) (start, end int64) {
	if opt.Interval.IsZero() {
		return opt.StartTime, opt.EndTime + 1
	}

	// Subtract the offset to the time so we calculate the correct base interval.
	t -= int64(opt.Interval.Offset)

	// Retrieve the zone offset for the start time.
	var zone int64
	if opt.Location != nil {
		_, zone = opt.Zone(t)
	}

	// Truncate time by duration.
	dt := (t + zone) % int64(opt.Interval.Duration)
	if dt < 0 {
		// Negative modulo rounds up instead of down, so offset
		// with the duration.
		dt += int64(opt.Interval.Duration)
	}

	// Find the start time.
	if influxql.MinTime+dt >= t {
		start = influxql.MinTime
	} else {
		start = t - dt
	}

	start += int64(opt.Interval.Offset)

	// Look for the start offset again because the first time may have been
	// after the offset switch. Now that we are at midnight in UTC, we can
	// lookup the zone offset again to get the real starting offset.
	if opt.Location != nil {
		_, startOffset := opt.Zone(start)
		// Do not adjust the offset if the offset change is greater than or
		// equal to the duration.
		if o := zone - startOffset; o != 0 && hybridqp.Abs(o) < int64(opt.Interval.Duration) {
			start += o
		}
	}

	// Find the end time.
	if dt := int64(opt.Interval.Duration) - dt; influxql.MaxTime-dt <= t {
		end = influxql.MaxTime
	} else {
		end = t + dt
	}

	// Retrieve the zone offset for the end time.
	if opt.Location != nil {
		_, endOffset := opt.Zone(end)
		// Adjust the end time if the offset is different from the start offset.
		// Only apply the offset if it is smaller than the duration.
		// This prevents going back in time and creating time windows
		// that don't make any sense.
		if o := zone - endOffset; o != 0 && hybridqp.Abs(o) < int64(opt.Interval.Duration) {
			// If the offset is greater than 0, that means we are adding time.
			// Added time goes into the previous interval because the clocks
			// move backwards. If the offset is less than 0, then we are skipping
			// time. Skipped time comes after the switch so if we have a time
			// interval that lands on the switch, it comes from the next
			// interval and not the current one. For this reason, we need to know
			// when the actual switch happens by seeing if the time switch is within
			// the current interval. We calculate the zone offset with the offset
			// and see if the value is the same. If it is, we apply the
			// offset.
			if o > 0 {
				end += o
			} else if _, z := opt.Zone(end + o); z == endOffset {
				end += o
			}
		}
	}
	end += int64(opt.Interval.Offset)
	return
}

// DerivativeInterval returns the time interval for the derivative function.
func (opt *ProcessorOptions) DerivativeInterval() hybridqp.Interval {
	// Use the interval on the derivative() call, if specified.
	if expr, ok := opt.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return hybridqp.Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}

	// Otherwise use the group by interval, if specified.
	if opt.Interval.Duration > 0 {
		return hybridqp.Interval{Duration: opt.Interval.Duration}
	}

	return hybridqp.Interval{Duration: time.Second}
}

// ElapsedInterval returns the time interval for the elapsed function.
func (opt *ProcessorOptions) ElapsedInterval() hybridqp.Interval {
	// Use the interval on the elapsed() call, if specified.
	if expr, ok := opt.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return hybridqp.Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}

	return hybridqp.Interval{Duration: time.Nanosecond}
}

// IntegralInterval returns the time interval for the integral function.
func (opt *ProcessorOptions) IntegralInterval() hybridqp.Interval {
	// Use the interval on the integral() call, if specified.
	if expr, ok := opt.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return hybridqp.Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}

	return hybridqp.Interval{Duration: time.Second}
}

// GetDimensions retrieves the dimensions for this query.
func (opt *ProcessorOptions) GetDimensions() []string {
	if len(opt.GroupBy) > 0 {
		dimensions := make([]string, 0, len(opt.GroupBy))
		for dim := range opt.GroupBy {
			dimensions = append(dimensions, dim)
		}
		return dimensions
	}
	return opt.Dimensions
}

func (opt *ProcessorOptions) GetOptDimension() []string {
	return opt.Dimensions
}

func (opt *ProcessorOptions) CanLimitPushDown() bool {
	return opt.Limit > 0 && len(opt.SortFields) > 0
}

func (opt *ProcessorOptions) CanTimeLimitPushDown() bool {
	return opt.CanLimitPushDown() && opt.SortFields[0].Name == "time"
}

// Zone returns the zone information for the given time. The offset is in nanoseconds.
func (opt *ProcessorOptions) Zone(ns int64) (string, int64) {
	if opt.Location == nil {
		return "", 0
	}

	t := time.Unix(0, ns).In(opt.Location)
	name, offset := t.Zone()
	return name, hybridqp.SecToNs * int64(offset)
}

func (opt *ProcessorOptions) GetCondition() influxql.Expr {
	return opt.Condition
}

func (opt *ProcessorOptions) GetLocation() *time.Location {
	return opt.Location
}

func (opt *ProcessorOptions) IsUnifyPlan() bool {
	return config.IsLogKeeper()
}

func (opt *ProcessorOptions) IsTimeSorted() bool {
	for _, source := range opt.Sources {
		if mst, ok := source.(*influxql.Measurement); ok {
			if !mst.IsTimeSorted {
				return false
			}
		}
	}
	return true
}

func (opt *ProcessorOptions) SetSortFields(sortFields influxql.SortFields) {
	opt.SortFields = sortFields
}

func (opt *ProcessorOptions) GetSortFields() influxql.SortFields {
	return opt.SortFields
}

func (opt *ProcessorOptions) GetHintType() hybridqp.HintType {
	return opt.HintType
}

func (opt *ProcessorOptions) IsGroupByAllDims() bool {
	return opt.GroupByAllDims
}

func (opt *ProcessorOptions) GetLimit() int {
	return opt.Limit
}

func (opt *ProcessorOptions) GetOffset() int {
	return opt.Offset
}

func (opt *ProcessorOptions) GetGroupBy() map[string]struct{} {
	return opt.GroupBy
}

func (opt *ProcessorOptions) HasInterval() bool {
	return !opt.Interval.IsZero()
}

func (opt *ProcessorOptions) ISChunked() bool {
	return opt.Chunked
}

func (opt *ProcessorOptions) SetHintType(h hybridqp.HintType) {
	opt.HintType = h
}

func (opt *ProcessorOptions) GetInterval() time.Duration {
	return opt.Interval.Duration
}

func (opt *ProcessorOptions) GetSourcesNames() []string {
	names := make([]string, len(opt.Sources))
	for i := range names {
		names[i] = opt.Sources[i].GetName()
	}
	return names
}

func (opt *ProcessorOptions) GetMeasurements() []*influxql.Measurement {
	msts := make([]*influxql.Measurement, 0, len(opt.Sources))
	for _, source := range opt.Sources {
		if mst, ok := source.(*influxql.Measurement); ok {
			msts = append(msts, mst)
		}
	}
	return msts
}

func (opt *ProcessorOptions) HaveOnlyCSStore() bool {
	msts := opt.GetMeasurements()
	if len(msts) == 0 {
		return false
	}
	for i := range msts {
		if !msts[i].IsCSStore() {
			return false
		}
	}
	return true
}

func (opt *ProcessorOptions) HaveLocalMst() bool {
	msts := opt.GetMeasurements()
	if len(msts) == 0 {
		return true
	}
	for i := range msts {
		if !msts[i].IsRWSplit() {
			return true
		}
	}
	return false
}

func (opt *ProcessorOptions) SetFill(fill influxql.FillOption) {
	opt.Fill = fill
}

func (opt *ProcessorOptions) FieldWildcard() bool {
	return opt.HasFieldWildcard
}

func ContainDim(des []string, src string) bool {
	for i := range des {
		if src == des[i] {
			return false
		}
	}
	return true
}

func (opt *ProcessorOptions) GetLogQueryCurrId() string {
	return opt.LogQueryCurrId
}

func (opt *ProcessorOptions) GetIterId() int32 {
	return opt.IterID
}

func (opt *ProcessorOptions) IsIncQuery() bool {
	return opt.IncQuery
}

func validateTypes(stmt *influxql.SelectStatement) error {
	valuer := influxql.TypeValuerEval{
		TypeMapper: influxql.MultiTypeMapper(
			FunctionTypeMapper{},
			MathTypeMapper{},
			StringFunctionTypeMapper{},
			LabelFunctionTypeMapper{},
			PromTimeFunctionTypeMapper{},
		),
	}
	for _, f := range stmt.Fields {
		if _, err := valuer.EvalType(f.Expr, false); err != nil {
			return err
		}
	}
	return nil
}

type StmtBuilder interface {
}

type StmtBuilderCreator interface {
	Create(stmt *influxql.SelectStatement, opt hybridqp.Options,
		shards interface {
			LogicalPlanCreator
			io.Closer
		}, columns []string, MaxPointN int, now time.Time) PreparedStatement
}

func RegistryStmtBuilderCreator(creator StmtBuilderCreator) bool {
	factory := GetStmtBuilderFactoryInstance()

	factory.Attach(creator)

	return true
}

type StmtBuilderCreatorFactory struct {
	creator StmtBuilderCreator
}

func NewStmtBuilderCreatorFactory() *StmtBuilderCreatorFactory {
	return &StmtBuilderCreatorFactory{
		creator: nil,
	}
}

func (r *StmtBuilderCreatorFactory) Attach(creator StmtBuilderCreator) {
	r.creator = creator
}

func (r *StmtBuilderCreatorFactory) Get() StmtBuilderCreator {
	return r.creator
}

func (r *StmtBuilderCreatorFactory) Create(stmt *influxql.SelectStatement, opt hybridqp.Options,
	shards interface {
		LogicalPlanCreator
		io.Closer
	}, columns []string, MaxPointN int, now time.Time) StmtBuilder {
	return r.creator.Create(stmt, opt,
		shards, columns, MaxPointN, now)
}

var instanceStmtBuilder *StmtBuilderCreatorFactory
var onceStmtBuilder sync.Once

func GetStmtBuilderFactoryInstance() *StmtBuilderCreatorFactory {
	onceStmtBuilder.Do(func() {
		instanceStmtBuilder = NewStmtBuilderCreatorFactory()
	})

	return instanceStmtBuilder
}

func NewPreparedStatement(stmt *influxql.SelectStatement, opt hybridqp.Options,
	shards interface {
		LogicalPlanCreator
		io.Closer
	}, columns []string, MaxPointN int, now time.Time) PreparedStatement {
	c := GetStmtBuilderFactoryInstance().Get()
	return c.Create(stmt, opt, shards, columns, MaxPointN, now)
}
