/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxql/blob/v1.1.0/ast.go

2022.01.23 changed.
Add statement cases:
AlterShardKeyStatement
ShowFieldKeysStatement
ShowFieldKeyCardinalityStatement
ShowTagKeyCardinalityStatement
ShowSeriesStatement
ShowTagValuesCardinalityStatement
PrepareSnapshotStatement
EndPrepareSnapshotStatement
GetRuntimeInfoStatement
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/
package influxql

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"net"
	"regexp"
	"regexp/syntax"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/obs"
	internal "github.com/openGemini/openGemini/lib/util/lifted/influx/influxql/internal"
	"github.com/openGemini/openGemini/lib/util/lifted/protobuf/proto"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	POW      = "pow"
	POW_OP   = 57342
	ATAN2    = "ATAN2"
	ATAN2_OP = 57343
)

// DataType represents the primitive data typ available in InfluxQL.
type DataType int

const (
	// Unknown primitive data type.
	Unknown DataType = 0
	// Float means the data type is a float.
	Float DataType = 1
	// Integer means the data type is an integer.
	Integer DataType = 2
	// String means the data type is a string of text.
	String DataType = 3
	// Boolean means the data type is a boolean.
	Boolean DataType = 4
	// Time means the data type is a time.
	Time DataType = 5
	// Duration means the data type is a duration of time.
	Duration DataType = 6
	// Tag means the data type is a tag.
	Tag DataType = 7
	// AnyField means the data type is any field.
	AnyField DataType = 8
	// Unsigned means the data type is an unsigned integer.
	Unsigned DataType = 9
	// FloatTuple means the data type is a float tuple.
	FloatTuple      DataType = 10
	DefaultFieldKey string   = "value"
	Graph           DataType = 11
)

const (
	// MinTime is the minumum time that can be represented.
	//
	// 1677-09-21 00:12:43.145224194 +0000 UTC
	//
	// The two lowest minimum integers are used as sentinel values.  The
	// minimum value needs to be used as a value lower than any other value for
	// comparisons and another separate value is needed to act as a sentinel
	// default value that is unusable by the user, but usable internally.
	// Because these two values need to be used for a special purpose, we do
	// not allow users to write points at these two times.
	MinTime = int64(math.MinInt64) + 2

	// MaxTime is the maximum time that can be represented.
	//
	// 2262-04-11 23:47:16.854775806 +0000 UTC
	//
	// The highest time represented by a nanosecond needs to be used for an
	// exclusive range in the shard group, so the maximum time needs to be one
	// less than the possible maximum number of nanoseconds representable by an
	// int64 so that we don't lose a point at that one time.
	MaxTime = int64(math.MaxInt64) - 1

	HASH  = "hash"
	RANGE = "range"

	SeqIDField   = "__seq_id___"
	ShardIDField = "__shard_id___"
)

var (
	// ErrInvalidTime is returned when the timestamp string used to
	// compare against time field is invalid.
	ErrInvalidTime            = errors.New("invalid timestamp string")
	ErrNeedBatchMap           = errors.New("try batch map later")
	ErrUnsupportBatchMap      = errors.New("remote node unsupport batch map type")
	ErrDeclareEmptyCollection = errors.New("declare empty collection")
	ErrNotMst                 = errors.New("expect measurement type")
	ErrNotMstForCTE           = errors.New("expect measurement type of CTE")
	ErrNoMstName              = errors.New("expect measurement name when converting the measurement type to the subquery type in the join scenario")
	ErrEmptyFields            = errors.New("empty fields when converting the measurement type to the subquery type in the join scenario")
	ErrUnionSourceCheck       = errors.New("only support union or subquery")
	ErrUnionLeftSource        = errors.New("union left need select statement")
	ErrUnionRightSource       = errors.New("union right need select statement")
	ErrUnionColumnsCount      = errors.New("union/union all can only apply to expressions with the same number of result columns")
	ErrUnionTypeByIndex       = errors.New("columns in the same index position must have the same data type when using union/union all")
	ErrUnionTypeByName        = errors.New("columns with same name must have the same data type when using union by name/union all by name")
	ErrSourceOfCTE            = errors.New("err type of src: only CTE or SubQuery is needed")
)

// InspectDataType returns the data type of a given value.
func InspectDataType(v interface{}) DataType {
	switch v.(type) {
	case float64:
		return Float
	case int64, int32, int:
		return Integer
	case string:
		return String
	case bool:
		return Boolean
	case uint64:
		return Unsigned
	case time.Time:
		return Time
	case time.Duration:
		return Duration
	default:
		return Unknown
	}
}

// DataTypeFromString returns a data type given the string representation of that
// data type.
func DataTypeFromString(s string) DataType {
	switch s {
	case "float":
		return Float
	case "floatTuple":
		return FloatTuple
	case "integer":
		return Integer
	case "unsigned":
		return Unsigned
	case "string":
		return String
	case "boolean":
		return Boolean
	case "time":
		return Time
	case "duration":
		return Duration
	case "tag":
		return Tag
	case "field":
		return AnyField
	default:
		return Unknown
	}
}

type FieldNameSpace struct {
	DataType DataType
	RealName string
}

// LessThan returns true if the other DataType has greater precedence than the
// current data type. Unknown has the lowest precedence.
//
// NOTE: This is not the same as using the `<` or `>` operator because the
// integers used decrease with higher precedence, but Unknown is the lowest
// precedence at the zero value.
func (d DataType) LessThan(other DataType) bool {
	if d == Unknown {
		return true
	} else if d == Unsigned {
		return other != Unknown && other <= Integer
	} else if other == Unsigned {
		return d >= String
	}
	return other != Unknown && other < d
}

var (
	zeroFloat64    interface{} = float64(0)
	zeroInt64      interface{} = int64(0)
	zeroUint64     interface{} = uint64(0)
	zeroString     interface{} = ""
	zeroBoolean    interface{} = false
	zeroTime       interface{} = time.Time{}
	zeroDuration   interface{} = time.Duration(0)
	zeroFloatTuple interface{} = []float64{}
)

// Zero returns the zero value for the DataType.
// The return value of this method, when sent back to InspectDataType,
// may not produce the same value.
func (d DataType) Zero() interface{} {
	switch d {
	case Float:
		return zeroFloat64
	case FloatTuple:
		return zeroFloatTuple
	case Integer:
		return zeroInt64
	case Unsigned:
		return zeroUint64
	case String, Tag:
		return zeroString
	case Boolean:
		return zeroBoolean
	case Time:
		return zeroTime
	case Duration:
		return zeroDuration
	}
	return nil
}

// String returns the human-readable string representation of the DataType.
func (d DataType) String() string {
	switch d {
	case Float:
		return "float"
	case FloatTuple:
		return "floatTuple"
	case Integer:
		return "integer"
	case Unsigned:
		return "unsigned"
	case String:
		return "string"
	case Boolean:
		return "boolean"
	case Time:
		return "time"
	case Duration:
		return "duration"
	case Tag:
		return "tag"
	case AnyField:
		return "field"
	case Graph:
		return "graph"
	}
	return "unknown"
}

type Schema struct {
	TagKeys  []string
	Files    int64
	FileSize int64
	MinTime  int64
	MaxTime  int64
}

func (s *Schema) AddMinMaxTime(minT, maxT int64) {
	if s.MinTime > minT {
		s.MinTime = minT
	}
	if s.MaxTime < maxT {
		s.MaxTime = maxT
	}
}

type Position struct {
	Begin int
	End   int
}

type BufPositionsMap map[Node]Position

// Structure to represent single large expression string representation with ability
// to get sub-expressions string representations as string slices of one single string.
// Effective to mitigate multiple allocations and re-rendering of the same bits.
// NOTE: should not be used on expression that is being mutated during the process.
type NestedNodeStringRepresentation struct {
	QueryString   string
	ExprPositions BufPositionsMap
}

func NewNestedNodeStringRepresentation(n Node) *NestedNodeStringRepresentation {
	nsr := &NestedNodeStringRepresentation{ExprPositions: BufPositionsMap{}}
	nsr.QueryString = n.RenderBytes(&bytes.Buffer{}, nsr.ExprPositions).String()
	return nsr
}

func (nsr *NestedNodeStringRepresentation) NodeString(n Node) string {
	pos, ok := nsr.ExprPositions[n]
	if !ok { // Fallback to avoid panics in case of code bug
		return n.String()
	}
	return nsr.QueryString[pos.Begin:pos.End]
}

// Node represents a node in the InfluxDB abstract syntax tree.
type Node interface {
	// node is unexported to ensure implementations of Node
	// can only originate in this package.
	node()
	// Recursively renders text representation of node and all its descendants
	// in a given buffer and if map is given, writes slice coordinates into map.
	RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer
	// Renders text representation of the node as string using RenderBytes.
	String() string
}

type nodeDepthTester interface {
	// Test-only method to compute depth. Could be used to validate that
	// depth is correctly computed by parsing methods and to initialize
	// nodes for reflect.DeepEqual later comparison in other tests.
	UpdateDepthForTests() int
}

// Workaround for nil-nil interfaces
func CallUpdateDepthForTests(n nodeDepthTester) int {
	if n == nil {
		return 0
	}
	return n.UpdateDepthForTests()
}

type nodeGuard interface {
	// Any AST Node tracks its depth for complexity enforcement.
	// To make sure lists don't have Depth method, we wrap them and
	// stack guards only run at nodeGuard instances, unwrapping for Node.
	Depth() int
}

func (*Query) node() {}

func (Statements) node() {}

func (*AlterRetentionPolicyStatement) node() {}

func (*CreateContinuousQueryStatement) node() {}

func (*CreateDatabaseStatement) node() {}

func (*CreateMeasurementStatement) node() {}

func (*AlterShardKeyStatement) node() {}

func (*CreateRetentionPolicyStatement) node() {}

func (*CreateSubscriptionStatement) node() {}

func (*CreateUserStatement) node() {}

func (*Distinct) node() {}

func (*DeleteSeriesStatement) node() {}

func (*DeleteStatement) node() {}

func (*DropContinuousQueryStatement) node() {}

func (*DropDatabaseStatement) node() {}

func (*DropMeasurementStatement) node() {}

func (*DropRetentionPolicyStatement) node() {}

func (*DropSeriesStatement) node() {}

func (*DropShardStatement) node() {}

func (*DropSubscriptionStatement) node() {}

func (*DropUserStatement) node() {}

func (*ExplainStatement) node() {}

func (*GrantStatement) node() {}

func (*GrantAdminStatement) node() {}

func (*KillQueryStatement) node() {}

func (*RevokeStatement) node() {}

func (*RevokeAdminStatement) node() {}

func (*SelectStatement) node() {}

func (*SetPasswordUserStatement) node() {}

func (*ShowContinuousQueriesStatement) node() {}

func (*ShowGrantsForUserStatement) node() {}

func (*ShowDatabasesStatement) node() {}

func (*ShowFieldKeyCardinalityStatement) node() {}

func (*ShowFieldKeysStatement) node() {}

func (*ShowRetentionPoliciesStatement) node() {}

func (*ShowMeasurementCardinalityStatement) node() {}

func (*ShowMeasurementsStatement) node() {}

func (*ShowMeasurementsDetailStatement) node() {}

func (*ShowQueriesStatement) node() {}

func (*ShowSeriesStatement) node() {}

func (*ShowSeriesCardinalityStatement) node() {}

func (*ShowShardGroupsStatement) node() {}

func (*ShowShardsStatement) node() {}

func (*ShowStatsStatement) node() {}

func (*ShowSubscriptionsStatement) node() {}

func (*ShowDiagnosticsStatement) node() {}

func (*ShowTagKeyCardinalityStatement) node() {}

func (*ShowTagKeysStatement) node() {}

func (*ShowTagValuesCardinalityStatement) node() {}

func (*ShowTagValuesStatement) node() {}

func (*ShowUsersStatement) node() {}

func (*WithSelectStatement) node() {}

func (*GraphStatement) node() {}

func (*BinaryExpr) node()                  {}
func (*BooleanLiteral) node()              {}
func (*BoundParameter) node()              {}
func (*Call) node()                        {}
func (*Dimension) node()                   {}
func (Dimensions) node()                   {}
func (*DurationLiteral) node()             {}
func (*IntegerLiteral) node()              {}
func (*UnsignedLiteral) node()             {}
func (*Field) node()                       {}
func (Fields) node()                       {}
func (*Measurement) node()                 {}
func (Measurements) node()                 {}
func (*NilLiteral) node()                  {}
func (*NumberLiteral) node()               {}
func (*ParenExpr) node()                   {}
func (*RegexLiteral) node()                {}
func (*ListLiteral) node()                 {}
func (*SetLiteral) node()                  {}
func (*SortField) node()                   {}
func (SortFields) node()                   {}
func (Sources) node()                      {}
func (*StringLiteral) node()               {}
func (*SubQuery) node()                    {}
func (*Join) node()                        {}
func (*Union) node()                       {}
func (*Target) node()                      {}
func (*TimeLiteral) node()                 {}
func (*VarRef) node()                      {}
func (*Wildcard) node()                    {}
func (*PrepareSnapshotStatement) node()    {}
func (*EndPrepareSnapshotStatement) node() {}

func (*GetRuntimeInfoStatement) node() {}

func (*Hint) node() {}

func (Hints) node() {}

func (*MatchExpr) node() {}

func (*InCondition) node() {}

func (*Unnest) node() {}

func (*BinOp) node() {}

func (*CTE) node() {}

func (CTES) node() {}

func (*TableFunction) node() {}

// Query represents a collection of ordered statements.
type Query struct {
	Statements Statements
	depth      int
}

func (q *Query) Depth() int {
	if q != nil {
		return q.depth
	}
	return 0
}

func (q *Query) UpdateDepthForTests() int {
	if q != nil {
		q.depth = 1 + q.Statements.UpdateDepthForTests()
		return q.depth
	} else {
		return 0
	}
}

// String returns a string representation of the query.
func (q *Query) String() string { return q.RenderBytes(&bytes.Buffer{}, nil).String() }

func (q *Query) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_ = q.Statements.RenderBytes(buf, posmap)

	if posmap != nil {
		posmap[q] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (q *Query) StringAndLocs() (string, [][2]int) { return q.Statements.StringAndLocs() }

func (q *Query) SetReturnErr(returnErr bool) {
	for i := range q.Statements {
		if s, ok := q.Statements[i].(*SelectStatement); ok {
			s.ReturnErr = returnErr
		}
	}
}

func (q *Query) SetPromRemoteRead(isPromRemoteRead bool) {
	for i := range q.Statements {
		if s, ok := q.Statements[i].(*SelectStatement); ok {
			s.IsPromRemoteRead = isPromRemoteRead
		}
	}
}

// Statements represents a list of statements.
type Statements []Statement

func (a Statements) UpdateDepthForTests() int {
	if a != nil {
		depth := 0
		for i, s := range a {
			depth = max(depth, 1+i+CallUpdateDepthForTests(s))
		}
		return depth
	}
	return 0
}

// String returns a string representation of the statements.
func (a Statements) String() string { return a.RenderBytes(&bytes.Buffer{}, nil).String() }

func (a Statements) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	for i, stmt := range a {
		if i > 0 {
			_, _ = buf.WriteString(";\n")
		}
		_ = stmt.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[a] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (a Statements) StringAndLocs() (string, [][2]int) {
	var str []string
	locs := make([][2]int, len(a))
	loc := 0
	for i, stmt := range a {
		locs[i][0] = loc
		tmpStr := stmt.String()
		str = append(str, tmpStr)
		loc += len(tmpStr)
		locs[i][1] = loc
		loc += 2
	}
	return strings.Join(str, ";\n"), locs
}

// Statement represents a single command in InfluxQL.
type Statement interface {
	Node
	nodeGuard
	nodeDepthTester
	// stmt is unexported to ensure implementations of Statement
	// can only originate in this package.
	stmt()
	RequiredPrivileges() (ExecutionPrivileges, error)
}

// HasDefaultDatabase provides an interface to get the default database from a Statement.
type HasDefaultDatabase interface {
	Node
	// stmt is unexported to ensure implementations of HasDefaultDatabase
	// can only originate in this package.
	stmt()
	DefaultDatabase() string
}

// ExecutionPrivilege is a privilege required for a user to execute
// a statement on a database or resource.
type ExecutionPrivilege struct {
	// Admin privilege required.
	Admin bool

	// Name of the database.
	Name string

	//flag the stmt can be executed by rwuser
	Rwuser bool

	// Database privilege required.
	Privilege Privilege
}

// ExecutionPrivileges is a list of privileges required to execute a statement.
type ExecutionPrivileges []ExecutionPrivilege

func (*AlterRetentionPolicyStatement) stmt() {}

func (*CreateContinuousQueryStatement) stmt() {}

func (*CreateDatabaseStatement) stmt() {}

func (*CreateMeasurementStatement) stmt() {}

func (*AlterShardKeyStatement) stmt() {}

func (*CreateRetentionPolicyStatement) stmt() {}

func (*CreateSubscriptionStatement) stmt() {}

func (*CreateUserStatement) stmt() {}

func (*DeleteSeriesStatement) stmt() {}

func (*DeleteStatement) stmt() {}

func (*DropContinuousQueryStatement) stmt() {}

func (*DropDatabaseStatement) stmt() {}

func (*DropMeasurementStatement) stmt() {}

func (*DropRetentionPolicyStatement) stmt() {}

func (*DropSeriesStatement) stmt() {}

func (*DropSubscriptionStatement) stmt() {}

func (*DropUserStatement) stmt() {}

func (*ExplainStatement) stmt() {}

func (*GrantStatement) stmt() {}

func (*GrantAdminStatement) stmt() {}

func (*KillQueryStatement) stmt() {}

func (*ShowContinuousQueriesStatement) stmt() {}

func (*ShowGrantsForUserStatement) stmt() {}

func (*ShowDatabasesStatement) stmt() {}

func (*ShowFieldKeyCardinalityStatement) stmt() {}

func (*ShowFieldKeysStatement) stmt() {}

func (*ShowMeasurementCardinalityStatement) stmt() {}

func (*ShowMeasurementsStatement) stmt() {}

func (*ShowMeasurementsDetailStatement) stmt() {}

func (*ShowQueriesStatement) stmt() {}

func (*ShowRetentionPoliciesStatement) stmt() {}

func (*ShowSeriesStatement) stmt() {}

func (*ShowSeriesCardinalityStatement) stmt() {}

func (*ShowShardGroupsStatement) stmt() {}

func (*ShowShardsStatement) stmt() {}

func (*ShowStatsStatement) stmt() {}

func (*DropShardStatement) stmt() {}

func (*ShowSubscriptionsStatement) stmt() {}

func (*ShowDiagnosticsStatement) stmt() {}

func (*ShowTagKeyCardinalityStatement) stmt() {}

func (*ShowTagKeysStatement) stmt() {}

func (*ShowTagValuesCardinalityStatement) stmt() {}

func (*ShowTagValuesStatement) stmt() {}

func (*ShowUsersStatement) stmt() {}

func (*RevokeStatement) stmt() {}

func (*RevokeAdminStatement) stmt() {}

func (*SelectStatement) stmt() {}

func (*SetPasswordUserStatement) stmt() {}

func (*PrepareSnapshotStatement) stmt() {}

func (*EndPrepareSnapshotStatement) stmt() {}

func (*GetRuntimeInfoStatement) stmt() {}

func (*WithSelectStatement) stmt() {}

func (*GraphStatement) stmt() {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	nodeGuard
	nodeDepthTester
	// expr is unexported to ensure implementations of Expr
	// can only originate in this package.
	expr()
	RewriteNameSpace(alias, mst string)
}

func (*InCondition) expr()          {}
func (*BinaryExpr) expr()           {}
func (*BooleanLiteral) expr()       {}
func (*BoundParameter) expr()       {}
func (*Call) expr()                 {}
func (*Distinct) expr()             {}
func (*DurationLiteral) expr()      {}
func (*IntegerLiteral) expr()       {}
func (*UnsignedLiteral) expr()      {}
func (*NilLiteral) expr()           {}
func (*NumberLiteral) expr()        {}
func (*ParenExpr) expr()            {}
func (*RegexLiteral) expr()         {}
func (*ListLiteral) expr()          {}
func (*SetLiteral) expr()           {}
func (*StringLiteral) expr()        {}
func (*TimeLiteral) expr()          {}
func (*VarRef) expr()               {}
func (*Wildcard) expr()             {}
func (*ShowClusterStatement) expr() {}

// Literal represents a static literal.
type Literal interface {
	Expr
	// literal is unexported to ensure implementations of Literal
	// can only originate in this package.
	literal()
}

func (*BooleanLiteral) literal() {}

func (*BoundParameter) literal() {}

func (*DurationLiteral) literal() {}

func (*IntegerLiteral) literal() {}

func (*UnsignedLiteral) literal() {}
func (*NilLiteral) literal()      {}
func (*NumberLiteral) literal()   {}
func (*RegexLiteral) literal()    {}
func (*ListLiteral) literal()     {}
func (*SetLiteral) literal()      {}
func (*StringLiteral) literal()   {}
func (*TimeLiteral) literal()     {}

// Source represents a source of data for a statement.
type Source interface {
	Node
	nodeGuard
	nodeDepthTester
	// source is unexported to ensure implementations of Source
	// can only originate in this package.
	source()
	GetName() string
}

func (*Measurement) source() {}

func (*SubQuery) source() {}

func (*Join) source() {}

func (*Union) source() {}

func (*Unnest) source() {}

func (*BinOp) source() {}

func (*CTE) source() {}

func (*TableFunction) source() {}

// Sources represents a list of sources.
type Sources []Source

func (s Sources) UpdateDepthForTests() int {
	depth := 0
	for i, n := range s {
		depth = max(depth, 1+i+CallUpdateDepthForTests(n))
	}
	return depth
}

// String returns a string representation of a Sources array.
func (a Sources) String() string { return a.RenderBytes(&bytes.Buffer{}, nil).String() }

func (a Sources) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	for i, src := range a {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_ = src.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[a] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// Measurements returns all measurements including ones embedded in subqueries.
func (a Sources) Measurements() []*Measurement {
	mms := make([]*Measurement, 0, len(a))
	for _, src := range a {
		switch src := src.(type) {
		case *Measurement:
			mms = append(mms, src)
		case *SubQuery:
			mms = append(mms, src.Statement.Sources.Measurements()...)
		case *CTE:
			// todo: graph only support tsstore
			if src.GraphQuery != nil {
				return []*Measurement{&Measurement{EngineType: config.TSSTORE}}
			}
			mms = append(mms, src.Query.Sources.Measurements()...)
		}
	}
	return mms
}

func (a Sources) HaveMultiStore() bool {
	msts := a.Measurements()
	if len(msts) <= 1 {
		return false
	}
	initStoreType := msts[0].EngineType
	for i := 1; i < len(msts); i++ {
		if msts[i].EngineType != initStoreType {
			return true
		}
	}
	return false
}

func (a Sources) HaveOnlyTSStore() bool {
	msts := a.Measurements()
	for i := range msts {
		if msts[i].EngineType != config.TSSTORE {
			return false
		}
	}
	return true
}

func (a Sources) HaveOnlyCSStore() bool {
	msts := a.Measurements()
	for i := range msts {
		if msts[i].EngineType != config.COLUMNSTORE {
			return false
		}
	}
	return true
}

func (a Sources) IsUnifyPlan() bool {
	return config.IsLogKeeper()
}

func (a Sources) IsSubQuery() bool {
	if len(a) != 1 {
		return false
	}
	_, ok := a[0].(*SubQuery)
	return ok
}

func (a Sources) IsCTE() bool {
	if len(a) != 1 {
		return false
	}
	_, ok := a[0].(*CTE)
	return ok
}

// MarshalBinary encodes a list of sources to a binary format.
func (a Sources) MarshalBinary() ([]byte, error) {
	var pb internal.Measurements
	pb.Items = make([]*internal.Measurement, len(a))
	for i, source := range a {
		pb.Items[i] = encodeMeasurement(source.(*Measurement))
	}
	return proto.Marshal(&pb)
}

// UnmarshalBinary decodes binary data into a list of sources.
func (a *Sources) UnmarshalBinary(buf []byte) error {
	var pb internal.Measurements
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	*a = make(Sources, len(pb.GetItems()))
	for i := range pb.GetItems() {
		mm, err := decodeMeasurement(pb.GetItems()[i])
		if err != nil {
			return err
		}
		(*a)[i] = mm
	}
	return nil
}

// RequiredPrivileges recursively returns a list of execution privileges required.
func (a Sources) RequiredPrivileges() (ExecutionPrivileges, error) {
	var ep ExecutionPrivileges
	for _, source := range a {
		switch source := source.(type) {
		case *Measurement:
			ep = append(ep, ExecutionPrivilege{
				Name:      source.Database,
				Privilege: ReadPrivilege,
				Rwuser:    true,
			})
		case *SubQuery:
			privs, err := source.Statement.RequiredPrivileges()
			if err != nil {
				return nil, err
			}
			ep = append(ep, privs...)
		case *Join:
			var sources Sources
			sources = append(sources, source.LSrc)
			sources = append(sources, source.RSrc)
			privs, err := sources.RequiredPrivileges()
			if err != nil {
				return nil, err
			}
			ep = append(ep, privs...)
		case *Union:
			var sources Sources
			sources = append(sources, source.LSrc)
			sources = append(sources, source.RSrc)
			privs, err := sources.RequiredPrivileges()
			if err != nil {
				return nil, err
			}
			ep = append(ep, privs...)
		default:
			return nil, fmt.Errorf("invalid source: %s", source)
		}
	}
	return ep, nil
}

func (a Sources) HasSubQuery() bool {
	for _, src := range a {
		switch src.(type) {
		case *SubQuery:
			return true
		case *Join, *Union:
			return true
		case *BinOp:
			return true
		}
	}
	return false
}

// Temporary node to build sources list preserving depth.
// Does not intended to be included into the resulting AST,
// thus does not implement node()
type sourcesList struct {
	sources Sources
	depth   int
}

func (s sourcesList) Depth() int { return s.depth }

// IsSystemName returns true if name is an data system name.
func IsSystemName(name string) bool {
	switch name {
	case "_fieldKeys",
		"_measurements",
		"_name",
		"_series",
		"_tagKey",
		"_tagKeys",
		"_tags":
		return true
	default:
		return false
	}
}

// SortField represents a field to sort results by.
type SortField struct {
	// Name of the field.
	Name string

	// Sort order.
	Ascending bool
}

func (field *SortField) Depth() int { return 1 }

func (field *SortField) UpdateDepthForTests() int {
	if field != nil {
		return 1
	}
	return 0
}

// String returns a string representation of a sort field.
func (field *SortField) String() string { return field.RenderBytes(&bytes.Buffer{}, nil).String() }

func (field *SortField) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if field.Name != "" {
		_, _ = buf.WriteString(field.Name)
		_, _ = buf.WriteString(" ")
	}
	if field.Ascending {
		_, _ = buf.WriteString("ASC")
	} else {
		_, _ = buf.WriteString("DESC")
	}

	if posmap != nil {
		posmap[field] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// SortFields represents an ordered list of ORDER BY fields.
type SortFields []*SortField

// Since SortField has constant complexity, this depth is O(1) thus safe without wrapper
func (a SortFields) Depth() int { return 1 + len(a) }

func (a SortFields) UpdateDepthForTests() int {
	if a != nil {
		return 1 + len(a)
	}
	return 0
}

// String returns a string representation of sort fields.
func (a SortFields) String() string { return a.RenderBytes(&bytes.Buffer{}, nil).String() }

func (a SortFields) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	for i, field := range a {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_ = field.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[a] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type DatabasePolicy struct {
	// Replicas indicates the replica num of the database
	Replicas uint32

	// EnableTagArray indicates whether to enable the tag array feature
	EnableTagArray bool
}

// CreateDatabaseStatement represents a command for creating a new database.
type CreateDatabaseStatement struct {
	// Name of the database to be created.
	Name string

	// RetentionPolicyCreate indicates whether the user explicitly wants to create a retention policy.
	RetentionPolicyCreate bool

	// RetentionPolicyDuration indicates retention duration for the new database.
	RetentionPolicyDuration *time.Duration

	// RetentionPolicyReplication indicates retention replication for the new database.
	RetentionPolicyReplication *int

	// RetentionPolicyName indicates retention name for the new database.
	RetentionPolicyName string

	// RetentionPolicyShardGroupDuration indicates shard group duration for the new database.
	RetentionPolicyShardGroupDuration time.Duration

	RetentionPolicyShardMergeDuration time.Duration

	ReplicaNum uint32

	RetentionPolicyHotDuration time.Duration

	RetentionPolicyWarmDuration time.Duration

	RetentionPolicyIndexColdDuration time.Duration

	RetentionPolicyIndexGroupDuration time.Duration

	ShardKey []string

	DatabaseAttr DatabasePolicy

	ObsOptions *obs.ObsOptions
}

// Create Database only has complexity in ShardKey list, so no extra tracking necessary.
func (s *CreateDatabaseStatement) Depth() int {
	if s != nil {
		return 1 + len(s.ShardKey)
	}
	return 0
}

func (s *CreateDatabaseStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1 + len(s.ShardKey)
	}
	return 0
}

// String returns a string representation of the create database statement.
func (s *CreateDatabaseStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *CreateDatabaseStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("CREATE DATABASE ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	if s.RetentionPolicyCreate {
		_, _ = buf.WriteString(" WITH")
		if s.RetentionPolicyDuration != nil {
			_, _ = buf.WriteString(" DURATION ")
			_, _ = buf.WriteString(s.RetentionPolicyDuration.String())
		}
		if s.RetentionPolicyReplication != nil {
			_, _ = buf.WriteString(" REPLICATION ")
			_, _ = buf.WriteString(strconv.Itoa(*s.RetentionPolicyReplication))
		}
		if s.RetentionPolicyShardGroupDuration > 0 {
			_, _ = buf.WriteString(" SHARD DURATION ")
			_, _ = buf.WriteString(s.RetentionPolicyShardGroupDuration.String())
		}
		if s.RetentionPolicyShardMergeDuration > 0 {
			_, _ = buf.WriteString(" SHARDMERGE DURATION ")
			_, _ = buf.WriteString(s.RetentionPolicyShardMergeDuration.String())
		}
		if s.RetentionPolicyName != "" {
			_, _ = buf.WriteString(" NAME ")
			_, _ = buf.WriteString(QuoteIdent(s.RetentionPolicyName))
		}
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a CreateDatabaseStatement.
func (s *CreateDatabaseStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

type CreateMeasurementStatement struct {
	Database            string
	RetentionPolicy     string
	Name                string
	ShardKey            []string
	NumOfShards         int64
	EngineType          string
	PrimaryKey          []string
	SortKey             []string
	Property            [][]string
	Type                string
	Tags                map[string]int32
	Fields              map[string]int32
	IndexType           []string
	IndexList           [][]string
	IndexOption         []*IndexOption
	TimeClusterDuration time.Duration
	CompactType         string
	TTL                 time.Duration
}

func (s *CreateMeasurementStatement) Depth() int {
	if s != nil {
		return 1 + max(len(s.ShardKey), len(s.PrimaryKey), len(s.SortKey), len(s.Property), len(s.Tags), len(s.Fields), len(s.IndexType), len(s.IndexList), len(s.IndexOption))
	}
	return 0
}

func (s *CreateMeasurementStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1 + max(len(s.ShardKey), len(s.PrimaryKey), len(s.SortKey), len(s.Property), len(s.Tags), len(s.Fields), len(s.IndexType), len(s.IndexList), len(s.IndexOption))
	}
	return 0
}

type CreateMeasurementStatementOption struct {
	IndexType           []string
	IndexList           [][]string
	ShardKey            []string
	NumOfShards         int64
	Type                string
	EngineType          string
	PrimaryKey          []string
	SortKey             []string
	Property            [][]string
	TimeClusterDuration time.Duration
	CompactType         string
	TTL                 time.Duration
}

type IndexOption struct {
	Tokens              string
	TokensTable         []byte // not stored in meta,converted from 'IndexOption.Tokens'
	Tokenizers          string
	TimeClusterDuration time.Duration
}

func (io *IndexOption) Clone() *IndexOption {
	clone := *io
	return &clone
}

type IndexOptions struct {
	Options []*IndexOption
}

func (ios *IndexOptions) Clone() *IndexOptions {
	if ios == nil {
		return nil
	}
	clone := IndexOptions{
		Options: make([]*IndexOption, len(ios.Options)),
	}
	for i := range ios.Options {
		clone.Options[i] = ios.Options[i].Clone()
	}
	return &clone
}

func (s *CreateMeasurementStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *CreateMeasurementStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("CREATE MEASUREMENT ")
	if s.Database != "" {
		_, _ = buf.WriteString(QuoteIdent(s.Database))
		_, _ = buf.WriteString(".")
	}

	if s.RetentionPolicy != "" {
		_, _ = buf.WriteString(QuoteIdent(s.RetentionPolicy))
		_, _ = buf.WriteString(".")
	}

	if s.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(s.Name))
	}

	_, _ = buf.WriteString(" WITH")
	if len(s.ShardKey) > 0 {
		shardKey := strings.Join(s.ShardKey, ",")
		_, _ = buf.WriteString(" SHARDKEY ")
		_, _ = buf.WriteString(shardKey)
	}

	if len(s.IndexList) > 0 {
		for i := range s.IndexType {
			_, _ = buf.WriteString(" INDEXTYPE ")
			_, _ = buf.WriteString(s.IndexType[i])

			IndexList := strings.Join(s.IndexList[i], ",")
			_, _ = buf.WriteString(" INDEXLIST ")
			_, _ = buf.WriteString(IndexList)
		}

	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *CreateMeasurementStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

type AlterShardKeyStatement struct {
	Database        string
	RetentionPolicy string
	Name            string
	ShardKey        []string
	Type            string
}

func (s *AlterShardKeyStatement) Depth() int {
	if s != nil {
		return 1 + len(s.ShardKey)
	}
	return 0
}

func (s *AlterShardKeyStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1 + len(s.ShardKey)
	}
	return 0
}

func (s *AlterShardKeyStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *AlterShardKeyStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("ALTER MEASUREMENT ")
	if s.Database != "" {
		_, _ = buf.WriteString(QuoteIdent(s.Database))
		_, _ = buf.WriteString(".")
	}

	if s.RetentionPolicy != "" {
		_, _ = buf.WriteString(QuoteIdent(s.RetentionPolicy))
		_, _ = buf.WriteString(".")
	}

	if s.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(s.Name))
	}

	_, _ = buf.WriteString(" WITH")
	shardKey := strings.Join(s.ShardKey, ",")
	_, _ = buf.WriteString(" SHARDKEY ")
	_, _ = buf.WriteString(shardKey)

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *AlterShardKeyStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// DropDatabaseStatement represents a command to drop a database.
type DropDatabaseStatement struct {
	// Name of the database to be dropped.
	Name string
}

func (s *DropDatabaseStatement) Depth() int { return 1 }

func (s *DropDatabaseStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the drop database statement.
func (s *DropDatabaseStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *DropDatabaseStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DROP DATABASE ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a DropDatabaseStatement.
func (s *DropDatabaseStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// DropRetentionPolicyStatement represents a command to drop a retention policy from a database.
type DropRetentionPolicyStatement struct {
	// Name of the policy to drop.
	Name string

	// Name of the database to drop the policy from.
	Database string
}

func (s *DropRetentionPolicyStatement) Depth() int { return 1 }

func (s *DropRetentionPolicyStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the drop retention policy statement.
func (s *DropRetentionPolicyStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *DropRetentionPolicyStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DROP RETENTION POLICY ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.Database))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a DropRetentionPolicyStatement.
func (s *DropRetentionPolicyStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: WritePrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *DropRetentionPolicyStatement) DefaultDatabase() string {
	return s.Database
}

// CreateUserStatement represents a command for creating a new user.
type CreateUserStatement struct {
	// Name of the user to be created.
	Name string

	// User's password.
	Password string

	// User's admin privilege.
	Admin bool

	//rwuser
	Rwuser bool
}

func (s *CreateUserStatement) Depth() int { return 1 }

func (s *CreateUserStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the create user statement.
func (s *CreateUserStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *CreateUserStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("CREATE USER ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" WITH PASSWORD ")
	_, _ = buf.WriteString("[REDACTED]")
	if s.Admin {
		_, _ = buf.WriteString(" WITH ALL PRIVILEGES")
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a CreateUserStatement.
func (s *CreateUserStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// DropUserStatement represents a command for dropping a user.
type DropUserStatement struct {
	// Name of the user to drop.
	Name string
}

func (s *DropUserStatement) Depth() int { return 1 }

func (s *DropUserStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the drop user statement.
func (s *DropUserStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *DropUserStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DROP USER ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a DropUserStatement.
func (s *DropUserStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// Privilege is a type of action a user can be granted the right to use.
type Privilege int

const (
	// NoPrivileges means no privileges required / granted / revoked.
	NoPrivileges Privilege = iota
	// ReadPrivilege means read privilege required / granted / revoked.
	ReadPrivilege
	// WritePrivilege means write privilege required / granted / revoked.
	WritePrivilege
	// AllPrivileges means all privileges required / granted / revoked.
	AllPrivileges
)

// NewPrivilege returns an initialized *Privilege.
func NewPrivilege(p Privilege) *Privilege { return &p }

// String returns a string representation of a Privilege.
func (p Privilege) String() string {
	switch p {
	case NoPrivileges:
		return "NO PRIVILEGES"
	case ReadPrivilege:
		return "READ"
	case WritePrivilege:
		return "WRITE"
	case AllPrivileges:
		return "ALL PRIVILEGES"
	}
	return ""
}

// GrantStatement represents a command for granting a privilege.
type GrantStatement struct {
	// The privilege to be granted.
	Privilege Privilege

	// Database to grant the privilege to.
	On string

	// Who to grant the privilege to.
	User string
}

func (s *GrantStatement) Depth() int { return 1 }

func (s *GrantStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the grant statement.
func (s *GrantStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *GrantStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("GRANT ")
	_, _ = buf.WriteString(s.Privilege.String())
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.On))
	_, _ = buf.WriteString(" TO ")
	_, _ = buf.WriteString(QuoteIdent(s.User))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a GrantStatement.
func (s *GrantStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *GrantStatement) DefaultDatabase() string {
	return s.On
}

// GrantAdminStatement represents a command for granting admin privilege.
type GrantAdminStatement struct {
	// Who to grant the privilege to.
	User string
}

func (s *GrantAdminStatement) Depth() int { return 1 }

func (s *GrantAdminStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the grant admin statement.
func (s *GrantAdminStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *GrantAdminStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("GRANT ALL PRIVILEGES TO ")
	_, _ = buf.WriteString(QuoteIdent(s.User))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a GrantAdminStatement.
func (s *GrantAdminStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// KillQueryStatement represents a command for killing a query.
type KillQueryStatement struct {
	// The query to kill.
	QueryID uint64

	// The host to delegate the kill to.
	Host string
}

func (s *KillQueryStatement) Depth() int { return 1 }

func (s *KillQueryStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the kill query statement.
func (s *KillQueryStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *KillQueryStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("KILL QUERY ")
	_, _ = buf.WriteString(strconv.FormatUint(s.QueryID, 10))
	if s.Host != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Host))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a KillQueryStatement.
func (s *KillQueryStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// SetPasswordUserStatement represents a command for changing user password.
type SetPasswordUserStatement struct {
	// Plain-text password.
	Password string

	// Who to grant the privilege to.
	Name string
}

func (s *SetPasswordUserStatement) Depth() int { return 1 }

func (s *SetPasswordUserStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the set password statement.
func (s *SetPasswordUserStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *SetPasswordUserStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SET PASSWORD FOR ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" = ")
	_, _ = buf.WriteString("[REDACTED]")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a SetPasswordUserStatement.
func (s *SetPasswordUserStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// RevokeStatement represents a command to revoke a privilege from a user.
type RevokeStatement struct {
	// The privilege to be revoked.
	Privilege Privilege

	// Database to revoke the privilege from.
	On string

	// Who to revoke privilege from.
	User string
}

func (s *RevokeStatement) Depth() int { return 1 }

func (s *RevokeStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the revoke statement.
func (s *RevokeStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *RevokeStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("REVOKE ")
	_, _ = buf.WriteString(s.Privilege.String())
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.On))
	_, _ = buf.WriteString(" FROM ")
	_, _ = buf.WriteString(QuoteIdent(s.User))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a RevokeStatement.
func (s *RevokeStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *RevokeStatement) DefaultDatabase() string {
	return s.On
}

// RevokeAdminStatement represents a command to revoke admin privilege from a user.
type RevokeAdminStatement struct {
	// Who to revoke admin privilege from.
	User string
}

func (s *RevokeAdminStatement) Depth() int { return 1 }

func (s *RevokeAdminStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the revoke admin statement.
func (s *RevokeAdminStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *RevokeAdminStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("REVOKE ALL PRIVILEGES FROM ")
	_, _ = buf.WriteString(QuoteIdent(s.User))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a RevokeAdminStatement.
func (s *RevokeAdminStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// CreateRetentionPolicyStatement represents a command to create a retention policy.
type CreateRetentionPolicyStatement struct {
	// Name of policy to create.
	Name string

	// Name of database this policy belongs to.
	Database string

	// Duration data written to this policy will be retained.
	Duration time.Duration

	// Replication factor for data written to this policy.
	Replication int

	// Should this policy be set as default for the database?
	Default bool

	// Shard Duration.
	ShardGroupDuration time.Duration

	// Hot Duration
	HotDuration time.Duration

	// Warm Duration
	WarmDuration time.Duration

	// Index Cold Duration
	IndexColdDuration time.Duration

	// Index Duration
	IndexGroupDuration time.Duration

	// ShardMerge Duration
	ShardMergeDuration time.Duration
}

func (s *CreateRetentionPolicyStatement) Depth() int { return 1 }

func (s *CreateRetentionPolicyStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the create retention policy.
func (s *CreateRetentionPolicyStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *CreateRetentionPolicyStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("CREATE RETENTION POLICY ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.Database))
	_, _ = buf.WriteString(" DURATION ")
	_, _ = buf.WriteString(FormatDuration(s.Duration))
	_, _ = buf.WriteString(" REPLICATION ")
	_, _ = buf.WriteString(strconv.Itoa(s.Replication))
	if s.ShardGroupDuration > 0 {
		_, _ = buf.WriteString(" SHARD DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.ShardGroupDuration))
	}

	if s.ShardMergeDuration > 0 {
		_, _ = buf.WriteString(" SHARDMERGE DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.ShardMergeDuration))
	}

	if s.HotDuration > 0 {
		_, _ = buf.WriteString(" HOT DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.HotDuration))
	}

	if s.WarmDuration > 0 {
		_, _ = buf.WriteString(" WARM DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.WarmDuration))
	}

	if s.IndexColdDuration > 0 {
		_, _ = buf.WriteString(" INDEXCOLD DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.IndexColdDuration))
	}

	if s.IndexGroupDuration > 0 {
		_, _ = buf.WriteString(" INDEX DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.IndexGroupDuration))
	}
	if s.Default {
		_, _ = buf.WriteString(" DEFAULT")
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a CreateRetentionPolicyStatement.
func (s *CreateRetentionPolicyStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *CreateRetentionPolicyStatement) DefaultDatabase() string {
	return s.Database
}

// AlterRetentionPolicyStatement represents a command to alter an existing retention policy.
type AlterRetentionPolicyStatement struct {
	// Name of policy to alter.
	Name string

	// Name of the database this policy belongs to.
	Database string

	// Duration data written to this policy will be retained.
	Duration *time.Duration

	// Replication factor for data written to this policy.
	Replication *int

	// Should this policy be set as defalut for the database?
	Default bool

	// Duration of the Shard.
	ShardGroupDuration *time.Duration

	// Hot Duration
	HotDuration *time.Duration

	// Warm Duration
	WarmDuration *time.Duration

	// Index Cold Duration
	IndexColdDuration *time.Duration

	// Index Duration
	IndexGroupDuration *time.Duration
}

func (s *AlterRetentionPolicyStatement) Depth() int { return 1 }

func (s *AlterRetentionPolicyStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the alter retention policy statement.
func (s *AlterRetentionPolicyStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *AlterRetentionPolicyStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("ALTER RETENTION POLICY ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.Database))

	if s.Duration != nil {
		_, _ = buf.WriteString(" DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.Duration))
	}

	if s.Replication != nil {
		_, _ = buf.WriteString(" REPLICATION ")
		_, _ = buf.WriteString(strconv.Itoa(*s.Replication))
	}

	if s.ShardGroupDuration != nil {
		_, _ = buf.WriteString(" SHARD DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.ShardGroupDuration))
	}

	if s.HotDuration != nil {
		_, _ = buf.WriteString(" HOT DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.HotDuration))
	}

	if s.WarmDuration != nil {
		_, _ = buf.WriteString(" WARM DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.WarmDuration))
	}

	if s.IndexColdDuration != nil {
		_, _ = buf.WriteString(" INDEXCOLD DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.IndexColdDuration))
	}

	if s.IndexGroupDuration != nil {
		_, _ = buf.WriteString(" INDEX DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.IndexGroupDuration))
	}

	if s.Default {
		_, _ = buf.WriteString(" DEFAULT")
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute an AlterRetentionPolicyStatement.
func (s *AlterRetentionPolicyStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *AlterRetentionPolicyStatement) DefaultDatabase() string {
	return s.Database
}

// FillOption represents different options for filling aggregate windows.
type FillOption int

const (
	// NullFill means that empty aggregate windows will just have null values.
	NullFill FillOption = iota
	// NoFill means that empty aggregate windows will be purged from the result.
	NoFill
	// NumberFill means that empty aggregate windows will be filled with a provided number.
	NumberFill
	// PreviousFill means that empty aggregate windows will be filled with whatever the previous aggregate window had.
	PreviousFill
	// LinearFill means that empty aggregate windows will be filled with whatever a linear value between non null windows.
	LinearFill
)

// SelectStatement represents a command for extracting data from the database.
type SelectStatement struct {
	// Expressions returned from the selection.
	Fields Fields

	// Target (destination) for the result of a SELECT INTO query.
	Target *Target

	// Expressions used for grouping the selection.
	Dimensions Dimensions

	ExceptDimensions Dimensions

	// Whether to drop the given labels rather than keep them.
	Without bool

	//Expressions used for optimize querys
	Hints Hints

	// Data sources (measurements) that fields are extracted from.
	Sources Sources

	// An expression evaluated on data point.
	Condition Expr

	// Fields to sort results by.
	SortFields SortFields

	// Maximum number of rows to be returned. Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int

	// Maxiumum number of series to be returned. Unlimited if zero.
	SLimit int

	// Returns series starting at an offset from the first one.
	SOffset int

	// Memoized group by interval from GroupBy().
	groupByInterval time.Duration

	// Whether it's a query for raw data values (i.e. not an aggregate).
	IsRawQuery bool

	// What fill option the select statement uses, if any.
	Fill FillOption

	// The value to fill empty aggregate buckets with, if any.
	FillValue interface{}

	// The timezone for the query, if any.
	Location *time.Location

	// Renames the implicit time field name.
	TimeAlias string

	// Removes the "time" column from the output.
	OmitTime bool

	// Removes measurement name from resulting query. Useful for meta queries.
	StripName bool

	// Overrides the output measurement name.
	EmitName string

	// Removes duplicate rows from raw queries.
	Dedupe bool

	// GroupByAllDims is true when group by single series
	GroupByAllDims bool

	Schema Schema

	JoinSource []*Join

	UnionSource []*Union

	UnnestSource []*Unnest

	Scroll Scroll

	HasWildcardField bool

	SubQueryHasDifferentAscending bool
	// only useful for topQuery
	StmtId int

	// whether to return error
	ReturnErr bool

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

	CompareOffset time.Duration

	IsCompareCall bool

	BinOpSource []*BinOp

	IsPromQuery bool

	IsPromRemoteRead bool

	PromSubCalls []*PromSubCall

	SelectAllTags bool

	SelectTagToAux bool

	IsCreateStream bool

	// RemoveMetric indicates whether to remove the metric label during the materialize transformation
	// in the query execution of PromQL
	RemoveMetric bool

	InConditons []*InCondition

	// Direct CTE that current selectStatement rely on
	DirectDependencyCTEs CTES
	// All CTE that current selectStatement rely on
	AllDependencyCTEs CTES

	depth int
}

func (s *SelectStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *SelectStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Fields),
			CallUpdateDepthForTests(s.Target),
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Dimensions),
			CallUpdateDepthForTests(s.ExceptDimensions),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.SortFields),
		)
		return s.depth
	} else {
		return 0
	}
}

func (s *SelectStatement) SetStmtId(id int) {
	s.StmtId = id
}

func (s *SelectStatement) StreamCheck(supportTable map[string]bool) error {
	for i := range s.Fields {
		if c, ok := s.Fields[i].Expr.(*Call); ok && !supportTable[c.Name] {
			return errors.New("unsupported call function in stream")
		}
	}

	if s.groupByInterval == 0 {
		return errors.New("should have group by interval time")
	}

	if s.groupByInterval < 5*time.Second {
		return errors.New("streaming aggregation within 5 seconds is not supported")
	}
	return nil
}

func (s *SelectStatement) RewriteUnnestSource() {
	sources := make([]*Unnest, 0, 8)
	s.RewriteUnnestSourceDFS(&sources, s.Sources)
	s.UnnestSource = sources
}

func (s *SelectStatement) RewriteUnnestSourceDFS(unnestSources *[]*Unnest, sources Sources) {
	for i := range sources {
		switch sub := sources[i].(type) {
		case *SubQuery:
			s.RewriteUnnestSourceDFS(&(sub.Statement.UnnestSource), sub.Statement.Sources)
		case *Unnest:
			*unnestSources = append(*unnestSources, CloneSource(sub).(*Unnest))
		}
	}
}

// TimeAscending returns true if the time field is sorted in chronological order.
func (s *SelectStatement) TimeAscending() bool {
	for _, f := range s.SortFields {
		if f.Name == "time" {
			return f.Ascending
		}
	}
	return true
}

func (s *SelectStatement) SetTimeInterval(t time.Duration) {
	s.groupByInterval = t
}

// TimeFieldName returns the name of the time field.
func (s *SelectStatement) TimeFieldName() string {
	if s.TimeAlias != "" {
		return s.TimeAlias
	}
	return "time"
}

// Clone returns a deep copy of the statement.
func (s *SelectStatement) Clone() *SelectStatement {
	clone := *s
	clone.Fields = make(Fields, 0, len(s.Fields))
	clone.Dimensions = make(Dimensions, 0, len(s.Dimensions))
	clone.Sources = cloneSources(s.Sources)
	clone.SortFields = make(SortFields, 0, len(s.SortFields))
	clone.Condition = CloneExpr(s.Condition)
	clone.Location = s.Location

	if s.Target != nil {
		clone.Target = &Target{
			Measurement: &Measurement{
				Database:        s.Target.Measurement.Database,
				RetentionPolicy: s.Target.Measurement.RetentionPolicy,
				Name:            s.Target.Measurement.Name,
				Regex:           CloneRegexLiteral(s.Target.Measurement.Regex),
				MstType:         s.Target.Measurement.MstType,
			},
			depth: s.Target.depth,
		}
	}
	for _, f := range s.Fields {
		clone.Fields = append(clone.Fields, &Field{Expr: CloneExpr(f.Expr), Alias: f.Alias, depth: f.depth})
	}
	for _, d := range s.Dimensions {
		clone.Dimensions = append(clone.Dimensions, &Dimension{Expr: CloneExpr(d.Expr), depth: d.depth})
	}
	for _, f := range s.SortFields {
		clone.SortFields = append(clone.SortFields, &SortField{Name: f.Name, Ascending: f.Ascending})
	}
	return &clone
}

func CloneSource(source Source) Source {
	return cloneSource(source)
}

func cloneSources(sources Sources) Sources {
	clone := make(Sources, 0, len(sources))
	for _, s := range sources {
		clone = append(clone, cloneSource(s))
	}
	return clone
}

func cloneSource(s Source) Source {
	if s == nil {
		return nil
	}

	switch s := s.(type) {
	case *Measurement:
		return s.Clone()
	case *SubQuery:
		return &SubQuery{Statement: s.Statement.Clone(), Alias: s.Alias, depth: s.depth}
	case *Join:
		return &Join{
			LSrc:      cloneSource(s.LSrc),
			RSrc:      cloneSource(s.RSrc),
			Condition: CloneExpr(s.Condition),
			JoinType:  s.JoinType,
			depth:     s.depth,
		}
	case *Union:
		return &Union{
			LSrc:      cloneSource(s.LSrc),
			RSrc:      cloneSource(s.RSrc),
			UnionType: s.UnionType,
			depth:     s.depth,
		}
	case *Unnest:
		return s.Clone()
	case *BinOp:
		return &BinOp{
			LSrc:        cloneSource(s.LSrc),
			RSrc:        cloneSource(s.RSrc),
			LExpr:       CloneExpr(s.LExpr),
			RExpr:       CloneExpr(s.RExpr),
			OpType:      s.OpType,
			On:          s.On,
			MatchKeys:   s.MatchKeys,
			MatchCard:   s.MatchCard,
			IncludeKeys: s.IncludeKeys,
			ReturnBool:  s.ReturnBool,
			NilMst:      s.NilMst,
			depth:       s.depth,
		}
	case *CTE:
		cte := &CTE{Alias: s.Alias, TimeRange: s.TimeRange, Csming: s.Csming, GraphQuery: s.GraphQuery, depth: s.depth}
		if s.Query != nil {
			cte.Query = s.Query.Clone()
		}
		return cte
	case *TableFunction:
		return &TableFunction{FunctionName: s.FunctionName, TableFunctionSource: s.GetTableFunctionSource(), Params: s.GetParams(), depth: s.depth}
	default:
		panic("unreachable")
	}
}

// FieldMapper returns the data type for the field inside of the measurement.
type FieldMapper interface {
	FieldDimensions(m *Measurement) (fields map[string]DataType, dimensions map[string]struct{}, schema *Schema, err error)

	TypeMapper
}

func RewriteMstNameSpace(fields Fields, source Source, hasJoin bool, alias string) error {
	s, ok := source.(*Measurement)
	if !ok {
		return ErrNotMst
	}
	RewriteNameSpace(fields, s.Alias, s.Name, hasJoin, alias)
	return nil
}

func RewriteCTENameSpace(fields Fields, source Source, hasJoin bool, alias string) error {
	s, ok := source.(*CTE)
	if !ok {
		return ErrNotMstForCTE
	}
	RewriteNameSpace(fields, s.Alias, s.GetName(), hasJoin, alias)
	return nil
}

func RewriteNameSpace(fields Fields, sAlias string, sName string, hasJoin bool, alias string) {
	for i := range fields {
		fields[i].Expr.RewriteNameSpace(sAlias, sName)
		f, ok := fields[i].Expr.(*VarRef)
		if !ok || len(fields[i].Alias) > 0 {
			continue
		}
		if len(f.Alias) > 0 {
			fields[i].Alias = f.Alias
		} else if hasJoin && alias != "" {
			if strings.HasPrefix(f.Val, alias+".") {
				fields[i].Alias = f.Val
			} else {
				fields[i].Alias = alias + "." + f.Val
			}
		}
	}
}

func RewriteTableFunctionNameSpace(fields Fields, source Source, hasJoin bool, alias string) error {
	s, ok := source.(*TableFunction)
	if !ok {
		return errors.New("expect TableFunctionParam type")
	}
	RewriteNameSpace(fields, "", s.GetName(), hasJoin, alias)
	return nil
}

func RewriteSubQueryNameSpace(source Source, hasJoin bool) error {
	s, ok := source.(*SubQuery)
	if !ok {
		return ErrNotMst
	}
	for i := range s.Statement.Sources {
		switch src := s.Statement.Sources[i].(type) {
		case *Measurement:
			if e := RewriteMstNameSpace(s.Statement.Fields, src, hasJoin, s.GetName()); e != nil {
				return e
			}
		case *SubQuery:
			if e := RewriteSubQueryNameSpace(src, hasJoin); e != nil {
				return e
			}
		case *CTE:
			if e := RewriteCTENameSpace(s.Statement.Fields, src, hasJoin, s.GetName()); e != nil {
				return e
			}
		}
	}
	return nil
}

func (s *SelectStatement) RewriteJoinCase(src Source, m FieldMapper, batchEn bool, fields Fields, hasJoin bool, joinType JoinType) (Source, error) {
	switch src := src.(type) {
	case *SubQuery:
		if e := RewriteSubQueryNameSpace(src, hasJoin); e != nil {
			return nil, e
		}
		if joinType != FullJoin && joinType != UnknownJoin && len(src.Statement.Dimensions) == 0 {
			src.Statement.Dimensions = s.Dimensions
		}
		stmt, err := src.Statement.RewriteFields(m, batchEn, hasJoin)
		src.Statement = stmt
		if e := RewriteSubQueryNameSpace(src, hasJoin); e != nil {
			return nil, e
		}
		if err == nil {
			src.Statement = stmt
			return src, nil
		}
		if err != ErrDeclareEmptyCollection {
			return nil, err
		}
		return nil, nil
	case *Measurement:
		if e := RewriteMstNameSpace(fields, src, hasJoin, src.GetName()); e != nil {
			return nil, e
		}
		return nil, nil
	}
	return nil, nil
}

func RewriteJoinMeasurement(m *Measurement, outerStatement *SelectStatement, joinKeys []string) (*SubQuery, error) {
	alias := m.Name
	if m.Alias != "" {
		alias = m.Alias
	}
	if alias == "" {
		return nil, ErrNoMstName
	}
	allFields := outerStatement.Fields
	if len(allFields) == 0 {
		return nil, ErrEmptyFields
	}

	statement := outerStatement.Clone()
	subFields := make([]*Field, 0, len(allFields))
	hasWildcard := false
	exprFieldNames := make(map[string]bool)
	for _, field := range allFields {
		switch expr := field.Expr.(type) {
		case *Wildcard:
			subFields = []*Field{&Field{Expr: &Wildcard{Type: 0}, Alias: "", depth: 2}}
			hasWildcard = true
			break
		default:
			GetExprFieldNames(expr, exprFieldNames)
		}
	}
	hasDimWildCard := false
	exprDimNames := make(map[string]bool)
	for _, dim := range outerStatement.Dimensions {
		switch expr := dim.Expr.(type) {
		case *Wildcard:
			hasDimWildCard = true
			break
		default:
			GetExprFieldNames(expr, exprDimNames)
		}
	}
	delete(exprDimNames, "time")

	if !hasWildcard {
		for fieldName := range exprFieldNames {
			if strings.HasPrefix(fieldName, alias+".") {
				subFields = append(subFields, &Field{Expr: &VarRef{Val: fieldName[len(alias)+1:], Type: 0, Alias: ""}, Alias: "", depth: 2})
			}
		}
		for _, key := range joinKeys {
			if !hasDimWildCard && !exprFieldNames[key] && !exprDimNames[key] && !exprDimNames[key[len(alias)+1:]] {
				subFields = append(subFields, &Field{Expr: &VarRef{Val: key[len(alias)+1:], Type: 0, Alias: ""}, Alias: "", depth: 2, Auxiliary: true})
			}
		}
	}
	statement.Fields = subFields
	m.Alias = ""
	statement.Sources = []Source{m}
	statement.JoinSource = nil
	if statement.depth < 3 {
		statement.depth = 3
	}
	return &SubQuery{
		Statement: statement,
		Alias:     alias,
		depth:     1 + statement.depth,
	}, nil
}

func GetExprFieldNames(exp Expr, seen map[string]bool) {
	switch expr := exp.(type) {
	case *VarRef:
		if !seen[expr.Val] {
			seen[expr.Val] = true
		}
	case *Call:
		for _, expr := range expr.Args {
			GetExprFieldNames(expr, seen)
		}
	case *BinaryExpr:
		GetExprFieldNames(expr.LHS, seen)
		GetExprFieldNames(expr.RHS, seen)
	case *ParenExpr:
		GetExprFieldNames(expr.Expr, seen)
	default:
	}
}

func (s *SelectStatement) RewriteUnionCase(src Source, m FieldMapper, batchEn bool) (Source, error) {
	switch src := src.(type) {
	case *SubQuery:
		if e := RewriteSubQueryNameSpace(src, false); e != nil {
			return nil, e
		}
		stmt, err := src.Statement.RewriteFields(m, batchEn, false)
		if err != nil && err != ErrDeclareEmptyCollection {
			return nil, err
		}
		if dims := stmt.Dimensions; len(dims) > 0 {
			for _, dim := range dims {
				fieldFromDim := &Field{
					Expr:  CloneExpr(dim.Expr),
					Alias: "",
					depth: dim.depth,
				}
				if feidRef, ok := fieldFromDim.Expr.(*VarRef); ok {
					feidRef.Type = Tag
				}
				stmt.Fields = append(stmt.Fields, fieldFromDim)
			}
		}
		stmt.Dimensions = nil
		src.Statement = stmt
		if e := RewriteSubQueryNameSpace(src, false); e != nil {
			return nil, e
		}
		if err == nil {
			src.Statement = stmt
			return src, nil
		}
		return nil, nil
	case *Union:
		_, lErr := s.RewriteUnionCase(src.LSrc, m, batchEn)
		if lErr != nil {
			return nil, lErr
		}
		_, rErr := s.RewriteUnionCase(src.RSrc, m, batchEn)
		if rErr != nil {
			return nil, rErr
		}
		return nil, nil
	default:
		return nil, ErrUnionSourceCheck
	}
	return nil, nil
}

func CheckUnionAllFields(unionType UnionType, lSource Source, rSource Source, m FieldMapper) error {
	var lStatement, rStatement *SelectStatement
	if lSubQuery, ok := lSource.(*SubQuery); ok {
		lStatement = lSubQuery.Statement
	} else {
		return ErrUnionLeftSource
	}
	if rSubQuery, ok := rSource.(*SubQuery); ok {
		rStatement = rSubQuery.Statement
	} else {
		return ErrUnionRightSource
	}
	lFields := lStatement.Fields
	rFields := rStatement.Fields
	if unionType == UnionDistinct || unionType == UnionAll {
		if len(lFields) != len(rFields) {
			return ErrUnionColumnsCount
		}
		for i, f := range rFields {
			lFieldType := EvalType(lFields[i].Expr, lStatement.Sources, m)
			rFieldType := EvalType(f.Expr, rStatement.Sources, m)
			if lFieldType != rFieldType {
				return ErrUnionTypeByIndex
			}
			f.Alias = lFields[i].Name()
		}

	} else {
		for _, rField := range rFields {
			rName := rField.Name()
			for _, lField := range lFields {
				if lField.Name() == rName {
					lFieldType := EvalType(lField.Expr, lStatement.Sources, m)
					rFieldType := EvalType(rField.Expr, rStatement.Sources, m)
					if lFieldType != rFieldType {
						return ErrUnionTypeByName
					}
					break
				}
			}
		}
	}
	return nil
}

// for the pipe-systax parser of logkeeper
func rewriteVarType(expr *BinaryExpr, allVarRef map[string]Expr, change *bool) error {
	leftVar, ok := expr.LHS.(*VarRef)
	if !ok || leftVar.Type == String {
		return nil
	}
	rightVal, ok := expr.RHS.(*StringLiteral)
	if !ok {
		return nil
	}
	ref, ok := allVarRef[leftVar.Val]
	if !ok {
		return nil
	}
	e := ref.(*VarRef)

	switch e.Type {
	case Float:
		val, err := strconv.ParseFloat(rightVal.Val, 64)
		if err != nil {
			return err
		}
		expr.RHS = &NumberLiteral{Val: val}
		*change = true
	case Integer:
		val, err := strconv.ParseInt(rightVal.Val, 10, 64)
		if err != nil {
			return err
		}
		expr.RHS = &IntegerLiteral{Val: val}
		*change = true
	case Unsigned:
		val, err := strconv.ParseUint(rightVal.Val, 10, 64)
		if err != nil {
			return err
		}
		expr.RHS = &UnsignedLiteral{Val: val}
		*change = true
	case Boolean:
		val, err := strconv.ParseBool(rightVal.Val)
		if err != nil {
			return err
		}
		expr.RHS = &BooleanLiteral{Val: val}
		*change = true
	}
	return nil
}

func rewriteIpString(expr *BinaryExpr) error {
	rightVar, ok := expr.RHS.(*StringLiteral)
	if !ok {
		return nil
	}
	ipString := rightVar.Val
	parsedIP := net.ParseIP(ipString)
	if parsedIP != nil {
		if parsedIP.To4() != nil {
			ipString = fmt.Sprintf("%s/32", ipString)
		} else if parsedIP.To16() != nil {
			ipString = fmt.Sprintf("%s/128", ipString)
		}
	}
	expr.RHS = &StringLiteral{Val: ipString}
	_, _, err := net.ParseCIDR(ipString)
	if err != nil {
		return fmt.Errorf("invalid CIDR address: %s", ipString)
	}
	return nil
}

func RewriteCondVarRef(condition Expr, allVarRef map[string]Expr) error {
	if condition == nil {
		return nil
	}
	// global
	err := rewriteConfVarRefForAll(condition, allVarRef)
	if err != nil {
		return err
	}
	// logKeeper only
	if config.IsLogKeeper() {
		return rewriteConfVarRefForLog(condition, allVarRef)
	}
	return nil
}

func rewriteConfVarRefForAll(condition Expr, allVarRef map[string]Expr) error {
	switch expr := condition.(type) {
	case *BinaryExpr:
		if expr.Op == AND || expr.Op == OR {
			err := rewriteConfVarRefForAll(expr.LHS, allVarRef)
			if err != nil {
				return err
			}
			return rewriteConfVarRefForAll(expr.RHS, allVarRef)
		}
		// rewrite default cidr mask: IPv4: /32 IPv6: /128
		if expr.Op == IPINRANGE {
			return rewriteIpString(expr)
		}
	}
	return nil
}

// for the pipe-systax parser of logkeeper
func rewriteConfVarRefForLog(condition Expr, allVarRef map[string]Expr) error {
	if !config.IsLogKeeper() || condition == nil {
		return nil
	}
	var change bool
	switch expr := condition.(type) {
	case *BinaryExpr:
		if expr.Op == AND || expr.Op == OR {
			err := rewriteConfVarRefForLog(expr.LHS, allVarRef)
			if err != nil {
				return err
			}
			return rewriteConfVarRefForLog(expr.RHS, allVarRef)
		}
		if expr.Op == LT || expr.Op == LTE || expr.Op == GT || expr.Op == GTE {
			return rewriteVarType(expr, allVarRef, &change)
		}
		if expr.Op == MATCHPHRASE {
			err := rewriteVarType(expr, allVarRef, &change)
			if change {
				expr.Op = EQ
			}
			return err
		}
	case *ParenExpr:
		return rewriteConfVarRefForLog(expr.Expr, allVarRef)
	}
	return nil
}

func (s *SelectStatement) GetUnnestVarRef(allVarRef map[string]Expr) {
	if len(s.UnnestSource) == 0 {
		return
	}
	for _, unnest := range s.UnnestSource {
		for i, alias := range unnest.Aliases {
			expr, ok := allVarRef[alias]
			if ok {
				e, ok := expr.(*VarRef)
				if !ok {
					return
				}
				e.Type = unnest.DstType[i]
			} else {
				allVarRef[alias] = &VarRef{
					Val:  alias,
					Type: unnest.DstType[i],
				}
			}
		}
	}
}

func (s *SelectStatement) GetUnnestSchema(fields map[string]DataType) error {
	if len(s.UnnestSource) == 0 {
		return nil
	}
	for _, unnest := range s.UnnestSource {
		for i, alias := range unnest.Aliases {
			if _, ok := fields[alias]; ok {
				return fmt.Errorf("the extracted alias '%s' has the same name as an existing field", alias)
			}
			fields[alias] = unnest.DstType[i]
		}
	}
	return nil
}

func (s *SelectStatement) checkField(m FieldMapper, sources Sources) error {
	for i := range sources {
		mst, ok := sources[i].(*Measurement)
		if !ok {
			continue
		}
		fks, tks, _, err := m.FieldDimensions(mst)
		if err != nil {
			return err
		}
		if err := s.GetUnnestSchema(fks); err != nil {
			return err
		}

		// Check whether fields that do not exist in the table.
		for j := range s.Fields {
			if err = checkField(s.Fields[j].Expr, fks, tks); err != nil {
				return err
			}
		}
		// Check whether condition that do not exist in the table.
		if err = checkField(s.Condition, fks, tks); err != nil {
			return err
		}
		// Check whether dimension that do not exist in the table.
		for j := range s.Dimensions {
			if err = checkField(s.Dimensions[j].Expr, fks, tks); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkField(expr Expr, fks map[string]DataType, tks map[string]struct{}) error {
	switch v := expr.(type) {
	case *VarRef:
		if v.Val == "time" || v.Val == "__log___" {
			return nil
		}
		_, ok1 := fks[v.Val]
		_, ok2 := tks[v.Val]
		if !ok1 && !ok2 {
			return errno.NewError(errno.NoFieldSelected, v.Val)
		}
	case *Call:
		for i := range v.Args {
			if err := checkField(v.Args[i], fks, tks); err != nil {
				return err
			}
		}
	case *BinaryExpr:
		if err := checkField(v.LHS, fks, tks); err != nil {
			return err
		}
		if err := checkField(v.RHS, fks, tks); err != nil {
			return err
		}
	}
	return nil
}

func (s *SelectStatement) RewriteSeqIdForLogStore(fields map[string]DataType) {
	if config.IsLogKeeper() {
		_, ok := fields[SeqIDField]
		if !ok {
			fields[SeqIDField] = influx.Field_Type_Int
		}
		if !s.hasCall() {
			_, ok = fields[ShardIDField]
			if !ok {
				fields[ShardIDField] = Integer
			}
		}
	}
}

func (s *SelectStatement) hasCall() bool {
	for _, v := range s.Fields {
		if _, ok := v.Expr.(*Call); ok {
			return true
		}
	}
	return false
}

func (s *SelectStatement) RewriteFieldsForSelectAllTags(m FieldMapper) error {
	_, tagSet, err := FieldDimensions(s.Sources, m, &s.Schema)
	if err != nil {
		return err
	}
	if len(tagSet) == 0 {
		return nil
	}
	tagWildIdx := 0
	for tagWildIdx < len(s.Fields) {
		if w, ok := s.Fields[tagWildIdx].Expr.(*Wildcard); ok && w.Type == TAG {
			break
		}
		tagWildIdx++
	}
	s.Fields = append(s.Fields[:tagWildIdx], s.Fields[tagWildIdx+1:]...)
	tagSlice := make([]string, 0, len(tagSet))
	for tagKey := range tagSet {
		tagSlice = append(tagSlice, tagKey)
	}
	sort.Strings(tagSlice)
	for _, tagKey := range tagSlice {
		s.Fields = append(s.Fields, &Field{Expr: &VarRef{Val: tagKey, Type: Tag}, depth: 2})
	}
	return nil
}

// todo: graphStatement col schema
func (s *GraphStatement) RewriteFields() (*GraphStatement, error) {
	return s, nil
}

// RewriteFields returns the re-written form of the select statement. Any wildcard query
// fields are replaced with the supplied fields, and any wildcard GROUP BY fields are replaced
// with the supplied dimensions. Any fields with no type specifier are rewritten with the
// appropriate type.
func (s *SelectStatement) RewriteFields(m FieldMapper, batchEn bool, hasJoin bool) (*SelectStatement, error) {
	// Clone the statement so we aren't rewriting the original.
	other := s.Clone()

	// Iterate through the sources and rewrite any subqueries first.
	sources := make(Sources, 0, len(other.Sources))

	if len(other.DirectDependencyCTEs) > 0 {
		for _, cte := range other.DirectDependencyCTEs {
			if cte.Query != nil {
				stmt, err := cte.Query.RewriteFields(cte.FMapper, batchEn, false)
				if err == nil {
					cte.Query = stmt
					sources = append(sources, cte)
				}
			} else if cte.GraphQuery != nil {
				graphStmt, err := cte.GraphQuery.RewriteFields()
				if err == nil {
					cte.GraphQuery = graphStmt
					sources = append(sources, cte)
				}
			}
		}
	}

	for i, src := range other.Sources {
		switch src := src.(type) {
		case *SubQuery:
			stmt, err := src.Statement.RewriteFields(m, batchEn, hasJoin)
			if err == nil {
				src.Statement = stmt
				sources = append(sources, src)
				continue
			}

			if err != ErrDeclareEmptyCollection {
				return nil, err
			}
		case *Join:
			joinKeyMap := make(map[string]string)
			if err := GetJoinKeyByCondition(other.JoinSource[i].Condition, joinKeyMap); err != nil {
				return nil, err
			}
			var auxFields []*Field
			if lSrc, ok := src.LSrc.(*Measurement); ok {
				lKeys := getJoinKeyByName(joinKeyMap, lSrc.GetName())
				subQuerySrc, err := RewriteJoinMeasurement(lSrc, other, lKeys)
				if err != nil {
					return nil, err
				}
				src.LSrc = subQuerySrc
				other.JoinSource[i].LSrc = subQuerySrc
				sources = SetCTEForCTESubQuery(lSrc, subQuerySrc, other, sources)
				auxFields = append(auxFields, getAuxiliaryField(subQuerySrc.Statement.Fields)...)
			}
			if rSrc, ok := src.RSrc.(*Measurement); ok {
				rKeys := getJoinKeyByName(joinKeyMap, rSrc.GetName())
				subQuerySrc, err := RewriteJoinMeasurement(rSrc, other, rKeys)
				if err != nil {
					return nil, err
				}
				src.RSrc = subQuerySrc
				other.JoinSource[i].RSrc = subQuerySrc
				sources = SetCTEForCTESubQuery(rSrc, subQuerySrc, other, sources)
				auxFields = append(auxFields, getAuxiliaryField(subQuerySrc.Statement.Fields)...)
			}
			lSources, lErr := s.RewriteJoinCase(src.LSrc, m, batchEn, other.Fields, true, src.JoinType)
			if lErr != nil {
				return nil, lErr
			}
			if lSources != nil {
				sources = append(sources, lSources)
			}
			rSources, rErr := s.RewriteJoinCase(src.RSrc, m, batchEn, other.Fields, true, src.JoinType)
			if rErr != nil {
				return nil, rErr
			}
			if rSources != nil {
				sources = append(sources, rSources)
			}
			if len(auxFields) > 0 {
				var err error
				other, err = rewriteAuxiliaryStmt(other, sources, auxFields, m, batchEn, hasJoin)
				if err != nil {
					return nil, err
				}
				sources = other.Sources
			}
		case *Union:
			lSource, lErr := s.RewriteUnionCase(src.LSrc, m, batchEn)
			if lErr != nil {
				return nil, lErr
			}
			if lSource != nil {
				sources = append(sources, lSource)
			}
			rSource, rErr := s.RewriteUnionCase(src.RSrc, m, batchEn)
			if rErr != nil {
				return nil, rErr
			}
			if rSource != nil {
				if checkErr := CheckUnionAllFields(src.UnionType, lSource, rSource, m); checkErr != nil {
					return nil, checkErr
				}
				sources = append(sources, rSource)
			}
		case *BinOp:
			if src.NilMst != LNilMst {
				lSources, lErr := s.RewriteJoinCase(src.LSrc, m, batchEn, other.Fields, false, UnknownJoin)
				if lErr != nil {
					return nil, lErr
				}
				sources = append(sources, lSources)
			}
			if src.NilMst != RNilMst {
				rSources, rErr := s.RewriteJoinCase(src.RSrc, m, batchEn, other.Fields, false, UnknownJoin)
				if rErr != nil {
					return nil, rErr
				}
				sources = append(sources, rSources)
			}
		case *TableFunction:
			for _, childSource := range src.GetTableFunctionSource() {
				if !containCte(sources, childSource) {
					sources = append(sources, childSource)
				}
			}
		default:
			s, ok := src.(*Measurement)
			if !ok {
				return nil, ErrNotMst
			}
			if s.MstType != TEMPORARY {
				if e := RewriteMstNameSpace(other.Fields, src, hasJoin, src.GetName()); e != nil {
					return nil, e
				}
				sources = append(sources, src)
			}
		}
	}
	if len(sources) == 0 {
		if !s.ReturnErr {
			return nil, ErrDeclareEmptyCollection
		}
		return nil, errno.NewError(errno.NoFieldSelected, "source")
	}

	if len(s.InConditons) > 0 {
		in := s.InConditons[0]
		stmt, err := in.Stmt.RewriteFields(m, batchEn, hasJoin)
		if err != nil {
			return nil, err
		}
		in.Stmt = stmt
	}

	if s.ReturnErr {
		if err := s.checkField(m, sources); err != nil {
			return nil, err
		}
	}

	tableFunction := false
	for _, src := range other.Sources {
		switch src := src.(type) {
		case *TableFunction:
			src.SetTableFunctionSource(sources)
			tableFunction = true
		}
	}

	if !tableFunction {
		other.Sources = sources
	}

	allVarRef := make(map[string]Expr, len(other.Fields))
	fieldVarRef := make(map[string]Expr, len(other.Fields))
	condVarRef := make(map[string]Expr, len(other.Fields))
	containTimeField := false

	getFieldVarRef := func(n Node) {
		ref, ok := n.(*VarRef)

		if ok {
			fieldVarRef[ref.Val] = ref
		}

		if ok && strings.ToLower(ref.Val) == "time" {
			ref.Type = Integer
			ref.Val = "time"
			containTimeField = true
			return
		}

		if !ok || (ref.Type != Unknown && ref.Type != AnyField) || strings.ToLower(ref.Val) == "time" {
			return
		}

		if _, ok := allVarRef[ref.Val]; ok {
			return
		}
		allVarRef[ref.Val] = ref
	}

	getCondVarRef := func(n Node) {
		ref, ok := n.(*VarRef)

		if ok {
			condVarRef[ref.Val] = ref
		}

		if ok && strings.ToLower(ref.Val) == "time" {
			ref.Type = Integer
			ref.Val = "time"
			return
		}

		if !ok || (ref.Type != Unknown && ref.Type != AnyField) || strings.ToLower(ref.Val) == "time" {
			return
		}

		if _, ok := allVarRef[ref.Val]; ok {
			return
		}
		allVarRef[ref.Val] = ref
	}

	// Rewrite all variable references in the fields with their typ if one
	// hasn't been specified.
	rewrite := func(n Node) {
		ref, ok := n.(*VarRef)
		if !ok || (ref.Type != Unknown && ref.Type != AnyField) {
			return
		}

		var typ DataType
		expr, _ := allVarRef[ref.Val]
		if expr != nil {
			e := expr.(*VarRef)
			typ = e.Type
		}

		ref.Type = typ
	}

	// Rewrite all variable references in the fields with their typ if one
	// hasn't been specified.
	mapType := func(n Node) {
		ref, ok := n.(*VarRef)
		if !ok || (ref.Type != Unknown && ref.Type != AnyField) {
			return
		}

		typ := EvalType(ref, other.Sources, m)
		if typ == Tag && ref.Type == AnyField {
			return
		}
		ref.Type = typ
	}

	WalkFunc(other.Fields, getFieldVarRef)
	WalkFunc(other.Condition, getCondVarRef)

	if len(allVarRef) > 0 {
		s.GetUnnestVarRef(allVarRef)
		err := EvalTypeBatch(allVarRef, other.Sources, m, &other.Schema, batchEn)
		if err != nil {
			if err == ErrUnsupportBatchMap {
				WalkFunc(other.Fields, mapType)
				WalkFunc(other.Condition, mapType)
			} else {
				return nil, err
			}
		} else {
			WalkFunc(other.Fields, rewrite)
			WalkFunc(other.Condition, rewrite)
			rewriteErr := RewriteCondVarRef(other.Condition, allVarRef)
			if rewriteErr != nil {
				return nil, rewriteErr
			}
		}
	}

	isEmptyCollByField := func(varRefmap map[string]Expr) bool {
		for _, expr := range varRefmap {
			if varRef, ok := expr.(*VarRef); ok {
				if varRef.Type != Unknown && varRef.Type != AnyField {
					return false
				}
			}
		}
		return true
	}

	isEmptyCollByCond := func(varRefmap map[string]Expr) bool {
		if len(varRefmap) == 0 {
			return false
		}

		for _, expr := range varRefmap {
			if varRef, ok := expr.(*VarRef); ok {
				if varRef.Type != Unknown && varRef.Type != AnyField {
					return false
				}
			}
		}
		return true
	}

	rewriteDefaultTypeOfVarRef := func(n Node) {
		ref, ok := n.(*VarRef)
		if ok && ref.Type == Unknown {
			ref.Type = Integer
		}
	}

	// Ignore if there are no wildcards.
	hasFieldWildcard := other.HasFieldWildcard()
	hasDimensionWildcard := other.HasDimensionWildcard()

	if !hasFieldWildcard && !containTimeField && isEmptyCollByField(fieldVarRef) {
		return nil, ErrDeclareEmptyCollection
	}
	if isEmptyCollByCond(condVarRef) {
		if !(s.IsPromQuery || s.IsPromRemoteRead) {
			return nil, ErrDeclareEmptyCollection
		}
	}

	WalkFunc(other.Fields, rewriteDefaultTypeOfVarRef)

	if len(other.Dimensions) == 1 {
		if _, ok := other.Dimensions[0].Expr.(*Wildcard); ok {
			other.GroupByAllDims = !other.SelectTagToAux
			// 1.group by * + promquery
			if (other.IsPromQuery && !other.SelectTagToAux) || other.IsPromRemoteRead {
				other.Dimensions = nil
				return other, nil
			}
		}
	}

	if other.Without {
		// 2.without dims + promquery
		if other.SelectAllTags {
			if err := other.RewriteFieldsForSelectAllTags(m); err != nil {
				return nil, err
			}
		}
		return other, nil
	}

	if !hasFieldWildcard && !hasDimensionWildcard {
		return other, nil
	}

	fieldSet, dimensionSet, err := FieldDimensions(other.Sources, m, &other.Schema)
	if err != nil {
		return nil, err
	}
	if err := s.GetUnnestSchema(fieldSet); err != nil {
		return nil, err
	}
	s.RewriteSeqIdForLogStore(fieldSet)

	// If there are no dimension wildcards then merge dimensions to fields.
	if !hasDimensionWildcard {
		// Remove the dimensions present in the group by so they don't get added as fields.
		for _, d := range other.Dimensions {
			switch expr := d.Expr.(type) {
			case *VarRef:
				delete(dimensionSet, expr.Val)
			}
		}
	}

	// Sort the field and dimension names for wildcard expansion.
	var fields []VarRef
	if len(fieldSet) > 0 {
		fields = make([]VarRef, 0, len(fieldSet))
		for name, typ := range fieldSet {
			fields = append(fields, VarRef{Val: name, Type: typ})
		}
		if !hasDimensionWildcard {
			for name := range dimensionSet {
				fields = append(fields, VarRef{Val: name, Type: Tag})
			}
			dimensionSet = nil
		}
		sort.Sort(VarRefs(fields))
	}
	dimensions := stringSetSlice(dimensionSet)

	// Rewrite all wildcard query fields
	if hasFieldWildcard && !tableFunction {
		// Allocate a slice assuming there is exactly one wildcard for efficiency.
		rwFields := make(Fields, 0, len(other.Fields)+len(fields)-1)
		for _, f := range other.Fields {
			switch expr := f.Expr.(type) {
			case *Wildcard:
				for _, ref := range fields {
					if expr.Type == FIELD && ref.Type == Tag {
						continue
					} else if expr.Type == TAG && ref.Type != Tag {
						continue
					}
					rwFields = append(rwFields, &Field{Expr: &VarRef{Val: ref.Val, Type: ref.Type}, depth: 2})
				}
			case *RegexLiteral:
				for _, ref := range fields {
					if expr.Val.MatchString(ref.Val) {
						rwFields = append(rwFields, &Field{Expr: &VarRef{Val: ref.Val, Type: ref.Type}, depth: 2})
					}
				}
			case *Call:
				// Clone a template that we can modify and use for new fields.
				template := CloneExpr(expr).(*Call)

				// Search for the call with a wildcard by continuously descending until
				// we no longer have a call.
				call := template
				for len(call.Args) > 0 {
					arg, ok := call.Args[0].(*Call)
					if !ok {
						break
					}
					call = arg
				}

				// Check if this field value is a wildcard.
				if len(call.Args) == 0 {
					rwFields = append(rwFields, f)
					continue
				}

				// Retrieve if this is a wildcard or a regular expression.
				var re *regexp.Regexp
				switch expr := call.Args[0].(type) {
				case *Wildcard:
					if expr.Type == TAG {
						return nil, fmt.Errorf("unable to use tag wildcard in %s()", call.Name)
					}
				case *RegexLiteral:
					re = expr.Val
				default:
					rwFields = append(rwFields, f)
					continue
				}

				// All typ that can expand wildcards support float, integer, and unsigned.
				supportedTypes := map[DataType]struct{}{
					Float:    {},
					Integer:  {},
					Unsigned: {},
				}

				// Add additional typ for certain functions.
				switch call.Name {
				case "count", "first", "last", "distinct", "elapsed", "mode", "sample", "absent":
					supportedTypes[String] = struct{}{}
					fallthrough
				case "min", "max":
					supportedTypes[Boolean] = struct{}{}
				case "holt_winters", "holt_winters_with_fit":
					delete(supportedTypes, Unsigned)
				case "str", "strlen", "substr":
					supportedTypes[String] = struct{}{}
					delete(supportedTypes, Integer)
					delete(supportedTypes, Float)
					delete(supportedTypes, Unsigned)
				case "sliding_window":
					delete(supportedTypes, Unsigned)
				}

				for _, ref := range fields {
					// Do not expand tags within a function call. It likely won't do anything
					// anyway and will be the wrong thing in 99% of cases.
					if ref.Type == Tag {
						continue
					} else if _, ok := supportedTypes[ref.Type]; !ok {
						continue
					} else if re != nil && !re.MatchString(ref.Val) {
						continue
					}

					// Make a new expression and replace the wildcard within this cloned expression.
					call.Args[0] = &VarRef{Val: ref.Val, Type: ref.Type}
					rwFields = append(rwFields, &Field{
						Expr:  CloneExpr(template),
						Alias: fmt.Sprintf("%s_%s", f.Name(), ref.Val),
						depth: 1 + template.depth,
					})
				}
			case *BinaryExpr:
				// Search for regexes or wildcards within the binary
				// expression. If we find any, throw an error indicating that
				// it's illegal.
				var regex, wildcard bool
				WalkFunc(expr, func(n Node) {
					switch n.(type) {
					case *RegexLiteral:
						regex = true
					case *Wildcard:
						wildcard = true
					}
				})

				if wildcard {
					return nil, fmt.Errorf("unsupported expression with wildcard: %s", f.Expr)
				} else if regex {
					return nil, fmt.Errorf("unsupported expression with regex field: %s", f.Expr)
				}
				rwFields = append(rwFields, f)
			default:
				rwFields = append(rwFields, f)
			}
		}
		other.Fields = rwFields
		other.HasWildcardField = hasFieldWildcard
	}

	// Rewrite all wildcard GROUP BY fields
	if hasDimensionWildcard {
		// Allocate a slice assuming there is exactly one wildcard for efficiency.
		rwDimensions := make(Dimensions, 0, len(other.Dimensions)+len(dimensions)-1)
		for _, d := range other.Dimensions {
			switch expr := d.Expr.(type) {
			case *Wildcard:
				if other.IsPromQuery && !other.SelectTagToAux {
					other.GroupByAllDims = true
					continue
				}
				for _, name := range dimensions {
					rwDimensions = append(rwDimensions, &Dimension{Expr: &VarRef{Val: name}, depth: 2})
				}
			case *RegexLiteral:
				for _, name := range dimensions {
					if expr.Val.MatchString(name) {
						rwDimensions = append(rwDimensions, &Dimension{Expr: &VarRef{Val: name}, depth: 2})
					}
				}
			default:
				rwDimensions = append(rwDimensions, d)
			}
		}
		other.Dimensions = rwDimensions
	}

	return other, nil
}

func containCte(sources []Source, source Source) bool {
	for _, cte := range sources {
		if source.GetName() == cte.GetName() {
			return true
		}
	}
	return false
}

func SetCTEForCTESubQuery(mst *Measurement, subQuerySrc *SubQuery, other *SelectStatement, sources Sources) Sources {
	if mst.MstType == TEMPORARY {
		subQuerySrc.Statement.DirectDependencyCTEs = make(CTES, 0, 1)
		for _, e := range other.DirectDependencyCTEs {
			if e.Alias == mst.Name {
				subQuerySrc.Statement.DirectDependencyCTEs = append(subQuerySrc.Statement.DirectDependencyCTEs, e.Clone())
				break
			}
		}
		newSources := make(Sources, 0, len(sources))
		for _, e := range sources {
			if e.GetName() != mst.Name {
				newSources = append(newSources, e)
			}
		}
		sources = newSources
	} else {
		subQuerySrc.Statement.DirectDependencyCTEs = nil
	}
	return sources
}

func (s *SelectStatement) RewriteRegexConditionsDFS(otherWrite func(e Expr)) {
	s.RewriteRegexConditions(otherWrite)
	for _, s := range s.Sources {
		if subquery, ok := s.(*SubQuery); ok {
			subquery.Statement.RewriteRegexConditions(otherWrite)
		}
	}
}

func (s *SelectStatement) GetInConditions() {
	s.Condition = RewriteExpr(s.Condition, func(e Expr) Expr {
		if in, ok := e.(*InCondition); ok {
			s.InConditons = append(s.InConditons, in)
		}
		return e
	})
}

// RewriteRegexConditions rewrites regex conditions to make better use of the
// database index.
//
// Conditions that can currently be simplified are:
//
//   - host =~ /^foo$/ becomes host = 'foo'
//   - host !~ /^foo$/ becomes host != 'foo'
//
// Note: if the regex contains groups, character classes, repetition or
// similar, it's likely it won't be rewritten. In order to support rewriting
// regexes with these characters would be a lot more work.
func (s *SelectStatement) RewriteRegexConditions(otherRewrite func(e Expr)) {
	s.Condition = RewriteExpr(s.Condition, func(e Expr) Expr {
		if otherRewrite != nil {
			otherRewrite(e)
		}

		be, ok := e.(*BinaryExpr)
		if !ok || (be.Op != EQREGEX && be.Op != NEQREGEX) {
			// This expression is not a binary condition or doesn't have a
			// regex based operator.
			return e
		}

		// Handle regex-based condition.
		rhs := be.RHS.(*RegexLiteral) // This must be a regex.

		vals, ok := matchExactRegex(rhs.Val.String())
		if !ok {
			// Regex didn't match.
			return e
		}

		// Update the condition operator.
		var concatOp Token
		if be.Op == EQREGEX {
			be.Op = EQ
			concatOp = OR
		} else {
			be.Op = NEQ
			concatOp = AND
		}

		// Remove leading and trailing ^ and $.
		switch {
		case len(vals) == 0:
			be.RHS = &StringLiteral{}
		case len(vals) == 1:
			be.RHS = &StringLiteral{Val: vals[0]}
		default:
			expr := &BinaryExpr{
				Op:    be.Op,
				LHS:   be.LHS,
				RHS:   &StringLiteral{Val: vals[0]},
				depth: 1 + be.LHS.Depth(),
			}
			for i := 1; i < len(vals); i++ {
				expr = &BinaryExpr{
					Op:  concatOp,
					LHS: expr,
					RHS: &BinaryExpr{
						Op:    be.Op,
						LHS:   be.LHS,
						RHS:   &StringLiteral{Val: vals[i]},
						depth: 1 + be.LHS.Depth(),
					},
					depth: 1 + expr.depth, // the RHS has the same depth as the first constructed expr, but then expr grows
				}
			}
			return &ParenExpr{Expr: expr, depth: 1 + expr.depth}
		}
		return be
	})

	// Unwrap any top level parenthesis.
	if cond, ok := s.Condition.(*ParenExpr); ok {
		s.Condition = cond.Expr
	}
}

// matchExactRegex matches regexes into literals if possible. This will match the
// pattern /^foo$/ or /^(foo|bar)$/. It considers /^$/ to be a matching regex.
func matchExactRegex(v string) ([]string, bool) {
	re, err := syntax.Parse(v, syntax.Perl)
	if err != nil {
		// Nothing we can do or log.
		return nil, false
	}
	re = re.Simplify()

	if re.Op != syntax.OpConcat {
		return nil, false
	}

	if len(re.Sub) < 2 {
		// Regex has too few subexpressions.
		return nil, false
	}

	start := re.Sub[0]
	if !(start.Op == syntax.OpBeginLine || start.Op == syntax.OpBeginText) {
		// Regex does not begin with ^
		return nil, false
	}

	end := re.Sub[len(re.Sub)-1]
	if !(end.Op == syntax.OpEndLine || end.Op == syntax.OpEndText) {
		// Regex does not end with $
		return nil, false
	}

	// Remove the begin and end text from the regex.
	re.Sub = re.Sub[1 : len(re.Sub)-1]

	if len(re.Sub) == 0 {
		// The regex /^$/
		return nil, true
	}
	return matchRegex(re)
}

// matchRegex will match a regular expression to literals if possible.
func matchRegex(re *syntax.Regexp) ([]string, bool) {
	// Exit if we see a case-insensitive flag as it is not something we support at this time.
	if re.Flags&syntax.FoldCase != 0 {
		return nil, false
	}

	switch re.Op {
	case syntax.OpLiteral:
		// We can rewrite this regex.
		return []string{string(re.Rune)}, true
	case syntax.OpCapture:
		return matchRegex(re.Sub[0])
	case syntax.OpConcat:
		// Go through each of the subs and concatenate the result to each one.
		names, ok := matchRegex(re.Sub[0])
		if !ok {
			return nil, false
		}

		for _, sub := range re.Sub[1:] {
			vals, ok := matchRegex(sub)
			if !ok {
				return nil, false
			}

			// If there is only one value, concatenate it to all strings rather
			// than allocate a new slice.
			if len(vals) == 1 {
				for i := range names {
					names[i] += vals[0]
				}
				continue
			} else if len(names) == 1 {
				// If there is only one value, then do this concatenation in
				// the opposite direction.
				for i := range vals {
					vals[i] = names[0] + vals[i]
				}
				names = vals
				continue
			}

			// The long method of using multiple concatenations.
			concat := make([]string, len(names)*len(vals))
			for i := range names {
				for j := range vals {
					concat[i*len(vals)+j] = names[i] + vals[j]
				}
			}
			names = concat
		}
		return names, true
	case syntax.OpCharClass:
		var sz int
		for i := 0; i < len(re.Rune); i += 2 {
			sz += int(re.Rune[i+1]) - int(re.Rune[i]) + 1
		}

		names := make([]string, 0, sz)
		for i := 0; i < len(re.Rune); i += 2 {
			for r := int(re.Rune[i]); r <= int(re.Rune[i+1]); r++ {
				names = append(names, string([]rune{rune(r)}))
			}
		}
		return names, true
	case syntax.OpAlternate:
		var names []string
		for _, sub := range re.Sub {
			vals, ok := matchRegex(sub)
			if !ok {
				return nil, false
			}
			names = append(names, vals...)
		}
		return names, true
	}
	return nil, false
}

// RewriteTopBottomStatement converts a query with a tag in the top or bottom to a subquery
func RewriteTopBottomStatement(node Node) Node {
	s, ok := node.(*SelectStatement)
	if !ok {
		return s
	}
	for i := 0; i < len(s.Fields); i++ {
		if call, ok := s.Fields[i].Expr.(*Call); ok && (call.Name == "top" || call.Name == "bottom") && len(call.Args) > 2 {
			// rewrite the max/min fields for subquery
			var auxFields []*Field
			nst := &SelectStatement{Condition: s.Condition, OmitTime: true, depth: s.depth}
			nst.Sources = append(nst.Sources, s.Sources...)
			for _, tagArg := range call.Args[1 : len(call.Args)-1] {
				nst.Dimensions = append(nst.Dimensions, &Dimension{Expr: tagArg, depth: 1 + tagArg.Depth()})
				auxFields = append(auxFields, &Field{Expr: tagArg, depth: 1 + tagArg.Depth()})
			}
			callField := &Field{
				Expr: &Call{
					Args:  []Expr{call.Args[0]},
					depth: 2 + call.Args[0].Depth(),
				},
				Alias: call.Args[0].(*VarRef).Val,
				depth: 3 + call.Args[0].Depth(),
			}
			if call.Name == "top" {
				callField.Expr.(*Call).Name = "max"
			} else {
				callField.Expr.(*Call).Name = "min"
			}
			nst.Fields = append(nst.Fields, callField)
			nst.Fields = append(nst.Fields, auxFields...)
			nst.Fields = append(nst.Fields, s.Fields[:i]...)
			nst.Fields = append(nst.Fields, s.Fields[i+1:]...)

			s.Sources = s.Sources[:0]
			s.Sources = append(s.Sources, &SubQuery{Statement: nst, depth: 1 + nst.depth})

			// rewrite the top/bottom fields
			var newArgs []Expr
			newArgs = append(newArgs, call.Args[0], call.Args[len(call.Args)-1])
			call.Args = call.Args[:0]
			call.Args = append(call.Args, newArgs...)

			var newFields []*Field
			newFields = append(newFields, s.Fields[:i+1]...)
			newFields = append(newFields, auxFields...)
			if i+1 <= len(s.Fields)-1 {
				newFields = append(newFields, s.Fields[i+1:]...)
			}
			s.Fields = s.Fields[:0]
			s.Fields = append(s.Fields, newFields...)
			return s
		}
	}
	return s
}

type RewriterOpsNest interface {
	RewriteOpsNest(Node) Node
}

// RewriteOpsNest recursively invokes the rewriter to replace each node.
// Nodes are traversed depth-first and rewritten from leaf to root.
func RewriteOpsNest(r RewriterOpsNest, node Node) Node {
	switch n := node.(type) {
	case *SelectStatement:
		for i := range n.Sources {
			if st, ok := n.Sources[i].(*SubQuery); ok {
				st.Statement = RewriteOpsNest(r, st.Statement).(*SelectStatement)
			}
		}
	case *SubQuery:
		n.Statement = RewriteOpsNest(r, n.Statement).(*SelectStatement)
	}
	return r.RewriteOpsNest(node)
}

// RewriteOpsNestFunc rewrites a node hierarchy.
func RewriteOpsNestFunc(node Node, fn func(Node) Node) Node {
	return RewriteOpsNest(rewriterOpsNestFunc(fn), node)
}

type rewriterOpsNestFunc func(Node) Node

func (fn rewriterOpsNestFunc) RewriteOpsNest(n Node) Node { return fn(n) }

// RewriteTopBottom converts a query with a tag in the top or bottom to a subquery
func (s *SelectStatement) RewriteTopBottom() {
	RewriteOpsNestFunc(s, RewriteTopBottomStatement)
}

// RewritePercentileOGSketchStatement converts a query with percentile_ogsketch that has a subquery to the percentile_approx
func RewritePercentileOGSketchStatement(node Node) Node {
	s, ok := node.(*SelectStatement)
	if !ok {
		return s
	}
	var haveSubQuery bool
	for i := range s.Sources {
		if s.Sources[i] == nil {
			continue
		}
		if _, ok := s.Sources[i].(*SubQuery); ok {
			haveSubQuery = true
			break
		}
	}
	if !haveSubQuery {
		return s
	}
	for i := 0; i < len(s.Fields); i++ {
		if call, ok := s.Fields[i].Expr.(*Call); ok && call.Name == "percentile_ogsketch" {
			call.Name = "percentile_approx"
		}
	}
	return s
}

func (s *SelectStatement) RewriteCompare(timeRange *TimeRange) error {
	for i := 0; i < len(s.Fields); i++ {
		if call, ok := s.Fields[i].Expr.(*Call); ok && (call.Name == "compare") && len(call.Args) >= 2 {
			groupTime := GetGroupTime(s.Dimensions)
			stmt := s.Clone()
			stmt.IsCompareCall = true
			stmt.IsRawQuery = true
			stmt.Sources = make(Sources, 0, len(call.Args))
			stmt.Fields = make(Fields, 0, 2*len(call.Args))
			stmt.Fill = NoFill
			err := s.BuildCompareStatement(timeRange, call, stmt, groupTime)
			if err != nil {
				return err
			}
			*s = *stmt
		}
	}
	return nil
}

func GetGroupTime(dimensions Dimensions) time.Duration {
	var groupTime time.Duration
	for _, dim := range dimensions {
		if timeDim, ok := dim.Expr.(*Call); ok && timeDim.Name == "time" {
			if dur, ok := timeDim.Args[0].(*DurationLiteral); ok {
				groupTime = dur.Val
			}
		}
	}
	return groupTime
}

func (s *SelectStatement) BuildCompareStatement(timeRange *TimeRange, call *Call, stmt *SelectStatement, groupTime time.Duration) error {
	if len(s.Sources) != 1 {
		return fmt.Errorf("%s func expect 1 subquery", call.Name)
	}
	var subQuery *SubQuery
	switch source := s.Sources[0].(type) {
	case *SubQuery:
		subQuery = source
	case *Measurement:
		subQuery = &SubQuery{
			Statement: &SelectStatement{
				Fields:  []*Field{{Expr: call.Args[0], Alias: call.Args[0].String(), depth: 1 + call.Args[0].Depth()}},
				Sources: s.Sources,
				depth:   2 + call.Args[0].Depth(),
			},
			depth: 3 + call.Args[0].Depth(),
		}

	default:
		return fmt.Errorf("%s func expect 1 subquery", call.Name)

	}

	compareFieldIndex := -1
	for i, f := range subQuery.Statement.Fields {
		if f.Name() == call.Args[0].String() {
			compareFieldIndex = i
			break
		}
	}
	if compareFieldIndex == -1 {
		return fmt.Errorf("%s func values not found", call.Name)
	}

	var maxOffset int64
	for i := 0; i < len(call.Args); i++ {
		arg, ok := call.Args[i].(*IntegerLiteral)
		if !ok {
			arg = &IntegerLiteral{
				Val: 0,
			}
		}
		if arg.Val > maxOffset {
			maxOffset = arg.Val
		}
		sq := subQuery.Statement.Clone()
		sq.IsRawQuery = true
		sq.OmitTime = true
		sq.Fields[compareFieldIndex].Alias += strconv.Itoa(i + 1)
		sq.Dimensions = cloneDimensions(s)
		sq.Fill = NoFill
		compareOffset := arg.Val * 1e9
		buildCompareTimeDimension(sq, timeRange, compareOffset, groupTime)
		cond := buildCompareCondition(timeRange, compareOffset)
		valuer := NowValuer{Now: time.Now(), Location: sq.Location}
		// ignore time condition in subquery
		originCond, _, err := ConditionExpr(sq.Condition, &valuer)
		if err != nil {
			return err
		}
		expr, ok := originCond.(*BinaryExpr)
		if ok {
			cond = &BinaryExpr{
				Op:    AND,
				LHS:   expr,
				RHS:   cond,
				depth: 1 + max(expr.Depth(), cond.Depth()),
			}
		}
		sq.Condition = cond
		sq.CompareOffset = time.Duration(compareOffset)
		sq.depth = max(1+cond.depth, sq.depth)
		stmt.Sources = append(stmt.Sources, &SubQuery{Statement: sq, depth: 1 + sq.depth})
		stmt.depth = max(stmt.depth, 1+sq.depth)
	}

	// build fields
	n := call.Args[0].String()
	buildCompareFields(s.Fields, n, stmt)

	buildCompareTimeDimension(stmt, timeRange, 0, groupTime)
	// reset timeRange
	timeRange.Min = time.Unix(0, timeRange.MinTimeNano()-maxOffset*1e9)
	return nil
}

func cloneDimensions(s *SelectStatement) Dimensions {
	cloneDimension := make(Dimensions, 0, len(s.Dimensions))
	for _, d := range s.Dimensions {
		cloneDimension = append(cloneDimension, &Dimension{Expr: CloneExpr(d.Expr), depth: 1 + d.Expr.Depth()})
	}
	return cloneDimension
}

func buildCompareTimeDimension(sq *SelectStatement, timeRange *TimeRange, compareOffset int64, groupTime time.Duration) {
	var groupOffset int64
	if groupTime > 0 {
		groupOffset = (timeRange.MinTimeNano() - compareOffset) % int64(groupTime)
	}
	if groupOffset == 0 {
		return
	}
	for _, dim := range sq.Dimensions {
		if timeDim, ok := dim.Expr.(*Call); ok && timeDim.Name == "time" {
			if _, ok := timeDim.Args[0].(*DurationLiteral); ok {
				timeDim.Args = append(timeDim.Args, &DurationLiteral{Val: time.Duration(groupOffset)})
			}
		}
	}
}

func buildCompareCondition(timeRange *TimeRange, compareOffset int64) *BinaryExpr {
	return &BinaryExpr{
		Op: AND,
		LHS: &BinaryExpr{
			Op: GTE,
			LHS: &VarRef{
				Val: "time",
			},
			RHS: &IntegerLiteral{
				Val: timeRange.MinTimeNano() - compareOffset,
			},
			depth: 2,
		},
		RHS: &BinaryExpr{
			Op: LTE,
			LHS: &VarRef{
				Val: "time",
			},
			RHS: &IntegerLiteral{
				Val: timeRange.MaxTimeNano() - compareOffset,
			},
			depth: 2,
		},
		depth: 3,
	}
}

func buildCompareFields(fields []*Field, n string, stmt *SelectStatement) {
	for _, f := range fields {
		compareFunc, ok := f.Expr.(*Call)
		if !(ok && compareFunc.Name == "compare" && len(compareFunc.Args) >= 2) {
			stmt.Fields = append(stmt.Fields, &Field{Expr: CloneExpr(f.Expr), Alias: f.Alias, depth: 1 + f.Expr.Depth()})
			continue
		}
		argLen := len(compareFunc.Args)
		compareFields := make([]*Field, 0, 2*argLen)
		for i := 0; i < argLen; i++ {
			compareFields = append(compareFields, &Field{Expr: &VarRef{Val: n + strconv.Itoa(i+1)}, depth: 2})
		}
		stmt.Fields = append(stmt.Fields, compareFields...)
		for i := 1; i < argLen; i++ {
			stmt.Fields = append(stmt.Fields, &Field{
				Expr: &BinaryExpr{
					Op: DIV,
					LHS: &VarRef{
						Val: compareFields[0].Name(),
					},
					RHS: &VarRef{
						Val: compareFields[i].Name(),
					},
					depth: 2,
				},
				Alias: fmt.Sprintf("%s/%s", compareFields[0].Name(), compareFields[i].Name()),
				depth: 3,
			})
		}
	}
}

type JoinKeyPair struct {
	key     string
	sortKey string
}

func (s *SelectStatement) RewriteJoinDims(joinKeys []*JoinKeyPair) {
	if len(joinKeys) > 0 {
		rewriteDims(s, joinKeys)
	}
	for _, source := range s.Sources {
		switch source := source.(type) {
		case *SubQuery:
			source.Statement.RewriteJoinDims(joinKeys)
		case *Join:
			lsrc, ok := source.LSrc.(*SubQuery)
			if !ok {
				return
			}
			rsrc, ok := source.RSrc.(*SubQuery)
			if !ok {
				return
			}
			lJoinKeys := make([]*JoinKeyPair, 0)
			rJoinKeys := make([]*JoinKeyPair, 0)
			appendJoinKeys(source.Condition, lsrc.Alias, rsrc.Alias, &lJoinKeys, &rJoinKeys)
			sort.SliceStable(lJoinKeys, func(i, j int) bool {
				return lJoinKeys[i].sortKey < rJoinKeys[j].sortKey
			})
			sort.SliceStable(rJoinKeys, func(i, j int) bool {
				return rJoinKeys[i].sortKey < rJoinKeys[j].sortKey
			})
			commonJoinKeys := make([]*JoinKeyPair, 0, len(lJoinKeys)+len(rJoinKeys))
			keyMap := make(map[string]struct{})
			for _, k := range lJoinKeys {
				commonJoinKeys = append(commonJoinKeys, k)
				keyMap[k.key] = struct{}{}
			}
			for _, k := range rJoinKeys {
				if _, ok := keyMap[k.key]; ok {
					continue
				}
				commonJoinKeys = append(commonJoinKeys, k)
				keyMap[k.key] = struct{}{}
			}

			rewriteDims(s, commonJoinKeys)
			lsrc.Statement.RewriteJoinDims(lJoinKeys)
			rsrc.Statement.RewriteJoinDims(rJoinKeys)
		}
	}
}

func appendJoinKeys(cond Expr, lAlias, rAlias string, lJoinKeys, rJoinKeys *[]*JoinKeyPair) {
	switch expr := cond.(type) {
	case *ParenExpr:
		appendJoinKeys(expr.Expr, lAlias, rAlias, lJoinKeys, rJoinKeys)
	case *BinaryExpr:
		if expr.Op == EQ {
			appendKeys(expr, lAlias, rAlias, lJoinKeys, rJoinKeys)
			return
		}
		appendJoinKeys(expr.LHS, lAlias, rAlias, lJoinKeys, rJoinKeys)
		appendJoinKeys(expr.RHS, lAlias, rAlias, lJoinKeys, rJoinKeys)
	}
}

func appendKeys(expr *BinaryExpr, lAlias, rAlias string, lJoinKeys, rJoinKeys *[]*JoinKeyPair) {
	lv, ok := expr.LHS.(*VarRef)
	if !ok {
		return
	}
	rv, ok := expr.RHS.(*VarRef)
	if !ok {
		return
	}

	if strings.HasPrefix(lv.Val, lAlias+".") && strings.HasPrefix(rv.Val, rAlias+".") {
		n1 := lv.Val[len(lAlias)+1:]
		*lJoinKeys = append(*lJoinKeys, &JoinKeyPair{key: n1, sortKey: n1})

		n2 := rv.Val[len(rAlias)+1:]
		*rJoinKeys = append(*rJoinKeys, &JoinKeyPair{key: n2, sortKey: n1})
		return
	}

	if strings.HasPrefix(rv.Val, lAlias+".") && strings.HasPrefix(lv.Val, rAlias+".") {
		n1 := rv.Val[len(lAlias)+1:]
		*lJoinKeys = append(*lJoinKeys, &JoinKeyPair{key: n1, sortKey: n1})

		n2 := lv.Val[len(rAlias)+1:]
		*rJoinKeys = append(*rJoinKeys, &JoinKeyPair{key: n2, sortKey: n1})
		return
	}
}

func rewriteDims(stmt *SelectStatement, joinKeys []*JoinKeyPair) {
	newDims := make(Dimensions, 0, len(stmt.Dimensions))
	dimensions := stmt.Dimensions

	for _, n := range joinKeys {
		for i, dim := range dimensions {
			if v, ok := dim.Expr.(*VarRef); ok {
				if n.key != v.Val {
					continue
				}
				newDims = append(newDims, dimensions[i])
				dimensions = append(dimensions[:i], dimensions[i+1:]...)
				break
			}
		}
	}
	stmt.Dimensions = append(newDims, dimensions...)
}

// RewritePercentileOGSketch converts a query with percentile_ogsketch that has a subquery to the percentile_approx
func (s *SelectStatement) RewritePercentileOGSketch() {
	RewriteOpsNestFunc(s, RewritePercentileOGSketchStatement)
}

// RewriteDistinct rewrites the expression to be a call for map/reduce to work correctly.
// This method assumes all validation has passed.
func (s *SelectStatement) RewriteDistinct() {
	WalkFunc(s.Fields, func(n Node) {
		switch n := n.(type) {
		case *Field:
			if expr, ok := n.Expr.(*Distinct); ok {
				n.Expr = expr.NewCall()
				s.IsRawQuery = false
			}
		case *Call:
			for i, arg := range n.Args {
				if arg, ok := arg.(*Distinct); ok {
					n.Args[i] = arg.NewCall()
				}
			}
		}
	})
}

// RewriteTimeFields removes any "time" field references.
func (s *SelectStatement) RewriteTimeFields() {
	for i := 0; i < len(s.Fields); i++ {
		switch expr := s.Fields[i].Expr.(type) {
		case *VarRef:
			if expr.Val == "time" {
				s.TimeAlias = s.Fields[i].Alias
				s.Fields = append(s.Fields[:i], s.Fields[i+1:]...)
			}
		}
	}
}

// ColumnNames will walk all fields and functions and return the appropriate field names for the select statement
// while maintaining order of the field names.
func (s *SelectStatement) ColumnNames() []string {
	// First walk each field to determine the number of columns.
	columnFields := Fields{}
	for _, field := range s.Fields {
		columnFields = append(columnFields, field)

		switch f := field.Expr.(type) {
		case *Call:
			if s.Target == nil && (f.Name == "top" || f.Name == "bottom") {
				for _, arg := range f.Args[1:] {
					ref, ok := arg.(*VarRef)
					if ok {
						columnFields = append(columnFields, &Field{Expr: ref, depth: 2})
					}
				}
			}
		}
	}

	// Determine if we should add an extra column for an implicit time.
	offset := 0
	if !s.OmitTime {
		offset++
	}

	columnNames := make([]string, len(columnFields)+offset)
	if !s.OmitTime {
		// Add the implicit time if requested.
		columnNames[0] = s.TimeFieldName()
	}

	// Keep track of the encountered column names.
	names := make(map[string]int)

	// Resolve aliases first.
	for i, col := range columnFields {
		if col.Alias != "" {
			columnNames[i+offset] = col.Alias
			names[col.Alias] = 1
		}
	}

	// Resolve any generated names and resolve conflicts.
	for i, col := range columnFields {
		if columnNames[i+offset] != "" {
			continue
		}

		name := col.Name()
		count, conflict := names[name]
		if conflict {
			for {
				resolvedName := fmt.Sprintf("%s_%d", name, count)
				_, conflict = names[resolvedName]
				if !conflict {
					names[name] = count + 1
					name = resolvedName
					break
				}
				count++
			}
		}
		names[name]++
		columnNames[i+offset] = name
	}
	return columnNames
}

// FieldExprByName returns the expression that matches the field name and the
// index where this was found. If the name matches one of the arguments to
// "top" or "bottom", the variable reference inside of the function is returned
// and the index is of the function call rather than the variable reference.
// If no expression is found, -1 is returned for the index and the expression
// will be nil.
func (s *SelectStatement) FieldExprByName(name string, mstAlias string) (int, Expr) {
	for i, f := range s.Fields {
		if f.Name() == name || IsSameField(mstAlias, f.Name(), name) {
			return i, f.Expr
		} else if call, ok := f.Expr.(*Call); ok && (call.Name == "top" || call.Name == "bottom") && len(call.Args) > 2 {
			for _, arg := range call.Args[1 : len(call.Args)-1] {
				if arg, ok := arg.(*VarRef); ok && (arg.Val == name || IsSameField(mstAlias, arg.Val, name)) {
					return i, arg
				}
			}
		}
	}
	return -1, nil
}

// Reduce calls the Reduce function on the different components of the
// SelectStatement to reduce the statement.
func (s *SelectStatement) Reduce(valuer Valuer) *SelectStatement {
	stmt := s.Clone()
	stmt.Condition = Reduce(stmt.Condition, valuer)
	for _, d := range stmt.Dimensions {
		d.Expr = Reduce(d.Expr, valuer)
	}

	for _, source := range stmt.Sources {
		switch source := source.(type) {
		case *SubQuery:
			source.Statement = source.Statement.Reduce(valuer)
		}
	}
	return stmt
}

// String returns a string representation of the select statement.
func (s *SelectStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *SelectStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SELECT ")

	if len(s.Hints) > 0 {
		_, _ = buf.WriteString("/*+ ")
		for _, hint := range s.Hints {
			_ = hint.RenderBytes(buf, posmap)
			_, _ = buf.WriteString(" ")
		}
		_, _ = buf.WriteString("*/")
	}

	_ = s.Fields.RenderBytes(buf, posmap)

	if s.Target != nil {
		_, _ = buf.WriteString(" ")
		_ = s.Target.RenderBytes(buf, posmap)
	}
	if len(s.Sources) > 0 {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_ = s.Dimensions.RenderBytes(buf, posmap)
	}
	switch s.Fill {
	case NoFill:
		_, _ = buf.WriteString(" fill(none)")
	case NumberFill:
		_, _ = fmt.Fprintf(buf, " fill(%v)", s.FillValue)
	case LinearFill:
		_, _ = buf.WriteString(" fill(linear)")
	case PreviousFill:
		_, _ = buf.WriteString(" fill(previous)")
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_ = s.SortFields.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	if s.SLimit > 0 {
		_, _ = fmt.Fprintf(buf, " SLIMIT %d", s.SLimit)
	}
	if s.SOffset > 0 {
		_, _ = fmt.Fprintf(buf, " SOFFSET %d", s.SOffset)
	}
	if s.Location != nil {
		_, _ = fmt.Fprintf(buf, ` TZ('%s')`, s.Location)
	}
	if len(s.PromSubCalls) > 0 {
		_, _ = buf.WriteString(" SUBCALL ")
		for _, subCall := range s.PromSubCalls {
			_ = subCall.RenderBytes(buf, posmap)
		}
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute the SelectStatement.
// NOTE: Statement should be normalized first (database name(s) in Sources and
// Target should be populated). If the statement has not been normalized, an
// empty string will be returned for the database name and it is up to the caller
// to interpret that as the default database.
func (s *SelectStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	ep, err := s.Sources.RequiredPrivileges()
	if err != nil {
		return nil, err
	}

	if s.Target != nil {
		ep = append(ep, ExecutionPrivilege{Admin: false, Name: s.Target.Measurement.Database, Rwuser: true, Privilege: WritePrivilege})
	}
	return ep, nil
}

// HasWildcard returns whether or not the select statement has at least 1 wildcard.
func (s *SelectStatement) HasWildcard() bool {
	return s.HasFieldWildcard() || s.HasDimensionWildcard()
}

// HasFieldWildcard returns whether or not the select statement has at least 1 wildcard in the fields.
func (s *SelectStatement) HasFieldWildcard() (hasWildcard bool) {
	WalkFunc(s.Fields, func(n Node) {
		if hasWildcard {
			return
		}
		switch n.(type) {
		case *Wildcard, *RegexLiteral:
			hasWildcard = true
		}
	})
	return hasWildcard
}

// HasDimensionWildcard returns whether or not the select statement has
// at least 1 wildcard in the dimensions aka `GROUP BY`.
func (s *SelectStatement) HasDimensionWildcard() bool {
	for _, d := range s.Dimensions {
		switch d.Expr.(type) {
		case *Wildcard, *RegexLiteral:
			return true
		}
	}

	return false
}

// GroupByInterval extracts the time interval, if specified.
func (s *SelectStatement) GroupByInterval() (time.Duration, error) {
	// return if we've already pulled it out
	if s.groupByInterval != 0 {
		return s.groupByInterval, nil
	}

	// Ignore if there are no dimensions.
	if len(s.Dimensions) == 0 {
		return 0, nil
	}

	for _, d := range s.Dimensions {
		if call, ok := d.Expr.(*Call); ok && call.Name == "time" {
			// Make sure there is exactly one argument.
			if got := len(call.Args); got < 1 || got > 2 {
				return 0, errors.New("time dimension expected 1 or 2 arguments")
			}

			// Ensure the argument is a duration.
			lit, ok := call.Args[0].(*DurationLiteral)
			if !ok {
				return 0, errors.New("time dimension must have duration argument")
			}
			s.groupByInterval = lit.Val
			return lit.Val, nil
		}
	}
	return 0, nil
}

// GroupByOffset extracts the time interval offset, if specified.
func (s *SelectStatement) GroupByOffset() (time.Duration, error) {
	interval, err := s.GroupByInterval()
	if err != nil {
		return 0, err
	}

	// Ignore if there are no dimensions.
	if len(s.Dimensions) == 0 {
		return 0, nil
	}

	for _, d := range s.Dimensions {
		if call, ok := d.Expr.(*Call); ok && call.Name == "time" {
			if len(call.Args) == 2 {
				switch expr := call.Args[1].(type) {
				case *DurationLiteral:
					return expr.Val % interval, nil
				case *TimeLiteral:
					return expr.Val.Sub(expr.Val.Truncate(interval)), nil
				default:
					return 0, fmt.Errorf("invalid time dimension offset: %s", expr)
				}
			}
			return 0, nil
		}
	}
	return 0, nil
}

// SetTimeRange sets the start and end time of the select statement to [start, end). i.e. start inclusive, end exclusive.
// This is used commonly for continuous queries so the start and end are in buckets.
func (s *SelectStatement) SetTimeRange(start, end time.Time) error {
	cond := fmt.Sprintf("time >= '%s' AND time < '%s'", start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano))
	if s.Condition != nil {
		cond = fmt.Sprintf("%s AND %s", s.rewriteWithoutTimeDimensions(), cond)
	}

	expr, err := NewParser(strings.NewReader(cond)).ParseExpr()
	if err != nil {
		return err
	}

	// Fold out any previously replaced time dimensions and set the condition.
	s.Condition = Reduce(expr, nil)

	return nil
}

// rewriteWithoutTimeDimensions will remove any WHERE time... clauses from the select statement.
// This is necessary when setting an explicit time range to override any that previously existed.
func (s *SelectStatement) rewriteWithoutTimeDimensions() string {
	n := RewriteFunc(s.Condition, func(n Node) Node {
		switch n := n.(type) {
		case *BinaryExpr:
			if n.LHS.String() == "time" {
				return &BooleanLiteral{Val: true}
			}
			return n
		case *Call:
			return &BooleanLiteral{Val: true}
		default:
			return n
		}
	})

	return n.String()
}

func encodeMeasurement(mm *Measurement) *internal.Measurement {
	pb := &internal.Measurement{
		Database:        proto.String(mm.Database),
		RetentionPolicy: proto.String(mm.RetentionPolicy),
		Name:            proto.String(mm.Name),
		IsTarget:        proto.Bool(mm.IsTarget),
		EngineType:      proto.Uint32(uint32(mm.EngineType)),
	}
	if mm.Regex != nil {
		pb.Regex = proto.String(mm.Regex.Val.String())
	}
	return pb
}

func decodeMeasurement(pb *internal.Measurement) (*Measurement, error) {
	mm := &Measurement{
		Database:        pb.GetDatabase(),
		RetentionPolicy: pb.GetRetentionPolicy(),
		Name:            pb.GetName(),
		IsTarget:        pb.GetIsTarget(),
		EngineType:      config.EngineType(pb.GetEngineType()),
	}

	if pb.Regex != nil {
		regex, err := regexp.Compile(pb.GetRegex())
		if err != nil {
			return nil, fmt.Errorf("invalid binary measurement regex: value=%q, err=%s", pb.GetRegex(), err)
		}
		mm.Regex = &RegexLiteral{Val: regex}
	}

	return mm, nil
}

// walkNames will walk the Expr and return the identifier names used.
func walkNames(exp Expr) []string {
	switch expr := exp.(type) {
	case *VarRef:
		return []string{expr.Val}
	case *Call:
		var a []string
		for _, expr := range expr.Args {
			if ref, ok := expr.(*VarRef); ok {
				a = append(a, ref.Val)
			}
		}
		return a
	case *BinaryExpr:
		var ret []string
		ret = append(ret, walkNames(expr.LHS)...)
		ret = append(ret, walkNames(expr.RHS)...)
		return ret
	case *ParenExpr:
		return walkNames(expr.Expr)
	}

	return nil
}

// walkRefs will walk the Expr and return the var refs used.
func walkRefs(exp Expr) []VarRef {
	refs := make(map[VarRef]struct{})
	var walk func(exp Expr)
	walk = func(exp Expr) {
		switch expr := exp.(type) {
		case *VarRef:
			refs[*expr] = struct{}{}
		case *Call:
			for _, expr := range expr.Args {
				if ref, ok := expr.(*VarRef); ok {
					refs[*ref] = struct{}{}
				}
			}
		case *BinaryExpr:
			walk(expr.LHS)
			walk(expr.RHS)
		case *ParenExpr:
			walk(expr.Expr)
		}
	}
	walk(exp)

	// Turn the map into a slice.
	a := make([]VarRef, 0, len(refs))
	for ref := range refs {
		a = append(a, ref)
	}
	return a
}

// ExprNames returns a list of non-"time" field names from an expression.
func ExprNames(expr Expr) []VarRef {
	m := make(map[VarRef]struct{})
	for _, ref := range walkRefs(expr) {
		if ref.Val == "time" {
			continue
		}
		m[ref] = struct{}{}
	}

	a := make([]VarRef, 0, len(m))
	for k := range m {
		a = append(a, k)
	}
	sort.Sort(VarRefs(a))

	return a
}

// Target represents a target (destination) policy, measurement, and DB.
type Target struct {
	// Measurement to write into.
	Measurement *Measurement
	depth       int
}

func (t *Target) Depth() int {
	if t != nil {
		return t.depth
	}
	return 0
}

func (t *Target) UpdateDepthForTests() int {
	if t != nil {
		t.depth = 1 + CallUpdateDepthForTests(t.Measurement)
		return t.depth
	}
	return 0
}

// String returns a string representation of the Target.
func (t *Target) String() string { return t.RenderBytes(&bytes.Buffer{}, nil).String() }

func (t *Target) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if t == nil {
		return buf
	}

	_, _ = buf.WriteString("INTO ")
	_ = t.Measurement.RenderBytes(buf, posmap)
	if t.Measurement.Name == "" {
		_, _ = buf.WriteString(":MEASUREMENT")
	}

	if posmap != nil {
		posmap[t] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// ExplainStatement represents a command for explaining a select statement.
type ExplainStatement struct {
	Statement *SelectStatement
	Analyze   bool
	depth     int
}

func (e *ExplainStatement) Depth() int {
	if e != nil {
		return e.depth
	}
	return 0
}

func (e *ExplainStatement) UpdateDepthForTests() int {
	if e != nil {
		CallUpdateDepthForTests(e.Statement)
		e.depth = 1 + maxDepth(e.Statement)
		return e.depth
	}
	return 0
}

// String returns a string representation of the explain statement.
func (e *ExplainStatement) String() string { return e.RenderBytes(&bytes.Buffer{}, nil).String() }

func (e *ExplainStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("EXPLAIN ")
	if e.Analyze {
		_, _ = buf.WriteString("ANALYZE ")
	}
	_ = e.Statement.RenderBytes(buf, posmap)

	if posmap != nil {
		posmap[e] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ExplainStatement.
func (e *ExplainStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return e.Statement.RequiredPrivileges()
}

// DeleteStatement represents a command for deleting data from the database.
type DeleteStatement struct {
	// Data source that values are removed from.
	Source Source

	// An expression evaluated on data point.
	Condition Expr

	depth int
}

func (s *DeleteStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *DeleteStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Source),
			CallUpdateDepthForTests(s.Condition),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the delete statement.
func (s *DeleteStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *DeleteStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DELETE FROM ")
	_ = s.Source.RenderBytes(buf, posmap)
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a DeleteStatement.
func (s *DeleteStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: WritePrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *DeleteStatement) DefaultDatabase() string {
	if m, ok := s.Source.(*Measurement); ok {
		return m.Database
	}
	return ""
}

// ShowSeriesStatement represents a command for listing series in the database.
type ShowSeriesStatement struct {
	// Database to query. If blank, use the default database.
	// The database can also be specified per source in the Sources.
	Database string

	// Measurement(s) the series are listed for.
	Sources Sources

	// An expression evaluated on a series name or tag.
	Condition Expr

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int

	// Expressions used for optimize querys
	Hints Hints

	depth int
}

func (s *ShowSeriesStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowSeriesStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.SortFields),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the list series statement.
func (s *ShowSeriesStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowSeriesStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW ")

	if len(s.Hints) > 0 {
		_, _ = buf.WriteString("/*+ ")
		for _, hint := range s.Hints {
			_ = hint.RenderBytes(buf, posmap)
			_, _ = buf.WriteString(" ")
		}
		_, _ = buf.WriteString("*/ ")
	}

	_, _ = buf.WriteString("SERIES")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}

	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_ = s.SortFields.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowSeriesStatement.
func (s *ShowSeriesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowSeriesStatement) DefaultDatabase() string {
	return s.Database
}

// DropSeriesStatement represents a command for removing a series from the database.
type DropSeriesStatement struct {
	// Data source that fields are extracted from (optional)
	Sources Sources

	// An expression evaluated on data point (optional)
	Condition Expr

	depth int
}

func (s *DropSeriesStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *DropSeriesStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Condition),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the drop series statement.
func (s *DropSeriesStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *DropSeriesStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DROP SERIES")

	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a DropSeriesStatement.
func (s DropSeriesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: WritePrivilege}}, nil
}

// DeleteSeriesStatement represents a command for deleting all or part of a series from a database.
type DeleteSeriesStatement struct {
	// Data source that fields are extracted from (optional)
	Sources Sources

	// An expression evaluated on data point (optional)
	Condition Expr

	depth int
}

func (s *DeleteSeriesStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *DeleteSeriesStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Condition),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the delete series statement.
func (s *DeleteSeriesStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *DeleteSeriesStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DELETE")

	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a DeleteSeriesStatement.
func (s DeleteSeriesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: WritePrivilege}}, nil
}

// DropShardStatement represents a command for removing a shard from
// the node.
type DropShardStatement struct {
	// ID of the shard to be dropped.
	ID uint64
}

func (s *DropShardStatement) Depth() int { return 1 }

func (s *DropShardStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the drop series statement.
func (s *DropShardStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *DropShardStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DROP SHARD ")
	_, _ = buf.WriteString(strconv.FormatUint(s.ID, 10))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a
// DropShardStatement.
func (s *DropShardStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// ShowSeriesCardinalityStatement represents a command for listing series cardinality.
type ShowSeriesCardinalityStatement struct {
	// Database to query. If blank, use the default database.
	// The database can also be specified per source in the Sources.
	Database string

	// Specifies whether the user requires exact counting or not.
	Exact bool

	// Measurement(s) the series are listed for.
	Sources Sources

	// An expression evaluated on a series name or tag.
	Condition Expr

	// Expressions used for grouping the selection.
	Dimensions Dimensions

	Limit, Offset int

	depth int
}

func (s *ShowSeriesCardinalityStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowSeriesCardinalityStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.Dimensions),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the show continuous queries statement.
func (s *ShowSeriesCardinalityStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowSeriesCardinalityStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW SERIES")

	if s.Exact {
		_, _ = buf.WriteString(" EXACT")
	}
	_, _ = buf.WriteString(" CARDINALITY")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}

	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_ = s.Dimensions.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowSeriesCardinalityStatement.
func (s *ShowSeriesCardinalityStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	if !s.Exact {
		return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
	}
	return s.Sources.RequiredPrivileges()
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowSeriesCardinalityStatement) DefaultDatabase() string {
	return s.Database
}

// ShowContinuousQueriesStatement represents a command for listing continuous queries.
type ShowContinuousQueriesStatement struct{}

func (s *ShowContinuousQueriesStatement) Depth() int { return 1 }

func (s *ShowContinuousQueriesStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the show continuous queries statement.
func (s *ShowContinuousQueriesStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowContinuousQueriesStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW CONTINUOUS QUERIES")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowContinuousQueriesStatement.
func (s *ShowContinuousQueriesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// ShowGrantsForUserStatement represents a command for listing user privileges.
type ShowGrantsForUserStatement struct {
	// Name of the user to display privileges.
	Name string
}

func (s *ShowGrantsForUserStatement) Depth() int { return 1 }

func (s *ShowGrantsForUserStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the show grants for user.
func (s *ShowGrantsForUserStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowGrantsForUserStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW GRANTS FOR ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowGrantsForUserStatement
func (s *ShowGrantsForUserStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// ShowDatabasesStatement represents a command for listing all databases in the cluster.
type ShowDatabasesStatement struct {
	// ShowDetail indicates the detail attribute of the database, e.g., TAG ATTRIBUTE, REPLICAS
	ShowDetail bool
}

func (s *ShowDatabasesStatement) Depth() int { return 1 }

func (s *ShowDatabasesStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the show databases command.
func (s *ShowDatabasesStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowDatabasesStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString("SHOW DATABASES")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowDatabasesStatement.
func (s *ShowDatabasesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	// SHOW DATABASES is one of few statements that have no required privileges.
	// Anyone is allowed to execute it, but the returned results depend on the user's
	// individual database permissions.
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: NoPrivileges}}, nil
}

// CreateContinuousQueryStatement represents a command for creating a continuous query.
type CreateContinuousQueryStatement struct {
	// Name of the continuous query to be created.
	Name string

	// Name of the database to create the continuous query on.
	Database string

	// Source of data (SELECT statement).
	Source *SelectStatement

	// Interval to resample previous queries.
	ResampleEvery time.Duration

	// Maximum duration to resample previous queries.
	ResampleFor time.Duration

	depth int
}

func (s *CreateContinuousQueryStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *CreateContinuousQueryStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + CallUpdateDepthForTests(s.Source)
	}
	return 0
}

// String returns a string representation of the statement.
func (s *CreateContinuousQueryStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *CreateContinuousQueryStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	fmt.Fprintf(buf, "CREATE CONTINUOUS QUERY %s ON %s ", QuoteIdent(s.Name), QuoteIdent(s.Database))

	if s.ResampleEvery > 0 || s.ResampleFor > 0 {
		buf.WriteString("RESAMPLE ")
		if s.ResampleEvery > 0 {
			fmt.Fprintf(buf, "EVERY %s ", FormatDuration(s.ResampleEvery))
		}
		if s.ResampleFor > 0 {
			fmt.Fprintf(buf, "FOR %s ", FormatDuration(s.ResampleFor))
		}
	}
	_, _ = buf.WriteString("BEGIN ")
	_ = s.Source.RenderBytes(buf, posmap)
	_, _ = buf.WriteString(" END")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// DefaultDatabase returns the default database from the statement.
func (s *CreateContinuousQueryStatement) DefaultDatabase() string {
	return s.Database
}

// RequiredPrivileges returns the privilege required to execute a CreateContinuousQueryStatement.
func (s *CreateContinuousQueryStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	ep := ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}

	// Selecting into a database that's different from the source?
	if s.Source.Target.Measurement.Database != "" {
		// Change source database privilege requirement to read.
		ep[0].Privilege = ReadPrivilege

		// Add destination database privilege requirement and set it to write.
		p := ExecutionPrivilege{
			Admin:     false,
			Rwuser:    true,
			Name:      s.Source.Target.Measurement.Database,
			Privilege: WritePrivilege,
		}
		ep = append(ep, p)
	}

	return ep, nil
}

func (s *CreateContinuousQueryStatement) validate() error {
	interval, err := s.Source.GroupByInterval()
	if err != nil {
		return err
	}

	if s.ResampleFor != 0 {
		if s.ResampleEvery != 0 && s.ResampleEvery > interval {
			interval = s.ResampleEvery
		}
		if interval > s.ResampleFor {
			return fmt.Errorf("FOR duration must be >= GROUP BY time duration: must be a minimum of %s, got %s", FormatDuration(interval), FormatDuration(s.ResampleFor))
		}
	}
	return nil
}

// DropContinuousQueryStatement represents a command for removing a continuous query.
type DropContinuousQueryStatement struct {
	Name     string
	Database string
}

func (s *DropContinuousQueryStatement) Depth() int { return 1 }

func (s *DropContinuousQueryStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the statement.
func (s *DropContinuousQueryStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *DropContinuousQueryStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	fmt.Fprintf(buf, "DROP CONTINUOUS QUERY %s ON %s", QuoteIdent(s.Name), QuoteIdent(s.Database))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a DropContinuousQueryStatement
func (s *DropContinuousQueryStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: WritePrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *DropContinuousQueryStatement) DefaultDatabase() string {
	return s.Database
}

// ShowMeasurementCardinalityStatement represents a command for listing measurement cardinality.
type ShowMeasurementCardinalityStatement struct {
	Exact         bool // If false then cardinality estimation will be used.
	Database      string
	Sources       Sources
	Condition     Expr
	Dimensions    Dimensions
	Limit, Offset int
	depth         int
}

func (s *ShowMeasurementCardinalityStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowMeasurementCardinalityStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.Dimensions),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowMeasurementCardinalityStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowMeasurementCardinalityStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW MEASUREMENT")

	if s.Exact {
		_, _ = buf.WriteString(" EXACT")
	}
	_, _ = buf.WriteString(" CARDINALITY")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}

	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_ = s.Dimensions.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowMeasurementCardinalityStatement.
func (s *ShowMeasurementCardinalityStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	if !s.Exact {
		return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
	}
	return s.Sources.RequiredPrivileges()
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowMeasurementCardinalityStatement) DefaultDatabase() string {
	return s.Database
}

// ShowMeasurementsStatement represents a command for listing measurements.
type ShowMeasurementsStatement struct {
	// Database to query. If blank, use the default database.
	Database string

	// Measurement name or regex.
	Source Source

	// An expression evaluated on data point.
	Condition Expr

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int

	depth int
}

func (s *ShowMeasurementsStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowMeasurementsStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Source),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.SortFields),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowMeasurementsStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowMeasurementsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW MEASUREMENTS")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(s.Database)
	}
	if s.Source != nil {
		_, _ = buf.WriteString(" WITH MEASUREMENT ")
		if m, ok := s.Source.(*Measurement); ok && m.Regex != nil {
			_, _ = buf.WriteString("=~ ")
		} else {
			_, _ = buf.WriteString("= ")
		}
		_ = s.Source.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_ = s.SortFields.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowMeasurementsStatement.
func (s *ShowMeasurementsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowMeasurementsStatement) DefaultDatabase() string {
	return s.Database
}

// ShowMeasurementsDetailStatement represents a command for listing measurements detail, including retention policy,
// tag keys, field keys, and schemas.
type ShowMeasurementsDetailStatement struct {
	// Database to query. If blank, use the default database.
	Database string

	// Measurement name or regex.
	Source Source

	depth int
}

func (s *ShowMeasurementsDetailStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowMeasurementsDetailStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + CallUpdateDepthForTests(s.Source)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowMeasurementsDetailStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowMeasurementsDetailStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW MEASUREMENTS DETAIL")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(s.Database)
	}
	if s.Source != nil {
		_, _ = buf.WriteString(" WITH MEASUREMENT ")
		if m, ok := s.Source.(*Measurement); ok && m.Regex != nil {
			_, _ = buf.WriteString("=~ ")
		} else {
			_, _ = buf.WriteString("= ")
		}
		_ = s.Source.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowMeasurementsDetailStatement.
func (s *ShowMeasurementsDetailStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowMeasurementsDetailStatement) DefaultDatabase() string {
	return s.Database
}

// DropMeasurementStatement represents a command to drop a measurement.
type DropMeasurementStatement struct {
	// Name of the measurement to be dropped.
	Name string
	// retention policy of the measurement to be dropped
	RpName string
}

func (s *DropMeasurementStatement) Depth() int { return 1 }

func (s *DropMeasurementStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the drop measurement statement.
func (s *DropMeasurementStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *DropMeasurementStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DROP MEASUREMENT ")
	if s.RpName != "" {
		_, _ = buf.WriteString(QuoteIdent(s.RpName))
		_, _ = buf.WriteString(".")
	}
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a DropMeasurementStatement
func (s *DropMeasurementStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// ShowQueriesStatement represents a command for listing all running queries.
type ShowQueriesStatement struct{}

func (s *ShowQueriesStatement) Depth() int { return 1 }

func (s *ShowQueriesStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the show queries statement.
func (s *ShowQueriesStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowQueriesStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW QUERIES")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowQueriesStatement.
func (s *ShowQueriesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// ShowRetentionPoliciesStatement represents a command for listing retention policies.
type ShowRetentionPoliciesStatement struct {
	// Name of the database to list policies for.
	Database string
}

func (s *ShowRetentionPoliciesStatement) Depth() int { return 1 }

func (s *ShowRetentionPoliciesStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of a ShowRetentionPoliciesStatement.
func (s *ShowRetentionPoliciesStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowRetentionPoliciesStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW RETENTION POLICIES")
	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowRetentionPoliciesStatement
func (s *ShowRetentionPoliciesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowRetentionPoliciesStatement) DefaultDatabase() string {
	return s.Database
}

// ShowStatsStatement displays statistics for a given module.
type ShowStatsStatement struct {
	Module string
}

func (s *ShowStatsStatement) Depth() int { return 1 }

func (s *ShowStatsStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of a ShowStatsStatement.
func (s *ShowStatsStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowStatsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW STATS")
	if s.Module != "" {
		_, _ = buf.WriteString(" FOR ")
		_, _ = buf.WriteString(QuoteString(s.Module))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowStatsStatement
func (s *ShowStatsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// ShowShardGroupsStatement represents a command for displaying shard groups in the cluster.
type ShowShardGroupsStatement struct{}

func (s *ShowShardGroupsStatement) Depth() int { return 1 }

func (s *ShowShardGroupsStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the SHOW SHARD GROUPS command.
func (s *ShowShardGroupsStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowShardGroupsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString("SHOW SHARD GROUPS")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privileges required to execute the statement.
func (s *ShowShardGroupsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// ShowShardsStatement represents a command for displaying shards in the cluster.
type ShowShardsStatement struct {
	mstInfo *Measurement
}

func (s *ShowShardsStatement) Depth() int {
	if s.mstInfo != nil {
		return 2
	} else {
		return 1
	}
}

func (s *ShowShardsStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1 + CallUpdateDepthForTests(s.mstInfo)
	}
	return 0
}

// String returns a string representation.
func (s *ShowShardsStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowShardsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString("SHOW SHARDS")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf

}

// RequiredPrivileges returns the privileges required to execute the statement.
func (s *ShowShardsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowShardsStatement) GetMstInfo() *Measurement {
	return s.mstInfo
}

func (s *ShowShardsStatement) GetMstName() string {
	if s.mstInfo == nil {
		return ""
	}
	return s.mstInfo.Name
}

func (s *ShowShardsStatement) GetDBName() string {
	if s.mstInfo == nil {
		return ""
	}
	return s.mstInfo.Database
}

func (s *ShowShardsStatement) GetRPName() string {
	if s.mstInfo == nil {
		return ""
	}
	return s.mstInfo.RetentionPolicy
}

// ShowDiagnosticsStatement represents a command for show node diagnostics.
type ShowDiagnosticsStatement struct {
	// Module
	Module string
}

func (s *ShowDiagnosticsStatement) Depth() int { return 1 }

func (s *ShowDiagnosticsStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the ShowDiagnosticsStatement.
func (s *ShowDiagnosticsStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowDiagnosticsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW DIAGNOSTICS")
	if s.Module != "" {
		_, _ = buf.WriteString(" FOR ")
		_, _ = buf.WriteString(QuoteString(s.Module))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowDiagnosticsStatement
func (s *ShowDiagnosticsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// CreateSubscriptionStatement represents a command to add a subscription to the incoming data stream.
type CreateSubscriptionStatement struct {
	Name            string
	Database        string
	RetentionPolicy string
	Destinations    []string
	Mode            string
}

func (s *CreateSubscriptionStatement) Depth() int {
	if s != nil {
		return 1 + len(s.Destinations)
	}
	return 0
}

func (s *CreateSubscriptionStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1 + len(s.Destinations)
	}
	return 0
}

// String returns a string representation of the CreateSubscriptionStatement.
func (s *CreateSubscriptionStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *CreateSubscriptionStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("CREATE SUBSCRIPTION ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.Database))
	_, _ = buf.WriteString(".")
	_, _ = buf.WriteString(QuoteIdent(s.RetentionPolicy))
	_, _ = buf.WriteString(" DESTINATIONS ")
	_, _ = buf.WriteString(s.Mode)
	_, _ = buf.WriteString(" ")
	for i, dest := range s.Destinations {
		if i != 0 {
			_, _ = buf.WriteString(", ")
		}
		_, _ = buf.WriteString(QuoteString(dest))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a CreateSubscriptionStatement.
func (s *CreateSubscriptionStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *CreateSubscriptionStatement) DefaultDatabase() string {
	return s.Database
}

// DropSubscriptionStatement represents a command to drop a subscription to the incoming data stream.
type DropSubscriptionStatement struct {
	Name            string
	Database        string
	RetentionPolicy string
}

func (s *DropSubscriptionStatement) Depth() int { return 1 }

func (s *DropSubscriptionStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the DropSubscriptionStatement.
func (s *DropSubscriptionStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *DropSubscriptionStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if s.Name == "" {
		_, _ = buf.WriteString("DROP ALL SUBSCRIPTIONS")
		if s.Database != "" {
			_, _ = buf.WriteString(" ON ")
			_, _ = buf.WriteString(QuoteIdent(s.Database))
		}
	} else {
		_, _ = fmt.Fprintf(buf, `DROP SUBSCRIPTION %s ON %s.%s`, QuoteIdent(s.Name), QuoteIdent(s.Database), QuoteIdent(s.RetentionPolicy))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a DropSubscriptionStatement
func (s *DropSubscriptionStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *DropSubscriptionStatement) DefaultDatabase() string {
	return s.Database
}

// ShowSubscriptionsStatement represents a command to show a list of subscriptions.
type ShowSubscriptionsStatement struct {
}

func (s *ShowSubscriptionsStatement) Depth() int { return 1 }

func (s *ShowSubscriptionsStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the ShowSubscriptionsStatement.
func (s *ShowSubscriptionsStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowSubscriptionsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString("SHOW SUBSCRIPTIONS")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowSubscriptionsStatement.
func (s *ShowSubscriptionsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// ShowTagKeysStatement represents a command for listing tag keys.
type ShowTagKeysStatement struct {
	// Database to query. If blank, use the default database.
	// The database can also be specified per source in the Sources.
	Database string

	// Data sources that fields are extracted from.
	Sources Sources

	// An expression evaluated on data point.
	Condition Expr

	// Fields to sort results by.
	SortFields SortFields

	// Maximum number of tag keys per measurement. Unlimited if zero.
	Limit int

	// Returns tag keys starting at an offset from the first row.
	Offset int

	// Maxiumum number of series to be returned. Unlimited if zero.
	SLimit int

	// Returns series starting at an offset from the first one.
	SOffset int

	depth int
}

func (s *ShowTagKeysStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowTagKeysStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.SortFields),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowTagKeysStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowTagKeysStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW TAG KEYS")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_ = s.SortFields.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	if s.SLimit > 0 {
		_, _ = buf.WriteString(" SLIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.SLimit))
	}
	if s.SOffset > 0 {
		_, _ = buf.WriteString(" SOFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.SOffset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowTagKeysStatement.
func (s *ShowTagKeysStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowTagKeysStatement) DefaultDatabase() string {
	return s.Database
}

// ShowTagKeyCardinalityStatement represents a command for listing tag key cardinality.
type ShowTagKeyCardinalityStatement struct {
	Database      string
	Exact         bool
	Sources       Sources
	Condition     Expr
	Dimensions    Dimensions
	Limit, Offset int
	depth         int
}

func (s *ShowTagKeyCardinalityStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowTagKeyCardinalityStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.Dimensions),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowTagKeyCardinalityStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowTagKeyCardinalityStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW TAG KEY ")
	if s.Exact {
		_, _ = buf.WriteString("EXACT ")
	}
	_, _ = buf.WriteString("CARDINALITY")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_ = s.Dimensions.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowTagKeyCardinalityStatement.
func (s *ShowTagKeyCardinalityStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return s.Sources.RequiredPrivileges()
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowTagKeyCardinalityStatement) DefaultDatabase() string {
	return s.Database
}

// ShowTagValuesStatement represents a command for listing tag values.
type ShowTagValuesStatement struct {
	// Database to query. If blank, use the default database.
	// The database can also be specified per source in the Sources.
	Database string

	// Data source that fields are extracted from.
	Sources Sources

	// Operation to use when selecting tag key(s).
	Op Token

	// Literal to compare the tag key(s) with.
	TagKeyExpr Literal

	// An expression evaluated on tag key(s).
	TagKeyCondition Expr

	// An expression evaluated on data point.
	Condition Expr

	// Fields to sort results by.
	SortFields SortFields

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int

	// Expressions used for optimize querys
	Hints Hints

	depth int
}

func (s *ShowTagValuesStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowTagValuesStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.TagKeyExpr),
			CallUpdateDepthForTests(s.TagKeyCondition),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.SortFields),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowTagValuesStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowTagValuesStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW ")

	if len(s.Hints) > 0 {
		_, _ = buf.WriteString("/*+ ")
		for _, hint := range s.Hints {
			_ = hint.RenderBytes(buf, posmap)
			_, _ = buf.WriteString(" ")
		}
		_, _ = buf.WriteString("*/ ")
	}

	_, _ = buf.WriteString("TAG VALUES")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	_, _ = buf.WriteString(" WITH KEY ")
	_, _ = buf.WriteString(s.Op.String())
	_, _ = buf.WriteString(" ")
	if lit, ok := s.TagKeyExpr.(*StringLiteral); ok {
		_, _ = buf.WriteString(QuoteIdent(lit.Val))
	} else {
		_ = s.TagKeyExpr.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_ = s.SortFields.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowTagValuesStatement.
func (s *ShowTagValuesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowTagValuesStatement) DefaultDatabase() string {
	return s.Database
}

// ShowTagValuesCardinalityStatement represents a command for listing tag value cardinality.
type ShowTagValuesCardinalityStatement struct {
	Database      string
	Exact         bool
	Sources       Sources
	Op            Token
	TagKeyExpr    Literal
	Condition     Expr
	Dimensions    Dimensions
	Limit, Offset int

	TagKeyCondition Expr
	depth           int
}

func (s *ShowTagValuesCardinalityStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowTagValuesCardinalityStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.TagKeyExpr),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.Dimensions),
			CallUpdateDepthForTests(s.TagKeyCondition),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowTagValuesCardinalityStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowTagValuesCardinalityStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW TAG VALUES ")
	if s.Exact {
		_, _ = buf.WriteString("EXACT ")
	}
	_, _ = buf.WriteString("CARDINALITY")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	_, _ = buf.WriteString(" WITH KEY ")
	_, _ = buf.WriteString(s.Op.String())
	_, _ = buf.WriteString(" ")
	if lit, ok := s.TagKeyExpr.(*StringLiteral); ok {
		_, _ = buf.WriteString(QuoteIdent(lit.Val))
	} else {
		_ = s.TagKeyExpr.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_ = s.Dimensions.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowTagValuesCardinalityStatement.
func (s *ShowTagValuesCardinalityStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	privs, err := s.Sources.RequiredPrivileges()
	if err != nil {
		return nil, err
	}
	for i := range privs {
		p := &privs[i]
		if p.Name == "" {
			p.Name = s.Database
		}
	}
	return privs, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowTagValuesCardinalityStatement) DefaultDatabase() string {
	return s.Database
}

// ShowUsersStatement represents a command for listing users.
type ShowUsersStatement struct{}

func (s *ShowUsersStatement) Depth() int { return 1 }

func (s *ShowUsersStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the ShowUsersStatement.
func (s *ShowUsersStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowUsersStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW USERS")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowUsersStatement
func (s *ShowUsersStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: false, Privilege: AllPrivileges}}, nil
}

// ShowFieldKeyCardinalityStatement represents a command for listing field key cardinality.
type ShowFieldKeyCardinalityStatement struct {
	Database      string
	Exact         bool
	Sources       Sources
	Condition     Expr
	Dimensions    Dimensions
	Limit, Offset int
	depth         int
}

func (s *ShowFieldKeyCardinalityStatement) Depth() int { return 1 }

func (s *ShowFieldKeyCardinalityStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.Condition),
			CallUpdateDepthForTests(s.Dimensions),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowFieldKeyCardinalityStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowFieldKeyCardinalityStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW FIELD KEY ")

	if s.Exact {
		_, _ = buf.WriteString("EXACT ")
	}
	_, _ = buf.WriteString("CARDINALITY")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_ = s.Condition.RenderBytes(buf, posmap)
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_ = s.Dimensions.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a ShowFieldKeyCardinalityStatement.
func (s *ShowFieldKeyCardinalityStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return s.Sources.RequiredPrivileges()
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowFieldKeyCardinalityStatement) DefaultDatabase() string {
	return s.Database
}

// ShowFieldKeysStatement represents a command for listing field keys.
type ShowFieldKeysStatement struct {
	// Database to query. If blank, use the default database.
	// The database can also be specified per source in the Sources.
	Database string

	// Data sources that fields are extracted from.
	Sources Sources

	// Fields to sort results by
	SortFields SortFields

	// Maximum number of rows to be returned.
	// Unlimited if zero.
	Limit int

	// Returns rows starting at an offset from the first row.
	Offset int

	depth int
}

func (s *ShowFieldKeysStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *ShowFieldKeysStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Sources),
			CallUpdateDepthForTests(s.SortFields),
		)
		return s.depth
	}
	return 0
}

// String returns a string representation of the statement.
func (s *ShowFieldKeysStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowFieldKeysStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW FIELD KEYS")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_ = s.Sources.RenderBytes(buf, posmap)
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_ = s.SortFields.RenderBytes(buf, posmap)
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowFieldKeysStatement.
func (s *ShowFieldKeysStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: s.Database, Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// DefaultDatabase returns the default database from the statement.
func (s *ShowFieldKeysStatement) DefaultDatabase() string {
	return s.Database
}

// Fields represents a list of fields.
type Fields []*Field

func (a Fields) UpdateDepthForTests() int {
	if a != nil {
		depth := 0
		for i, f := range a {
			depth = max(depth, 1+i+CallUpdateDepthForTests(f))
		}
		return depth
	}
	return 0
}

// AliasNames returns a list of calculated field names in
// order of alias, function name, then field.
func (a Fields) AliasNames() []string {
	names := []string{}
	for _, f := range a {
		names = append(names, f.Name())
	}
	return names
}

// Names returns a list of field names.
func (a Fields) Names() []string {
	names := []string{}
	for _, f := range a {
		switch expr := f.Expr.(type) {
		case *Call:
			names = append(names, expr.Name)
		case *VarRef:
			names = append(names, expr.Val)
		case *BinaryExpr:
			names = append(names, walkNames(expr)...)
		case *ParenExpr:
			names = append(names, walkNames(expr)...)
		}
	}
	return names
}

// String returns a string representation of the fields.
func (a Fields) String() string { return a.RenderBytes(&bytes.Buffer{}, nil).String() }

func (a Fields) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	for i, f := range a {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_ = f.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[a] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type fieldsList struct {
	fields Fields
	depth  int
}

func (a fieldsList) Depth() int { return a.depth }

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr      Expr
	Alias     string
	Auxiliary bool // auxiliary fields not in the select clause
	depth     int
}

func (f *Field) Depth() int {
	if f != nil {
		return f.depth
	}
	return 0
}

func (f *Field) UpdateDepthForTests() int {
	if f != nil {
		f.depth = 1 + CallUpdateDepthForTests(f.Expr)
		return f.depth
	}
	return 0
}

// Name returns the name of the field. Returns alias, if set.
// Otherwise uses the function name or variable name.
func (f *Field) Name() string {
	// Return alias, if set.
	if f.Alias != "" {
		return f.Alias
	}

	// Return the function name or variable name, if available.
	switch expr := f.Expr.(type) {
	case *Call:
		return expr.Name
	case *BinaryExpr:
		return BinaryExprName(expr)
	case *ParenExpr:
		f := Field{Expr: expr.Expr, depth: 1 + expr.Expr.Depth()}
		return f.Name()
	case *VarRef:
		return expr.Val
	}

	// Otherwise return a blank name.
	return ""
}

// String returns a string representation of the field.
func (f *Field) String() string { return f.RenderBytes(&bytes.Buffer{}, nil).String() }

func (f *Field) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_ = f.Expr.RenderBytes(buf, posmap)

	if f.Alias != "" {
		buf.WriteString(" AS ")
		buf.WriteString(QuoteIdent(f.Alias))
	}

	if posmap != nil {
		posmap[f] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// Len implements sort.Interface.
func (a Fields) Len() int { return len(a) }

// Less implements sort.Interface.
func (a Fields) Less(i, j int) bool { return a[i].Name() < a[j].Name() }

// Swap implements sort.Interface.
func (a Fields) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Dimensions represents a list of dimensions.
type Dimensions []*Dimension

func (a Dimensions) UpdateDepthForTests() int {
	if a != nil {
		depth := 0
		for i, d := range a {
			depth = max(depth, 1+i+CallUpdateDepthForTests(d))
		}
		return depth
	}
	return 0
}

// String returns a string representation of the dimensions.
func (a Dimensions) String() string { return a.RenderBytes(&bytes.Buffer{}, nil).String() }

func (a Dimensions) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	for i, d := range a {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_ = d.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[a] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// Normalize returns the interval and tag dimensions separately.
// Returns 0 if no time interval is specified.
func (a Dimensions) Normalize() (time.Duration, []string) {
	var dur time.Duration
	var tags []string

	for _, dim := range a {
		switch expr := dim.Expr.(type) {
		case *Call:
			lit, _ := expr.Args[0].(*DurationLiteral)
			dur = lit.Val
		case *VarRef:
			tags = append(tags, expr.Val)
		}
	}

	return dur, tags
}

type dimensionsList struct {
	dims  Dimensions
	depth int
}

func (a dimensionsList) Depth() int { return a.depth }

// Dimension represents an expression that a select statement is grouped by.
type Dimension struct {
	Expr  Expr
	depth int
}

func (d *Dimension) Depth() int {
	if d != nil {
		return d.depth
	}
	return 0
}

func (d *Dimension) UpdateDepthForTests() int {
	if d != nil {
		d.depth = 1 + CallUpdateDepthForTests(d.Expr)
		return d.depth
	}
	return 0
}

// String returns a string representation of the dimension.
func (d *Dimension) String() string { return d.RenderBytes(&bytes.Buffer{}, nil).String() }

func (d *Dimension) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_ = d.Expr.RenderBytes(buf, posmap)

	if posmap != nil {
		posmap[d] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type Hint struct {
	Expr Expr
}

// String returns a string representation of the dimension.
func (d *Hint) String() string { return d.RenderBytes(&bytes.Buffer{}, nil).String() }

func (d *Hint) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString(d.Expr.(*StringLiteral).Val)

	if posmap != nil {
		posmap[d] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type Hints []*Hint

func (a Hints) String() string { return a.RenderBytes(&bytes.Buffer{}, nil).String() }

func (a Hints) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	for i, h := range a {
		if i > 0 {
			buf.WriteString(", ")
		}
		_ = h.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[a] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// Measurements represents a list of measurements.
type Measurements []*Measurement

// String returns a string representation of the measurements.
func (a Measurements) String() string { return a.RenderBytes(&bytes.Buffer{}, nil).String() }

func (a Measurements) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	for i, m := range a {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_ = m.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[a] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type IndexList struct {
	IList []string
}

func (il *IndexList) Clone() *IndexList {
	clone := &IndexList{
		IList: make([]string, len(il.IList)),
	}
	copy(clone.IList, il.IList)
	return clone
}

type IndexRelation struct {
	Rid          uint32
	Oids         []uint32
	IndexNames   []string
	IndexList    []*IndexList    // indexType to column name (all column indexed with indexType)
	IndexOptions []*IndexOptions // indexType to IndexOptions
}

func NewIndexRelation() *IndexRelation {
	return &IndexRelation{
		Oids:         make([]uint32, 0),
		IndexNames:   make([]string, 0),
		IndexList:    make([]*IndexList, 0),
		IndexOptions: make([]*IndexOptions, 0),
	}
}

func (ir *IndexRelation) FindIndexOption(oid uint32, field string) *IndexOption {
	if ir.IndexOptions == nil {
		return nil
	}
	for i, id := range ir.Oids {
		if id != oid || len(ir.IndexOptions[i].Options) == 0 {
			continue
		}
		if field == "" || len(ir.IndexOptions[i].Options) == 1 {
			// find full text option
			return ir.IndexOptions[i].Options[0]
		} else {
			for j, iList := range ir.IndexList[i].IList {
				if field == iList {
					if j < len(ir.IndexOptions[i].Options) {
						return ir.IndexOptions[i].Options[j]
					} else {
						// use first option
						return ir.IndexOptions[i].Options[0]
					}
				}
			}
		}
	}
	return nil
}

// Clone returns a deep clone of the IndexRelation.
func (ir *IndexRelation) Clone() *IndexRelation {
	if ir == nil {
		return nil
	}
	clone := &IndexRelation{
		Rid:          ir.Rid,
		Oids:         make([]uint32, len(ir.Oids)),
		IndexNames:   make([]string, len(ir.IndexNames)),
		IndexList:    make([]*IndexList, 0, len(ir.IndexList)),
		IndexOptions: make([]*IndexOptions, 0, len(ir.IndexOptions)),
	}
	copy(clone.Oids, ir.Oids)
	copy(clone.IndexNames, ir.IndexNames)
	for _, src := range ir.IndexList {
		clone.IndexList = append(clone.IndexList, src.Clone())
	}
	for _, opts := range ir.IndexOptions {
		clone.IndexOptions = append(clone.IndexOptions, opts.Clone())
	}
	return clone
}

func (ir *IndexRelation) GetIndexOidByName(indexName string) (uint32, bool) {
	if ir == nil || len(ir.IndexNames) == 0 {
		return 0, false
	}
	for i := range ir.IndexNames {
		if ir.IndexNames[i] == indexName {
			return ir.Oids[i], true
		}
	}
	return 0, false
}

func (ir *IndexRelation) GetBloomFilterColumns() []string {
	if ir == nil {
		return nil
	}
	for i := range ir.IndexNames {
		if ir.IndexNames[i] == index.BloomFilterIndex {
			return ir.IndexList[i].IList
		}
	}
	return nil
}

func (ir *IndexRelation) GetFullTextColumns() []string {
	if ir == nil || len(ir.Oids) == 0 {
		return nil
	}
	for i := range ir.Oids {
		if ir.Oids[i] == uint32(index.BloomFilterFullText) {
			return ir.IndexList[i].IList
		}
	}
	return nil
}

func (ir *IndexRelation) GetTimeClusterDuration() int64 {
	for i := range ir.Oids {
		if ir.Oids[i] == uint32(index.TimeCluster) {
			return int64(ir.IndexOptions[i].Options[0].TimeClusterDuration)
		}
	}
	return 0
}

func (ir *IndexRelation) IsSkipIndex(idx int) bool {
	if ir.Oids[idx] >= uint32(index.BloomFilter) && ir.Oids[idx] < uint32(index.IndexTypeAll) {
		return true
	}
	return false
}

const (
	TEMPORARY string = "temporary"
	GENERAL   string = "general"
)

// Measurement represents a single measurement used as a datasource.
type Measurement struct {
	Database        string
	RetentionPolicy string
	Name            string // name with version
	Regex           *RegexLiteral
	IsTarget        bool

	// This field indicates that the measurement should read be read from the
	// specified system iterator.
	SystemIterator    string
	IsSystemStatement bool
	Alias             string

	IsTimeSorted  bool
	IndexRelation *IndexRelation
	ObsOptions    *obs.ObsOptions
	EngineType    config.EngineType
	// "general" means a regular measurement, "temporary" means a temporary measurement
	MstType string
}

func (m *Measurement) Depth() int { return 1 }

func (m *Measurement) UpdateDepthForTests() int {
	if m != nil {
		return 1
	}
	return 0
}

func (m *Measurement) IsTemporary() bool {
	return m.MstType == TEMPORARY
}

// Clone returns a deep clone of the Measurement.
func (m *Measurement) Clone() *Measurement {
	var regexp *RegexLiteral
	if m.Regex != nil && m.Regex.Val != nil {
		regexp = &RegexLiteral{Val: m.Regex.Val.Copy()}
	}

	var indexRelation *IndexRelation
	if m.IndexRelation != nil {
		indexRelation = m.IndexRelation.Clone()
	}

	var obsOpts *obs.ObsOptions
	if m.ObsOptions != nil {
		obsOpts = m.ObsOptions.Clone()
	}

	return &Measurement{
		Database:          m.Database,
		RetentionPolicy:   m.RetentionPolicy,
		Name:              m.Name,
		Regex:             regexp,
		IsTarget:          m.IsTarget,
		SystemIterator:    m.SystemIterator,
		IsSystemStatement: m.IsSystemStatement,
		Alias:             m.Alias,
		IndexRelation:     indexRelation,
		ObsOptions:        obsOpts,
		EngineType:        m.EngineType,
		IsTimeSorted:      m.IsTimeSorted,
		MstType:           m.MstType,
	}
}

// Read/Write Splitting
func (m *Measurement) IsRWSplit() bool {
	if m == nil {
		return false
	}
	if config.IsLogKeeper() {
		return m.ObsOptions != nil
	}
	return false
}

func (m *Measurement) IsCSStore() bool {
	if m == nil {
		return false
	}
	return m.EngineType == config.COLUMNSTORE
}

// String returns a string representation of the measurement.
func (m *Measurement) String() string { return m.RenderBytes(&bytes.Buffer{}, nil).String() }

func (m *Measurement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if m.Database != "" {
		_, _ = buf.WriteString(QuoteIdent(m.Database))
		_, _ = buf.WriteString(".")
	}

	if m.RetentionPolicy != "" {
		_, _ = buf.WriteString(QuoteIdent(m.RetentionPolicy))
	}

	if m.Database != "" || m.RetentionPolicy != "" {
		_, _ = buf.WriteString(`.`)
	}

	if m.Name != "" && m.SystemIterator == "" {
		_, _ = buf.WriteString(QuoteIdent(m.Name))
	} else if m.SystemIterator != "" {
		_, _ = buf.WriteString(QuoteIdent(m.SystemIterator))
	} else if m.Regex != nil {
		_ = m.Regex.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[m] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (m *Measurement) GetName() string {
	if len(m.Alias) != 0 {
		return m.Alias
	}
	return m.Name
}

// SubQuery is a source with a SelectStatement as the backing store.
type SubQuery struct {
	Statement *SelectStatement
	Alias     string
	depth     int
}

func (s *SubQuery) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *SubQuery) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + CallUpdateDepthForTests(s.Statement)
		return s.depth
	}
	return 0
}

// String returns a string representation of the subquery.
func (s *SubQuery) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *SubQuery) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("(")
	_ = s.Statement.RenderBytes(buf, posmap)
	_, _ = buf.WriteString(")")

	if len(s.Alias) != 0 {
		_, _ = buf.WriteString(" as ")
		_, _ = buf.WriteString(s.Alias)
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *SubQuery) GetName() string {
	return s.Alias
}

type JoinType int8

const (
	InnerJoin JoinType = iota
	LeftOuterJoin
	RightOuterJoin
	OuterJoin
	FullJoin
	UnknownJoin
)

var JoinTypeMap = map[JoinType]string{
	InnerJoin:      "inner join",
	LeftOuterJoin:  "left outer join",
	RightOuterJoin: "right outer join",
	OuterJoin:      "outer join",
	FullJoin:       "full join",
}

type Join struct {
	LSrc      Source
	RSrc      Source
	Condition Expr
	JoinType  JoinType
	depth     int
}

func (j *Join) Depth() int {
	if j != nil {
		return j.depth
	}
	return 0
}

func (j *Join) UpdateDepthForTests() int {
	if j != nil {
		j.depth = 1 + max(
			CallUpdateDepthForTests(j.LSrc),
			CallUpdateDepthForTests(j.RSrc),
			CallUpdateDepthForTests(j.Condition),
		)
		return j.depth
	}
	return 0
}

func (j *Join) String() string { return j.RenderBytes(&bytes.Buffer{}, nil).String() }

func (j *Join) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_ = j.LSrc.RenderBytes(buf, posmap)
	_, _ = fmt.Fprintf(buf, " %s ", JoinTypeMap[j.JoinType])
	_ = j.RSrc.RenderBytes(buf, posmap)
	_, _ = buf.WriteString(" on ")
	_ = j.Condition.RenderBytes(buf, posmap)

	if posmap != nil {
		posmap[j] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (j *Join) GetName() string {
	return ""
}

type UnionType int8

const (
	UnionDistinct UnionType = iota
	UnionAll
	UnionDistinctByName
	UnionAllByName
)

var UnionTypeMap = map[UnionType]string{
	UnionDistinct:       "union",
	UnionAll:            "union all",
	UnionDistinctByName: "union by name",
	UnionAllByName:      "union all by name",
}

type Union struct {
	LSrc      Source
	RSrc      Source
	UnionType UnionType
	depth     int
}

func (u *Union) Depth() int {
	if u != nil {
		return u.depth
	}
	return 0
}

func (u *Union) UpdateDepthForTests() int {
	if u != nil {
		u.depth = 1 + max(
			CallUpdateDepthForTests(u.LSrc),
			CallUpdateDepthForTests(u.RSrc),
		)
		return u.depth
	}
	return 0
}

func (u *Union) String() string { return u.RenderBytes(&bytes.Buffer{}, nil).String() }

func (u *Union) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_ = u.LSrc.RenderBytes(buf, posmap)
	_, _ = fmt.Fprintf(buf, " %s ", UnionTypeMap[u.UnionType])
	_ = u.RSrc.RenderBytes(buf, posmap)

	if posmap != nil {
		posmap[u] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (j *Union) GetName() string {
	return ""
}

type MatchCardinality int

const (
	OneToOne MatchCardinality = iota
	ManyToOne
	OneToMany
)

type NilMstState int

const (
	NoNilMst NilMstState = iota
	LNilMst
	RNilMst
)

type BinOp struct {
	LSrc        Source
	RSrc        Source
	LExpr       Expr
	RExpr       Expr
	OpType      int
	On          bool     // true: on; false: ignore
	MatchKeys   []string // on(MatchKeys)
	MatchCard   MatchCardinality
	IncludeKeys []string // group_left/group_right(IncludeKeys)
	ReturnBool  bool
	NilMst      NilMstState
	depth       int
}

func (b *BinOp) Depth() int {
	if b != nil {
		return b.depth
	}
	return 0
}

func (b *BinOp) UpdateDepthForTests() int {
	if b != nil {
		b.depth = 1 + max(
			CallUpdateDepthForTests(b.LSrc),
			CallUpdateDepthForTests(b.RSrc),
			CallUpdateDepthForTests(b.LExpr),
			CallUpdateDepthForTests(b.RExpr),
			len(b.MatchKeys),
			len(b.IncludeKeys),
		)
		return b.depth
	}
	return 0
}

func (b *BinOp) String() string { return b.RenderBytes(&bytes.Buffer{}, nil).String() }

func (b *BinOp) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if b.LSrc == nil {
		_ = b.LExpr.RenderBytes(buf, posmap)
	} else {
		_ = b.LSrc.RenderBytes(buf, posmap)
	}

	_, _ = buf.WriteString(" binary op ")

	if b.RSrc == nil {
		_ = b.RExpr.RenderBytes(buf, posmap)
	} else {
		_ = b.RSrc.RenderBytes(buf, posmap)
	}

	_, _ = fmt.Fprintf(buf, " %t %t(", b.ReturnBool, b.On)

	for _, matchKey := range b.MatchKeys {
		_, _ = buf.WriteString(matchKey)
	}
	_, _ = fmt.Fprintf(buf, ") %d(", b.MatchCard)

	for _, includeKey := range b.IncludeKeys {
		_, _ = buf.WriteString(includeKey)
	}

	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[b] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (b *BinOp) GetName() string {
	return ""
}

func AllowNilMst(BinOpType int) bool {
	return BinOpType == parser.LOR || BinOpType == parser.LUNLESS
}

type PromSubCall struct {
	Name               string
	Interval           int64         // ns
	StartTime          int64         // ns
	EndTime            int64         // ns
	Range              time.Duration // ns
	Offset             time.Duration // ns
	InArgs             []Expr
	LowerStepInvariant bool
	SubStartT, SubEndT int64
	SubStep            int64
}

func (j *PromSubCall) node() {}

func (j *PromSubCall) String() string { return j.RenderBytes(&bytes.Buffer{}, nil).String() }

func (j *PromSubCall) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	fmt.Fprintf(buf, "%s(%d, %d, %d, %d, %d, %t, %d, %d, %d) ", j.Name, j.Interval, j.StartTime, j.EndTime, j.Range, j.Offset, j.LowerStepInvariant, j.SubStartT, j.SubEndT, j.SubStep)

	if posmap != nil {
		posmap[j] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type InCondition struct {
	Stmt      *SelectStatement
	Column    Expr
	TimeRange TimeRange   // use for Prepare MapShard for InCondition.stmt
	Csming    interface{} // same as shardMapping, use for mapShard of InCondition stmt
	TimeCond  *BinaryExpr
	FMapper   FieldMapper
	depth     int
	NotEqual  bool
}

func (j *InCondition) Depth() int {
	if j != nil {
		return j.depth
	}
	return 0
}

func (j *InCondition) UpdateDepthForTests() int {
	if j != nil {
		j.depth = 1 + max(
			CallUpdateDepthForTests(j.Stmt),
			CallUpdateDepthForTests(j.TimeCond),
		)
		return j.depth
	}
	return 0
}

func (j *InCondition) String() string { return j.RenderBytes(&bytes.Buffer{}, nil).String() }

func (j *InCondition) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("in condition,select statement: ")
	_ = j.Stmt.RenderBytes(buf, posmap)
	_, _ = buf.WriteString(", column name: ")
	_ = j.Column.RenderBytes(buf, posmap)

	if posmap != nil {
		posmap[j] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (j *InCondition) GetName() string {
	return ""
}

func (j *InCondition) RewriteNameSpace(alias, mst string) {}

func DefaultGraphVarRef() *VarRef {
	return &VarRef{
		Val:  "graph",
		Type: Graph,
	}
}

// VarRef represents a reference to a variable.
type VarRef struct {
	Val   string
	Type  DataType
	Alias string
}

func (r *VarRef) Depth() int { return 1 }

func (r *VarRef) UpdateDepthForTests() int {
	if r != nil {
		return 1
	}
	return 0
}

func (r *VarRef) SetDataType(t DataType) {
	r.Type = t
}

func (r *VarRef) SetVal(s string) {
	r.Val = s
}

func (r *VarRef) RewriteNameSpace(alias, mst string) {
	if len(r.Alias) == 0 && strings.HasPrefix(r.Val, alias+".") {
		al := make([]byte, len(r.Val))
		copy(al, r.Val)
		r.Val = mst + "." + r.Val[len(alias)+1:]
		r.Alias = string(al)
	}
}

// String returns a string representation of the variable reference.
func (r *VarRef) String() string { return r.RenderBytes(&bytes.Buffer{}, nil).String() }

func (r *VarRef) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString(QuoteIdent(r.Val))
	if r.Type != Unknown {
		buf.WriteString("::")
		buf.WriteString(r.Type.String())
	}

	if posmap != nil {
		posmap[r] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// VarRefs represents a slice of VarRef typ.
type VarRefPointers []*VarRef

// Len implements sort.Interface.
func (a VarRefPointers) Len() int { return len(a) }

// Less implements sort.Interface.
func (a VarRefPointers) Less(i, j int) bool {
	if a[i].Val != a[j].Val {
		return a[i].Val < a[j].Val
	}
	return a[i].Type < a[j].Type
}

// Swap implements sort.Interface.
func (a VarRefPointers) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Strings returns a slice of the variable names.
func (a VarRefPointers) Strings() []string {
	s := make([]string, len(a))
	for i, ref := range a {
		s[i] = ref.Val
	}
	return s
}

// VarRefs represents a slice of VarRef typ.
type VarRefs []VarRef

// Len implements sort.Interface.
func (a VarRefs) Len() int { return len(a) }

// Less implements sort.Interface.
func (a VarRefs) Less(i, j int) bool {
	if a[i].Val != a[j].Val {
		return a[i].Val < a[j].Val
	}
	return a[i].Type < a[j].Type
}

// Swap implements sort.Interface.
func (a VarRefs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Strings returns a slice of the variable names.
func (a VarRefs) Strings() []string {
	s := make([]string, len(a))
	for i, ref := range a {
		s[i] = ref.Val
	}
	return s
}

// Call represents a function call.
type Call struct {
	Name  string
	Args  []Expr
	depth int
}

func (c *Call) Depth() int {
	if c != nil {
		return c.depth
	}
	return 0
}

func (c *Call) UpdateDepthForTests() int {
	if c != nil {
		argdepth := 0
		for i, a := range c.Args {
			argdepth = max(argdepth, 1+i+CallUpdateDepthForTests(a))
		}
		c.depth = 1 + argdepth
		return c.depth
	}
	return 0
}

func (c *Call) RewriteNameSpace(alias, mst string) {
	for i := range c.Args {
		c.Args[i].RewriteNameSpace(alias, mst)
	}
}

// String returns a string representation of the call.
func (c *Call) String() string { return c.RenderBytes(&bytes.Buffer{}, nil).String() }

func (c *Call) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	// format fmt.Sprintf("%s(%s)", c.Name, strings.Join(str, ", "))
	_, _ = buf.WriteString(c.Name)
	_, _ = buf.WriteString("(")
	for i, arg := range c.Args {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_ = arg.RenderBytes(buf, posmap)
	}
	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[c] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// Distinct represents a DISTINCT expression.
type Distinct struct {
	// Identifier following DISTINCT
	Val string
}

func (d *Distinct) Depth() int { return 1 }

func (d *Distinct) UpdateDepthForTests() int {
	if d != nil {
		return 1
	}
	return 0
}

func (d *Distinct) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the expression.
func (d *Distinct) String() string { return d.RenderBytes(&bytes.Buffer{}, nil).String() }

func (d *Distinct) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DISTINCT ")
	_, _ = buf.WriteString(d.Val)

	if posmap != nil {
		posmap[d] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// NewCall returns a new call expression from this expressions.
func (d *Distinct) NewCall() *Call {
	return &Call{
		Name: "distinct",
		Args: []Expr{
			&VarRef{Val: d.Val},
		},
		depth: 3,
	}
}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val float64
}

func (l *NumberLiteral) Depth() int { return 1 }

func (l *NumberLiteral) UpdateDepthForTests() int {
	if l != nil {
		return 1
	}
	return 0
}

func (l *NumberLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *NumberLiteral) String() string { return l.RenderBytes(&bytes.Buffer{}, nil).String() }

func (l *NumberLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()
	if l.Val > math.MaxInt {
		_, _ = buf.WriteString(strconv.FormatFloat(l.Val, 'f', 1, 64))
	} else {
		_, _ = buf.WriteString(strconv.FormatFloat(l.Val, 'f', -1, 64))
	}
	if posmap != nil {
		posmap[l] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// IntegerLiteral represents an integer literal.
type IntegerLiteral struct {
	Val int64
}

func (l *IntegerLiteral) Depth() int { return 1 }

func (l *IntegerLiteral) UpdateDepthForTests() int {
	if l != nil {
		return 1
	}
	return 0
}

func (l *IntegerLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *IntegerLiteral) String() string { return l.RenderBytes(&bytes.Buffer{}, nil).String() }

func (l *IntegerLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = fmt.Fprintf(buf, "%d", l.Val)

	if posmap != nil {
		posmap[l] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// UnsignedLiteral represents an unsigned literal. The parser will only use an unsigned literal if the parsed
// integer is greater than math.MaxInt64.
type UnsignedLiteral struct {
	Val uint64
}

func (l *UnsignedLiteral) Depth() int { return 1 }

func (l *UnsignedLiteral) UpdateDepthForTests() int {
	if l != nil {
		return 1
	}
	return 0
}

func (l *UnsignedLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *UnsignedLiteral) String() string { return l.RenderBytes(&bytes.Buffer{}, nil).String() }

func (l *UnsignedLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString(strconv.FormatUint(l.Val, 10))

	if posmap != nil {
		posmap[l] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

func (l *BooleanLiteral) Depth() int { return 1 }

func (l *BooleanLiteral) UpdateDepthForTests() int {
	if l != nil {
		return 1
	}
	return 0
}

func (l *BooleanLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string { return l.RenderBytes(&bytes.Buffer{}, nil).String() }

func (l *BooleanLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if l.Val {
		_, _ = buf.WriteString("true")
	} else {
		_, _ = buf.WriteString("false")
	}

	if posmap != nil {
		posmap[l] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// isTrueLiteral returns true if the expression is a literal "true" value.
func isTrueLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == true
	}
	return false
}

// isFalseLiteral returns true if the expression is a literal "false" value.
func isFalseLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == false
	}
	return false
}

// ListLiteral represents a list of tag key literals.
type ListLiteral struct {
	Vals []string
}

func (s *ListLiteral) Depth() int {
	if s != nil {
		return 1 + len(s.Vals)
	}
	return 0
}

func (s *ListLiteral) UpdateDepthForTests() int {
	if s != nil {
		return 1 + len(s.Vals)
	}
	return 0
}

func (s *ListLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (s *ListLiteral) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ListLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("(")
	for idx, tagKey := range s.Vals {
		if idx != 0 {
			_, _ = buf.WriteString(", ")
		}
		_, _ = buf.WriteString(QuoteIdent(tagKey))
	}
	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// SetLiteral represents a list of tag value literals.
type SetLiteral struct {
	Vals map[interface{}]bool
}

func (s *SetLiteral) Depth() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *SetLiteral) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *SetLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (s *SetLiteral) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *SetLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("(")
	isFirst := true
	for tagValue, _ := range s.Vals {
		if !isFirst {
			_, _ = buf.WriteString(",")
		}
		switch v := tagValue.(type) {
		case float64:
			if v > math.MaxInt {
				_, _ = buf.WriteString(strconv.FormatFloat(v, 'f', 1, 64))
			} else {
				_, _ = buf.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
			}
		default:
			_, _ = buf.WriteString(QuoteString(fmt.Sprintf("%v", tagValue)))
		}
		isFirst = false
	}
	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

func (l *StringLiteral) Depth() int { return 1 }

func (l *StringLiteral) UpdateDepthForTests() int {
	if l != nil {
		return 1
	}
	return 0
}

func (l *StringLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string { return l.RenderBytes(&bytes.Buffer{}, nil).String() }

func (l *StringLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString(QuoteString(l.Val))

	if posmap != nil {
		posmap[l] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// IsTimeLiteral returns if this string can be interpreted as a time literal.
func (l *StringLiteral) IsTimeLiteral() bool {
	return isDateTimeString(l.Val) || isDateString(l.Val)
}

// ToTimeLiteral returns a time literal if this string can be converted to a time literal.
func (l *StringLiteral) ToTimeLiteral(loc *time.Location) (*TimeLiteral, error) {
	if loc == nil {
		loc = time.UTC
	}

	if isDateTimeString(l.Val) {
		t, err := time.ParseInLocation(DateTimeFormat, l.Val, loc)
		if err != nil {
			// try to parse it as an RFCNano time
			t, err = time.ParseInLocation(time.RFC3339Nano, l.Val, loc)
			if err != nil {
				return nil, ErrInvalidTime
			}
		}
		return &TimeLiteral{Val: t}, nil
	} else if isDateString(l.Val) {
		t, err := time.ParseInLocation(DateFormat, l.Val, loc)
		if err != nil {
			return nil, ErrInvalidTime
		}
		return &TimeLiteral{Val: t}, nil
	}
	return nil, ErrInvalidTime
}

// TimeLiteral represents a point-in-time literal.
type TimeLiteral struct {
	Val time.Time
}

func (l *TimeLiteral) Depth() int { return 1 }

func (l *TimeLiteral) UpdateDepthForTests() int {
	if l != nil {
		return 1
	}
	return 0
}

func (l *TimeLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *TimeLiteral) String() string { return l.RenderBytes(&bytes.Buffer{}, nil).String() }

func (l *TimeLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString(`'`)
	_, _ = buf.WriteString(l.Val.UTC().Format(time.RFC3339Nano))
	_, _ = buf.WriteString(`'`)

	if posmap != nil {
		posmap[l] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// DurationLiteral represents a duration literal.
type DurationLiteral struct {
	Val time.Duration
}

func (l *DurationLiteral) Depth() int { return 1 }

func (l *DurationLiteral) UpdateDepthForTests() int {
	if l != nil {
		return 1
	}
	return 0
}

func (l *DurationLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *DurationLiteral) String() string { return l.RenderBytes(&bytes.Buffer{}, nil).String() }

func (l *DurationLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString(FormatDuration(l.Val))

	if posmap != nil {
		posmap[l] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// NilLiteral represents a nil literal.
// This is not available to the query language itself. It's only used internally.
type NilLiteral struct{}

func (l *NilLiteral) Depth() int { return 1 }

func (l *NilLiteral) UpdateDepthForTests() int {
	if l != nil {
		return 1
	}
	return 0
}

func (l *NilLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *NilLiteral) String() string { return l.RenderBytes(&bytes.Buffer{}, nil).String() }

func (l *NilLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString(`nil`)

	if posmap != nil {
		posmap[l] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// BoundParameter represents a bound parameter literal.
// This is not available to the query language itself, but can be used when
// constructing a query string from an AST.
type BoundParameter struct {
	Name string
}

func (bp *BoundParameter) Depth() int { return 1 }

func (bp *BoundParameter) UpdateDepthForTests() int {
	if bp != nil {
		return 1
	}
	return 0
}

func (bp *BoundParameter) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the bound parameter.
func (bp *BoundParameter) String() string { return bp.RenderBytes(&bytes.Buffer{}, nil).String() }

func (bp *BoundParameter) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("$")
	_, _ = buf.WriteString(QuoteIdent(bp.Name))

	if posmap != nil {
		posmap[bp] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
	// If a comparison operator, return 0/1 rather than filtering.
	ReturnBool bool
	depth      int
}

func (e *BinaryExpr) Depth() int {
	if e != nil {
		return e.depth
	}
	return 0
}

func (e *BinaryExpr) UpdateDepthForTests() int {
	if e != nil {
		e.depth = 1 + max(
			CallUpdateDepthForTests(e.LHS),
			CallUpdateDepthForTests(e.RHS),
		)
		return e.depth
	}
	return 0
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string { return e.RenderBytes(&bytes.Buffer{}, nil).String() }

func (e *BinaryExpr) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_ = e.LHS.RenderBytes(buf, posmap)
	_, _ = fmt.Fprintf(buf, " %s ", e.Op.String())
	_ = e.RHS.RenderBytes(buf, posmap)

	if posmap != nil {
		posmap[e] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (e *BinaryExpr) RewriteNameSpace(alias, mst string) {
	e.LHS.RewriteNameSpace(alias, mst)
	e.RHS.RewriteNameSpace(alias, mst)
}

// BinaryExprName returns the name of a binary expression by concatenating
// the variables in the binary expression with underscores.
func BinaryExprName(expr *BinaryExpr) string {
	v := binaryExprNameVisitor{}
	Walk(&v, expr)
	return strings.Join(v.names, "_")
}

type binaryExprNameVisitor struct {
	names []string
}

func (v *binaryExprNameVisitor) Visit(n Node) Visitor {
	switch n := n.(type) {
	case *VarRef:
		v.names = append(v.names, n.Val)
	case *Call:
		v.names = append(v.names, n.Name)
		return nil
	}
	return v
}

type MatchExpr struct {
	Field Expr
	Value Expr
	Op    int
}

func (m *MatchExpr) String() string { return m.RenderBytes(&bytes.Buffer{}, nil).String() }

func (m *MatchExpr) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	switch m.Op {
	case MATCH:
		_ = m.Field.RenderBytes(buf, posmap)
		_, _ = buf.WriteString("match")
		_ = m.Value.RenderBytes(buf, posmap)
	case MATCHPHRASE:
		_ = m.Field.RenderBytes(buf, posmap)
		_, _ = buf.WriteString("match_phrase")
		_ = m.Value.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[m] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (m *MatchExpr) RewriteNameSpace(alias, mst string) {
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr  Expr
	depth int
}

func (e *ParenExpr) Depth() int {
	if e != nil {
		return e.depth
	}
	return 0
}

func (e *ParenExpr) UpdateDepthForTests() int {
	if e != nil {
		e.depth = 1 + CallUpdateDepthForTests(e.Expr)
		return e.depth
	}
	return 0
}

func (e *ParenExpr) RewriteNameSpace(alias, mst string) {
	e.Expr.RewriteNameSpace(alias, mst)
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string { return e.RenderBytes(&bytes.Buffer{}, nil).String() }

func (e *ParenExpr) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("(")
	_ = e.Expr.RenderBytes(buf, posmap)
	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[e] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RegexLiteral represents a regular expression.
type RegexLiteral struct {
	Val *regexp.Regexp
}

func (r *RegexLiteral) Depth() int { return 1 }

func (r *RegexLiteral) UpdateDepthForTests() int {
	if r != nil {
		return 1
	}
	return 0
}

func (r *RegexLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (r *RegexLiteral) String() string { return r.RenderBytes(&bytes.Buffer{}, nil).String() }

func (r *RegexLiteral) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if r.Val != nil {
		fmt.Fprintf(buf, "/%s/", strings.Replace(r.Val.String(), `/`, `\/`, -1))
	}

	if posmap != nil {
		posmap[r] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// CloneRegexLiteral returns a clone of the RegexLiteral.
func CloneRegexLiteral(r *RegexLiteral) *RegexLiteral {
	if r == nil {
		return nil
	}

	clone := &RegexLiteral{}
	if r.Val != nil {
		clone.Val = regexp.MustCompile(r.Val.String())
	}

	return clone
}

// Wildcard represents a wild card expression.
type Wildcard struct {
	Type Token
}

func (w *Wildcard) Depth() int { return 1 }

func (w *Wildcard) UpdateDepthForTests() int {
	if w != nil {
		return 1
	}
	return 0
}

func (e *Wildcard) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the wildcard.
func (e *Wildcard) String() string { return e.RenderBytes(&bytes.Buffer{}, nil).String() }

func (e *Wildcard) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString("*")
	switch e.Type {
	case FIELD:
		buf.WriteString("::field")
	case TAG:
		buf.WriteString("::tag")
	}

	if posmap != nil {
		posmap[e] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func CloneArrExpr(exprarr []Expr) []Expr {
	arr := make([]Expr, len(exprarr))
	for i, expr := range exprarr {
		arr[i] = CloneExpr(expr)
	}
	return arr
}

// CloneExpr returns a deep copy of the expression.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *BinaryExpr:
		return &BinaryExpr{Op: expr.Op, LHS: CloneExpr(expr.LHS), RHS: CloneExpr(expr.RHS), ReturnBool: expr.ReturnBool, depth: expr.depth}
	case *BooleanLiteral:
		return &BooleanLiteral{Val: expr.Val}
	case *Call:
		return &Call{Name: expr.Name, Args: CloneArrExpr(expr.Args), depth: expr.depth}
	case *Distinct:
		return &Distinct{Val: expr.Val}
	case *DurationLiteral:
		return &DurationLiteral{Val: expr.Val}
	case *IntegerLiteral:
		return &IntegerLiteral{Val: expr.Val}
	case *UnsignedLiteral:
		return &UnsignedLiteral{Val: expr.Val}
	case *NumberLiteral:
		return &NumberLiteral{Val: expr.Val}
	case *ParenExpr:
		return &ParenExpr{Expr: CloneExpr(expr.Expr), depth: expr.depth}
	case *RegexLiteral:
		return &RegexLiteral{Val: expr.Val}
	case *StringLiteral:
		return &StringLiteral{Val: expr.Val}
	case *TimeLiteral:
		return &TimeLiteral{Val: expr.Val}
	case *VarRef:
		return &VarRef{Val: expr.Val, Type: expr.Type}
	case *Wildcard:
		return &Wildcard{Type: expr.Type}
	case *InCondition:
		return expr // fix rewrite field type err of in.stmt
	case *CaseWhenExpr:
		return &CaseWhenExpr{
			Conditions: CloneArrExpr(expr.Conditions),
			Assigners:  CloneArrExpr(expr.Assigners),
			Args:       CloneArrExpr(expr.Args),
			depth:      expr.depth,
		}
	case *SetLiteral:
		valsCopy := make(map[interface{}]bool, len(expr.Vals))
		for k, v := range expr.Vals {
			valsCopy[k] = v
		}
		return &SetLiteral{Vals: valsCopy}
	}
	panic("unreachable")
}

func CloneVarRef(expr *VarRef) *VarRef {
	return &VarRef{Val: expr.Val, Type: expr.Type}
}

// HasTimeExpr returns true if the expression has a time term.
func HasTimeExpr(expr Expr) bool {
	switch n := expr.(type) {
	case *BinaryExpr:
		if n.Op == AND || n.Op == OR {
			return HasTimeExpr(n.LHS) || HasTimeExpr(n.RHS)
		}
		if ref, ok := n.LHS.(*VarRef); ok && strings.ToLower(ref.Val) == "time" {
			return true
		}
		return false
	case *ParenExpr:
		// walk down the tree
		return HasTimeExpr(n.Expr)
	default:
		return false
	}
}

type TimeRangeValue struct {
	Min, Max int64
}

// SplitCondByTime used to determine whether only time is used for filtering.
func OnlyHaveTimeCond(expr Expr) bool {
	if expr == nil {
		return true
	}
	cond, _, _ := SplitCondByTime(CloneExpr(expr))
	return cond == nil
}

// SplitCondByTime used to split conditions by time.
func SplitCondByTime(condition Expr) (Expr, TimeRangeValue, error) {
	valuer := NowValuer{Now: time.Now(), Location: nil}
	con, tr, err := ConditionExpr(condition, &valuer)
	trValue := TimeRangeValue{Min: MinTime, Max: MaxTime}
	if !tr.Min.IsZero() {
		trValue.Min = tr.Min.UnixNano()
	}
	if !tr.Max.IsZero() {
		trValue.Max = tr.Max.UnixNano()
	}
	return con, trValue, err
}

// Visitor can be called by Walk to traverse an AST hierarchy.
// The Visit() function is called once per node.
type Visitor interface {
	Visit(Node) Visitor
}

// Walk traverses a node hierarchy in depth-first order.
func Walk(v Visitor, node Node) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *Call:
		for _, expr := range n.Args {
			Walk(v, expr)
		}

	case *CreateContinuousQueryStatement:
		Walk(v, n.Source)

	case *Dimension:
		Walk(v, n.Expr)

	case Dimensions:
		for _, c := range n {
			Walk(v, c)
		}

	case *DeleteSeriesStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case *DropSeriesStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case *ExplainStatement:
		Walk(v, n.Statement)

	case *Field:
		Walk(v, n.Expr)

	case Fields:
		for _, c := range n {
			Walk(v, c)
		}

	case *ParenExpr:
		Walk(v, n.Expr)

	case *Query:
		Walk(v, n.Statements)

	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Target)
		Walk(v, n.Dimensions)
		Walk(v, n.Sources)
		Walk(v, n.Condition)
		Walk(v, n.SortFields)

	case *ShowFieldKeyCardinalityStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case *ShowSeriesStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case *ShowSeriesCardinalityStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case *ShowMeasurementCardinalityStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case *ShowTagKeyCardinalityStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case *ShowTagKeysStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)
		Walk(v, n.SortFields)

	case *ShowTagValuesCardinalityStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case *ShowTagValuesStatement:
		Walk(v, n.Sources)
		Walk(v, n.Condition)
		Walk(v, n.SortFields)

	case *ShowFieldKeysStatement:
		Walk(v, n.Sources)
		Walk(v, n.SortFields)

	case SortFields:
		for _, sf := range n {
			Walk(v, sf)
		}

	case Sources:
		for _, s := range n {
			Walk(v, s)
		}

	case *Join:
		Walk(v, n.LSrc)
		Walk(v, n.RSrc)
		Walk(v, n.Condition)

	case *Union:
		Walk(v, n.LSrc)
		Walk(v, n.RSrc)

	case *SubQuery:
		Walk(v, n.Statement)

	case Statements:
		for _, s := range n {
			Walk(v, s)
		}

	case *Target:
		if n != nil {
			Walk(v, n.Measurement)
		}
	case *BinOp:
		Walk(v, n.LSrc)
		Walk(v, n.RSrc)
	case *InCondition:
		Walk(v, n.Column)

	case *WithSelectStatement:
		for _, s := range n.CTEs {
			Walk(v, s)
		}
		Walk(v, n.Query)

	case *CTE:
		if n.Query != nil {
			Walk(v, n.Query)
		}
		if n.GraphQuery != nil {
			Walk(v, n.GraphQuery)
		}

	case *CaseWhenExpr:
		for _, cond := range n.Conditions {
			Walk(v, cond)
		}
		for _, expr := range n.Assigners {
			Walk(v, expr)
		}
		for _, expr := range n.Args {
			Walk(v, expr)
		}
	}
}

// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node)

func (fn walkFuncVisitor) Visit(n Node) Visitor { fn(n); return fn }

// Rewriter can be called by Rewrite to replace nodes in the AST hierarchy.
// The Rewrite() function is called once per node.
type Rewriter interface {
	Rewrite(Node) Node
}

// Rewrite recursively invokes the rewriter to replace each node.
// Nodes are traversed depth-first and rewritten from leaf to root.
func Rewrite(r Rewriter, node Node) Node {
	switch n := node.(type) {
	case *Query:
		n.Statements = Rewrite(r, n.Statements).(Statements)

	case Statements:
		for i, s := range n {
			n[i] = Rewrite(r, s).(Statement)
		}

	case *SelectStatement:
		n.Fields = Rewrite(r, n.Fields).(Fields)
		n.Dimensions = Rewrite(r, n.Dimensions).(Dimensions)
		n.Sources = Rewrite(r, n.Sources).(Sources)

		// Rewrite may return nil. Nil does not satisfy the Expr
		// interface. We only assert the rewritten result to be an
		// Expr if it is not nil:
		if cond := Rewrite(r, n.Condition); cond != nil {
			n.Condition = cond.(Expr)
		} else {
			n.Condition = nil
		}

	case *SubQuery:
		n.Statement = Rewrite(r, n.Statement).(*SelectStatement)

	case Fields:
		for i, f := range n {
			n[i] = Rewrite(r, f).(*Field)
		}

	case *Field:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case Dimensions:
		for i, d := range n {
			n[i] = Rewrite(r, d).(*Dimension)
		}

	case *Dimension:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *BinaryExpr:
		n.LHS = Rewrite(r, n.LHS).(Expr)
		n.RHS = Rewrite(r, n.RHS).(Expr)

	case *ParenExpr:
		n.Expr = Rewrite(r, n.Expr).(Expr)

	case *Call:
		for i, expr := range n.Args {
			n.Args[i] = Rewrite(r, expr).(Expr)
		}
	}

	return r.Rewrite(node)
}

// RewriteFunc rewrites a node hierarchy.
func RewriteFunc(node Node, fn func(Node) Node) Node {
	return Rewrite(rewriterFunc(fn), node)
}

type rewriterFunc func(Node) Node

func (fn rewriterFunc) Rewrite(n Node) Node { return fn(n) }

// RewriteExpr recursively invokes the function to replace each expr.
// Nodes are traversed depth-first and rewritten from leaf to root.
func RewriteExpr(expr Expr, fn func(Expr) Expr) Expr {
	switch e := expr.(type) {
	case *BinaryExpr:
		e.LHS = RewriteExpr(e.LHS, fn)
		e.RHS = RewriteExpr(e.RHS, fn)
		if e.LHS != nil && e.RHS == nil {
			expr = e.LHS
		} else if e.RHS != nil && e.LHS == nil {
			expr = e.RHS
		} else if e.LHS == nil && e.RHS == nil {
			return nil
		}

	case *ParenExpr:
		e.Expr = RewriteExpr(e.Expr, fn)
		if e.Expr == nil {
			return nil
		}

	case *Call:
		for i, expr := range e.Args {
			e.Args[i] = RewriteExpr(expr, fn)
		}
	}

	return fn(expr)
}

// Eval evaluates expr against a map.
func Eval(expr Expr, m map[string]interface{}) interface{} {
	eval := ValuerEval{Valuer: MapValuer(m)}
	return eval.Eval(expr)
}

// MapValuer is a valuer that substitutes values for the mapped interface.
type MapValuer map[string]interface{}

// Value returns the value for a key in the MapValuer.
func (m MapValuer) Value(key string) (interface{}, bool) {
	v, ok := m[key]
	return v, ok
}

func (m MapValuer) SetValuer(v Valuer, index int) {

}

// ValuerEval will evaluate an expression using the Valuer.
type ValuerEval struct {
	Valuer Valuer

	// IntegerFloatDivision will set the eval system to treat
	// a division between two integers as a floating point division.
	IntegerFloatDivision bool
}

// Eval evaluates an expression and returns a value.
func (v *ValuerEval) Eval(expr Expr) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *BinaryExpr:
		return v.wrapEvalBinaryExpr(expr)
	case *BooleanLiteral:
		return expr.Val
	case *IntegerLiteral:
		return expr.Val
	case *NumberLiteral:
		return expr.Val
	case *UnsignedLiteral:
		return expr.Val
	case *ParenExpr:
		return v.Eval(expr.Expr)
	case *RegexLiteral:
		return expr.Val
	case *StringLiteral:
		return expr.Val
	case *Call:
		if valuer, ok := v.Valuer.(CallValuer); ok {
			var args []interface{}
			if len(expr.Args) > 0 {
				args = make([]interface{}, len(expr.Args))
				for i := range expr.Args {
					args[i] = v.Eval(expr.Args[i])
				}
			}
			val, _ := valuer.Call(expr.Name, args)
			return val
		}
		return nil
	case *VarRef:
		val, _ := v.Valuer.Value(expr.Val)
		return val
	case *SetLiteral:
		return expr.Vals
	default:
		return nil
	}
}

// EvalBool evaluates expr and returns true if result is a boolean true.
// Otherwise returns false.
func (v *ValuerEval) EvalBool(expr Expr) bool {
	val, _ := v.Eval(expr).(bool)
	return val
}

func (v *ValuerEval) wrapEvalBinaryExpr(expr *BinaryExpr) interface{} {
	val := v.evalBinaryExpr(expr)
	if expr.ReturnBool {
		if val.(bool) {
			return float64(1)
		}
		return float64(0)
	}
	return val
}

func (v *ValuerEval) evalBinaryExpr(expr *BinaryExpr) interface{} {
	lhs := v.Eval(expr.LHS)
	rhs := v.Eval(expr.RHS)
	if lhs == nil && rhs != nil {
		// When the LHS is nil and the RHS is a boolean, implicitly cast the
		// nil to false.
		if _, ok := rhs.(bool); ok {
			lhs = false
		}
	} else if lhs != nil && rhs == nil {
		// Implicit cast of the RHS nil to false when the LHS is a boolean.
		if _, ok := lhs.(bool); ok {
			rhs = false
		}
	}

	// Evaluate if both sides are simple typ.
	switch lhs := lhs.(type) {
	case bool:
		rhs, ok := rhs.(bool)
		switch expr.Op {
		case AND:
			return ok && (lhs && rhs)
		case OR:
			return ok && (lhs || rhs)
		case BITWISE_AND:
			return ok && (lhs && rhs)
		case BITWISE_OR:
			return ok && (lhs || rhs)
		case BITWISE_XOR:
			return ok && (lhs != rhs)
		case EQ:
			return ok && (lhs == rhs)
		case NEQ:
			return ok && (lhs != rhs)
		}
	case float64:
		rhsm := rhs
		// Try the rhs as a float64, int64, or uint64
		rhsf, ok := rhs.(float64)
		if !ok {
			switch val := rhs.(type) {
			case int64:
				rhsf, ok = float64(val), true
			case uint64:
				rhsf, ok = float64(val), true
			}
		}

		rhs := rhsf
		switch expr.Op {
		case EQ:
			return ok && (lhs == rhs)
		case NEQ:
			return ok && (lhs != rhs)
		case LT:
			return ok && (lhs < rhs)
		case LTE:
			return ok && (lhs <= rhs)
		case GT:
			return ok && (lhs > rhs)
		case GTE:
			return ok && (lhs >= rhs)
		case ADD:
			if !ok {
				return nil
			}
			return lhs + rhs
		case SUB:
			if !ok {
				return nil
			}
			return lhs - rhs
		case MUL:
			if !ok {
				return nil
			}
			return lhs * rhs
		case DIV:
			if !ok {
				return nil
			}
			return lhs / rhs
		case MOD:
			if !ok {
				return nil
			}
			return math.Mod(lhs, rhs)
		case ATAN2_OP:
			if !ok {
				return nil
			}
			return math.Atan2(lhs, rhs)
		case IN:
			rhs, ok := rhsm.(map[interface{}]bool)
			if !ok {
				return false
			}
			_, ok = rhs[lhs]
			return ok
		case NOTIN:
			rhs, ok := rhsm.(map[interface{}]bool)
			if !ok {
				return false
			}
			_, ok = rhs[lhs]
			return !ok
		}
	case int64:
		// Try as a float64 to see if a float cast is required.
		switch rhs := rhs.(type) {
		case float64:
			lhs := float64(lhs)
			switch expr.Op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				return lhs / rhs
			case MOD:
				return math.Mod(lhs, rhs)
			}
		case int64:
			switch expr.Op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if v.IntegerFloatDivision {
					return float64(lhs) / float64(rhs)
				}

				if rhs == 0 {
					return int64(0)
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return int64(0)
				}
				return lhs % rhs
			case BITWISE_AND:
				return lhs & rhs
			case BITWISE_OR:
				return lhs | rhs
			case BITWISE_XOR:
				return lhs ^ rhs
			}
		case uint64:
			switch expr.Op {
			case EQ:
				return uint64(lhs) == rhs
			case NEQ:
				return uint64(lhs) != rhs
			case LT:
				if lhs < 0 {
					return true
				}
				return uint64(lhs) < rhs
			case LTE:
				if lhs < 0 {
					return true
				}
				return uint64(lhs) <= rhs
			case GT:
				if lhs < 0 {
					return false
				}
				return uint64(lhs) > rhs
			case GTE:
				if lhs < 0 {
					return false
				}
				return uint64(lhs) >= rhs
			case ADD:
				return uint64(lhs) + rhs
			case SUB:
				return uint64(lhs) - rhs
			case MUL:
				return uint64(lhs) * rhs
			case DIV:
				if rhs == 0 {
					return uint64(0)
				}
				return uint64(lhs) / rhs
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return uint64(lhs) % rhs
			case BITWISE_AND:
				return uint64(lhs) & rhs
			case BITWISE_OR:
				return uint64(lhs) | rhs
			case BITWISE_XOR:
				return uint64(lhs) ^ rhs
			}
		case map[interface{}]bool:
			lhs := float64(lhs)
			switch expr.Op {
			case IN:
				_, ok := rhs[lhs]
				return ok
			case NOTIN:
				_, ok := rhs[lhs]
				return !ok
			}
		}
	case uint64:
		// Try as a float64 to see if a float cast is required.
		switch rhs := rhs.(type) {
		case float64:
			lhs := float64(lhs)
			switch expr.Op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				return lhs / rhs
			case MOD:
				return math.Mod(lhs, rhs)
			}
		case int64:
			switch expr.Op {
			case EQ:
				return lhs == uint64(rhs)
			case NEQ:
				return lhs != uint64(rhs)
			case LT:
				if rhs < 0 {
					return false
				}
				return lhs < uint64(rhs)
			case LTE:
				if rhs < 0 {
					return false
				}
				return lhs <= uint64(rhs)
			case GT:
				if rhs < 0 {
					return true
				}
				return lhs > uint64(rhs)
			case GTE:
				if rhs < 0 {
					return true
				}
				return lhs >= uint64(rhs)
			case ADD:
				return lhs + uint64(rhs)
			case SUB:
				return lhs - uint64(rhs)
			case MUL:
				return lhs * uint64(rhs)
			case DIV:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs / uint64(rhs)
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs % uint64(rhs)
			case BITWISE_AND:
				return lhs & uint64(rhs)
			case BITWISE_OR:
				return lhs | uint64(rhs)
			case BITWISE_XOR:
				return lhs ^ uint64(rhs)
			}
		case uint64:
			switch expr.Op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs / rhs
			case MOD:
				if rhs == 0 {
					return uint64(0)
				}
				return lhs % rhs
			case BITWISE_AND:
				return lhs & rhs
			case BITWISE_OR:
				return lhs | rhs
			case BITWISE_XOR:
				return lhs ^ rhs
			}
		case map[interface{}]bool:
			lhs := float64(lhs)
			switch expr.Op {
			case IN:
				_, ok := rhs[lhs]
				return ok
			case NOTIN:
				_, ok := rhs[lhs]
				return !ok
			}
		}
	case string:
		switch expr.Op {
		case EQ:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs == rhs
		case NEQ:
			rhs, ok := rhs.(string)
			if !ok {
				return false
			}
			return lhs != rhs
		case EQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return false
			}
			return rhs.MatchString(lhs)
		case NEQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			if !ok {
				return false
			}
			return !rhs.MatchString(lhs)
		case IN:
			rhs, ok := rhs.(map[interface{}]bool)
			if !ok {
				return false
			}
			_, ok = rhs[lhs]
			return ok
		case NOTIN:
			rhs, ok := rhs.(map[interface{}]bool)
			if !ok {
				return false
			}
			_, ok = rhs[lhs]
			return !ok
		}
	}

	// The typ were not comparable. If our operation was an equality operation,
	// return false instead of true.
	switch expr.Op {
	case EQ, NEQ, LT, LTE, GT, GTE:
		return false
	}
	return nil
}

// EvalBool evaluates expr and returns true if result is a boolean true.
// Otherwise returns false.
func EvalBool(expr Expr, m map[string]interface{}) bool {
	v, _ := Eval(expr, m).(bool)
	return v
}

// TypeMapper maps a data type to the measurement and field.
type TypeMapper interface {
	MapType(measurement *Measurement, field string) DataType
	MapTypeBatch(measurement *Measurement, field map[string]*FieldNameSpace, schema *Schema) error
}

// CallTypeMapper maps a data type to the function call.
type CallTypeMapper interface {
	TypeMapper

	CallType(name string, args []DataType) (DataType, error)
}

type nilTypeMapper struct{}

func (nilTypeMapper) MapType(*Measurement, string) DataType { return Unknown }

func (nilTypeMapper) MapTypeBatch(*Measurement, map[string]*FieldNameSpace, *Schema) error {
	return nil
}

type multiTypeMapper []TypeMapper

// MultiTypeMapper combines multiple TypeMappers into a single one.
// The MultiTypeMapper will return the first type that is not Unknown.
// It will not iterate through all of them to find the highest priority one.
func MultiTypeMapper(mappers ...TypeMapper) TypeMapper {
	return multiTypeMapper(mappers)
}

func (a multiTypeMapper) MapType(measurement *Measurement, field string) DataType {
	for _, m := range a {
		if typ := m.MapType(measurement, field); typ != Unknown {
			return typ
		}
	}
	return Unknown
}

func (a multiTypeMapper) MapTypeBatch(m *Measurement, fields map[string]*FieldNameSpace, schema *Schema) error {
	for field, v := range fields {
		if typ := a.MapType(m, field); v.DataType.LessThan(typ) {
			fields[field].DataType = typ
		}
	}
	return nil
}

func (a multiTypeMapper) CallType(name string, args []DataType) (DataType, error) {
	for _, m := range a {
		call, ok := m.(CallTypeMapper)
		if ok {
			typ, err := call.CallType(name, args)
			if err != nil {
				return Unknown, err
			} else if typ != Unknown {
				return typ, nil
			}
		}
	}
	return Unknown, nil
}

// TypeValuerEval evaluates an expression to determine its output type.
type TypeValuerEval struct {
	TypeMapper TypeMapper
	Sources    Sources
}

// EvalType returns the type for an expression. If the expression cannot
// be evaluated for some reason, like incompatible typ, it is returned
// as a TypeError in the error. If the error is non-fatal so we can continue
// even though an error happened, true will be returned.
// This function assumes that the expression has already been reduced.
func (v *TypeValuerEval) EvalType(expr Expr, batchEn bool) (DataType, error) {
	switch expr := expr.(type) {
	case *VarRef:
		return v.evalVarRefExprType(expr, batchEn)
	case *Call:
		return v.evalCallExprType(expr, batchEn)
	case *BinaryExpr:
		return v.evalBinaryExprType(expr, batchEn)
	case *ParenExpr:
		return v.EvalType(expr.Expr, batchEn)
	case *NumberLiteral:
		return Float, nil
	case *IntegerLiteral:
		return Integer, nil
	case *UnsignedLiteral:
		return Unsigned, nil
	case *StringLiteral:
		return String, nil
	case *BooleanLiteral:
		return Boolean, nil
	}
	return Unknown, nil
}

func cloneVarTypeMap(src map[string]DataType) (other map[string]DataType) {
	other = make(map[string]DataType, len(src))
	for k, _ := range src {
		other[k] = Unknown
	}
	return
}

func cloneVarNameSpace(src map[string]*FieldNameSpace) (other map[string]*FieldNameSpace) {
	other = make(map[string]*FieldNameSpace, len(src))
	for k, _ := range src {
		other[k] = &FieldNameSpace{DataType: Unknown}
	}
	return
}

func (v *TypeValuerEval) EvalTypeBatch(exprs map[string]Expr, schema *Schema, batchEn bool) (err error) {
	fields := make(map[string]*FieldNameSpace, len(exprs))
	for _, expr := range exprs {
		ref, _ := expr.(*VarRef)
		if ref == nil {
			continue
		}
		ty, err := v.EvalType(expr, batchEn)
		if err != nil {
			if err == ErrNeedBatchMap {
				fields[ref.Val] = &FieldNameSpace{DataType: Unknown}
				continue
			}
			return err
		}

		ref.Type = ty
	}

	if len(fields) == 0 {
		return nil
	}

	errs := make(chan error, len(v.Sources)+1)
	batchMapType := func(mm *Measurement, ft map[string]*FieldNameSpace, wg *sync.WaitGroup) {
		er := v.TypeMapper.MapTypeBatch(mm, ft, schema)
		errs <- er
		wg.Done()
	}

	fTypes := make([]map[string]*FieldNameSpace, 0, len(v.Sources))
	var wg sync.WaitGroup
	for i, src := range v.Sources {
		switch src := src.(type) {
		case *Measurement:
			wg.Add(1)
			if i == 0 {
				fTypes = append(fTypes, fields)
				go batchMapType(src, fields, &wg)
			} else {
				fts := cloneVarNameSpace(fields)
				fTypes = append(fTypes, fts)
				go batchMapType(src, fts, &wg)
			}
		case *SubQuery:
			for k, _ := range fields {
				_, e := src.Statement.FieldExprByName(k, src.Alias)
				if e != nil {
					valuer := TypeValuerEval{
						TypeMapper: v.TypeMapper,
						Sources:    src.Statement.Sources,
					}

					if t, err := valuer.EvalType(e, false); err != nil {
						return err
					} else {
						fields[k].DataType = t
					}
				}
			}
			fTypes = append(fTypes, fields)
		}
	}

	wg.Wait()
	close(errs)
	for er := range errs {
		if err == nil {
			err = er
		}

		if er == ErrUnsupportBatchMap {
			return er
		} else if err != nil {
			err = er
		}
	}

	if err != nil {
		return err
	}

	for _, fts := range fTypes {
		for k, v := range fts {
			expr, _ := exprs[k]
			if expr != nil {
				ref, _ := expr.(*VarRef)
				if ref != nil && ref.Type.LessThan(v.DataType) {
					ref.Type = v.DataType
					if len(v.RealName) != 0 {
						ref.Alias = ref.Val
						ref.Val = v.RealName
					}
				}
			}
		}
	}

	return err
}

func (v *TypeValuerEval) evalVarRefExprType(expr *VarRef, batchEn bool) (DataType, error) {
	// If this variable already has an assigned type, just use that.
	if expr.Type != Unknown && expr.Type != AnyField {
		return expr.Type, nil
	}

	var typ DataType
	var e error
	if v.TypeMapper != nil {
		for _, src := range v.Sources {
			switch src := src.(type) {
			case *CTE:
				if src.GraphQuery != nil {
					typ = Graph
					continue
				}
				typ, e = v.evalDataTypeForCTEAndSubquery(expr, batchEn, src, typ)
				if e != nil {
					return Unknown, e
				}
			case *Measurement:
				if batchEn {
					return Unknown, ErrNeedBatchMap
				}
				if t := v.TypeMapper.MapType(src, expr.Val); typ.LessThan(t) {
					typ = t
				}
			case *SubQuery:
				typ, e = v.evalDataTypeForCTEAndSubquery(expr, batchEn, src, typ)
				if e != nil {
					return Unknown, e
				}
			}
		}
	}
	return typ, nil
}

func (v *TypeValuerEval) evalDataTypeForCTEAndSubquery(expr *VarRef, batchEn bool, src Source, typ DataType) (DataType, error) {
	var statement *SelectStatement
	var alias string
	switch src := src.(type) {
	case *CTE:
		statement = src.Query
		alias = src.Alias
	case *SubQuery:
		statement = src.Statement
		alias = src.Alias
	default:
		return Unknown, ErrSourceOfCTE
	}
	_, e := statement.FieldExprByName(expr.Val, alias)
	if e != nil {
		valuer := TypeValuerEval{
			TypeMapper: v.TypeMapper,
			Sources:    statement.Sources,
		}
		if t, err := valuer.EvalType(e, batchEn); err != nil {
			return Unknown, err
		} else if typ.LessThan(t) {
			typ = t
		}
	}

	if typ == Unknown {
		for _, d := range statement.Dimensions {
			if d, ok := d.Expr.(*VarRef); ok && (expr.Val == d.Val || IsSameField(alias, d.Val, expr.Val)) {
				typ = Tag
				break
			}
		}
	}
	return typ, nil
}

func IsSameField(sourceAlias, inField, outField string) bool {
	return len(sourceAlias) > 0 && (sourceAlias+"."+inField) == outField
}

func (v *TypeValuerEval) evalCallExprType(expr *Call, batchCall bool) (DataType, error) {
	typmap, ok := v.TypeMapper.(CallTypeMapper)
	if !ok {
		return Unknown, nil
	}

	// Evaluate all of the data typ for the arguments.
	args := make([]DataType, len(expr.Args))
	for i, arg := range expr.Args {
		typ, err := v.EvalType(arg, batchCall)
		if err != nil {
			return Unknown, err
		}
		args[i] = typ
	}

	// Pass in the data typ for the call so it can be type checked and
	// the resulting type can be returned.
	return typmap.CallType(expr.Name, args)
}

func (v *TypeValuerEval) evalBinaryExprType(expr *BinaryExpr, batchCall bool) (DataType, error) {
	// Find the data type for both sides of the expression.
	lhs, err := v.EvalType(expr.LHS, batchCall)
	if err != nil {
		return Unknown, err
	}
	rhs, err := v.EvalType(expr.RHS, batchCall)
	if err != nil {
		return Unknown, err
	}

	// If one of the two is unsigned and the other is an integer, we cannot add
	// the two without an explicit cast unless the integer is a literal.
	if lhs == Unsigned && rhs == Integer {
		if isLiteral(expr.LHS) {
			return Unknown, &TypeError{
				Expr:    expr,
				Message: fmt.Sprintf("cannot use %s with an integer and unsigned literal", expr.Op),
			}
		} else if !isLiteral(expr.RHS) {
			return Unknown, &TypeError{
				Expr:    expr,
				Message: fmt.Sprintf("cannot use %s between an integer and unsigned, an explicit cast is required", expr.Op),
			}
		}
	} else if lhs == Integer && rhs == Unsigned {
		if isLiteral(expr.RHS) {
			return Unknown, &TypeError{
				Expr:    expr,
				Message: fmt.Sprintf("cannot use %s with an integer and unsigned literal", expr.Op),
			}
		} else if !isLiteral(expr.LHS) {
			return Unknown, &TypeError{
				Expr:    expr,
				Message: fmt.Sprintf("cannot use %s between an integer and unsigned, an explicit cast is required", expr.Op),
			}
		}
	}

	// If one of the two is unknown, then return the other as the type.
	if lhs == Unknown {
		return rhs, nil
	} else if rhs == Unknown {
		return lhs, nil
	}

	// Rather than re-implement the ValuerEval here, we create a dummy binary
	// expression with the zero values and inspect the resulting value back into
	// a data type to determine the output.
	e := BinaryExpr{
		LHS:   &VarRef{Val: "lhs"},
		RHS:   &VarRef{Val: "rhs"},
		Op:    expr.Op,
		depth: 3,
	}
	result := Eval(&e, map[string]interface{}{
		"lhs": lhs.Zero(),
		"rhs": rhs.Zero(),
	})

	typ := InspectDataType(result)
	if typ == Unknown {
		// If the type is unknown, then the two typ were not compatible.
		return Unknown, &TypeError{
			Expr:    expr,
			Message: fmt.Sprintf("incompatible typ: %s and %s", lhs, rhs),
		}
	}
	// If the operator is DIV, the result should be float.
	if expr.Op == DIV {
		return Float, nil
	}
	if expr.ReturnBool {
		return Float, nil
	}
	return typ, nil
}

// TypeError is an error when two typ are incompatible.
type TypeError struct {
	// Expr contains the expression that generated the type error.
	Expr Expr
	// Message contains the informational message about the type error.
	Message string
}

func (e *TypeError) Error() string {
	return fmt.Sprintf("type error: %s: %s", e.Expr, e.Message)
}

// EvalType evaluates the expression's type.
func EvalType(expr Expr, sources Sources, typmap TypeMapper) DataType {
	if typmap == nil {
		typmap = nilTypeMapper{}
	}

	valuer := TypeValuerEval{
		TypeMapper: typmap,
		Sources:    sources,
	}
	typ, _ := valuer.EvalType(expr, false)
	return typ
}

// EvalType evaluates the expression's type.
func EvalTypeBatch(exprs map[string]Expr, sources Sources, typmap TypeMapper, schema *Schema, batchEn bool) error {
	if typmap == nil {
		typmap = nilTypeMapper{}
	}

	valuer := TypeValuerEval{
		TypeMapper: typmap,
		Sources:    sources,
	}

	return valuer.EvalTypeBatch(exprs, schema, batchEn)
}

func FieldDimensions(sources Sources, m FieldMapper, schema *Schema) (fields map[string]DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]DataType)
	dimensions = make(map[string]struct{})
	schema.MinTime, schema.MaxTime = math.MaxInt64, math.MinInt64
	for _, src := range sources {
		switch src := src.(type) {
		case *CTE:
			if src.Query == nil {
				continue
			}
			for _, f := range src.Query.Fields {
				k := f.Name()
				typ := EvalType(f.Expr, src.Query.Sources, m)

				if fields[k].LessThan(typ) {
					fields[k] = typ
				}
			}

			for _, d := range src.Query.Dimensions {
				if expr, ok := d.Expr.(*VarRef); ok {
					dimensions[expr.Val] = struct{}{}
				}
			}
		case *TableFunction:
			fields, dimensions, err := FieldDimensions(Sources(src.TableFunctionSource), m, schema)
			if err != nil {
				return nil, nil, err
			}
			return fields, dimensions, nil
		case *Measurement:
			f, d, sc, err := m.FieldDimensions(src)
			if err != nil {
				return nil, nil, err
			}

			if f == nil || d == nil {
				continue
			}

			for k, typ := range f {
				if fields[k].LessThan(typ) {
					fields[k] = typ
				}
			}
			for k := range d {
				dimensions[k] = struct{}{}
			}
			schema.Files += sc.Files
			schema.FileSize += sc.FileSize
			schema.AddMinMaxTime(sc.MinTime, sc.MaxTime)
		case *SubQuery:
			for _, f := range src.Statement.Fields {
				k := f.Name()
				typ := EvalType(f.Expr, src.Statement.Sources, m)

				if fields[k].LessThan(typ) {
					fields[k] = typ
				}
			}

			for _, d := range src.Statement.Dimensions {
				if expr, ok := d.Expr.(*VarRef); ok {
					dimensions[expr.Val] = struct{}{}
				}
			}
		}
	}
	return
}

// Reduce evaluates expr using the available values in valuer.
// References that don't exist in valuer are ignored.
func Reduce(expr Expr, valuer Valuer) Expr {
	expr = reduce(expr, valuer)

	// Unwrap parens at top level.
	if expr, ok := expr.(*ParenExpr); ok {
		return expr.Expr
	}
	return expr
}

func reduce(expr Expr, valuer Valuer) Expr {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *BinaryExpr:
		return reduceBinaryExpr(expr, valuer)
	case *Call:
		return reduceCall(expr, valuer)
	case *ParenExpr:
		return reduceParenExpr(expr, valuer)
	case *VarRef:
		return reduceVarRef(expr, valuer)
	case *NilLiteral:
		return expr
	default:
		return expr
	}
}

func reduceBinaryExpr(expr *BinaryExpr, valuer Valuer) Expr {
	// Reduce both sides first.
	op := expr.Op
	lhs := reduce(expr.LHS, valuer)
	rhs := reduce(expr.RHS, valuer)

	loc := time.UTC
	if valuer, ok := valuer.(ZoneValuer); ok {
		if l := valuer.Zone(); l != nil {
			loc = l
		}
	}

	// Do not evaluate if one side is nil.
	if lhs == nil || rhs == nil {
		if lhs == expr.LHS && rhs == expr.RHS {
			return expr
		}
		depth := 0
		if lhs != nil && lhs.Depth() > depth {
			depth = lhs.Depth()
		}
		if rhs != nil && rhs.Depth() > depth {
			depth = rhs.Depth()
		}
		return &BinaryExpr{LHS: lhs, RHS: rhs, Op: expr.Op, ReturnBool: expr.ReturnBool, depth: 1 + depth}
	}

	// If we have a logical operator (AND, OR) and one side is a boolean literal
	// then we need to have special handling.
	if op == AND {
		if isFalseLiteral(lhs) || isFalseLiteral(rhs) {
			return &BooleanLiteral{Val: false}
		} else if isTrueLiteral(lhs) {
			return rhs
		} else if isTrueLiteral(rhs) {
			return lhs
		}
	} else if op == OR {
		if isTrueLiteral(lhs) || isTrueLiteral(rhs) {
			return &BooleanLiteral{Val: true}
		} else if isFalseLiteral(lhs) {
			return rhs
		} else if isFalseLiteral(rhs) {
			return lhs
		}
	}

	// Evaluate if both sides are simple typ.
	switch lhs := lhs.(type) {
	case *BooleanLiteral:
		return reduceBinaryExprBooleanLHS(expr, op, lhs, rhs)
	case *DurationLiteral:
		return reduceBinaryExprDurationLHS(expr, op, lhs, rhs, loc)
	case *IntegerLiteral:
		return reduceBinaryExprIntegerLHS(expr, op, lhs, rhs, loc)
	case *UnsignedLiteral:
		return reduceBinaryExprUnsignedLHS(expr, op, lhs, rhs)
	case *NilLiteral:
		return reduceBinaryExprNilLHS(expr, op, lhs, rhs)
	case *NumberLiteral:
		return wrapReduceBinaryExprNumberLHS(expr, op, lhs, rhs)
	case *StringLiteral:
		return reduceBinaryExprStringLHS(expr, op, lhs, rhs, loc)
	case *TimeLiteral:
		return reduceBinaryExprTimeLHS(expr, op, lhs, rhs, loc)
	default:
		// If we didn't change anything, return as is
		if lhs == expr.LHS && rhs == expr.RHS {
			return expr
		}
		return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, ReturnBool: expr.ReturnBool, depth: 1 + max(lhs.Depth(), rhs.Depth())}
	}
}

func reduceBinaryExprBooleanLHS(expr *BinaryExpr, op Token, lhs *BooleanLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *BooleanLiteral:
		switch op {
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case AND:
			return &BooleanLiteral{Val: lhs.Val && rhs.Val}
		case OR:
			return &BooleanLiteral{Val: lhs.Val || rhs.Val}
		case BITWISE_AND:
			return &BooleanLiteral{Val: lhs.Val && rhs.Val}
		case BITWISE_OR:
			return &BooleanLiteral{Val: lhs.Val || rhs.Val}
		case BITWISE_XOR:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		}
	case *NilLiteral:
		return &BooleanLiteral{Val: false}
	}
	if lhs == expr.LHS && rhs == expr.RHS {
		return expr
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, depth: 1 + rhs.Depth()}
}

func reduceBinaryExprDurationLHS(expr *BinaryExpr, op Token, lhs *DurationLiteral, rhs Expr, loc *time.Location) Expr {
	switch rhs := rhs.(type) {
	case *DurationLiteral:
		switch op {
		case ADD:
			return &DurationLiteral{Val: lhs.Val + rhs.Val}
		case SUB:
			return &DurationLiteral{Val: lhs.Val - rhs.Val}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case GT:
			return &BooleanLiteral{Val: lhs.Val > rhs.Val}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
		case LT:
			return &BooleanLiteral{Val: lhs.Val < rhs.Val}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
		}
	case *NumberLiteral:
		switch op {
		case MUL:
			return &DurationLiteral{Val: lhs.Val * time.Duration(rhs.Val)}
		case DIV:
			if rhs.Val == 0 {
				return &DurationLiteral{Val: 0}
			}
			return &DurationLiteral{Val: lhs.Val / time.Duration(rhs.Val)}
		}
	case *IntegerLiteral:
		switch op {
		case MUL:
			return &DurationLiteral{Val: lhs.Val * time.Duration(rhs.Val)}
		case DIV:
			if rhs.Val == 0 {
				return &DurationLiteral{Val: 0}
			}
			return &DurationLiteral{Val: lhs.Val / time.Duration(rhs.Val)}
		}
	case *TimeLiteral:
		switch op {
		case ADD:
			return &TimeLiteral{Val: rhs.Val.Add(lhs.Val)}
		}
	case *StringLiteral:
		t, err := rhs.ToTimeLiteral(loc)
		if err != nil {
			break
		}
		expr := reduceBinaryExprDurationLHS(expr, op, lhs, t, loc)

		// If the returned expression is still a binary expr, that means
		// we couldn't reduce it so this wasn't used in a time literal context.
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *NilLiteral:
		return &BooleanLiteral{Val: false}
	}
	if lhs == expr.LHS && rhs == expr.RHS {
		return expr
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, depth: 1 + rhs.Depth()}
}

func reduceBinaryExprIntegerLHS(expr *BinaryExpr, op Token, lhs *IntegerLiteral, rhs Expr, loc *time.Location) Expr {
	switch rhs := rhs.(type) {
	case *NumberLiteral:
		return reduceBinaryExprNumberLHS(op, &NumberLiteral{Val: float64(lhs.Val)}, rhs)
	case *IntegerLiteral:
		switch op {
		case ADD:
			return &IntegerLiteral{Val: lhs.Val + rhs.Val}
		case SUB:
			return &IntegerLiteral{Val: lhs.Val - rhs.Val}
		case MUL:
			return &IntegerLiteral{Val: lhs.Val * rhs.Val}
		case DIV:
			if rhs.Val == 0 {
				return &NumberLiteral{Val: 0}
			}
			return &NumberLiteral{Val: float64(lhs.Val) / float64(rhs.Val)}
		case MOD:
			if rhs.Val == 0 {
				return &IntegerLiteral{Val: 0}
			}
			return &IntegerLiteral{Val: lhs.Val % rhs.Val}
		case BITWISE_AND:
			return &IntegerLiteral{Val: lhs.Val & rhs.Val}
		case BITWISE_OR:
			return &IntegerLiteral{Val: lhs.Val | rhs.Val}
		case BITWISE_XOR:
			return &IntegerLiteral{Val: lhs.Val ^ rhs.Val}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case GT:
			return &BooleanLiteral{Val: lhs.Val > rhs.Val}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
		case LT:
			return &BooleanLiteral{Val: lhs.Val < rhs.Val}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
		}
	case *UnsignedLiteral:
		// Comparisons between an unsigned and integer literal will not involve
		// a cast if the integer is negative as that will have an improper result.
		// Look for those situations here.
		if lhs.Val < 0 {
			switch op {
			case LT, LTE:
				return &BooleanLiteral{Val: true}
			case GT, GTE:
				return &BooleanLiteral{Val: false}
			}
		}
		return reduceBinaryExprUnsignedLHS(expr, op, &UnsignedLiteral{Val: uint64(lhs.Val)}, rhs)
	case *DurationLiteral:
		// Treat the integer as a timestamp.
		switch op {
		case ADD:
			return &TimeLiteral{Val: time.Unix(0, lhs.Val).Add(rhs.Val)}
		case SUB:
			return &TimeLiteral{Val: time.Unix(0, lhs.Val).Add(-rhs.Val)}
		}
	case *TimeLiteral:
		d := &DurationLiteral{Val: time.Duration(lhs.Val)}
		expr := reduceBinaryExprDurationLHS(expr, op, d, rhs, loc)
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *StringLiteral:
		t, err := rhs.ToTimeLiteral(loc)
		if err != nil {
			break
		}
		d := &DurationLiteral{Val: time.Duration(lhs.Val)}
		expr := reduceBinaryExprDurationLHS(expr, op, d, t, loc)
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *NilLiteral:
		return &BooleanLiteral{Val: false}
	}
	if lhs == expr.LHS && rhs == expr.RHS {
		return expr
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, depth: 1 + rhs.Depth()}
}

func reduceBinaryExprUnsignedLHS(expr *BinaryExpr, op Token, lhs *UnsignedLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *NumberLiteral:
		return reduceBinaryExprNumberLHS(op, &NumberLiteral{Val: float64(lhs.Val)}, rhs)
	case *IntegerLiteral:
		// Comparisons between an unsigned and integer literal will not involve
		// a cast if the integer is negative as that will have an improper result.
		// Look for those situations here.
		if rhs.Val < 0 {
			switch op {
			case LT, LTE:
				return &BooleanLiteral{Val: false}
			case GT, GTE:
				return &BooleanLiteral{Val: true}
			}
		}
		return reduceBinaryExprUnsignedLHS(expr, op, lhs, &UnsignedLiteral{Val: uint64(rhs.Val)})
	case *UnsignedLiteral:
		switch op {
		case ADD:
			return &UnsignedLiteral{Val: lhs.Val + rhs.Val}
		case SUB:
			return &UnsignedLiteral{Val: lhs.Val - rhs.Val}
		case MUL:
			return &UnsignedLiteral{Val: lhs.Val * rhs.Val}
		case DIV:
			if rhs.Val == 0 {
				return &UnsignedLiteral{Val: 0}
			}
			return &UnsignedLiteral{Val: lhs.Val / rhs.Val}
		case MOD:
			if rhs.Val == 0 {
				return &UnsignedLiteral{Val: 0}
			}
			return &UnsignedLiteral{Val: lhs.Val % rhs.Val}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case GT:
			return &BooleanLiteral{Val: lhs.Val > rhs.Val}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
		case LT:
			return &BooleanLiteral{Val: lhs.Val < rhs.Val}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
		}
	}
	if lhs == expr.LHS && rhs == expr.RHS {
		return expr
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, depth: 1 + rhs.Depth()}
}

func reduceBinaryExprNilLHS(expr *BinaryExpr, op Token, lhs *NilLiteral, rhs Expr) Expr {
	switch op {
	case EQ, NEQ:
		return &BooleanLiteral{Val: false}
	}
	if lhs == expr.LHS && rhs == expr.RHS {
		return expr
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, depth: 1 + rhs.Depth()}
}

func wrapReduceBinaryExprNumberLHS(expr *BinaryExpr, op Token, lhs *NumberLiteral, rhs Expr) Expr {
	if !expr.ReturnBool {
		return reduceBinaryExprNumberLHS(op, lhs, rhs)
	}
	res := reduceBinaryExprNumberLHS(op, lhs, rhs)
	switch val := res.(type) {
	case *BooleanLiteral:
		if val.Val {
			return &NumberLiteral{Val: 1}
		}
		return &NumberLiteral{Val: 0}
	case *BinaryExpr:
		val.ReturnBool = expr.ReturnBool
		return val
	default:
		return res
	}
}

func reduceBinaryExprNumberLHS(op Token, lhs *NumberLiteral, rhs Expr) Expr {
	switch rhs := rhs.(type) {
	case *NumberLiteral:
		switch op {
		case ADD:
			return &NumberLiteral{Val: lhs.Val + rhs.Val}
		case SUB:
			return &NumberLiteral{Val: lhs.Val - rhs.Val}
		case MUL:
			return &NumberLiteral{Val: lhs.Val * rhs.Val}
		case DIV:
			if rhs.Val == 0 {
				return &NumberLiteral{Val: 0}
			}
			return &NumberLiteral{Val: lhs.Val / rhs.Val}
		case MOD:
			return &NumberLiteral{Val: math.Mod(lhs.Val, rhs.Val)}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == rhs.Val}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != rhs.Val}
		case GT:
			return &BooleanLiteral{Val: lhs.Val > rhs.Val}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val >= rhs.Val}
		case LT:
			return &BooleanLiteral{Val: lhs.Val < rhs.Val}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val <= rhs.Val}
		}
	case *IntegerLiteral:
		switch op {
		case ADD:
			return &NumberLiteral{Val: lhs.Val + float64(rhs.Val)}
		case SUB:
			return &NumberLiteral{Val: lhs.Val - float64(rhs.Val)}
		case MUL:
			return &NumberLiteral{Val: lhs.Val * float64(rhs.Val)}
		case DIV:
			if float64(rhs.Val) == 0 {
				return &NumberLiteral{Val: 0}
			}
			return &NumberLiteral{Val: lhs.Val / float64(rhs.Val)}
		case MOD:
			return &NumberLiteral{Val: math.Mod(lhs.Val, float64(rhs.Val))}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val == float64(rhs.Val)}
		case NEQ:
			return &BooleanLiteral{Val: lhs.Val != float64(rhs.Val)}
		case GT:
			return &BooleanLiteral{Val: lhs.Val > float64(rhs.Val)}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val >= float64(rhs.Val)}
		case LT:
			return &BooleanLiteral{Val: lhs.Val < float64(rhs.Val)}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val <= float64(rhs.Val)}
		}
	case *UnsignedLiteral:
		return reduceBinaryExprNumberLHS(op, lhs, &NumberLiteral{Val: float64(rhs.Val)})
	case *NilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, depth: 1 + rhs.Depth()}
}

func reduceBinaryExprStringLHS(expr *BinaryExpr, op Token, lhs *StringLiteral, rhs Expr, loc *time.Location) Expr {
	switch rhs := rhs.(type) {
	case *StringLiteral:
		switch op {
		case EQ:
			var bexpr Expr = &BooleanLiteral{Val: lhs.Val == rhs.Val}
			// This might be a comparison between time literals.
			// If it is, parse the time literals and then compare since it
			// could be a different result if they use different formats
			// for the same time.
			if lhs.IsTimeLiteral() && rhs.IsTimeLiteral() {
				tlhs, err := lhs.ToTimeLiteral(loc)
				if err != nil {
					return bexpr
				}

				trhs, err := rhs.ToTimeLiteral(loc)
				if err != nil {
					return bexpr
				}

				t := reduceBinaryExprTimeLHS(expr, op, tlhs, trhs, loc)
				if _, ok := t.(*BinaryExpr); !ok {
					bexpr = t
				}
			}
			return bexpr
		case NEQ:
			var bexpr Expr = &BooleanLiteral{Val: lhs.Val != rhs.Val}
			// This might be a comparison between time literals.
			// If it is, parse the time literals and then compare since it
			// could be a different result if they use different formats
			// for the same time.
			if lhs.IsTimeLiteral() && rhs.IsTimeLiteral() {
				tlhs, err := lhs.ToTimeLiteral(loc)
				if err != nil {
					return bexpr
				}

				trhs, err := rhs.ToTimeLiteral(loc)
				if err != nil {
					return bexpr
				}

				t := reduceBinaryExprTimeLHS(expr, op, tlhs, trhs, loc)
				if _, ok := t.(*BinaryExpr); !ok {
					bexpr = t
				}
			}
			return bexpr
		case ADD:
			return &StringLiteral{Val: lhs.Val + rhs.Val}
		default:
			// Attempt to convert the string literal to a time literal.
			t, err := lhs.ToTimeLiteral(loc)
			if err != nil {
				break
			}
			expr := reduceBinaryExprTimeLHS(expr, op, t, rhs, loc)

			// If the returned expression is still a binary expr, that means
			// we couldn't reduce it so this wasn't used in a time literal context.
			if _, ok := expr.(*BinaryExpr); !ok {
				return expr
			}
		}
	case *DurationLiteral:
		// Attempt to convert the string literal to a time literal.
		t, err := lhs.ToTimeLiteral(loc)
		if err != nil {
			break
		}
		expr := reduceBinaryExprTimeLHS(expr, op, t, rhs, loc)

		// If the returned expression is still a binary expr, that means
		// we couldn't reduce it so this wasn't used in a time literal context.
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *TimeLiteral:
		// Attempt to convert the string literal to a time literal.
		t, err := lhs.ToTimeLiteral(loc)
		if err != nil {
			break
		}
		expr := reduceBinaryExprTimeLHS(expr, op, t, rhs, loc)

		// If the returned expression is still a binary expr, that means
		// we couldn't reduce it so this wasn't used in a time literal context.
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *IntegerLiteral:
		// Attempt to convert the string literal to a time literal.
		t, err := lhs.ToTimeLiteral(loc)
		if err != nil {
			break
		}
		expr := reduceBinaryExprTimeLHS(expr, op, t, rhs, loc)

		// If the returned expression is still a binary expr, that means
		// we couldn't reduce it so this wasn't used in a time literal context.
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *NilLiteral:
		switch op {
		case EQ, NEQ:
			return &BooleanLiteral{Val: false}
		}
	}
	if lhs == expr.LHS && rhs == expr.RHS {
		return expr
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, depth: 1 + rhs.Depth()}
}

func reduceBinaryExprTimeLHS(expr *BinaryExpr, op Token, lhs *TimeLiteral, rhs Expr, loc *time.Location) Expr {
	switch rhs := rhs.(type) {
	case *DurationLiteral:
		switch op {
		case ADD:
			return &TimeLiteral{Val: lhs.Val.Add(rhs.Val)}
		case SUB:
			return &TimeLiteral{Val: lhs.Val.Add(-rhs.Val)}
		}
	case *IntegerLiteral:
		d := &DurationLiteral{Val: time.Duration(rhs.Val)}
		expr := reduceBinaryExprTimeLHS(expr, op, lhs, d, loc)
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *TimeLiteral:
		switch op {
		case SUB:
			return &DurationLiteral{Val: lhs.Val.Sub(rhs.Val)}
		case EQ:
			return &BooleanLiteral{Val: lhs.Val.Equal(rhs.Val)}
		case NEQ:
			return &BooleanLiteral{Val: !lhs.Val.Equal(rhs.Val)}
		case GT:
			return &BooleanLiteral{Val: lhs.Val.After(rhs.Val)}
		case GTE:
			return &BooleanLiteral{Val: lhs.Val.After(rhs.Val) || lhs.Val.Equal(rhs.Val)}
		case LT:
			return &BooleanLiteral{Val: lhs.Val.Before(rhs.Val)}
		case LTE:
			return &BooleanLiteral{Val: lhs.Val.Before(rhs.Val) || lhs.Val.Equal(rhs.Val)}
		}
	case *StringLiteral:
		t, err := rhs.ToTimeLiteral(loc)
		if err != nil {
			break
		}
		expr := reduceBinaryExprTimeLHS(expr, op, lhs, t, loc)

		// If the returned expression is still a binary expr, that means
		// we couldn't reduce it so this wasn't used in a time literal context.
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *NilLiteral:
		return &BooleanLiteral{Val: false}
	}
	if lhs == expr.LHS && rhs == expr.RHS {
		return expr
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, depth: 1 + rhs.Depth()}
}

func reduceCall(expr *Call, valuer Valuer) Expr {
	// Otherwise reduce arguments.
	var args []Expr
	literalsOnly := true
	changed := false
	depth := 0
	if len(expr.Args) > 0 {
		args = make([]Expr, len(expr.Args))
		for i, arg := range expr.Args {
			args[i] = reduce(arg, valuer)
			if args[i] != arg {
				changed = true
			}
			depth = max(depth, args[i].Depth())
			if !isLiteral(args[i]) {
				literalsOnly = false
			}
		}
	}

	// Evaluate a function call if the valuer is a CallValuer and
	// the arguments are only literals.
	if literalsOnly {
		if valuer, ok := valuer.(CallValuer); ok {
			argVals := make([]interface{}, len(args))
			for i := range args {
				argVals[i] = Eval(args[i], nil)
			}
			if v, ok := valuer.Call(expr.Name, argVals); ok {
				return asLiteral(v)
			}
		}
	}
	if changed {
		return &Call{Name: expr.Name, Args: args, depth: 1 + depth}
	}
	return expr
}

func reduceParenExpr(expr *ParenExpr, valuer Valuer) Expr {
	subexpr := reduce(expr.Expr, valuer)
	if subexpr, ok := subexpr.(*BinaryExpr); ok {
		if subexpr == expr.Expr {
			return expr
		}
		return &ParenExpr{Expr: subexpr, depth: 1 + subexpr.Depth()}
	}
	return subexpr
}

func reduceVarRef(expr *VarRef, valuer Valuer) Expr {
	// Ignore if there is no valuer.
	if valuer == nil {
		return expr
	}

	// Retrieve the value of the ref.
	// Ignore if the value doesn't exist.
	v, ok := valuer.Value(expr.Val)
	if !ok {
		return expr
	}

	// Return the value as a literal.
	return asLiteral(v)
}

// asLiteral takes an interface and converts it into an influxql literal.
func asLiteral(v interface{}) Literal {
	switch v := v.(type) {
	case bool:
		return &BooleanLiteral{Val: v}
	case time.Duration:
		return &DurationLiteral{Val: v}
	case float64:
		return &NumberLiteral{Val: v}
	case int64:
		return &IntegerLiteral{Val: v}
	case string:
		return &StringLiteral{Val: v}
	case time.Time:
		return &TimeLiteral{Val: v}
	default:
		return &NilLiteral{}
	}
}

// isLiteral returns if the expression is a literal.
func isLiteral(expr Expr) bool {
	_, ok := expr.(Literal)
	return ok
}

// Valuer is the interface that wraps the Value() method.
type Valuer interface {
	// Value returns the value and existence flag for a given key.
	Value(key string) (interface{}, bool)
	SetValuer(v Valuer, index int)
}

// CallValuer implements the Call method for evaluating function calls.
type CallValuer interface {
	Valuer

	// Call is invoked to evaluate a function call (if possible).
	Call(name string, args []interface{}) (interface{}, bool)
}

// ZoneValuer is the interface that specifies the current time zone.
type ZoneValuer interface {
	Valuer

	// Zone returns the time zone location. This function may return nil
	// if no time zone is known.
	Zone() *time.Location
}

var _ CallValuer = (*NowValuer)(nil)

var _ ZoneValuer = (*NowValuer)(nil)

// NowValuer returns only the value for "now()".
type NowValuer struct {
	Now      time.Time
	Location *time.Location
}

// Value is a method that returns the value and existence flag for a given key.
func (v *NowValuer) Value(key string) (interface{}, bool) {
	if !v.Now.IsZero() && key == "now()" {
		return v.Now, true
	}
	return nil, false
}

func (v *NowValuer) SetValuer(vv Valuer, index int) {

}

// Call evaluates the now() function to replace now() with the current time.
func (v *NowValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if name == "now" && len(args) == 0 {
		return v.Now, true
	}
	return nil, false
}

// Zone is a method that returns the time.Location.
func (v *NowValuer) Zone() *time.Location {
	if v.Location != nil {
		return v.Location
	}
	return nil
}

// MultiValuer returns a Valuer that iterates over multiple Valuer instances
// to find a match.
func MultiValuer(valuers ...Valuer) Valuer {
	return SliceValuer(valuers)
}

func SliceValuer(valuers []Valuer) Valuer {
	return multiValuer(valuers)
}

type multiValuer []Valuer

var _ CallValuer = multiValuer(nil)

var _ ZoneValuer = multiValuer(nil)

func (a multiValuer) SetValuer(v Valuer, index int) {
	a[index] = v
}

func (a multiValuer) Value(key string) (interface{}, bool) {
	for _, valuer := range a {
		if v, ok := valuer.Value(key); ok {
			return v, true
		}
	}
	return nil, false
}

func (a multiValuer) Call(name string, args []interface{}) (interface{}, bool) {
	for _, valuer := range a {
		if valuer, ok := valuer.(CallValuer); ok {
			if v, ok := valuer.Call(name, args); ok {
				return v, true
			}
		}
	}
	return nil, false
}

func (a multiValuer) Zone() *time.Location {
	for _, valuer := range a {
		if valuer, ok := valuer.(ZoneValuer); ok {
			if v := valuer.Zone(); v != nil {
				return v
			}
		}
	}
	return nil
}

// ContainsVarRef returns true if expr is a VarRef or contains one.
func ContainsVarRef(expr Expr) bool {
	var v containsVarRefVisitor
	Walk(&v, expr)
	return v.contains
}

type containsVarRefVisitor struct {
	contains bool
}

func (v *containsVarRefVisitor) Visit(n Node) Visitor {
	switch n.(type) {
	case *Call:
		return nil
	case *VarRef:
		v.contains = true
	}
	return v
}

func IsSelector(expr Expr) bool {
	if call, ok := expr.(*Call); ok {
		switch call.Name {
		case "first", "last", "min", "max", "percentile", "sample", "top", "bottom":
			return true
		}
	}
	return false
}

// stringSetSlice returns a sorted slice of keys from a string set.
func stringSetSlice(m map[string]struct{}) []string {
	if m == nil {
		return nil
	}

	a := make([]string, 0, len(m))
	for k := range m {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// TimeRange represents a range of time from Min to Max. The times are inclusive.
type TimeRange struct {
	Min, Max time.Time
}

// Intersect joins this TimeRange with another TimeRange.
func (t TimeRange) Intersect(other TimeRange) TimeRange {
	if !other.Min.IsZero() {
		if t.Min.IsZero() || other.Min.After(t.Min) {
			t.Min = other.Min
		}
	}
	if !other.Max.IsZero() {
		if t.Max.IsZero() || other.Max.Before(t.Max) {
			t.Max = other.Max
		}
	}
	return t
}

// IsZero is true if the min and max of the time range are zero.
func (t TimeRange) IsZero() bool {
	return t.Min.IsZero() && t.Max.IsZero()
}

// Used by TimeRange methods.
var minTime = time.Unix(0, MinTime)

var maxTime = time.Unix(0, MaxTime)

// MinTime returns the minimum time of the TimeRange.
// If the minimum time is zero, this returns the minimum possible time.
func (t TimeRange) MinTime() time.Time {
	if t.Min.IsZero() {
		return minTime
	}
	return t.Min
}

// MaxTime returns the maximum time of the TimeRange.
// If the maximum time is zero, this returns the maximum possible time.
func (t TimeRange) MaxTime() time.Time {
	if t.Max.IsZero() {
		return maxTime
	}
	return t.Max
}

// MinTimeNano returns the minimum time in nanoseconds since the epoch.
// If the minimum time is zero, this returns the minimum possible time.
func (t TimeRange) MinTimeNano() int64 {
	if t.Min.IsZero() {
		return MinTime
	}
	return t.Min.UnixNano()
}

// MaxTimeNano returns the maximum time in nanoseconds since the epoch.
// If the maximum time is zero, this returns the maximum possible time.
func (t TimeRange) MaxTimeNano() int64 {
	if t.Max.IsZero() {
		return MaxTime
	}
	return t.Max.UnixNano()
}

// ConditionExpr extracts the time range and the condition from an expression.
// We only support simple time ranges that are constrained with AND and are not nested.
// This throws an error when we encounter a time condition that is combined with OR
// to prevent returning unexpected results that we do not support.
func ConditionExpr(cond Expr, valuer Valuer) (Expr, TimeRange, error) {
	// Pre-reduce expression to replace "now" and fold any constants.
	expr1 := Reduce(cond, valuer)

	// Extract time range from conditions if it possible
	expr, tr, err := conditionExpr(expr1, valuer)

	// Try to reduce the expression if we changed it
	if expr != nil && expr != expr1 {
		expr = Reduce(expr, valuer)
	}

	if e, ok := expr.(*BooleanLiteral); ok && e.Val {
		// If the condition is true, return nil instead to indicate there
		// is no condition.
		expr = nil
	}
	return expr, tr, err
}

func conditionExpr(cond Expr, valuer Valuer) (Expr, TimeRange, error) {
	if cond == nil {
		return nil, TimeRange{}, nil
	}

	switch cond := cond.(type) {
	case *BinaryExpr:
		if cond.Op == AND || cond.Op == OR {
			lhsExpr, lhsTime, err := conditionExpr(cond.LHS, valuer)
			if err != nil {
				return nil, TimeRange{}, err
			}

			rhsExpr, rhsTime, err := conditionExpr(cond.RHS, valuer)
			if err != nil {
				return nil, TimeRange{}, err
			}

			// Always intersect the time range even if it makes no sense.
			// There is no such thing as using OR with a time range.
			timeRange := lhsTime.Intersect(rhsTime)

			// Combine the left and right expression.
			if rhsExpr == nil {
				return lhsExpr, timeRange, nil
			} else if lhsExpr == nil {
				return rhsExpr, timeRange, nil
			}
			if lhsExpr == cond.LHS && rhsExpr == cond.RHS {
				return cond, timeRange, nil
			}
			return &BinaryExpr{
				Op:    cond.Op,
				LHS:   lhsExpr,
				RHS:   rhsExpr,
				depth: 1 + max(lhsExpr.Depth(), rhsExpr.Depth()),
			}, timeRange, nil
		}

		// If either the left or the right side is "time", we are looking at
		// a time range.
		if lhs, ok := cond.LHS.(*VarRef); ok && strings.ToLower(lhs.Val) == "time" {
			timeRange, err := getTimeRange(cond.Op, cond.RHS, valuer)
			return nil, timeRange, err
		} else if rhs, ok := cond.RHS.(*VarRef); ok && strings.ToLower(rhs.Val) == "time" {
			// Swap the op for the opposite if it is a comparison.
			op := cond.Op
			switch op {
			case GT:
				op = LT
			case LT:
				op = GT
			case GTE:
				op = LTE
			case LTE:
				op = GTE
			}
			timeRange, err := getTimeRange(op, cond.LHS, valuer)
			return nil, timeRange, err
		}
		return cond, TimeRange{}, nil
	case *ParenExpr:
		expr, timeRange, err := conditionExpr(cond.Expr, valuer)
		if err != nil {
			return nil, TimeRange{}, err
		} else if expr == nil {
			return nil, timeRange, nil
		}
		if expr == cond.Expr {
			return cond, timeRange, nil
		}
		return &ParenExpr{Expr: expr, depth: 1 + expr.Depth()}, timeRange, nil
	case *BooleanLiteral:
		return cond, TimeRange{}, nil
	case *InCondition:
		return cond, TimeRange{}, nil
	default:
		return nil, TimeRange{}, fmt.Errorf("invalid condition expression: %s", cond)
	}
}

// getTimeRange returns the time range associated with this comparison.
// op is the operation that is used for comparison and rhs is the right hand side
// of the expression. The left hand side is always assumed to be "time".
func getTimeRange(op Token, rhs Expr, valuer Valuer) (TimeRange, error) {
	// If literal looks like a date time then parse it as a time literal.
	if strlit, ok := rhs.(*StringLiteral); ok {
		if strlit.IsTimeLiteral() {
			var loc *time.Location
			if valuer, ok := valuer.(ZoneValuer); ok {
				loc = valuer.Zone()
			}
			t, err := strlit.ToTimeLiteral(loc)
			if err != nil {
				return TimeRange{}, err
			}
			rhs = t
		}
	}

	var value time.Time
	switch lit := rhs.(type) {
	case *TimeLiteral:
		if lit.Val.After(time.Unix(0, MaxTime)) {
			return TimeRange{}, fmt.Errorf("time %s overflows time literal", lit.Val.Format(time.RFC3339))
		} else if lit.Val.Before(time.Unix(0, MinTime+1)) {
			// The minimum allowable time literal is one greater than the minimum time because the minimum time
			// is a sentinel value only used internally.
			return TimeRange{}, fmt.Errorf("time %s underflows time literal", lit.Val.Format(time.RFC3339))
		}
		value = lit.Val
	case *DurationLiteral:
		value = time.Unix(0, int64(lit.Val)).UTC()
	case *NumberLiteral:
		value = time.Unix(0, int64(lit.Val)).UTC()
	case *IntegerLiteral:
		value = time.Unix(0, lit.Val).UTC()
	default:
		return TimeRange{}, fmt.Errorf("invalid operation: time and %T are not compatible", lit)
	}

	timeRange := TimeRange{}
	switch op {
	case GT:
		timeRange.Min = value.Add(time.Nanosecond)
	case GTE:
		timeRange.Min = value
	case LT:
		timeRange.Max = value.Add(-time.Nanosecond)
	case LTE:
		timeRange.Max = value
	case EQ:
		timeRange.Min, timeRange.Max = value, value
	default:
		return TimeRange{}, fmt.Errorf("invalid time comparison operator: %s", op)
	}
	return timeRange, nil
}

// PrepareSnapshotStatement represents a command for preparing preparing.
type PrepareSnapshotStatement struct{}

func (s *PrepareSnapshotStatement) Depth() int { return 1 }

func (s *PrepareSnapshotStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the prepare preparing command.
func (s *PrepareSnapshotStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *PrepareSnapshotStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString("Prepare Snapshot")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a PrepareSnapshotStatement.
func (s *PrepareSnapshotStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	// PrepareSnapshot is one of few statements that have no required privileges.
	// Anyone is allowed to execute it, but the returned results depend on the user's
	// individual PrepareSnapshotStatement permissions.
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: NoPrivileges}}, nil
}

// EndPrepareSnapshotStatement represents a command for preparing preparing.
type EndPrepareSnapshotStatement struct{}

func (s *EndPrepareSnapshotStatement) Depth() int { return 1 }

func (s *EndPrepareSnapshotStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the end prepare preparing command.
func (s *EndPrepareSnapshotStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *EndPrepareSnapshotStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString("End Snapshot")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a EndPrepareSnapshotStatement.
func (s *EndPrepareSnapshotStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	// EndPrepareSnapshot is one of few statements that have no required privileges.
	// Anyone is allowed to execute it, but the returned results depend on the user's
	// individual EndPrepareSnapshotStatement permissions.
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: NoPrivileges}}, nil
}

// GetRuntimeInfoStatement represents a command for get runtimeinfo.
type GetRuntimeInfoStatement struct{}

func (s *GetRuntimeInfoStatement) Depth() int { return 1 }

func (s *GetRuntimeInfoStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

// String returns a string representation of the get runtimeinfo command.
func (s *GetRuntimeInfoStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *GetRuntimeInfoStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	buf.WriteString("Get RuntimeInfo")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

// RequiredPrivileges returns the privilege required to execute a GetRuntimeInfoStatement.
func (s *GetRuntimeInfoStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	// GetRuntimeInfo is one of few statements that have no required privileges.
	// Anyone is allowed to execute it, but the returned results depend on the user's
	// individual GetRuntimeInfoStatement permissions.
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: NoPrivileges}}, nil
}

type CaseWhenExpr struct {
	Conditions []Expr
	Assigners  []Expr
	Args       []Expr
	depth      int
}

func (p *CaseWhenExpr) Depth() int {
	if p != nil {
		return p.depth
	}
	return 0
}

func (p *CaseWhenExpr) UpdateDepthForTests() int {
	if p != nil {
		subdepth := 0
		for i, s := range p.Conditions {
			subdepth = max(subdepth, 1+i+CallUpdateDepthForTests(s))
		}
		for i, s := range p.Assigners {
			subdepth = max(subdepth, 1+i+CallUpdateDepthForTests(s))
		}
		for i, s := range p.Args {
			subdepth = max(subdepth, 1+i+CallUpdateDepthForTests(s))
		}
		p.depth = 1 + subdepth
		return p.depth
	}
	return 0
}

func (p *CaseWhenExpr) RewriteNameSpace(alias, mst string) {}

func (p *CaseWhenExpr) node() {}

func (p *CaseWhenExpr) expr() {}

func (p *CaseWhenExpr) String() string { return p.RenderBytes(&bytes.Buffer{}, nil).String() }

func (p *CaseWhenExpr) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("CASE")

	for i := range p.Conditions {
		_, _ = buf.WriteString(" WHEN ")
		_ = p.Conditions[i].RenderBytes(buf, posmap)
		_, _ = buf.WriteString(" THEN ")
		_ = p.Assigners[i].RenderBytes(buf, posmap)
	}
	_, _ = buf.WriteString(" ELSE ")
	_ = p.Assigners[len(p.Conditions)].RenderBytes(buf, posmap)
	_, _ = buf.WriteString(" END")

	if posmap != nil {
		posmap[p] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type CreateDownSampleStatement struct {
	DbName         string
	RpName         string
	Duration       time.Duration
	SampleInterval []time.Duration
	TimeInterval   []time.Duration
	WaterMark      []time.Duration
	Ops            Fields
	depth          int
}

func (d *CreateDownSampleStatement) Depth() int {
	if d != nil {
		return d.depth
	}
	return 0
}

func (d *CreateDownSampleStatement) UpdateDepthForTests() int {
	if d != nil {
		d.depth = 1 + max(
			len(d.SampleInterval),
			len(d.TimeInterval),
			len(d.WaterMark),
			d.Ops.UpdateDepthForTests(),
		)
		return d.depth
	}
	return 0
}

func (d *CreateDownSampleStatement) stmt() {}

func (d *CreateDownSampleStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (d *CreateDownSampleStatement) node() {}

func Times2String(times []time.Duration) string {
	var s []byte
	for i, t := range times {
		s = append(s, t.String()...)
		if i < len(times)-1 {
			s = append(s, ","...)
		}
	}
	return string(s)
}

func (d *CreateDownSampleStatement) String() string {
	return d.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (d *CreateDownSampleStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("CREATE DOWNSAMPLE")

	if d.DbName != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(d.DbName))
	}

	if d.RpName != "" {
		if d.DbName == "" {
			_, _ = buf.WriteString(" ON ")
		}
		_, _ = buf.WriteString(QuoteIdent(d.RpName))
	}

	if len(d.Ops) != 0 {
		_, _ = buf.WriteString("(")
		for i, op := range d.Ops {
			if i > 0 {
				_, _ = buf.WriteString(",")
			}
			c, ok := op.Expr.(*Call)
			if !ok {
				_, _ = buf.WriteString("<ERROR>")
			} else {
				_ = c.RenderBytes(buf, posmap)
			}
		}
		_, _ = buf.WriteString(")")
	}

	if d.Duration != 0 {
		_, _ = buf.WriteString(" WITH TTL ")
		_, _ = buf.WriteString(QuoteIdent(d.Duration.String()))
	}

	if len(d.SampleInterval) != 0 {
		_, _ = buf.WriteString(" SAMPLEINTERVAL ")
		s := Times2String(d.SampleInterval)
		_, _ = buf.WriteString(QuoteIdent(s))
	}

	if len(d.TimeInterval) != 0 {
		_, _ = buf.WriteString(" TIMEINTERVAL ")
		s := Times2String(d.TimeInterval)
		_, _ = buf.WriteString(QuoteIdent(s))
	}

	if len(d.WaterMark) != 0 {
		_, _ = buf.WriteString(" WATERMARK ")
		s := Times2String(d.WaterMark)
		_, _ = buf.WriteString(QuoteIdent(s))
	}

	if posmap != nil {
		posmap[d] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type DropDownSampleStatement struct {
	DbName  string
	RpName  string
	DropAll bool
}

func (d *DropDownSampleStatement) Depth() int { return 1 }

func (d *DropDownSampleStatement) UpdateDepthForTests() int {
	if d != nil {
		return 1
	}
	return 0
}

func (d *DropDownSampleStatement) stmt() {}

func (d *DropDownSampleStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (d *DropDownSampleStatement) node() {}

func (d *DropDownSampleStatement) String() string {
	return d.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (d *DropDownSampleStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DROP DOWNSAMPLE")

	if d.DbName != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(d.DbName))
	}

	if d.RpName != "" {
		if d.DbName == "" {
			_, _ = buf.WriteString(" ON ")
		}
		_, _ = buf.WriteString(QuoteIdent(d.RpName))
	}

	if posmap != nil {
		posmap[d] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type ShowDownSampleStatement struct {
	DbName string
}

func (d *ShowDownSampleStatement) Depth() int { return 1 }

func (d *ShowDownSampleStatement) UpdateDepthForTests() int {
	if d != nil {
		return 1
	}
	return 0
}

func (d *ShowDownSampleStatement) stmt() {}

func (d *ShowDownSampleStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (d *ShowDownSampleStatement) node() {}

func (d *ShowDownSampleStatement) String() string {
	return d.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (d *ShowDownSampleStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("Show DownSampling")

	if d.DbName != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(d.DbName))
	}

	if posmap != nil {
		posmap[d] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type CreateStreamStatement struct {
	Name   string
	Target *Target
	Query  Statement
	Delay  time.Duration
	depth  int
}

func (c *CreateStreamStatement) Depth() int {
	if c != nil {
		return c.depth
	}
	return 0
}

func (c *CreateStreamStatement) UpdateDepthForTests() int {
	if c != nil {
		c.depth = 1 + max(
			CallUpdateDepthForTests(c.Target),
			CallUpdateDepthForTests(c.Query),
		)
		return c.depth
	}
	return 0
}

func (c *CreateStreamStatement) stmt() {}

func (c *CreateStreamStatement) node() {}

func (c *CreateStreamStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (c *CreateStreamStatement) String() string { return c.RenderBytes(&bytes.Buffer{}, nil).String() }

func (c *CreateStreamStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("CREATE STREAM ")
	_, _ = buf.WriteString(QuoteIdent(c.Name))
	_, _ = buf.WriteString(" INTO ")
	_ = c.Target.RenderBytes(buf, posmap)
	_, _ = buf.WriteString(" ")
	_ = c.Query.RenderBytes(buf, posmap)
	_, _ = buf.WriteString(" OFFSET ")
	_, _ = buf.WriteString(QuoteIdent(c.Delay.String()))

	if posmap != nil {
		posmap[c] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (c *CreateStreamStatement) CheckSource(sources Sources) (*Measurement, error) {
	if len(sources) > 1 {
		return nil, errors.New("only one source can be selected")
	}

	if srcMst, ok := sources[0].(*Measurement); !ok {
		return nil, errors.New("only measurement is allowed for the data source")
	} else {
		return srcMst, nil
	}
}

func (c *CreateStreamStatement) Check(stmt *SelectStatement, supportTable map[string]bool, srcTagCount int) error {
	if stmt.groupByInterval == 0 && len(stmt.Dimensions) == 0 {
		// Duplicate fields may exist in the select_stmt,
		// like: select tag_1, tag_1, field_1 from ....
		dstTags := make(map[string]struct{})
		for _, field := range stmt.Fields {
			if varRef, ok := field.Expr.(*VarRef); !ok {
				return errors.New("the filter-only stream task can contain only var_ref(tag or field)")
			} else if varRef.Type == Tag {
				dstTags[varRef.Val] = struct{}{}
			}
		}

		if len(dstTags) != srcTagCount {
			return errors.New("all tags must be selected for the filter-only stream task")
		}

		// Check sources. Only one source can be queried, and the source must be *influxql.Measurement.
		if _, err := c.CheckSource(stmt.Sources); err != nil {
			return err
		}
		sourceMst := stmt.Sources[0].(*Measurement)
		if c.Target.Measurement.Database != sourceMst.Database {
			return errors.New("the source and destination measurement must be in the same database")
		}
		if c.Target.Measurement.RetentionPolicy != sourceMst.RetentionPolicy {
			return errors.New("the source and destination measurement must be in the same retention policy")
		}

		// Check condition. Only int/float/bool/string/tag condition is supported.
		if !c.CheckCondition(stmt.Condition) {
			return errors.New("only support int/float/bool/string/field condition")
		}

		return nil
	}

	if err := stmt.StreamCheck(supportTable); err != nil {
		return err
	}

	if stmt.groupByInterval*10 < c.Delay {
		return errors.New("delay time must be smaller than 10 times of group by interval time")
	}
	return nil
}

func (c *CreateStreamStatement) CheckCondition(cond Expr) bool {
	switch expr := cond.(type) {
	case *ParenExpr:
		return c.CheckCondition(expr.Expr)
	case *BinaryExpr:
		if !c.CheckCondition(expr.LHS) {
			return false
		}
		return c.CheckCondition(expr.RHS)
	case *StringLiteral, *IntegerLiteral, *NumberLiteral, *BooleanLiteral:
		return true
	case *VarRef:
		if expr.Type == Tag {
			return false
		}
		return true
	default:
		return false
	}
}

type ShowStreamsStatement struct {
	Database string
}

func (s *ShowStreamsStatement) Depth() int { return 1 }

func (s *ShowStreamsStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *ShowStreamsStatement) stmt() {}

func (s *ShowStreamsStatement) node() {}

func (s *ShowStreamsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowStreamsStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowStreamsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("SHOW STREAMS")
	if len(s.Database) > 0 {
		_, _ = buf.WriteString(" ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type DropStreamsStatement struct {
	Name string
}

func (s *DropStreamsStatement) Depth() int { return 1 }

func (s *DropStreamsStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *DropStreamsStatement) stmt() {}

func (s *DropStreamsStatement) node() {}

func (s *DropStreamsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *DropStreamsStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *DropStreamsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("DROP STREAMS ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type ShowMeasurementKeysStatement struct {
	Name        string
	Database    string
	Rp          string
	Measurement string
}

func (s *ShowMeasurementKeysStatement) Depth() int { return 1 }

func (s *ShowMeasurementKeysStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *ShowMeasurementKeysStatement) stmt() {}

func (s *ShowMeasurementKeysStatement) node() {}

func (s *ShowMeasurementKeysStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowMeasurementKeysStatement) String() string {
	return s.RenderBytes(&bytes.Buffer{}, nil).String()
}

func (s *ShowMeasurementKeysStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("Show Measurement ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type ShowConfigsStatement struct {
	Scope string
	Key   *ConfigKey
}

type ConfigKey struct {
	Name  string
	Regex *RegexLiteral
}

func (ck *ConfigKey) node() {}

// String returns a string representation of the measurement.
func (ck *ConfigKey) String() string { return ck.RenderBytes(&bytes.Buffer{}, nil).String() }

func (ck *ConfigKey) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if ck.Name != "" {
		_, _ = buf.WriteString(QuoteIdent(ck.Name))
	} else if ck.Regex != nil {
		_ = ck.Regex.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[ck] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *ShowConfigsStatement) Depth() int { return 1 }

func (s *ShowConfigsStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *ShowConfigsStatement) stmt() {}

func (s *ShowConfigsStatement) node() {}

func (s *ShowConfigsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowConfigsStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowConfigsStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString(`SHOW CONFIGS`)

	joinword := ` WHERE `

	if s.Scope != "" {
		_, _ = buf.WriteString(joinword)
		_, _ = buf.WriteString(`scope = `)
		_, _ = buf.WriteString(s.Scope)
		joinword = ` AND `
	}
	if s.Key != nil {
		_, _ = buf.WriteString(joinword)
		_, _ = buf.WriteString(`name = `)
		_ = s.Key.RenderBytes(buf, posmap)
		joinword = ` AND `
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type SetConfigStatement struct {
	Component string
	Key       string
	Value     interface{}
}

func (s *SetConfigStatement) Depth() int { return 1 }

func (s *SetConfigStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *SetConfigStatement) stmt() {}

func (s *SetConfigStatement) node() {}

func (s *SetConfigStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *SetConfigStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *SetConfigStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if _, ok := s.Value.(string); ok {
		_, _ = fmt.Fprintf(buf, `SET CONFIG %s "%s" = "%s"`, s.Component, s.Key, s.Value)
	} else {
		_, _ = fmt.Fprintf(buf, `SET CONFIG %s "%s" = %v`, s.Component, s.Key, s.Value)
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type ShowClusterStatement struct {
	NodeType string
	NodeID   int64
}

func (s *ShowClusterStatement) Depth() int { return 1 }

func (s *ShowClusterStatement) UpdateDepthForTests() int {
	if s != nil {
		return 1
	}
	return 0
}

func (s *ShowClusterStatement) RewriteNameSpace(alias, mst string) {}

func (s *ShowClusterStatement) stmt() {}

func (s *ShowClusterStatement) node() {}

func (s *ShowClusterStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowClusterStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *ShowClusterStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString(`SHOW CLUSTER`)

	joinword := ` WHERE `

	if s.NodeType != "" {
		_, _ = buf.WriteString(joinword)
		_, _ = buf.WriteString(`nodeType = `)
		_, _ = buf.WriteString(s.NodeType)
		joinword = ` AND `
	}
	if s.NodeID != 0 {
		_, _ = buf.WriteString(joinword)
		_, _ = buf.WriteString(`nodeID = `)
		_, _ = buf.WriteString(strconv.FormatInt(s.NodeID, 10))
		joinword = ` AND `
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type Unnests []*Unnest

func (us Unnests) node() {}

func (us Unnests) String() string { return us.RenderBytes(&bytes.Buffer{}, nil).String() }

func (us Unnests) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	for i, u := range us {
		if i > 0 {
			_, _ = buf.WriteString(", ")
		}
		_ = u.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[us] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type Unnest struct {
	Expr    Expr
	Aliases []string
	DstType []DataType
	depth   int
}

func (u *Unnest) Depth() int {
	if u != nil {
		return u.depth
	}
	return 0
}

func (u *Unnest) UpdateDepthForTests() int {
	if u != nil {
		u.depth = 1 + max(
			CallUpdateDepthForTests(u.Expr),
			len(u.DstType),
		)
		return u.depth
	}
	return 0
}

// output example: UNNEST(Expr) AS(key1, value1)
func (u *Unnest) String() string { return u.RenderBytes(&bytes.Buffer{}, nil).String() }

func (u *Unnest) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if u.Expr != nil {
		_, _ = buf.WriteString("UNNEST(")
		_ = u.Expr.RenderBytes(buf, posmap)
		_, _ = buf.WriteString(")")
	}

	if len(u.Aliases) != 0 {
		buf.WriteString(" AS(")
		for i, t := range u.Aliases {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(t)
		}
		buf.WriteString(")")
	}

	if posmap != nil {
		posmap[u] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (u *Unnest) Clone() *Unnest {
	expr, err := ParseExpr(u.Expr.String())
	if err != nil {
		return nil
	}
	clone := &Unnest{
		Expr:  expr,
		depth: 1 + expr.Depth(),
	}
	clone.Aliases = make([]string, 0, len(u.Aliases))
	clone.Aliases = append(clone.Aliases, u.Aliases...)
	clone.DstType = make([]DataType, 0, len(u.DstType))
	clone.DstType = append(clone.DstType, u.DstType...)

	return clone
}

func (u *Unnest) IsUnnestField(field string) bool {
	for _, alias := range u.Aliases {
		if alias == field {
			return true
		}
	}
	return false
}

func (u *Unnest) GetName() string {
	return ""
}

type LogPipeStatement struct {
	Cond   Expr
	Unnest *Unnest
	depth  int
}

func (s *LogPipeStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *LogPipeStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.Cond),
			CallUpdateDepthForTests(s.Unnest),
		)
		return s.depth
	}
	return 0
}

func (s *LogPipeStatement) stmt() {}

func (s *LogPipeStatement) node() {}

func (s *LogPipeStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *LogPipeStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *LogPipeStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	if s.Cond != nil {
		_ = s.Cond.RenderBytes(buf, posmap)
	}
	if s.Unnest != nil {
		_, _ = buf.WriteString("|")
		_ = s.Unnest.RenderBytes(buf, posmap)
	}

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

type Scroll struct {
	Timeout   int
	Scroll    string
	Scroll_id string
}

// IsExactStatisticQueryForDDL used to check whether the query is based on the exact time range.
func IsExactStatisticQueryForDDL(stmt Statement) bool {
	if stmt == nil {
		return false
	}
	var hints Hints
	switch s := stmt.(type) {
	case *ShowSeriesStatement:
		hints = s.Hints
	case *ShowTagValuesStatement:
		hints = s.Hints
	default:
		return false
	}
	for _, hint := range hints {
		if hint.String() == ExactStatisticQuery {
			return true
		}
	}
	return false
}

type WithSelectStatement struct {
	CTEs                  CTES
	Query                 *SelectStatement
	DependenciesCTEsAlias []string
	depth                 int
}

type CTE struct {
	Query                *SelectStatement
	GraphQuery           *GraphStatement
	Alias                string
	TimeRange            TimeRange   // use for Prepare MapShard for withCondition.stmt
	Csming               interface{} // same as shardMapping, use for mapShard of with Condition stmt
	DependenciesCTEAlias []string
	ReqId                string
	FMapper              FieldMapper
	depth                int
}

func (c *CTE) Depth() int {
	if c != nil {
		return c.depth
	}
	return 0
}

func (c *CTE) UpdateDepthForTests() int {
	if c != nil {
		c.depth = 1 + max(
			CallUpdateDepthForTests(c.Query),
			CallUpdateDepthForTests(c.GraphQuery),
		)
		return c.depth
	}
	return 0
}

func (c *CTE) String() string { return c.RenderBytes(&bytes.Buffer{}, nil).String() }

func (c *CTE) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("(")
	if c.GraphQuery != nil {
		_ = c.GraphQuery.RenderBytes(buf, posmap)
	} else {
		_ = c.Query.RenderBytes(buf, posmap)
	}
	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[c] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (c *CTE) Clone() *CTE {
	clone := &CTE{
		Alias:     c.Alias,
		TimeRange: c.TimeRange,
		Csming:    c.Csming,
		ReqId:     c.ReqId,
		depth:     c.depth,
	}
	if c.Query != nil {
		clone.Query = c.Query.Clone()
	}
	if c.GraphQuery != nil {
		clone.GraphQuery = c.GraphQuery.Clone()
	}
	return clone
}

func (c *CTE) GetName() string {
	return c.Alias
}

type CTES []*CTE

func (cs CTES) UpdateDepthForTests() int {
	if cs != nil {
		depth := 0
		for i, c := range cs {
			depth = max(depth, 1+i+CallUpdateDepthForTests(c))
		}
		return 1 + depth
	}
	return 0
}

type ctesList struct {
	ctes  CTES
	depth int
}

func (c ctesList) Depth() int { return c.depth }

func (s *WithSelectStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *WithSelectStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.CTEs),
			CallUpdateDepthForTests(s.Query),
		)
		return s.depth
	}
	return 0
}

func (s *WithSelectStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *WithSelectStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("With ")
	for _, cte := range s.CTEs {
		_, _ = buf.WriteString(cte.Alias)
		_, _ = buf.WriteString(" AS (")
		if cte.Query != nil {
			_ = cte.Query.RenderBytes(buf, posmap)
		} else if cte.GraphQuery != nil {
			_ = cte.GraphQuery.RenderBytes(buf, posmap)
		}
		_, _ = buf.WriteString(" ),")
	}
	_ = s.Query.RenderBytes(buf, posmap)

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *WithSelectStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

type TagSet struct {
	Key, Value string
}

type TagSets []TagSet

func (a TagSets) Len() int { return len(a) }

func (a TagSets) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a TagSets) Less(i, j int) bool {
	ki, kj := a[i].Key, a[j].Key
	if ki == kj {
		return a[i].Value < a[j].Value
	}
	return ki < kj
}

type TableTagSets struct {
	Name   string
	Values TagSets
}

type TablesTagSets []TableTagSets

func (a TablesTagSets) Len() int { return len(a) }

func (a TablesTagSets) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a TablesTagSets) Less(i, j int) bool { return a[i].Name < a[j].Name }

func (stmt *ShowTagValuesStatement) RewriteShowTagValStmt(in *InCondition) (*ShowTagValuesStatement, error) {
	if len(in.Stmt.Fields) == 0 || len(in.Stmt.Sources) == 0 {
		return nil, errors.New("convert show tag value statements fail")
	}
	ref, ok := in.Stmt.Fields[0].Expr.(*VarRef)
	if !ok {
		return nil, errors.New("type assertion fail")
	}
	s := ref.Val
	measurement, ok := in.Stmt.Sources[0].(*Measurement)
	if !ok {
		return nil, errors.New("type assertion fail")
	}
	d := measurement.Database
	stmt = &ShowTagValuesStatement{
		Database: d,
		Sources:  in.Stmt.Sources,
		Op:       EQ,
		TagKeyExpr: &ListLiteral{
			Vals: []string{s},
		},
		TagKeyCondition: nil,
		depth:           3, // sources are Measurements only
	}
	if in.TimeCond != nil {
		stmt.Condition = in.TimeCond
		stmt.depth = max(stmt.depth, 1+stmt.Condition.Depth())
		stringLiteral := &StringLiteral{
			Val: "exact_statistic_query",
		}
		hint := &Hint{
			Expr: stringLiteral,
		}
		stmt.Hints = Hints{hint}
	} else {
		stmt.Condition = nil
	}
	return stmt, nil
}

type GraphStatement struct {
	NodeCondition       Expr
	EdgeCondition       Expr
	AdditionalCondition Expr
	HopNum              int
	StartNodeId         string
	depth               int
}

func (s *GraphStatement) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *GraphStatement) UpdateDepthForTests() int {
	if s != nil {
		s.depth = 1 + max(
			CallUpdateDepthForTests(s.NodeCondition),
			CallUpdateDepthForTests(s.EdgeCondition),
		)
		return s.depth
	}
	return 0
}

func (s *GraphStatement) String() string { return s.RenderBytes(&bytes.Buffer{}, nil).String() }

func (s *GraphStatement) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()
	if s.NodeCondition == nil && s.EdgeCondition != nil {
		_, _ = fmt.Fprintf(buf, "%d %s ", s.HopNum, s.StartNodeId)
		_ = s.EdgeCondition.RenderBytes(buf, posmap)
	} else if s.EdgeCondition == nil && s.NodeCondition != nil {
		_, _ = fmt.Fprintf(buf, "%d %s ", s.HopNum, s.StartNodeId)
		_ = s.NodeCondition.RenderBytes(buf, posmap)
	} else if s.NodeCondition == nil && s.EdgeCondition == nil {
		_, _ = fmt.Fprintf(buf, "%d %s", s.HopNum, s.StartNodeId)
	} else {
		_, _ = fmt.Fprintf(buf, "%d %s ", s.HopNum, s.StartNodeId)
		_ = s.NodeCondition.RenderBytes(buf, posmap)
		_, _ = buf.WriteString(" ")
		_ = s.EdgeCondition.RenderBytes(buf, posmap)
	}
	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}

func (s *GraphStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// Clone returns a deep copy of the statement.
func (s *GraphStatement) Clone() *GraphStatement {
	clone := *s
	return &clone
}

type CypherCondition struct {
	PathName            string
	NodeCondition       Expr
	EdgeCondition       Expr
	AdditionalCondition Expr
}

type TableFunctionField struct {
	typ     int
	content string
	source  Source
	depth   int
}

func (s *TableFunctionField) Depth() int {
	if s != nil {
		return s.depth
	}
	return 0
}

func (s *TableFunctionField) GetType() int {
	return s.typ
}

func (s *TableFunctionField) GetContent() string {
	return s.content
}

func (s *TableFunctionField) GetSource() Source {
	return s.source
}

type TableFunction struct {
	FunctionName        string
	TableFunctionFields TableFunctionFields
	TableFunctionSource TableFunctionSource
	Params              string

	depth int
}

func (t *TableFunction) Depth() int {
	return t.depth
}

type TableFunctionFields []*TableFunctionField

type TableFunctionSource []Source

func (t *TableFunction) GetName() string {
	return t.FunctionName
}

func (t *TableFunction) GetTableFunctionFields() TableFunctionFields {
	return t.TableFunctionFields
}

func (t *TableFunction) GetTableFunctionSource() TableFunctionSource {
	return t.TableFunctionSource
}

func (t *TableFunction) SetTableFunctionSource(sources []Source) {
	t.TableFunctionSource = sources
}

func (t *TableFunction) GetParams() string {
	return t.Params
}

func (t *TableFunction) String() string {
	return fmt.Sprintf("(%s)", t.GetName())
}

func (t *TableFunction) UpdateDepthForTests() int {
	return 0
}

func (s *TableFunction) RenderBytes(buf *bytes.Buffer, posmap BufPositionsMap) *bytes.Buffer {
	Begin := buf.Len()

	_, _ = buf.WriteString("tablefunctionname:(")
	_, _ = buf.WriteString(s.FunctionName)
	_, _ = buf.WriteString(")")

	_, _ = buf.WriteString(" TableFunctionSource:(")
	for _, tableFunctionSource := range s.TableFunctionSource {
		_, _ = buf.WriteString(tableFunctionSource.GetName())
	}
	_, _ = buf.WriteString(")")

	_, _ = buf.WriteString(" param:(")
	_, _ = buf.WriteString(s.Params)
	_, _ = buf.WriteString(")")

	if posmap != nil {
		posmap[s] = Position{Begin: Begin, End: buf.Len()}
	}
	return buf
}
