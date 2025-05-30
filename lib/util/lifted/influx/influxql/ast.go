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
	POW_OP   = 57501
	ATAN2    = "ATAN2"
	ATAN2_OP = 57502
)

// DataType represents the primitive data types available in InfluxQL.
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

// Node represents a node in the InfluxDB abstract syntax tree.
type Node interface {
	// node is unexported to ensure implementations of Node
	// can only originate in this package.
	node()
	String() string
}

func (*Query) node()     {}
func (Statements) node() {}

func (*AlterRetentionPolicyStatement) node()       {}
func (*CreateContinuousQueryStatement) node()      {}
func (*CreateDatabaseStatement) node()             {}
func (*CreateMeasurementStatement) node()          {}
func (*AlterShardKeyStatement) node()              {}
func (*CreateRetentionPolicyStatement) node()      {}
func (*CreateSubscriptionStatement) node()         {}
func (*CreateUserStatement) node()                 {}
func (*Distinct) node()                            {}
func (*DeleteSeriesStatement) node()               {}
func (*DeleteStatement) node()                     {}
func (*DropContinuousQueryStatement) node()        {}
func (*DropDatabaseStatement) node()               {}
func (*DropMeasurementStatement) node()            {}
func (*DropRetentionPolicyStatement) node()        {}
func (*DropSeriesStatement) node()                 {}
func (*DropShardStatement) node()                  {}
func (*DropSubscriptionStatement) node()           {}
func (*DropUserStatement) node()                   {}
func (*ExplainStatement) node()                    {}
func (*GrantStatement) node()                      {}
func (*GrantAdminStatement) node()                 {}
func (*KillQueryStatement) node()                  {}
func (*RevokeStatement) node()                     {}
func (*RevokeAdminStatement) node()                {}
func (*SelectStatement) node()                     {}
func (*SetPasswordUserStatement) node()            {}
func (*ShowContinuousQueriesStatement) node()      {}
func (*ShowGrantsForUserStatement) node()          {}
func (*ShowDatabasesStatement) node()              {}
func (*ShowFieldKeyCardinalityStatement) node()    {}
func (*ShowFieldKeysStatement) node()              {}
func (*ShowRetentionPoliciesStatement) node()      {}
func (*ShowMeasurementCardinalityStatement) node() {}
func (*ShowMeasurementsStatement) node()           {}
func (*ShowMeasurementsDetailStatement) node()     {}
func (*ShowQueriesStatement) node()                {}
func (*ShowSeriesStatement) node()                 {}
func (*ShowSeriesCardinalityStatement) node()      {}
func (*ShowShardGroupsStatement) node()            {}
func (*ShowShardsStatement) node()                 {}
func (*ShowStatsStatement) node()                  {}
func (*ShowSubscriptionsStatement) node()          {}
func (*ShowDiagnosticsStatement) node()            {}
func (*ShowTagKeyCardinalityStatement) node()      {}
func (*ShowTagKeysStatement) node()                {}
func (*ShowTagValuesCardinalityStatement) node()   {}
func (*ShowTagValuesStatement) node()              {}
func (*ShowUsersStatement) node()                  {}
func (*WithSelectStatement) node()                 {}

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
func (*SortField) node()                   {}
func (SortFields) node()                   {}
func (Sources) node()                      {}
func (*StringLiteral) node()               {}
func (*SubQuery) node()                    {}
func (*Join) node()                        {}
func (*Target) node()                      {}
func (*TimeLiteral) node()                 {}
func (*VarRef) node()                      {}
func (*Wildcard) node()                    {}
func (*PrepareSnapshotStatement) node()    {}
func (*EndPrepareSnapshotStatement) node() {}
func (*GetRuntimeInfoStatement) node()     {}
func (*Hint) node()                        {}
func (Hints) node()                        {}
func (*MatchExpr) node()                   {}
func (*InCondition) node()                 {}
func (*Unnest) node()                      {}
func (*BinOp) node()                       {}
func (*CTE) node()                         {}
func (CTES) node()                         {}

// Query represents a collection of ordered statements.
type Query struct {
	Statements Statements
}

// String returns a string representation of the query.
func (q *Query) String() string { return q.Statements.String() }

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

// String returns a string representation of the statements.
func (a Statements) String() string {
	var str []string
	for _, stmt := range a {
		str = append(str, stmt.String())
	}
	return strings.Join(str, ";\n")
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

func (*AlterRetentionPolicyStatement) stmt()       {}
func (*CreateContinuousQueryStatement) stmt()      {}
func (*CreateDatabaseStatement) stmt()             {}
func (*CreateMeasurementStatement) stmt()          {}
func (*AlterShardKeyStatement) stmt()              {}
func (*CreateRetentionPolicyStatement) stmt()      {}
func (*CreateSubscriptionStatement) stmt()         {}
func (*CreateUserStatement) stmt()                 {}
func (*DeleteSeriesStatement) stmt()               {}
func (*DeleteStatement) stmt()                     {}
func (*DropContinuousQueryStatement) stmt()        {}
func (*DropDatabaseStatement) stmt()               {}
func (*DropMeasurementStatement) stmt()            {}
func (*DropRetentionPolicyStatement) stmt()        {}
func (*DropSeriesStatement) stmt()                 {}
func (*DropSubscriptionStatement) stmt()           {}
func (*DropUserStatement) stmt()                   {}
func (*ExplainStatement) stmt()                    {}
func (*GrantStatement) stmt()                      {}
func (*GrantAdminStatement) stmt()                 {}
func (*KillQueryStatement) stmt()                  {}
func (*ShowContinuousQueriesStatement) stmt()      {}
func (*ShowGrantsForUserStatement) stmt()          {}
func (*ShowDatabasesStatement) stmt()              {}
func (*ShowFieldKeyCardinalityStatement) stmt()    {}
func (*ShowFieldKeysStatement) stmt()              {}
func (*ShowMeasurementCardinalityStatement) stmt() {}
func (*ShowMeasurementsStatement) stmt()           {}
func (*ShowMeasurementsDetailStatement) stmt()     {}
func (*ShowQueriesStatement) stmt()                {}
func (*ShowRetentionPoliciesStatement) stmt()      {}
func (*ShowSeriesStatement) stmt()                 {}
func (*ShowSeriesCardinalityStatement) stmt()      {}
func (*ShowShardGroupsStatement) stmt()            {}
func (*ShowShardsStatement) stmt()                 {}
func (*ShowStatsStatement) stmt()                  {}
func (*DropShardStatement) stmt()                  {}
func (*ShowSubscriptionsStatement) stmt()          {}
func (*ShowDiagnosticsStatement) stmt()            {}
func (*ShowTagKeyCardinalityStatement) stmt()      {}
func (*ShowTagKeysStatement) stmt()                {}
func (*ShowTagValuesCardinalityStatement) stmt()   {}
func (*ShowTagValuesStatement) stmt()              {}
func (*ShowUsersStatement) stmt()                  {}
func (*RevokeStatement) stmt()                     {}
func (*RevokeAdminStatement) stmt()                {}
func (*SelectStatement) stmt()                     {}
func (*SetPasswordUserStatement) stmt()            {}
func (*PrepareSnapshotStatement) stmt()            {}
func (*EndPrepareSnapshotStatement) stmt()         {}
func (*GetRuntimeInfoStatement) stmt()             {}
func (*WithSelectStatement) stmt()                 {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
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

func (*BooleanLiteral) literal()  {}
func (*BoundParameter) literal()  {}
func (*DurationLiteral) literal() {}
func (*IntegerLiteral) literal()  {}
func (*UnsignedLiteral) literal() {}
func (*NilLiteral) literal()      {}
func (*NumberLiteral) literal()   {}
func (*RegexLiteral) literal()    {}
func (*ListLiteral) literal()     {}
func (*StringLiteral) literal()   {}
func (*TimeLiteral) literal()     {}

// Source represents a source of data for a statement.
type Source interface {
	Node
	// source is unexported to ensure implementations of Source
	// can only originate in this package.
	source()
	GetName() string
}

func (*Measurement) source() {}
func (*SubQuery) source()    {}
func (*Join) source()        {}
func (*Unnest) source()      {}
func (*BinOp) source()       {}

// Sources represents a list of sources.
type Sources []Source

// String returns a string representation of a Sources array.
func (a Sources) String() string {
	var buf bytes.Buffer

	ubound := len(a) - 1
	for i, src := range a {
		_, _ = buf.WriteString(src.String())
		if i < ubound {
			_, _ = buf.WriteString(", ")
		}
	}

	return buf.String()
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
		case *Join:
			return true
		case *BinOp:
			return true
		}
	}
	return false
}

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

// String returns a string representation of a sort field.
func (field *SortField) String() string {
	var buf bytes.Buffer
	if field.Name != "" {
		_, _ = buf.WriteString(field.Name)
		_, _ = buf.WriteString(" ")
	}
	if field.Ascending {
		_, _ = buf.WriteString("ASC")
	} else {
		_, _ = buf.WriteString("DESC")
	}
	return buf.String()
}

// SortFields represents an ordered list of ORDER BY fields.
type SortFields []*SortField

// String returns a string representation of sort fields.
func (a SortFields) String() string {
	fields := make([]string, 0, len(a))
	for _, field := range a {
		fields = append(fields, field.String())
	}
	return strings.Join(fields, ", ")
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

	ReplicaNum uint32

	RetentionPolicyHotDuration time.Duration

	RetentionPolicyWarmDuration time.Duration

	RetentionPolicyIndexGroupDuration time.Duration

	ShardKey []string

	DatabaseAttr DatabasePolicy
}

// String returns a string representation of the create database statement.
func (s *CreateDatabaseStatement) String() string {
	var buf bytes.Buffer
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
		if s.RetentionPolicyName != "" {
			_, _ = buf.WriteString(" NAME ")
			_, _ = buf.WriteString(QuoteIdent(s.RetentionPolicyName))
		}
	}

	return buf.String()
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
	var buf bytes.Buffer
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

	return buf.String()
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

func (s *AlterShardKeyStatement) String() string {
	var buf bytes.Buffer
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

	return buf.String()
}

func (s *AlterShardKeyStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// DropDatabaseStatement represents a command to drop a database.
type DropDatabaseStatement struct {
	// Name of the database to be dropped.
	Name string
}

// String returns a string representation of the drop database statement.
func (s *DropDatabaseStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP DATABASE ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	return buf.String()
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

// String returns a string representation of the drop retention policy statement.
func (s *DropRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP RETENTION POLICY ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.Database))
	return buf.String()
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

// String returns a string representation of the create user statement.
func (s *CreateUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("CREATE USER ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" WITH PASSWORD ")
	_, _ = buf.WriteString("[REDACTED]")
	if s.Admin {
		_, _ = buf.WriteString(" WITH ALL PRIVILEGES")
	}
	return buf.String()
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

// String returns a string representation of the drop user statement.
func (s *DropUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP USER ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	return buf.String()
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

// String returns a string representation of the grant statement.
func (s *GrantStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("GRANT ")
	_, _ = buf.WriteString(s.Privilege.String())
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.On))
	_, _ = buf.WriteString(" TO ")
	_, _ = buf.WriteString(QuoteIdent(s.User))
	return buf.String()
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

// String returns a string representation of the grant admin statement.
func (s *GrantAdminStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("GRANT ALL PRIVILEGES TO ")
	_, _ = buf.WriteString(QuoteIdent(s.User))
	return buf.String()
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

// String returns a string representation of the kill query statement.
func (s *KillQueryStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("KILL QUERY ")
	_, _ = buf.WriteString(strconv.FormatUint(s.QueryID, 10))
	if s.Host != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Host))
	}
	return buf.String()
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

// String returns a string representation of the set password statement.
func (s *SetPasswordUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SET PASSWORD FOR ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	_, _ = buf.WriteString(" = ")
	_, _ = buf.WriteString("[REDACTED]")
	return buf.String()
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

// String returns a string representation of the revoke statement.
func (s *RevokeStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("REVOKE ")
	_, _ = buf.WriteString(s.Privilege.String())
	_, _ = buf.WriteString(" ON ")
	_, _ = buf.WriteString(QuoteIdent(s.On))
	_, _ = buf.WriteString(" FROM ")
	_, _ = buf.WriteString(QuoteIdent(s.User))
	return buf.String()
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

// String returns a string representation of the revoke admin statement.
func (s *RevokeAdminStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("REVOKE ALL PRIVILEGES FROM ")
	_, _ = buf.WriteString(QuoteIdent(s.User))
	return buf.String()
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

	// Index Duration
	IndexGroupDuration time.Duration
}

// String returns a string representation of the create retention policy.
func (s *CreateRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
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

	if s.HotDuration > 0 {
		_, _ = buf.WriteString(" HOT DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.HotDuration))
	}

	if s.WarmDuration > 0 {
		_, _ = buf.WriteString(" WARM DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.WarmDuration))
	}

	if s.IndexGroupDuration > 0 {
		_, _ = buf.WriteString(" INDEX DURATION ")
		_, _ = buf.WriteString(FormatDuration(s.IndexGroupDuration))
	}
	if s.Default {
		_, _ = buf.WriteString(" DEFAULT")
	}
	return buf.String()
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

	// Index Duration
	IndexGroupDuration *time.Duration
}

// String returns a string representation of the alter retention policy statement.
func (s *AlterRetentionPolicyStatement) String() string {
	var buf bytes.Buffer
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

	if s.IndexGroupDuration != nil {
		_, _ = buf.WriteString(" INDEX DURATION ")
		_, _ = buf.WriteString(FormatDuration(*s.IndexGroupDuration))
	}

	if s.Default {
		_, _ = buf.WriteString(" DEFAULT")
	}

	return buf.String()
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
			},
		}
	}
	for _, f := range s.Fields {
		clone.Fields = append(clone.Fields, &Field{Expr: CloneExpr(f.Expr), Alias: f.Alias})
	}
	for _, d := range s.Dimensions {
		clone.Dimensions = append(clone.Dimensions, &Dimension{Expr: CloneExpr(d.Expr)})
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
		return &SubQuery{Statement: s.Statement.Clone(), Alias: s.Alias}
	case *Join:
		c := &Join{}
		c.LSrc = cloneSource(s.LSrc)
		c.RSrc = cloneSource(s.RSrc)
		c.Condition = CloneExpr(s.Condition)
		return c
	case *Unnest:
		return s.Clone()
	case *BinOp:
		c := &BinOp{}
		c.LSrc = cloneSource(s.LSrc)
		c.RSrc = cloneSource(s.RSrc)
		c.LExpr = CloneExpr(s.LExpr)
		c.RExpr = CloneExpr(s.RExpr)
		c.OpType = s.OpType
		c.On = s.On
		c.MatchKeys = s.MatchKeys
		c.MatchCard = s.MatchCard
		c.IncludeKeys = s.IncludeKeys
		c.ReturnBool = s.ReturnBool
		c.NilMst = s.NilMst
		return c
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
		return errors.New("expect measurement type")
	}
	for i := range fields {
		fields[i].Expr.RewriteNameSpace(s.Alias, s.Name)
		if f, ok := fields[i].Expr.(*VarRef); ok && len(fields[i].Alias) == 0 {
			if len(f.Alias) > 0 {
				fields[i].Alias = f.Alias
			} else if hasJoin && alias != "" {
				fields[i].Alias = alias + "." + f.Val
			}
		}
	}
	return nil
}

func RewriteSubQueryNameSpace(source Source, hasJoin bool) error {
	s, ok := source.(*SubQuery)
	if !ok {
		return errors.New("expect measurement type")
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
		}
	}
	return nil
}

func (s *SelectStatement) RewriteJoinCase(src Source, m FieldMapper, batchEn bool, fields Fields, hasJoin bool) (Source, error) {
	switch src := src.(type) {
	case *SubQuery:
		if e := RewriteSubQueryNameSpace(src, hasJoin); e != nil {
			return nil, e
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
		s.Fields = append(s.Fields, &Field{Expr: &VarRef{Val: tagKey, Type: Tag}})
	}
	return nil
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
	for _, src := range other.Sources {
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
			lSources, lErr := s.RewriteJoinCase(src.LSrc, m, batchEn, other.Fields, true)
			if lErr != nil {
				return nil, lErr
			}
			sources = append(sources, lSources)
			rSources, rErr := s.RewriteJoinCase(src.RSrc, m, batchEn, other.Fields, true)
			if rErr != nil {
				return nil, rErr
			}
			sources = append(sources, rSources)
		case *BinOp:
			if src.NilMst != LNilMst {
				lSources, lErr := s.RewriteJoinCase(src.LSrc, m, batchEn, other.Fields, false)
				if lErr != nil {
					return nil, lErr
				}
				sources = append(sources, lSources)
			}
			if src.NilMst != RNilMst {
				rSources, rErr := s.RewriteJoinCase(src.RSrc, m, batchEn, other.Fields, false)
				if rErr != nil {
					return nil, rErr
				}
				sources = append(sources, rSources)
			}
		default:
			if e := RewriteMstNameSpace(other.Fields, src, hasJoin, src.GetName()); e != nil {
				return nil, e
			}
			sources = append(sources, src)
		}
	}
	if len(sources) == 0 {
		if !s.ReturnErr {
			return nil, ErrDeclareEmptyCollection
		}
		return nil, errno.NewError(errno.NoFieldSelected, "source")
	}

	if in, ok := s.Condition.(*InCondition); ok {
		stmt, err := in.Stmt.RewriteFields(m, batchEn, hasJoin)
		if err != nil && err != ErrDeclareEmptyCollection {
			return nil, err
		}
		in.Stmt = stmt
		other.Condition = in
	}

	if s.ReturnErr {
		if err := s.checkField(m, sources); err != nil {
			return nil, err
		}
	}

	other.Sources = sources

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

	// Rewrite all variable references in the fields with their types if one
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

	// Rewrite all variable references in the fields with their types if one
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
	if hasFieldWildcard {
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
					rwFields = append(rwFields, &Field{Expr: &VarRef{Val: ref.Val, Type: ref.Type}})
				}
			case *RegexLiteral:
				for _, ref := range fields {
					if expr.Val.MatchString(ref.Val) {
						rwFields = append(rwFields, &Field{Expr: &VarRef{Val: ref.Val, Type: ref.Type}})
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

				// All types that can expand wildcards support float, integer, and unsigned.
				supportedTypes := map[DataType]struct{}{
					Float:    {},
					Integer:  {},
					Unsigned: {},
				}

				// Add additional types for certain functions.
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
					rwDimensions = append(rwDimensions, &Dimension{Expr: &VarRef{Val: name}})
				}
			case *RegexLiteral:
				for _, name := range dimensions {
					if expr.Val.MatchString(name) {
						rwDimensions = append(rwDimensions, &Dimension{Expr: &VarRef{Val: name}})
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

func (s *SelectStatement) RewriteRegexConditionsDFS() {
	s.RewriteRegexConditions()
	for _, s := range s.Sources {
		if subquery, ok := s.(*SubQuery); ok {
			subquery.Statement.RewriteRegexConditions()
		}
	}
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
func (s *SelectStatement) RewriteRegexConditions() {
	s.Condition = RewriteExpr(s.Condition, func(e Expr) Expr {
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
				Op:  be.Op,
				LHS: be.LHS,
				RHS: &StringLiteral{Val: vals[0]},
			}
			for i := 1; i < len(vals); i++ {
				expr = &BinaryExpr{
					Op:  concatOp,
					LHS: expr,
					RHS: &BinaryExpr{
						Op:  be.Op,
						LHS: be.LHS,
						RHS: &StringLiteral{Val: vals[i]},
					},
				}
			}
			return &ParenExpr{Expr: expr}
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
			nst := &SelectStatement{Condition: s.Condition, OmitTime: true}
			nst.Sources = append(nst.Sources, s.Sources...)
			for _, tagArg := range call.Args[1 : len(call.Args)-1] {
				nst.Dimensions = append(nst.Dimensions, &Dimension{Expr: tagArg})
				auxFields = append(auxFields, &Field{Expr: tagArg})
			}
			callField := &Field{
				Expr:  &Call{Args: []Expr{call.Args[0]}},
				Alias: call.Args[0].(*VarRef).Val,
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
			s.Sources = append(s.Sources, &SubQuery{Statement: nst})

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
						columnFields = append(columnFields, &Field{Expr: ref})
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
func (s *SelectStatement) FieldExprByName(name string) (int, Expr) {
	for i, f := range s.Fields {
		if f.Name() == name {
			return i, f.Expr
		} else if call, ok := f.Expr.(*Call); ok && (call.Name == "top" || call.Name == "bottom") && len(call.Args) > 2 {
			for _, arg := range call.Args[1 : len(call.Args)-1] {
				if arg, ok := arg.(*VarRef); ok && arg.Val == name {
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
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SELECT ")

	if len(s.Hints) > 0 {
		_, _ = buf.WriteString("/*+ ")
		for _, hit := range s.Hints {
			_, _ = buf.WriteString(hit.String())
			_, _ = buf.WriteString(" ")
		}
		_, _ = buf.WriteString("*/")
	}

	_, _ = buf.WriteString(s.Fields.String())

	if s.Target != nil {
		_, _ = buf.WriteString(" ")
		_, _ = buf.WriteString(s.Target.String())
	}
	if len(s.Sources) > 0 {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	switch s.Fill {
	case NoFill:
		_, _ = buf.WriteString(" fill(none)")
	case NumberFill:
		_, _ = buf.WriteString(fmt.Sprintf(" fill(%v)", s.FillValue))
	case LinearFill:
		_, _ = buf.WriteString(" fill(linear)")
	case PreviousFill:
		_, _ = buf.WriteString(" fill(previous)")
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	if s.SLimit > 0 {
		_, _ = fmt.Fprintf(&buf, " SLIMIT %d", s.SLimit)
	}
	if s.SOffset > 0 {
		_, _ = fmt.Fprintf(&buf, " SOFFSET %d", s.SOffset)
	}
	if s.Location != nil {
		_, _ = fmt.Fprintf(&buf, ` TZ('%s')`, s.Location)
	}
	if len(s.PromSubCalls) > 0 {
		_, _ = buf.WriteString(" SUBCALL ")
		for _, subCall := range s.PromSubCalls {
			_, _ = buf.WriteString(subCall.String())
		}
	}
	return buf.String()
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
}

// String returns a string representation of the Target.
func (t *Target) String() string {
	if t == nil {
		return ""
	}

	var buf bytes.Buffer
	_, _ = buf.WriteString("INTO ")
	_, _ = buf.WriteString(t.Measurement.String())
	if t.Measurement.Name == "" {
		_, _ = buf.WriteString(":MEASUREMENT")
	}

	return buf.String()
}

// ExplainStatement represents a command for explaining a select statement.
type ExplainStatement struct {
	Statement *SelectStatement

	Analyze bool
}

// String returns a string representation of the explain statement.
func (e *ExplainStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("EXPLAIN ")
	if e.Analyze {
		buf.WriteString("ANALYZE ")
	}
	buf.WriteString(e.Statement.String())
	return buf.String()
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
}

// String returns a string representation of the delete statement.
func (s *DeleteStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DELETE FROM ")
	_, _ = buf.WriteString(s.Source.String())
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	return buf.String()
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
}

// String returns a string representation of the list series statement.
func (s *ShowSeriesStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW ")

	if len(s.Hints) > 0 {
		_, _ = buf.WriteString("/*+ ")
		for _, hit := range s.Hints {
			_, _ = buf.WriteString(hit.String())
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
		_, _ = buf.WriteString(s.Sources.String())
	}

	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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
}

// String returns a string representation of the drop series statement.
func (s *DropSeriesStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP SERIES")

	if s.Sources != nil {
		buf.WriteString(" FROM ")
		buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		buf.WriteString(" WHERE ")
		buf.WriteString(s.Condition.String())
	}

	return buf.String()
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
}

// String returns a string representation of the delete series statement.
func (s *DeleteSeriesStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DELETE")

	if s.Sources != nil {
		buf.WriteString(" FROM ")
		buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		buf.WriteString(" WHERE ")
		buf.WriteString(s.Condition.String())
	}

	return buf.String()
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

// String returns a string representation of the drop series statement.
func (s *DropShardStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("DROP SHARD ")
	buf.WriteString(strconv.FormatUint(s.ID, 10))
	return buf.String()
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
}

// String returns a string representation of the show continuous queries statement.
func (s *ShowSeriesCardinalityStatement) String() string {
	var buf bytes.Buffer
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
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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

// String returns a string representation of the show continuous queries statement.
func (s *ShowContinuousQueriesStatement) String() string { return "SHOW CONTINUOUS QUERIES" }

// RequiredPrivileges returns the privilege required to execute a ShowContinuousQueriesStatement.
func (s *ShowContinuousQueriesStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: ReadPrivilege}}, nil
}

// ShowGrantsForUserStatement represents a command for listing user privileges.
type ShowGrantsForUserStatement struct {
	// Name of the user to display privileges.
	Name string
}

// String returns a string representation of the show grants for user.
func (s *ShowGrantsForUserStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW GRANTS FOR ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	return buf.String()
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

// String returns a string representation of the show databases command.
func (s *ShowDatabasesStatement) String() string { return "SHOW DATABASES" }

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
}

// String returns a string representation of the statement.
func (s *CreateContinuousQueryStatement) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CREATE CONTINUOUS QUERY %s ON %s ", QuoteIdent(s.Name), QuoteIdent(s.Database))

	if s.ResampleEvery > 0 || s.ResampleFor > 0 {
		buf.WriteString("RESAMPLE ")
		if s.ResampleEvery > 0 {
			fmt.Fprintf(&buf, "EVERY %s ", FormatDuration(s.ResampleEvery))
		}
		if s.ResampleFor > 0 {
			fmt.Fprintf(&buf, "FOR %s ", FormatDuration(s.ResampleFor))
		}
	}
	fmt.Fprintf(&buf, "BEGIN %s END", s.Source.String())
	return buf.String()
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

// String returns a string representation of the statement.
func (s *DropContinuousQueryStatement) String() string {
	return fmt.Sprintf("DROP CONTINUOUS QUERY %s ON %s", QuoteIdent(s.Name), QuoteIdent(s.Database))
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
}

// String returns a string representation of the statement.
func (s *ShowMeasurementCardinalityStatement) String() string {
	var buf bytes.Buffer
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
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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
}

// String returns a string representation of the statement.
func (s *ShowMeasurementsStatement) String() string {
	var buf bytes.Buffer
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
		_, _ = buf.WriteString(s.Source.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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
}

// String returns a string representation of the statement.
func (s *ShowMeasurementsDetailStatement) String() string {
	var buf bytes.Buffer
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
		_, _ = buf.WriteString(s.Source.String())
	}
	return buf.String()
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

// String returns a string representation of the drop measurement statement.
func (s *DropMeasurementStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP MEASUREMENT ")
	if s.RpName != "" {
		_, _ = buf.WriteString(QuoteIdent(s.RpName))
		_, _ = buf.WriteString(".")
	}
	_, _ = buf.WriteString(QuoteIdent(s.Name))
	return buf.String()
}

// RequiredPrivileges returns the privilege(s) required to execute a DropMeasurementStatement
func (s *DropMeasurementStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// ShowQueriesStatement represents a command for listing all running queries.
type ShowQueriesStatement struct{}

// String returns a string representation of the show queries statement.
func (s *ShowQueriesStatement) String() string {
	return "SHOW QUERIES"
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

// String returns a string representation of a ShowRetentionPoliciesStatement.
func (s *ShowRetentionPoliciesStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW RETENTION POLICIES")
	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	return buf.String()
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

// String returns a string representation of a ShowStatsStatement.
func (s *ShowStatsStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW STATS")
	if s.Module != "" {
		_, _ = buf.WriteString(" FOR ")
		_, _ = buf.WriteString(QuoteString(s.Module))
	}
	return buf.String()
}

// RequiredPrivileges returns the privilege(s) required to execute a ShowStatsStatement
func (s *ShowStatsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// ShowShardGroupsStatement represents a command for displaying shard groups in the cluster.
type ShowShardGroupsStatement struct{}

// String returns a string representation of the SHOW SHARD GROUPS command.
func (s *ShowShardGroupsStatement) String() string { return "SHOW SHARD GROUPS" }

// RequiredPrivileges returns the privileges required to execute the statement.
func (s *ShowShardGroupsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

// ShowShardsStatement represents a command for displaying shards in the cluster.
type ShowShardsStatement struct {
	mstInfo *Measurement
}

// String returns a string representation.
func (s *ShowShardsStatement) String() string { return "SHOW SHARDS" }

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

// String returns a string representation of the ShowDiagnosticsStatement.
func (s *ShowDiagnosticsStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW DIAGNOSTICS")
	if s.Module != "" {
		_, _ = buf.WriteString(" FOR ")
		_, _ = buf.WriteString(QuoteString(s.Module))
	}
	return buf.String()
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

// String returns a string representation of the CreateSubscriptionStatement.
func (s *CreateSubscriptionStatement) String() string {
	var buf bytes.Buffer
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

	return buf.String()
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

// String returns a string representation of the DropSubscriptionStatement.
func (s *DropSubscriptionStatement) String() string {
	if s.Name == "" {
		if s.Database == "" {
			return "DROP ALL SUBSCRIPTIONS"
		} else {
			return fmt.Sprintf(`DROP ALL SUBSCRIPTIONS ON %s`, QuoteIdent(s.Database))
		}
	}
	return fmt.Sprintf(`DROP SUBSCRIPTION %s ON %s.%s`, QuoteIdent(s.Name), QuoteIdent(s.Database), QuoteIdent(s.RetentionPolicy))
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

// String returns a string representation of the ShowSubscriptionsStatement.
func (s *ShowSubscriptionsStatement) String() string {
	return "SHOW SUBSCRIPTIONS"
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
}

// String returns a string representation of the statement.
func (s *ShowTagKeysStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW TAG KEYS")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
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
	return buf.String()
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
}

// String returns a string representation of the statement.
func (s *ShowTagKeyCardinalityStatement) String() string {
	var buf bytes.Buffer
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
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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
}

// String returns a string representation of the statement.
func (s *ShowTagValuesStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW ")

	if len(s.Hints) > 0 {
		_, _ = buf.WriteString("/*+ ")
		for _, hit := range s.Hints {
			_, _ = buf.WriteString(hit.String())
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
		_, _ = buf.WriteString(s.Sources.String())
	}
	_, _ = buf.WriteString(" WITH KEY ")
	_, _ = buf.WriteString(s.Op.String())
	_, _ = buf.WriteString(" ")
	if lit, ok := s.TagKeyExpr.(*StringLiteral); ok {
		_, _ = buf.WriteString(QuoteIdent(lit.Val))
	} else {
		_, _ = buf.WriteString(s.TagKeyExpr.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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
}

// String returns a string representation of the statement.
func (s *ShowTagValuesCardinalityStatement) String() string {
	var buf bytes.Buffer
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
		_, _ = buf.WriteString(s.Sources.String())
	}
	_, _ = buf.WriteString(" WITH KEY ")
	_, _ = buf.WriteString(s.Op.String())
	_, _ = buf.WriteString(" ")
	if lit, ok := s.TagKeyExpr.(*StringLiteral); ok {
		_, _ = buf.WriteString(QuoteIdent(lit.Val))
	} else {
		_, _ = buf.WriteString(s.TagKeyExpr.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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

// String returns a string representation of the ShowUsersStatement.
func (s *ShowUsersStatement) String() string {
	return "SHOW USERS"
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
}

// String returns a string representation of the statement.
func (s *ShowFieldKeyCardinalityStatement) String() string {
	var buf bytes.Buffer
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
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	if len(s.Dimensions) > 0 {
		_, _ = buf.WriteString(" GROUP BY ")
		_, _ = buf.WriteString(s.Dimensions.String())
	}
	if s.Limit > 0 {
		_, _ = fmt.Fprintf(&buf, " LIMIT %d", s.Limit)
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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
}

// String returns a string representation of the statement.
func (s *ShowFieldKeysStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW FIELD KEYS")

	if s.Database != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}
	if s.Sources != nil {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if len(s.SortFields) > 0 {
		_, _ = buf.WriteString(" ORDER BY ")
		_, _ = buf.WriteString(s.SortFields.String())
	}
	if s.Limit > 0 {
		_, _ = buf.WriteString(" LIMIT ")
		_, _ = buf.WriteString(strconv.Itoa(s.Limit))
	}
	if s.Offset > 0 {
		_, _ = buf.WriteString(" OFFSET ")
		_, _ = buf.WriteString(strconv.Itoa(s.Offset))
	}
	return buf.String()
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
func (a Fields) String() string {
	var str []string
	for _, f := range a {
		str = append(str, f.String())
	}
	return strings.Join(str, ", ")
}

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr  Expr
	Alias string
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
		f := Field{Expr: expr.Expr}
		return f.Name()
	case *VarRef:
		return expr.Val
	}

	// Otherwise return a blank name.
	return ""
}

// String returns a string representation of the field.
func (f *Field) String() string {
	str := f.Expr.String()

	if f.Alias == "" {
		return str
	}
	return fmt.Sprintf("%s AS %s", str, QuoteIdent(f.Alias))
}

// Len implements sort.Interface.
func (a Fields) Len() int { return len(a) }

// Less implements sort.Interface.
func (a Fields) Less(i, j int) bool { return a[i].Name() < a[j].Name() }

// Swap implements sort.Interface.
func (a Fields) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Dimensions represents a list of dimensions.
type Dimensions []*Dimension

// String returns a string representation of the dimensions.
func (a Dimensions) String() string {
	var str []string
	for _, d := range a {
		str = append(str, d.String())
	}
	return strings.Join(str, ", ")
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

// Dimension represents an expression that a select statement is grouped by.
type Dimension struct {
	Expr Expr
}

// String returns a string representation of the dimension.
func (d *Dimension) String() string { return d.Expr.String() }

type Hint struct {
	Expr Expr
}

// String returns a string representation of the dimension.
func (d *Hint) String() string { return d.Expr.(*StringLiteral).Val }

type Hints []*Hint

func (a Hints) String() string {
	var str []string
	for _, h := range a {
		str = append(str, h.String())
	}
	return strings.Join(str, ", ")
}

// Measurements represents a list of measurements.
type Measurements []*Measurement

// String returns a string representation of the measurements.
func (a Measurements) String() string {
	var str []string
	for _, m := range a {
		str = append(str, m.String())
	}
	return strings.Join(str, ", ")
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
func (m *Measurement) String() string {
	var buf bytes.Buffer
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
		_, _ = buf.WriteString(m.Regex.String())
	}

	return buf.String()
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
}

// String returns a string representation of the subquery.
func (s *SubQuery) String() string {
	return fmt.Sprintf("(%s)", s.Statement.String())
}

func (s *SubQuery) GetName() string {
	return s.Alias
}

type Join struct {
	LSrc      Source
	RSrc      Source
	Condition Expr
}

func (j *Join) String() string {
	return fmt.Sprintf("%s full join %s on %s", "1", "2", j.Condition.String())
}

func (j *Join) GetName() string {
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
}

func (b *BinOp) String() string {
	var matchKeys []byte
	for _, matchKey := range b.MatchKeys {
		matchKeys = append(matchKeys, []byte(matchKey)...)
	}
	var includeKeys []byte
	for _, includeKey := range b.IncludeKeys {
		includeKeys = append(includeKeys, []byte(includeKey)...)
	}
	if b.LSrc == nil {
		return fmt.Sprintf("%s binary op %s %t %t(%s) %d(%s)", b.LExpr.String(), b.RSrc.String(), b.ReturnBool, b.On, string(matchKeys), b.MatchCard, includeKeys)
	}
	if b.RSrc == nil {
		return fmt.Sprintf("%s binary op %s %t %t(%s) %d(%s)", b.LSrc.String(), b.RExpr.String(), b.ReturnBool, b.On, string(matchKeys), b.MatchCard, includeKeys)
	}
	return fmt.Sprintf("%s binary op %s %t %t(%s) %d(%s)", b.LSrc.String(), b.RSrc.String(), b.ReturnBool, b.On, string(matchKeys), b.MatchCard, includeKeys)
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

func (j *PromSubCall) String() string {
	return fmt.Sprintf("%s(%d, %d, %d, %d, %d, %t, %d, %d, %d) ", j.Name, j.Interval, j.StartTime, j.EndTime, j.Range, j.Offset, j.LowerStepInvariant, j.SubStartT, j.SubEndT, j.SubStep)
}

type InCondition struct {
	Stmt   *SelectStatement
	Column Expr
}

func (j *InCondition) String() string {
	return fmt.Sprintf("in condition,select statement: %s, column name: %s", j.Stmt.String(), j.Column.String())
}

func (j *InCondition) GetName() string {
	return ""
}

func (j *InCondition) RewriteNameSpace(alias, mst string) {}

// VarRef represents a reference to a variable.
type VarRef struct {
	Val   string
	Type  DataType
	Alias string
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
func (r *VarRef) String() string {
	buf := bytes.NewBufferString(QuoteIdent(r.Val))
	if r.Type != Unknown {
		buf.WriteString("::")
		buf.WriteString(r.Type.String())
	}
	return buf.String()
}

// VarRefs represents a slice of VarRef types.
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

// VarRefs represents a slice of VarRef types.
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
	Name string
	Args []Expr
}

func (c *Call) RewriteNameSpace(alias, mst string) {
	for i := range c.Args {
		c.Args[i].RewriteNameSpace(alias, mst)
	}
}

// String returns a string representation of the call.
func (c *Call) String() string {
	// Join arguments.
	var str []string
	for _, arg := range c.Args {
		str = append(str, arg.String())
	}

	// Write function name and args.
	return fmt.Sprintf("%s(%s)", c.Name, strings.Join(str, ", "))
}

func (c *Call) WriteString(b *bytes.Buffer) {
	// format fmt.Sprintf("%s(%s)", c.Name, strings.Join(str, ", "))
	b.WriteString(c.Name)
	b.WriteString("(")

	firstCall := true
	for _, arg := range c.Args {
		if firstCall {
			b.WriteString(arg.String())
			firstCall = false
			continue
		}
		b.WriteString(",")
		b.WriteString(arg.String())
	}
	b.WriteString(")")
	// Write function name and args.
	return
}

// Distinct represents a DISTINCT expression.
type Distinct struct {
	// Identifier following DISTINCT
	Val string
}

func (d *Distinct) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the expression.
func (d *Distinct) String() string {
	return fmt.Sprintf("DISTINCT %s", d.Val)
}

// NewCall returns a new call expression from this expressions.
func (d *Distinct) NewCall() *Call {
	return &Call{
		Name: "distinct",
		Args: []Expr{
			&VarRef{Val: d.Val},
		},
	}
}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val float64
}

func (l *NumberLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *NumberLiteral) String() string {
	if l.Val > math.MaxInt {
		return strconv.FormatFloat(l.Val, 'f', 1, 64)
	}
	return strconv.FormatFloat(l.Val, 'f', -1, 64)
}

// IntegerLiteral represents an integer literal.
type IntegerLiteral struct {
	Val int64
}

func (l *IntegerLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *IntegerLiteral) String() string { return fmt.Sprintf("%d", l.Val) }

// UnsignedLiteral represents an unsigned literal. The parser will only use an unsigned literal if the parsed
// integer is greater than math.MaxInt64.
type UnsignedLiteral struct {
	Val uint64
}

func (l *UnsignedLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *UnsignedLiteral) String() string { return strconv.FormatUint(l.Val, 10) }

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

func (l *BooleanLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string {
	if l.Val {
		return "true"
	}
	return "false"
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

func (s *ListLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (s *ListLiteral) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("(")
	for idx, tagKey := range s.Vals {
		if idx != 0 {
			_, _ = buf.WriteString(", ")
		}
		_, _ = buf.WriteString(QuoteIdent(tagKey))
	}
	_, _ = buf.WriteString(")")
	return buf.String()
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

func (l *StringLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string { return QuoteString(l.Val) }

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

func (l *TimeLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *TimeLiteral) String() string {
	return `'` + l.Val.UTC().Format(time.RFC3339Nano) + `'`
}

// DurationLiteral represents a duration literal.
type DurationLiteral struct {
	Val time.Duration
}

func (l *DurationLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *DurationLiteral) String() string { return FormatDuration(l.Val) }

// NilLiteral represents a nil literal.
// This is not available to the query language itself. It's only used internally.
type NilLiteral struct{}

func (l *NilLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (l *NilLiteral) String() string { return `nil` }

// BoundParameter represents a bound parameter literal.
// This is not available to the query language itself, but can be used when
// constructing a query string from an AST.
type BoundParameter struct {
	Name string
}

func (bp *BoundParameter) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the bound parameter.
func (bp *BoundParameter) String() string {
	return fmt.Sprintf("$%s", QuoteIdent(bp.Name))
}

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
	// If a comparison operator, return 0/1 rather than filtering.
	ReturnBool bool
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op.String(), e.RHS.String())
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

func (m *MatchExpr) String() string {
	switch m.Op {
	case MATCH:
		return m.Field.String() + "match" + m.Value.String()
	case MATCHPHRASE:
		return m.Field.String() + "match_phrase" + m.Value.String()
	}
	return ""
}

func (m *MatchExpr) RewriteNameSpace(alias, mst string) {
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

func (e *ParenExpr) RewriteNameSpace(alias, mst string) {
	e.Expr.RewriteNameSpace(alias, mst)
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string { return fmt.Sprintf("(%s)", e.Expr.String()) }

// RegexLiteral represents a regular expression.
type RegexLiteral struct {
	Val *regexp.Regexp
}

func (r *RegexLiteral) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the literal.
func (r *RegexLiteral) String() string {
	if r.Val != nil {
		return fmt.Sprintf("/%s/", strings.Replace(r.Val.String(), `/`, `\/`, -1))
	}
	return ""
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

func (e *Wildcard) RewriteNameSpace(alias, mst string) {}

// String returns a string representation of the wildcard.
func (e *Wildcard) String() string {
	switch e.Type {
	case FIELD:
		return "*::field"
	case TAG:
		return "*::tag"
	default:
		return "*"
	}
}

// CloneExpr returns a deep copy of the expression.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *BinaryExpr:
		return &BinaryExpr{Op: expr.Op, LHS: CloneExpr(expr.LHS), RHS: CloneExpr(expr.RHS), ReturnBool: expr.ReturnBool}
	case *BooleanLiteral:
		return &BooleanLiteral{Val: expr.Val}
	case *Call:
		args := make([]Expr, len(expr.Args))
		for i, arg := range expr.Args {
			args[i] = CloneExpr(arg)
		}
		return &Call{Name: expr.Name, Args: args}
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
		return &ParenExpr{Expr: CloneExpr(expr.Expr)}
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
		return &InCondition{Stmt: expr.Stmt.Clone(), Column: CloneExpr(expr.Column)}
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

	// Evaluate if both sides are simple types.
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
		}
	}

	// The types were not comparable. If our operation was an equality operation,
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
// be evaluated for some reason, like incompatible types, it is returned
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
				_, e := src.Statement.FieldExprByName(k)
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
	if v.TypeMapper != nil {
		for _, src := range v.Sources {
			switch src := src.(type) {
			case *Measurement:
				if batchEn {
					return Unknown, ErrNeedBatchMap
				}
				if t := v.TypeMapper.MapType(src, expr.Val); typ.LessThan(t) {
					typ = t
				}
			case *SubQuery:
				_, e := src.Statement.FieldExprByName(expr.Val)
				if e != nil {
					valuer := TypeValuerEval{
						TypeMapper: v.TypeMapper,
						Sources:    src.Statement.Sources,
					}
					if t, err := valuer.EvalType(e, batchEn); err != nil {
						return Unknown, err
					} else if typ.LessThan(t) {
						typ = t
					}
				}

				if typ == Unknown {
					for _, d := range src.Statement.Dimensions {
						if d, ok := d.Expr.(*VarRef); ok && expr.Val == d.Val {
							typ = Tag
						}
					}
				}
			}
		}
	}
	return typ, nil
}

func (v *TypeValuerEval) evalCallExprType(expr *Call, batchCall bool) (DataType, error) {
	typmap, ok := v.TypeMapper.(CallTypeMapper)
	if !ok {
		return Unknown, nil
	}

	// Evaluate all of the data types for the arguments.
	args := make([]DataType, len(expr.Args))
	for i, arg := range expr.Args {
		typ, err := v.EvalType(arg, batchCall)
		if err != nil {
			return Unknown, err
		}
		args[i] = typ
	}

	// Pass in the data types for the call so it can be type checked and
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
		LHS: &VarRef{Val: "lhs"},
		RHS: &VarRef{Val: "rhs"},
		Op:  expr.Op,
	}
	result := Eval(&e, map[string]interface{}{
		"lhs": lhs.Zero(),
		"rhs": rhs.Zero(),
	})

	typ := InspectDataType(result)
	if typ == Unknown {
		// If the type is unknown, then the two types were not compatible.
		return Unknown, &TypeError{
			Expr:    expr,
			Message: fmt.Sprintf("incompatible types: %s and %s", lhs, rhs),
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

// TypeError is an error when two types are incompatible.
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
		return CloneExpr(expr)
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
		return &BinaryExpr{LHS: lhs, RHS: rhs, Op: expr.Op, ReturnBool: expr.ReturnBool}
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

	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case *BooleanLiteral:
		return reduceBinaryExprBooleanLHS(op, lhs, rhs)
	case *DurationLiteral:
		return reduceBinaryExprDurationLHS(op, lhs, rhs, loc)
	case *IntegerLiteral:
		return reduceBinaryExprIntegerLHS(op, lhs, rhs, loc)
	case *UnsignedLiteral:
		return reduceBinaryExprUnsignedLHS(op, lhs, rhs)
	case *NilLiteral:
		return reduceBinaryExprNilLHS(op, lhs, rhs)
	case *NumberLiteral:
		return wrapReduceBinaryExprNumberLHS(expr, op, lhs, rhs)
	case *StringLiteral:
		return reduceBinaryExprStringLHS(op, lhs, rhs, loc)
	case *TimeLiteral:
		return reduceBinaryExprTimeLHS(op, lhs, rhs, loc)
	default:
		return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs, ReturnBool: expr.ReturnBool}
	}
}

func reduceBinaryExprBooleanLHS(op Token, lhs *BooleanLiteral, rhs Expr) Expr {
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
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprDurationLHS(op Token, lhs *DurationLiteral, rhs Expr, loc *time.Location) Expr {
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
		expr := reduceBinaryExprDurationLHS(op, lhs, t, loc)

		// If the returned expression is still a binary expr, that means
		// we couldn't reduce it so this wasn't used in a time literal context.
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *NilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprIntegerLHS(op Token, lhs *IntegerLiteral, rhs Expr, loc *time.Location) Expr {
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
		return reduceBinaryExprUnsignedLHS(op, &UnsignedLiteral{Val: uint64(lhs.Val)}, rhs)
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
		expr := reduceBinaryExprDurationLHS(op, d, rhs, loc)
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *StringLiteral:
		t, err := rhs.ToTimeLiteral(loc)
		if err != nil {
			break
		}
		d := &DurationLiteral{Val: time.Duration(lhs.Val)}
		expr := reduceBinaryExprDurationLHS(op, d, t, loc)
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *NilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprUnsignedLHS(op Token, lhs *UnsignedLiteral, rhs Expr) Expr {
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
		return reduceBinaryExprUnsignedLHS(op, lhs, &UnsignedLiteral{Val: uint64(rhs.Val)})
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
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprNilLHS(op Token, lhs *NilLiteral, rhs Expr) Expr {
	switch op {
	case EQ, NEQ:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
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
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprStringLHS(op Token, lhs *StringLiteral, rhs Expr, loc *time.Location) Expr {
	switch rhs := rhs.(type) {
	case *StringLiteral:
		switch op {
		case EQ:
			var expr Expr = &BooleanLiteral{Val: lhs.Val == rhs.Val}
			// This might be a comparison between time literals.
			// If it is, parse the time literals and then compare since it
			// could be a different result if they use different formats
			// for the same time.
			if lhs.IsTimeLiteral() && rhs.IsTimeLiteral() {
				tlhs, err := lhs.ToTimeLiteral(loc)
				if err != nil {
					return expr
				}

				trhs, err := rhs.ToTimeLiteral(loc)
				if err != nil {
					return expr
				}

				t := reduceBinaryExprTimeLHS(op, tlhs, trhs, loc)
				if _, ok := t.(*BinaryExpr); !ok {
					expr = t
				}
			}
			return expr
		case NEQ:
			var expr Expr = &BooleanLiteral{Val: lhs.Val != rhs.Val}
			// This might be a comparison between time literals.
			// If it is, parse the time literals and then compare since it
			// could be a different result if they use different formats
			// for the same time.
			if lhs.IsTimeLiteral() && rhs.IsTimeLiteral() {
				tlhs, err := lhs.ToTimeLiteral(loc)
				if err != nil {
					return expr
				}

				trhs, err := rhs.ToTimeLiteral(loc)
				if err != nil {
					return expr
				}

				t := reduceBinaryExprTimeLHS(op, tlhs, trhs, loc)
				if _, ok := t.(*BinaryExpr); !ok {
					expr = t
				}
			}
			return expr
		case ADD:
			return &StringLiteral{Val: lhs.Val + rhs.Val}
		default:
			// Attempt to convert the string literal to a time literal.
			t, err := lhs.ToTimeLiteral(loc)
			if err != nil {
				break
			}
			expr := reduceBinaryExprTimeLHS(op, t, rhs, loc)

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
		expr := reduceBinaryExprTimeLHS(op, t, rhs, loc)

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
		expr := reduceBinaryExprTimeLHS(op, t, rhs, loc)

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
		expr := reduceBinaryExprTimeLHS(op, t, rhs, loc)

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
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceBinaryExprTimeLHS(op Token, lhs *TimeLiteral, rhs Expr, loc *time.Location) Expr {
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
		expr := reduceBinaryExprTimeLHS(op, lhs, d, loc)
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
		expr := reduceBinaryExprTimeLHS(op, lhs, t, loc)

		// If the returned expression is still a binary expr, that means
		// we couldn't reduce it so this wasn't used in a time literal context.
		if _, ok := expr.(*BinaryExpr); !ok {
			return expr
		}
	case *NilLiteral:
		return &BooleanLiteral{Val: false}
	}
	return &BinaryExpr{Op: op, LHS: lhs, RHS: rhs}
}

func reduceCall(expr *Call, valuer Valuer) Expr {
	// Otherwise reduce arguments.
	var args []Expr
	literalsOnly := true
	if len(expr.Args) > 0 {
		args = make([]Expr, len(expr.Args))
		for i, arg := range expr.Args {
			args[i] = reduce(arg, valuer)
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
	return &Call{Name: expr.Name, Args: args}
}

func reduceParenExpr(expr *ParenExpr, valuer Valuer) Expr {
	subexpr := reduce(expr.Expr, valuer)
	if subexpr, ok := subexpr.(*BinaryExpr); ok {
		return &ParenExpr{Expr: subexpr}
	}
	return subexpr
}

func reduceVarRef(expr *VarRef, valuer Valuer) Expr {
	// Ignore if there is no valuer.
	if valuer == nil {
		return &VarRef{Val: expr.Val, Type: expr.Type}
	}

	// Retrieve the value of the ref.
	// Ignore if the value doesn't exist.
	v, ok := valuer.Value(expr.Val)
	if !ok {
		return &VarRef{Val: expr.Val, Type: expr.Type}
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
	expr, tr, err := conditionExpr(cond, valuer)

	// Remove top level parentheses
	if e, ok := expr.(*ParenExpr); ok {
		expr = e.Expr
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
			return reduce(&BinaryExpr{
				Op:  cond.Op,
				LHS: lhsExpr,
				RHS: rhsExpr,
			}, nil), timeRange, nil
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
		return reduce(cond, valuer), TimeRange{}, nil
	case *ParenExpr:
		expr, timeRange, err := conditionExpr(cond.Expr, valuer)
		if err != nil {
			return nil, TimeRange{}, err
		} else if expr == nil {
			return nil, timeRange, nil
		}
		return reduce(&ParenExpr{Expr: expr}, nil), timeRange, nil
	case *BooleanLiteral:
		return cond, TimeRange{}, nil
	case *InCondition:
		return reduce(cond, nil), TimeRange{}, nil
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

	// Evaluate the RHS to replace "now()" with the current time.
	rhs = Reduce(rhs, valuer)

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

// String returns a string representation of the prepare preparing command.
func (s *PrepareSnapshotStatement) String() string { return "Prepare Snapshot" }

// RequiredPrivileges returns the privilege required to execute a PrepareSnapshotStatement.
func (s *PrepareSnapshotStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	// PrepareSnapshot is one of few statements that have no required privileges.
	// Anyone is allowed to execute it, but the returned results depend on the user's
	// individual PrepareSnapshotStatement permissions.
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: NoPrivileges}}, nil
}

// EndPrepareSnapshotStatement represents a command for preparing preparing.
type EndPrepareSnapshotStatement struct{}

// String returns a string representation of the end prepare preparing command.
func (s *EndPrepareSnapshotStatement) String() string { return "End Snapshot" }

// RequiredPrivileges returns the privilege required to execute a EndPrepareSnapshotStatement.
func (s *EndPrepareSnapshotStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	// EndPrepareSnapshot is one of few statements that have no required privileges.
	// Anyone is allowed to execute it, but the returned results depend on the user's
	// individual EndPrepareSnapshotStatement permissions.
	return ExecutionPrivileges{{Admin: false, Name: "", Rwuser: true, Privilege: NoPrivileges}}, nil
}

// GetRuntimeInfoStatement represents a command for get runtimeinfo.
type GetRuntimeInfoStatement struct{}

// String returns a string representation of the get runtimeinfo command.
func (s *GetRuntimeInfoStatement) String() string { return "Get RuntimeInfo" }

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
}

func (p *CaseWhenExpr) RewriteNameSpace(alias, mst string) {}

func (p *CaseWhenExpr) node() {}
func (p *CaseWhenExpr) expr() {}
func (p *CaseWhenExpr) String() string {
	var s string
	for i := range p.Conditions {
		s += "Condition:" + p.Conditions[i].String() + "Assigner:" + p.Assigners[i].String()
	}
	s += "Else Condition:" + p.Assigners[len(p.Conditions)].String()
	return fmt.Sprintf(s)
}

type CreateDownSampleStatement struct {
	DbName         string
	RpName         string
	Duration       time.Duration
	SampleInterval []time.Duration
	TimeInterval   []time.Duration
	WaterMark      []time.Duration
	Ops            Fields
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
	var buf bytes.Buffer
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
			c := op.Expr.(*Call)
			_, _ = buf.WriteString(QuoteIdent(fmt.Sprintf("%s", c.String())))
			if i != len(d.Ops)-1 {
				_, _ = buf.WriteString(",")
			}
		}
	}

	if d.Duration != 0 {
		_, _ = buf.WriteString("WITH TTL")
		_, _ = buf.WriteString(QuoteIdent(d.Duration.String()))
	}

	if len(d.SampleInterval) != 0 {
		_, _ = buf.WriteString("SAMPLEINTERVAL")
		s := Times2String(d.SampleInterval)
		_, _ = buf.WriteString(QuoteIdent(s))
	}

	if len(d.TimeInterval) != 0 {
		_, _ = buf.WriteString("TIMEINTERVAL")
		s := Times2String(d.TimeInterval)
		_, _ = buf.WriteString(QuoteIdent(s))
	}

	if len(d.WaterMark) != 0 {
		_, _ = buf.WriteString("WATERMARK")
		s := Times2String(d.WaterMark)
		_, _ = buf.WriteString(QuoteIdent(s))
	}

	return buf.String()
}

type DropDownSampleStatement struct {
	DbName  string
	RpName  string
	DropAll bool
}

func (d *DropDownSampleStatement) stmt() {}

func (d *DropDownSampleStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (d *DropDownSampleStatement) node() {}

func (d *DropDownSampleStatement) String() string {
	var buf bytes.Buffer
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
	return buf.String()
}

type ShowDownSampleStatement struct {
	DbName string
}

func (d *ShowDownSampleStatement) stmt() {}

func (d *ShowDownSampleStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (d *ShowDownSampleStatement) node() {}

func (d *ShowDownSampleStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("Show DownSampling")

	if d.DbName != "" {
		_, _ = buf.WriteString(" ON ")
		_, _ = buf.WriteString(QuoteIdent(d.DbName))
	}

	return buf.String()
}

type CreateStreamStatement struct {
	Name   string
	Target *Target
	Query  Statement
	Delay  time.Duration
}

func (c *CreateStreamStatement) stmt() {}

func (c *CreateStreamStatement) node() {}

func (c *CreateStreamStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (c *CreateStreamStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("CREATE STREAM")
	_, _ = buf.WriteString(QuoteIdent(c.Name))
	_, _ = buf.WriteString("INTO")
	_, _ = buf.WriteString(QuoteIdent(c.Target.String()))
	_, _ = buf.WriteString(QuoteIdent(c.Query.String()))
	_, _ = buf.WriteString("OFFSET")
	_, _ = buf.WriteString(QuoteIdent(c.Delay.String()))

	return buf.String()
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

func (s *ShowStreamsStatement) stmt() {}

func (s *ShowStreamsStatement) node() {}

func (s *ShowStreamsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowStreamsStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SHOW STREAMS")
	if len(s.Database) > 0 {
		_, _ = buf.WriteString(QuoteIdent(s.Database))
	}

	return buf.String()
}

type DropStreamsStatement struct {
	Name string
}

func (s *DropStreamsStatement) stmt() {}

func (s *DropStreamsStatement) node() {}

func (s *DropStreamsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *DropStreamsStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("DROP STREAMS ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	return buf.String()
}

type ShowMeasurementKeysStatement struct {
	Name        string
	Database    string
	Rp          string
	Measurement string
}

func (s *ShowMeasurementKeysStatement) stmt() {}

func (s *ShowMeasurementKeysStatement) node() {}

func (s *ShowMeasurementKeysStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowMeasurementKeysStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("Show Measurement ")
	_, _ = buf.WriteString(QuoteIdent(s.Name))

	return buf.String()
}

type ShowConfigsStatement struct {
	Scope string
	Key   *ConfigKey
}

type ConfigKey struct {
	Name  string
	Regex *RegexLiteral
}

// String returns a string representation of the measurement.
func (ck *ConfigKey) String() string {
	if ck.Name != "" {
		return QuoteIdent(ck.Name)
	} else if ck.Regex != nil {
		return ck.Regex.String()
	}
	return ""
}

func (s *ShowConfigsStatement) stmt() {}

func (s *ShowConfigsStatement) node() {}

func (s *ShowConfigsStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowConfigsStatement) String() string {
	var buf bytes.Buffer

	if s.Scope == "" && s.Key == nil {
		return `SHOW CONFIGS`
	} else if s.Scope == "" {
		_, _ = buf.WriteString(fmt.Sprintf(`SHOW CONFIGS WHERE scope = %s`, s.Scope))
	} else if s.Key == nil {
		_, _ = buf.WriteString(fmt.Sprintf(`SHOW CONFIGS WHERE name = %s`, s.Key.String()))
	} else {
		_, _ = buf.WriteString(fmt.Sprintf(`SHOW CONFIGS WHERE scope = %s AND name = %s`, s.Scope, s.Key.String()))
	}
	return buf.String()
}

type SetConfigStatement struct {
	Component string
	Key       string
	Value     interface{}
}

func (s *SetConfigStatement) stmt() {}

func (s *SetConfigStatement) node() {}

func (s *SetConfigStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *SetConfigStatement) String() string {
	var buf bytes.Buffer

	if _, ok := s.Value.(string); ok {
		_, _ = buf.WriteString(fmt.Sprintf(`SET CONFIG %s "%s" = "%s"`, s.Component, s.Key, s.Value))
	} else {
		_, _ = buf.WriteString(fmt.Sprintf(`SET CONFIG %s "%s" = %v`, s.Component, s.Key, s.Value))

	}
	return buf.String()
}

type ShowClusterStatement struct {
	NodeType string
	NodeID   int64
}

func (s *ShowClusterStatement) RewriteNameSpace(alias, mst string) {}

func (s *ShowClusterStatement) stmt() {}

func (s *ShowClusterStatement) node() {}

func (s *ShowClusterStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}

func (s *ShowClusterStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(fmt.Sprintf(`SHOW CLUSTER`))

	if s.NodeType != "" && s.NodeID == 0 {
		_, _ = buf.WriteString(fmt.Sprintf(` WHERE nodeType = %s`, s.NodeType))
	} else if s.NodeID != 0 && s.NodeType == "" {
		_, _ = buf.WriteString(fmt.Sprintf(` WHERE nodeID = %s`, strconv.FormatInt(s.NodeID, 10)))
	} else if s.NodeID != 0 && s.NodeType != "" {
		_, _ = buf.WriteString(fmt.Sprintf(` WHERE nodeType = %s AND nodeID = %s`, s.NodeType, strconv.FormatInt(s.NodeID, 10)))
	}
	return buf.String()
}

type Unnests []*Unnest

func (us Unnests) String() string {
	var str []string
	for _, u := range us {
		str = append(str, u.String())
	}
	return strings.Join(str, ", ")
}

type Unnest struct {
	Expr    Expr
	Aliases []string
	DstType []DataType
}

// output example: UNNEST(Expr) AS(key1, value1)
func (u *Unnest) String() string {
	var buf bytes.Buffer
	if u.Expr != nil {
		buf.WriteString("UNNEST(")
		buf.WriteString(u.Expr.String())
		buf.WriteString(") ")
	}

	if len(u.Aliases) != 0 {
		buf.WriteString("AS(")
		for i, t := range u.Aliases {
			buf.WriteString(t)
			if i+1 != len(u.Aliases) {
				buf.WriteString(", ")
			}
		}
		buf.WriteString(")")
	}

	return buf.String()
}

func (u *Unnest) Clone() *Unnest {
	expr, err := ParseExpr(u.Expr.String())
	if err != nil {
		return nil
	}
	clone := &Unnest{
		Expr: expr,
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
}

func (s *LogPipeStatement) stmt() {}
func (s *LogPipeStatement) node() {}
func (s *LogPipeStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}
func (s *LogPipeStatement) String() string {
	var buf bytes.Buffer
	if s.Cond != nil {
		_, _ = buf.WriteString(s.Cond.String())
	}
	if s.Unnest != nil {
		buf.WriteString("|")
		_, _ = buf.WriteString(s.Unnest.String())
	}
	return buf.String()
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
	CTEs  CTES
	Query *SelectStatement
}

type CTE struct {
	Query *SelectStatement
	Alias string
}

type CTES []*CTE

func (s *WithSelectStatement) String() string {
	var buf bytes.Buffer
	buf.WriteString("With")
	for _, cte := range s.CTEs {
		_, _ = buf.WriteString(cte.Alias)
		_, _ = buf.WriteString(" AS (")
		_, _ = buf.WriteString(cte.Query.String())
		_, _ = buf.WriteString(" ),")
	}
	buf.WriteString(s.Query.String())
	return buf.String()
}

func (s *WithSelectStatement) RequiredPrivileges() (ExecutionPrivileges, error) {
	return ExecutionPrivileges{{Admin: true, Name: "", Rwuser: true, Privilege: AllPrivileges}}, nil
}
