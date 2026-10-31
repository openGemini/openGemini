// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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

package hybridqp

import (
	"errors"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const (
	DefaultNoHint HintType = iota
	FilterNullColumn
	ExactStatisticQuery
	FullSeriesQuery
	SpecificSeriesQuery
	QueryPushDown
)

var HintTypeMap = map[string]HintType{
	influxql.FilterNullColumn:    FilterNullColumn,
	influxql.ExactStatisticQuery: ExactStatisticQuery,
	influxql.FullSeriesQuery:     FullSeriesQuery,
	influxql.SpecificSeriesQuery: SpecificSeriesQuery,
	influxql.QueryPushDown:       QueryPushDown,
}

var (
	errUnsupportedHint = errors.New("unsupported hint:" + influxql.FilterNullColumn + " if chunked")
)

func VerifyHintStmt(stmt *influxql.SelectStatement, opt Options) error {
	if IsFilterNullColumnQuery(stmt) {
		if opt.ISChunked() {
			return errUnsupportedHint
		}
		opt.SetHintType(FilterNullColumn)
		return nil
	}

	if IsExactStatisticQuery(stmt) {
		opt.SetHintType(ExactStatisticQuery)
		return nil
	}

	if IsQueryPushDown(stmt) {
		opt.SetHintType(QueryPushDown)
		return nil
	}

	return nil
}

// IsFilterNullColumnQuery Hint
func IsFilterNullColumnQuery(stmt *influxql.SelectStatement) bool {
	return queryContainsHint(stmt, influxql.FilterNullColumn)
}

// IsExactStatisticQuery Hint
func IsExactStatisticQuery(stmt *influxql.SelectStatement) bool {
	if !config.GetCommon().PreAggEnabled {
		return true
	}

	return queryContainsHint(stmt, influxql.ExactStatisticQuery)
}

// IsFullSeriesQuery Hint
func IsFullSeriesQuery(stmt *influxql.SelectStatement) bool {
	return queryContainsHint(stmt, influxql.FullSeriesQuery)
}

// IsSpecificSeriesQuery Hint
func IsSpecificSeriesQuery(stmt *influxql.SelectStatement) bool {
	return queryContainsHint(stmt, influxql.SpecificSeriesQuery)
}

// IsQueryPushDown Hint
func IsQueryPushDown(stmt *influxql.SelectStatement) bool {
	return queryContainsHint(stmt, influxql.QueryPushDown)
}

// queryContainsHint recursively checks if a select statement or its subqueries contain a specific hint.
// It traverses the query's sources:
// - For a Measurement (the base case), it checks the current statement's hints.
// - For a SubQuery (the recursive step), it calls itself on the inner statement.
// The function returns as soon as it processes the first Measurement or SubQuery source.
func queryContainsHint(stmt *influxql.SelectStatement, hintToFind string) bool {
	for _, s := range stmt.Sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			// Base case: Check hints on the current statement.
			for _, hint := range stmt.Hints {
				if hint.String() == hintToFind {
					return true
				}
			}
			return false
		case *influxql.SubQuery:
			// Recursive step: Check the subquery.
			return queryContainsHint(s.Statement, hintToFind)
		default:
			continue
		}
	}
	// Return false if no Measurement or SubQuery sources were found.
	return false
}

func GetHintType(stmt *influxql.SelectStatement) HintType {
	for _, s := range stmt.Sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			// Base case: get hints on the current statement.
			for _, hint := range stmt.Hints {
				if hintType, ok := HintTypeMap[hint.String()]; ok {
					return hintType
				}
			}
		case *influxql.SubQuery:
			// Recursive step: Check the subquery.
			hintType := GetHintType(s.Statement)
			if hintType != DefaultNoHint {
				return hintType
			}
		default:
			continue
		}
	}
	// Return DefaultNoHint if no Measurement or SubQuery sources were found.
	return DefaultNoHint
}
