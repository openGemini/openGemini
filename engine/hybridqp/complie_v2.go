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

package hybridqp

import (
	"errors"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

const (
	DefaultNoHint HintType = iota
	FilterNullColumn
	ExactStatisticQuery
	FullSeriesQuery
	SpecificSeriesQuery
)

var (
	errUnsupportedHint = errors.New("unsupported hint:" + influxql.FilterNullColumn + " if chunked")
)

func VerifyHintStmt(stmt *influxql.SelectStatement, opt Options) error {
	if FilterNullColumnQuery(stmt) {
		if opt.ISChunked() {
			return errUnsupportedHint
		}
		opt.SetHintType(FilterNullColumn)
		return nil
	}
	if IsExactStatisticQuery(stmt) {
		opt.SetHintType(ExactStatisticQuery)
	}
	return nil
}

// FilterNullColumnQuery Hint
func FilterNullColumnQuery(stmt *influxql.SelectStatement) bool {
	for _, s := range stmt.Sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			var isFilterNullColumn bool
			for _, hint := range stmt.Hints {
				h := hint.String()
				if h == influxql.FilterNullColumn {
					isFilterNullColumn = true
					break
				}
			}
			return isFilterNullColumn
		case *influxql.SubQuery:
			return FilterNullColumnQuery(s.Statement)
		default:
			continue
		}
	}
	return false
}

// IsExactStatisticQuery Hint
func IsExactStatisticQuery(stmt *influxql.SelectStatement) bool {
	for _, s := range stmt.Sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			var isExactStatistic bool
			for _, hint := range stmt.Hints {
				h := hint.String()
				if h == influxql.ExactStatisticQuery {
					isExactStatistic = true
					break
				}
			}
			return isExactStatistic
		case *influxql.SubQuery:
			return IsExactStatisticQuery(s.Statement)
		default:
			continue
		}
	}
	return false
}

// IsFullSeriesQuery Hint
func IsFullSeriesQuery(stmt *influxql.SelectStatement) bool {
	for _, s := range stmt.Sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			var isFullSeries bool
			for _, hint := range stmt.Hints {
				h := hint.String()
				if h == influxql.FullSeriesQuery {
					isFullSeries = true
					break
				}
			}
			return isFullSeries
		case *influxql.SubQuery:
			return IsFullSeriesQuery(s.Statement)
		default:
			continue
		}
	}
	return false
}

// IsSpecificSeriesQuery Hint
func IsSpecificSeriesQuery(stmt *influxql.SelectStatement) bool {
	for _, s := range stmt.Sources {
		switch s := s.(type) {
		case *influxql.Measurement:
			var isSpecifySeries bool
			for _, hint := range stmt.Hints {
				h := hint.String()
				if h == influxql.SpecificSeriesQuery {
					isSpecifySeries = true
					break
				}
			}
			return isSpecifySeries
		case *influxql.SubQuery:
			return IsSpecificSeriesQuery(s.Statement)
		default:
			continue
		}
	}
	return false
}
