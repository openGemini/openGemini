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

package smart_query

import (
	"strconv"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

var WholeTimeRange = influxql.TimeRange{Min: time.Unix(0, influxql.MinTime), Max: time.Unix(0, influxql.MaxTime)}

type Field struct {
	Call string
	Val  string
	Typ  influxql.DataType
}

type Measurement struct {
	Database        string
	RetentionPolicy string
	Name            string
	NameVersion     string
}

type EqCond struct {
	Op  influxql.Token
	Typ influxql.DataType
	Key string
	Val string
}

type OrCond struct {
	Cond []EqCond
}

type SmartSelectStatement struct {
	Fields          []Field
	Source          Measurement
	Condition       OrCond
	Limit           int
	Offset          int
	GroupByInterval time.Duration
	Location        *time.Location
	Epoch           string
	StartTime       string
	EndTime         string
	StartT          time.Time
	EndT            time.Time
	HintType        int64
	HintSeriesKey   []byte
}

type SmartRemoteQuery struct {
	PtID     uint32
	NodeID   uint64
	ShardIDs []uint64
}

func CheckEqCond(lCond influxql.Expr, rCond influxql.Expr) (EqCond, bool) {
	lval, ok := lCond.(*influxql.VarRef)
	if !ok {
		return EqCond{}, false
	}
	rval, ok := rCond.(*influxql.StringLiteral)
	if !ok {
		return EqCond{}, false
	}
	// operator assignment
	return EqCond{Key: lval.Val, Val: rval.Val, Op: influxql.EQ}, true
}

func CheckOrCondDFS(cond influxql.Expr) (OrCond, bool) {
	switch cond := cond.(type) {
	case *influxql.BinaryExpr:
		if cond.Op == influxql.OR {
			lCond, ok := CheckOrCondDFS(cond.LHS)
			if !ok {
				return OrCond{}, false
			}
			rCond, ok := CheckOrCondDFS(cond.RHS)
			if !ok {
				return OrCond{}, false
			}
			lCond.Cond = append(lCond.Cond, rCond.Cond...)
			return lCond, true
		} else if cond.Op == influxql.EQ {
			eqCond, ok := CheckEqCond(cond.LHS, cond.RHS)
			if !ok {
				return OrCond{}, false
			}
			return OrCond{Cond: []EqCond{eqCond}}, true
		}
		return OrCond{}, false
	default:
		return OrCond{}, false
	}
}

func CheckOrCond(cond influxql.Expr) (OrCond, bool) {
	switch cond := cond.(type) {
	case *influxql.ParenExpr:
		return CheckOrCondDFS(cond.Expr)
	default:
		return OrCond{}, false
	}
}

func CheckTimeCond(cond influxql.Expr, op influxql.Token, loc *time.Location) (time.Time, bool) {
	eqCond, ok := cond.(*influxql.BinaryExpr)
	if !ok || eqCond.Op != op {
		return time.Time{}, false
	}
	lCond, ok := eqCond.LHS.(*influxql.VarRef)
	if !ok {
		return time.Time{}, false
	}
	if lCond.Val != "time" {
		return time.Time{}, false
	}
	rCond, ok := eqCond.RHS.(*influxql.StringLiteral)
	if !ok {
		return time.Time{}, false
	}
	t, err := rCond.ToTimeLiteral(loc)
	if err != nil {
		return time.Time{}, false
	}
	return t.Val, true
}

func ConvertExprIntoCond(expr *influxql.BinaryExpr) (EqCond, bool) {
	lhs, ok := expr.LHS.(*influxql.VarRef)
	if !ok {
		return EqCond{}, false
	}
	res := EqCond{Op: expr.Op, Key: lhs.Val}
	switch rhs := expr.RHS.(type) {
	case *influxql.StringLiteral:
		res.Val = rhs.Val
	case *influxql.IntegerLiteral:
		res.Val = strconv.FormatInt(rhs.Val, 10)
	case *influxql.NumberLiteral:
		res.Val = strconv.FormatFloat(rhs.Val, 'f', 10, 64)
	case *influxql.BooleanLiteral:
		res.Val = strconv.FormatBool(rhs.Val)
	default:
		return EqCond{}, false
	}
	return res, true
}

func CheckCond(cond influxql.Expr, loc *time.Location) (OrCond, *influxql.TimeRange, bool) {
	if cond == nil {
		return OrCond{}, &WholeTimeRange, true
	}
	switch cond := cond.(type) {
	case *influxql.BinaryExpr:
		switch cond.Op {
		case influxql.EQ, influxql.NEQ, influxql.LT, influxql.LTE, influxql.GT, influxql.GTE:
			condExpr, ok := ConvertExprIntoCond(cond)
			if !ok {
				return OrCond{}, nil, false
			}
			return OrCond{Cond: []EqCond{condExpr}}, &WholeTimeRange, true
		}
		if cond.Op == influxql.AND {
			lCond, ok := cond.LHS.(*influxql.BinaryExpr)
			if !ok || lCond.Op != influxql.AND {
				return OrCond{}, nil, false
			}
			orCond, ok := CheckOrCond(lCond.LHS)
			if !ok {
				return OrCond{}, nil, false
			}
			lTime, ok := CheckTimeCond(lCond.RHS, influxql.GTE, loc)
			if !ok {
				return OrCond{}, nil, false
			}
			rTime, ok := CheckTimeCond(cond.RHS, influxql.LT, loc)
			if !ok {
				return OrCond{}, nil, false
			}
			return orCond, &influxql.TimeRange{Min: lTime, Max: rTime}, true
		} else {
			return OrCond{}, nil, false
		}
	default:
		return OrCond{}, &WholeTimeRange, true
	}
}

func CheckFields(fields influxql.Fields) ([]Field, bool) {
	newFields := make([]Field, 0, len(fields))
	for _, f := range fields {
		switch expr := f.Expr.(type) {
		case *influxql.VarRef:
			newF := Field{Val: expr.Val}
			newFields = append(newFields, newF)
		case *influxql.Call:
			if len(expr.Args) != 1 {
				return nil, false
			}
			varRef, ok := expr.Args[0].(*influxql.VarRef)
			if !ok {
				return nil, false
			}
			newF := Field{Call: expr.Name, Val: varRef.Val}
			newFields = append(newFields, newF)
		default:
			return nil, false
		}
	}
	return newFields, true
}

func CheckSource(sources influxql.Sources) (Measurement, bool) {
	if len(sources) != 1 {
		return Measurement{}, false
	}
	source := sources[0]
	mst, ok := source.(*influxql.Measurement)
	if !ok {
		return Measurement{}, false
	}
	return Measurement{Name: mst.Name}, true
}
