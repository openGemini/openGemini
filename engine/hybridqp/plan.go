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
	"fmt"
	"reflect"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
	internal "github.com/openGemini/openGemini/lib/util/lifted/influx/query/proto"
)

type ExprOptions struct {
	Expr influxql.Expr
	Ref  influxql.VarRef
}

// IteratorCost contains statistics retrieved for explaining what potential
// cost may be incurred by instantiating an iterator.
type LogicalPlanCost struct {
	// The total number of shards that are touched by this Logical.
	NumShards int64

	// The total number of non-unique series that are accessed by this Logical.
	// This number matches the number of cursors created by the Logical since
	// one cursor is created for every series.
	NumSeries int64

	// CachedValues returns the number of cached values that may be read by this
	// Logical.
	CachedValues int64

	// The total number of non-unique files that may be accessed by this Logical.
	// This will count the number of files accessed by each series so files
	// will likely be double counted.
	NumFiles int64

	// The number of blocks that had the potential to be accessed.
	BlocksRead int64

	// The amount of data that can be potentially read.
	BlockSize int64
}

func (o *ExprOptions) ElapsedInterval() Interval {
	// Use the interval on the elapsed() call, if specified.
	if expr, ok := o.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}
	return Interval{Duration: time.Nanosecond}
}

func (o *ExprOptions) IntegralInterval() Interval {
	// Use the interval on the integral() call, if specified.
	if expr, ok := o.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}

	return Interval{Duration: time.Second}
}

// DerivativeInterval returns the time interval for the derivative function.
func (o *ExprOptions) DerivativeInterval(interval Interval) Interval {
	// Use the interval on the derivative() call, if specified.
	if expr, ok := o.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}

	// Otherwise use the group by interval, if specified.
	if interval.Duration > 0 {
		return Interval{Duration: interval.Duration}
	}

	return Interval{Duration: time.Second}
}

// SlidingWindowInterval returns the time interval for the sliding window function.
func (o *ExprOptions) SlidingWindowInterval(interval Interval) Interval {
	// Otherwise use the group by interval, if specified.
	if interval.Duration > 0 {
		return Interval{Duration: interval.Duration}
	}

	return Interval{Duration: time.Minute}
}

func (o *ExprOptions) Marshal() *internal.ExprOptions {
	return &internal.ExprOptions{
		Expr: o.Expr.String(),
		Ref:  o.Ref.String(),
	}
}

func (o *ExprOptions) Unmarshal(pb *internal.ExprOptions) error {
	expr, err := influxql.ParseExpr(pb.Expr)
	if err != nil {
		return err
	}
	o.Expr = expr

	expr, err = influxql.ParseExpr(pb.Ref)
	if err != nil {
		return err
	}
	ref, ok := expr.(*influxql.VarRef)
	if !ok {
		return fmt.Errorf("failed to unmarshal ExprOptions.Ref, exp: *influxql.VarRef, got: %s", reflect.TypeOf(expr))
	}

	o.Ref = *ref
	return nil
}
