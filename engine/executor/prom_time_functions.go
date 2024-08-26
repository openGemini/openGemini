// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package executor

import (
	"math"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func init() {
	RegistryPromTimeFunction("year_prom", &yearPromFunc{})
	RegistryPromTimeFunction("time_prom", &timePromFunc{})
	RegistryPromTimeFunction("vector_prom", &vectorPromFunc{})
	RegistryPromTimeFunction("month_prom", &monthPromFunc{})
	RegistryPromTimeFunction("day_of_month_prom", &dayOfMonthPromFunc{})
	RegistryPromTimeFunction("day_of_week_prom", &dayOfWeekPromFunc{})
	RegistryPromTimeFunction("hour_prom", &hourPromFunc{})
	RegistryPromTimeFunction("minute_prom", &minutePromFunc{})
	RegistryPromTimeFunction("days_in_month_prom", &daysInMonthPromFunc{})
	RegistryPromTimeFunction("pi_prom", &piPromFunc{})
	RegistryPromTimeFunction("timestamp_prom", &timestampPromFunc{})
}

type PromTimeFunction interface {
	CallFunc(name string, args []interface{}) (interface{}, bool)
}

var promTimeFuncInstance = make(map[string]PromTimeFunction)

func GetPromTimeFuncInstance() map[string]PromTimeFunction {
	return promTimeFuncInstance
}

func RegistryPromTimeFunction(name string, timeFunc PromTimeFunction) {
	_, ok := promTimeFuncInstance[name]
	if ok {
		return
	}
	promTimeFuncInstance[name] = timeFunc
}

type yearPromFunc struct{}

func (s *yearPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			rVals := float64(time.Unix(int64(iVal), 0).UTC().Year())
			return rVals, true
		}
	}
	return nil, true
}

type timePromFunc struct{}

func (s *timePromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			rVals := float64(iVal)
			return rVals, true
		}
	}
	return nil, true
}

type timestampPromFunc struct{}

func (s *timestampPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			return iVal, true
		}
	}
	return nil, true
}

type vectorPromFunc struct{}

func (s *vectorPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			rVals := float64(iVal)
			return rVals, true
		}
	}
	return nil, true
}

type monthPromFunc struct{}

func (s *monthPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			rVals := float64(time.Unix(int64(iVal), 0).UTC().Month())
			return rVals, true
		}
	}
	return nil, true
}

type dayOfMonthPromFunc struct{}

func (s *dayOfMonthPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			rVals := float64(time.Unix(int64(iVal), 0).UTC().Day())
			return rVals, true
		}
	}
	return nil, true
}

type dayOfWeekPromFunc struct{}

func (s *dayOfWeekPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			rVals := float64(time.Unix(int64(iVal), 0).UTC().Weekday())
			return rVals, true
		}
	}
	return nil, true
}

type hourPromFunc struct{}

func (s *hourPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			rVals := float64(time.Unix(int64(iVal), 0).UTC().Hour())
			return rVals, true
		}
	}
	return nil, true
}

type minutePromFunc struct{}

func (s *minutePromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			rVals := float64(time.Unix(int64(iVal), 0).UTC().Minute())
			return rVals, true
		}
	}
	return nil, true
}

type daysInMonthPromFunc struct{}

func (s *daysInMonthPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		if iVal, ok := args[0].(float64); ok {
			v := time.Unix(int64(iVal), 0).UTC()
			rVals := float64(32 - time.Date(v.Year(), v.Month(), 32, 0, 0, 0, 0, time.UTC).Day())
			return rVals, true
		}
	}
	return nil, true
}

type piPromFunc struct{}

func (s *piPromFunc) CallFunc(name string, args []interface{}) (interface{}, bool) {
	if len(args) > 0 {
		return math.Pi, true
	}
	return nil, true
}

// valuer
type PromTimeValuer struct{}

var _ influxql.CallValuer = PromTimeValuer{}

func (PromTimeValuer) Value(_ string) (interface{}, bool) {
	return nil, false
}

func (PromTimeValuer) SetValuer(_ influxql.Valuer, _ int) {
}

func (v PromTimeValuer) Call(name string, args []interface{}) (interface{}, bool) {
	if timeFunc := promTimeFuncInstance[name]; timeFunc != nil {
		return timeFunc.CallFunc(name, args)
	}
	return nil, false
}
