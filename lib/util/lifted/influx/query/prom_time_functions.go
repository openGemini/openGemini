/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

package query

import (
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func init() {
	RegistryPromTimeFunction("year_prom", &yearPromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
	RegistryPromTimeFunction("time_prom", &timePromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
	RegistryPromTimeFunction("vector_prom", &vectorPromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
	RegistryPromTimeFunction("month_prom", &monthPromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
	RegistryPromTimeFunction("day_of_month_prom", &dayOfMonthPromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
	RegistryPromTimeFunction("day_of_week_prom", &dayOfWeekPromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
	RegistryPromTimeFunction("hour_prom", &hourPromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
	RegistryPromTimeFunction("minute_prom", &minutePromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
	RegistryPromTimeFunction("days_in_month_prom", &daysInMonthPromFunc{
		BaseInfo: BaseInfo{FuncType: PROMTIME},
	})
}

func GetPromTimeFunction(name string) PromTimeFunc {
	time, ok := GetFunctionFactoryInstance().promTime[name]
	if ok && time.GetFuncType() == PROMTIME {
		return time
	}
	return nil
}

type yearPromFunc struct {
	BaseInfo
}

func (s *yearPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *yearPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type timePromFunc struct {
	BaseInfo
}

func (s *timePromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *timePromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type vectorPromFunc struct {
	BaseInfo
}

func (s *vectorPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *vectorPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type monthPromFunc struct {
	BaseInfo
}

func (s *monthPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *monthPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type dayOfMonthPromFunc struct {
	BaseInfo
}

func (s *dayOfMonthPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *dayOfMonthPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type dayOfWeekPromFunc struct {
	BaseInfo
}

func (s *dayOfWeekPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *dayOfWeekPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type hourPromFunc struct {
	BaseInfo
}

func (s *hourPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *hourPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type minutePromFunc struct {
	BaseInfo
}

func (s *minutePromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *minutePromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

type daysInMonthPromFunc struct {
	BaseInfo
}

func (s *daysInMonthPromFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	return nil
}

func (s *daysInMonthPromFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Float, nil
}

// type mapper
type PromTimeFunctionTypeMapper struct{}

func (m PromTimeFunctionTypeMapper) MapType(_ *influxql.Measurement, _ string) influxql.DataType {
	return influxql.Unknown
}

func (m PromTimeFunctionTypeMapper) MapTypeBatch(_ *influxql.Measurement, _ map[string]*influxql.FieldNameSpace, _ *influxql.Schema) error {
	return nil
}

func (m PromTimeFunctionTypeMapper) CallType(name string, args []influxql.DataType) (influxql.DataType, error) {
	if promTimeFunc := GetPromTimeFunction(name); promTimeFunc != nil {
		return promTimeFunc.CallTypeFunc(name, args)
	}
	return influxql.Unknown, nil
}
