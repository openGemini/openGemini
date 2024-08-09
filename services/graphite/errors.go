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

/*
Copyright (c) 2013-2018 InfluxData Inc.
This code is originally from:
https://github.com/influxdata/influxdb/blob/1.8/services/graphite/errors.go
*/

package graphite

import "fmt"

// An UnsupportedValueError is returned when a parsed value is not
// supported.
type UnsupportedValueError struct {
	Field string
	Value float64
}

func (err *UnsupportedValueError) Error() string {
	return fmt.Sprintf(`field "%s" value: "%v" is unsupported`, err.Field, err.Value)
}
