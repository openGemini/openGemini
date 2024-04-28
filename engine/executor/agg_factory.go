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

package executor

import (
	"github.com/openGemini/openGemini/engine/hybridqp"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/query"
)

type AggOperator interface {
	CreateRoutine(params *AggCallFuncParams) (Routine, error)
}

type AggCallFuncParams struct {
	InRowDataType, OutRowDataType hybridqp.RowDataType    // dataType
	ExprOpt                       hybridqp.ExprOptions    // aggregate column information
	IsSingleCall                  bool                    // is a single call flag, used for optimize performance
	AuxProcessor                  []*AuxProcessor         // eg: select first(v1),v2 from xxx,then v2 is aux
	Opt                           *query.ProcessorOptions // ProcessorOptions is an object passed to CreateIterator to specify creation options.
	ProRes                        *processorResults       //processor response
	IsSubQuery                    bool
	Name                          string
}

var factoryInstance = make(map[string]AggOperator)

func GetAggOperator(name string) AggOperator {
	return factoryInstance[name]
}

type AggFactory map[string]AggOperator

func RegistryAggOp(name string, aggOp AggOperator) {
	_, ok := factoryInstance[name]
	if ok {
		return
	}
	factoryInstance[name] = aggOp
}
