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

package castor

import "time"

type internalTagKey string // distingush data tag and meta-info tag in record

const (
	DataId          internalTagKey = "_dataID"
	Algorithm       internalTagKey = "_algo"
	ConfigFile      internalTagKey = "_cfg"
	ProcessType     internalTagKey = "_processType"
	MessageType     internalTagKey = "_msgType"
	ErrInfo         internalTagKey = "_errInfo"
	TaskID          internalTagKey = "_taskID"
	AnomalyNum      internalTagKey = "_anomalyNum"
	QueryMode       internalTagKey = "_queryMode"
	ConnID          internalTagKey = "_connID"
	FieldKeys       internalTagKey = "field_keys"
	PostProcessType internalTagKey = "type"
	ResultSeries    internalTagKey = "series"
	OutputInfo      internalTagKey = "_outputInfo"
	AlgoParams      internalTagKey = "_algoParams"
	Metric          internalTagKey = "_metric"
)

var internalKeySet = map[string]struct{}{
	string(DataId):          {},
	string(Algorithm):       {},
	string(ConfigFile):      {},
	string(ProcessType):     {},
	string(MessageType):     {},
	string(ErrInfo):         {},
	string(TaskID):          {},
	string(AnomalyNum):      {},
	string(QueryMode):       {},
	string(ConnID):          {},
	string(FieldKeys):       {},
	string(PostProcessType): {},
	string(ResultSeries):    {},
	string(OutputInfo):      {},
}

type resultFieldKey string // validate result from castor

const (
	AnomalyLevel  resultFieldKey = "anomalyLevel"
	DataTime      resultFieldKey = "time"
	DataTimeStamp resultFieldKey = "timestamp"
)

var DesiredFieldKeySet = map[string]struct{}{
	string(AnomalyLevel): {},
}

type messageType string

const (
	DATA messageType = "data"
)

type queryMode string

const (
	NormalQuery     queryMode = "0"
	ContinuousQuery queryMode = "1"
)

const (
	maxRespBufSize    int           = 1000
	chanBufferSize    int           = 100
	getCliTimeout     time.Duration = 2 * time.Second
	connCheckInterval time.Duration = 5 * time.Second
	maxSendRetry      int           = 1
)
