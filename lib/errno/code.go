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

package errno

// http
const (
	HttpBadRequest            = 6400
	HttpUnauthorized          = 6401
	HttpDatabaseNotFound      = 6404
	HttpForbidden             = 6403
	HttpRequestEntityTooLarge = 6413
)

// common error codes
const (
	InternalError      = 9001
	InvalidDataType    = 9002
	RecoverPanic       = 9003
	UnknownMessageType = 9004
	InvalidBufferSize  = 9005
	ShortBufferSize    = 9006

	// BuiltInError errors returned by built-in functions
	BuiltInError = 9007

	// ThirdPartyError errors returned by third-party packages
	ThirdPartyError = 9008

	ShortWrite         = 9009
	ShortRead          = 9010
	InvalidMeasurement = 9011
)

// network module error codes
const (
	NoConnectionAvailable = 1001
	NoNodeAvailable       = 1002
	NodeConflict          = 1003
	SelectClosedConn      = 1004
	UnsupportedFlags      = 1005
	InvalidHeaderSize     = 1006
	InvalidHeader         = 1007
	DuplicateSession      = 1008
	InvalidDataSize       = 1009
	TooManySessions       = 1010
	ConnectionClosed      = 1011
	SessionSelectTimeout  = 1012
	DuplicateEvent        = 1013
	InvalidPublicKey      = 1014
	ShortPublicKey        = 1015
	UnsupportedSignAlgo   = 1016
	CertificateExpired    = 1017
	PoolClosed            = 1018
	DuplicateConnection   = 1019
	NoReactorHandler      = 1020
	ResponserClosed       = 1021
	InvalidAddress        = 1022
	BadListen             = 1023
	FailedConvertToCodec  = 1024
	OpenSessionTimeout    = 1025
	RemoteError           = 1206
	DataACKTimeout        = 1027
	InvalidTLSConfig      = 1208
)

// query engine error codes
const (
	PipelineExecuting            = 1101
	LogicPlanNotInit             = 1102
	NotSupportUnmarshal          = 1103
	ProcessorNotFound            = 1104
	MissInputProcessor           = 1105
	MissOutputProcessor          = 1106
	MissInputTransform           = 1107
	CyclicVertex                 = 1108
	CyclicGraph                  = 1109
	UnsupportedLogicalPlan       = 1110
	UnsupportedDataType          = 1111
	LogicalPlanBuildFail         = 1112
	BucketLacks                  = 1113
	CreatePipelineExecutorFail   = 1114
	LogicalPlainBuildFailInShard = 1115
)

// store engine error codes
const (
	CreateIndexFailPointRowType        = 2101
	InvalidDataDir                     = 2102
	InvalidMetaDir                     = 2103
	InvalidImmTableMaxMemoryPercentage = 2104
	InvalidMaxConcurrentCompactions    = 2105
	InvalidMaxFullCompactions          = 2106
	InvalidShardMutableSizeLimit       = 2107
	InvalidNodeMutableSizeLimit        = 2108
	UnrecognizedEngine                 = 2109
	RecoverFileFailed                  = 2110
	NotAllTsspFileOpenSuccess          = 2111
	NotAllTsspFileLoadSuccess          = 2112
	ProcessCompactLogFailed            = 2113
	LoadFilesFailed                    = 2114
	CreateFileFailed                   = 2115
	RenameFileFailed                   = 2116
	WriteFileFailed                    = 2117
	RemoveFileFailed                   = 2118
	ReadFileFailed                     = 2119
	OpenFileFailed                     = 2120
	MapFileFailed                      = 2121
	CloseFileFailed                    = 2122
	ReadWalFileFailed                  = 2123
	DecompressWalRecordFailed          = 2124
	WalRecordHeaderCorrupted           = 2125
	WalRecordUnmarshalFailed           = 2126
	CompactPanicFail                   = 2127
)

// merge out of order
const (
	SeriesIdIsZero     = 2201
	DiffLengthOfColVal = 2202
	DiffSchemaType     = 2203
	MergeCanceled      = 2204
)

// query engine error codes
const (
	UnsupportedExprType       = 3001
	UnsupportedToFillPrevious = 3002
)

// meta
const (
	FieldTypeConflict        = 4001
	DatabaseNotFound         = 4002
	DataNodeNotFound         = 4003
	DataNoAlive              = 4004
	PtNotFound               = 4005
	ShardMetaNotFound        = 4006
	DataIsOlder              = 4007
	DatabaseIsBeingDelete    = 4008
	MetaIsNotLeader          = 4009
	RaftIsNotOpen            = 4010
	ShardKeyConflict         = 4011
	ErrMeasurementNotFound   = 4012
	NeedChangeStore          = 4013
	StateMachineIsNotRunning = 4014
	ConflictWithEvent        = 4015
	EventIsInterrupted       = 4016
	EventNotFound            = 4017
	PtChanged                = 4018
	OpIdIsInvalid            = 4019
)

// meta-client process
const (
	InvalidPwdLen      = 4101
	InvalidWeakPwd     = 4102
	InvalidPwdLooks    = 4103
	InvalidPwdComplex  = 4104
	InvalidUsernameLen = 4105
)

// write process
const (
	WriteNoShardGroup               = 5001
	WriteNoShardKey                 = 5002
	WritePointMustHaveAField        = 5003
	WritePointInvalidTimeField      = 5004
	WriteInvalidPoint               = 5005
	WritePointMustHaveAMeasurement  = 5006
	WritePointShouldHaveAllShardKey = 5007
	WritePointMap2Shard             = 5008

	// WriteMapMetaShardInfo abc
	WriteMapMetaShardInfo = 5009

	ErrUnmarshalPoints         = 5010
	ErrWriteReadonly           = 5011
	DuplicateField             = 5012
	WritePointOutOfRP          = 5013
	WritePointShardKeyTooLarge = 5014
	EngineClosed               = 5015
)

// index
const (
	ConvertToBinaryExprFailed = 6001
)

const (
	WatchFileTimeout = 7001
)

// castor service
const (
	DtypeNotSupport          = 8001
	DtypeNotMatch            = 8002
	NumOfFieldNotEqual       = 8003
	TimestampNotFound        = 8004
	TypeAssertFail           = 8005
	FailToProcessData        = 8006
	FailToFillUpConnPool     = 8007
	ClientQueueClosed        = 8008
	NoAvailableClient        = 8009
	ConnectionBroken         = 8010
	ResponseTimeout          = 8011
	FailToConnectToPyworker  = 8012
	UnknownDataMessage       = 8013
	UnknownDataMessageType   = 8014
	MessageNotFound          = 8015
	InvalidAddr              = 8016
	InvalidPort              = 8017
	AlgoConfNotFound         = 8018
	AlgoNotFound             = 8019
	AlgoTypeNotFound         = 8020
	InvalidResultWaitTimeout = 8021
	InvalidPoolSize          = 8022
	ServiceNotEnable         = 8023
	ServiceNotAlive          = 8024
	ResponseIncomplete       = 8025
	OnlySupportSingleField   = 8026
	InvalidArgsNum           = 8027
	DataTooMuch              = 8028
	FieldTypeNotEqual        = 8029
	FieldInfoNotFound        = 8030
	FieldNotFound            = 8031
	MultiFieldIndex          = 8032
	EmptyData                = 8033
)
