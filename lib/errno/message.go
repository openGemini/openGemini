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

type Message struct {
	format string
	level  Level
	module Module
}

func newMessage(format string, module Module, level Level) *Message {
	return &Message{
		format: format,
		level:  level,
		module: module,
	}
}

func newNoticeMessage(format string, module Module) *Message {
	return newMessage(format, module, LevelNotice)
}

func newWarnMessage(format string, module Module) *Message {
	return newMessage(format, module, LevelWarn)
}

func newFatalMessage(format string, module Module) *Message {
	return newMessage(format, module, LevelFatal)
}

var unknownMessage = newNoticeMessage("unknown error", ModuleUnknown)

// When an error message is initialized, the level and module corresponding to the error code are bound
// If the module to which the error code belongs cannot be determined during initialization, set to ModuleUnknown
// Can set module when recording logs
var messageMap = map[Errno]*Message{
	// common error codes
	InternalError:      newWarnMessage("%v", ModuleUnknown),
	InvalidDataType:    newWarnMessage("invalid data type, exp: %s, got: %s", ModuleUnknown),
	RecoverPanic:       newFatalMessage("runtime panic: %v", ModuleUnknown),
	UnknownMessageType: newFatalMessage("unknown message type: %v", ModuleUnknown),
	InvalidBufferSize:  newWarnMessage("invalid buffer size, excepted %d; actual %d", ModuleUnknown),
	ShortBufferSize:    newWarnMessage("invalid buffer size, expected greater than %d; actual %d", ModuleUnknown),
	ShortWrite:         newWarnMessage("short write. succeeded in writing %d bytes, but expected %d bytes", ModuleUnknown),
	ShortRead:          newWarnMessage("short read. succeeded in reading %d bytes, but expected %d bytes", ModuleUnknown),
	InvalidMeasurement: newWarnMessage("invalid measurement name: %s", ModuleUnknown),

	// write error codes
	WriteNoShardGroup:               newWarnMessage("nil shard group", ModuleWrite),
	WriteNoShardKey:                 newWarnMessage("measurement should have shard key", ModuleWrite),
	WritePointMustHaveAField:        newWarnMessage("parse fail, point without fields is unsupported", ModuleWrite),
	WritePointInvalidTimeField:      newWarnMessage("parse fail, time of field key is unsupported", ModuleWrite),
	WriteInvalidPoint:               newWarnMessage("point is invalid", ModuleWrite),
	WritePointMustHaveAMeasurement:  newWarnMessage("parse fail, point without measurement is unsupported", ModuleWrite),
	WritePointShouldHaveAllShardKey: newWarnMessage("point should have all shard key", ModuleWrite),
	WritePointMap2Shard:             newWarnMessage("can't map point to shard", ModuleWrite),
	WriteMapMetaShardInfo:           newWarnMessage("can't map meta.ShardInfo", ModuleWrite),
	WritePointOutOfRP:               newWarnMessage("point time is expired, compared with rp duration", ModuleWrite),

	ErrUnmarshalPoints:           newWarnMessage("unmarshal points error, err: %s", ModuleWrite),
	ErrWriteReadonly:             newWarnMessage("this node is readonly status", ModuleWrite),
	ParseFieldTypeConflict:       newWarnMessage("conflict field type: %s", ModuleWrite),
	EngineClosed:                 newWarnMessage("engine is closed", ModuleWrite),
	WriteMissTagValue:            newWarnMessage("missing tag value for %q", ModuleWrite),
	ErrorTagArrayFormat:          newWarnMessage("error tag array format", ModuleWrite),
	WriteErrorArray:              newWarnMessage("error tag array", ModuleWrite),
	TooManyTagKeys:               newWarnMessage("too many tag keys", ModuleWrite),
	SeriesLimited:                newWarnMessage("too many series in database %s. upper limit: %d; current: %d", ModuleWrite),
	RecordWriterFatalErr:         newFatalMessage("record writer raise fatal error", ModuleWrite),
	ArrowRecordTimeFieldErr:      newFatalMessage("the time field of arrow record should the last column", ModuleWrite),
	ArrowFlightGetRoleErr:        newFatalMessage("arrow flight only support the ts-server or ts-data", ModuleWrite),
	ArrowFlightGetShardGroupErr:  newFatalMessage("arrow flight no shard group that meets the data time is matched. shardGroup startTime: %d, data startTime: %d", ModuleWrite),
	ColumnStoreColNumErr:         newFatalMessage("checkSchema: the column num of column store should be large than 1. db: %s, rp: %s, mst: %s", ModuleWrite),
	ColumnStoreSchemaNullErr:     newFatalMessage("checkSchema: the mst and schema of column store should not be null. db: %s, rp: %s, mst: %s", ModuleWrite),
	ColumnStorePrimaryKeyNullErr: newFatalMessage("checkSchema: the primary key of column store should not be null. db: %s, rp: %s, mst: %s", ModuleWrite),
	ColumnStorePrimaryKeyLackErr: newFatalMessage("checkSchema: the primary key of column store should not be lacked. mst: %s, key: %s", ModuleWrite),
	ColumnStoreFieldNameErr:      newFatalMessage("checkSchema: the field name of column store is not found. mst: %s, field: %s", ModuleWrite),
	ColumnStoreFieldTypeErr:      newFatalMessage("checkSchema: the field type of column store is wrong. mst: %s, field: %s, fieldType: %d, colType: %d", ModuleWrite),
	WritePointHasInvalidTag:      newFatalMessage("column store write point has Invalid tag :%s", ModuleWrite),
	WritePointHasInvalidField:    newFatalMessage("column store write point has Invalid field :%s", ModuleWrite),
	WritePointSchemaInvalid:      newFatalMessage("point schema length does not match ddl schema length: %d != %d", ModuleWrite),
	WritePointPrimaryKeyErr:      newFatalMessage("checkSchema: write point is not match the number of primary key. mst: %s,  expect:%d but:%d", ModuleWrite),
	MeasurementNameTooLong:       newWarnMessage("measurement name is :%s. upper limit: %d; current: %d", ModuleWrite),

	// write interface error codes
	InvalidLogDataType:                 newWarnMessage("invalid log data type value", ModuleWriteInterface),
	InvalidMappingTimeKey:              newWarnMessage("mapping missing timestamp key", ModuleWriteInterface),
	InvalidMappingContentKeyMissing:    newWarnMessage("mapping missing content key", ModuleWriteInterface),
	InvalidMappingTimeKeyType:          newWarnMessage("mapping timestamp key type should be string", ModuleWriteInterface),
	InvalidMappingTimeKeyVal:           newWarnMessage("mapping timestamp key is empty", ModuleWriteInterface),
	InvalidMappingContentKeyType:       newWarnMessage("mapping content key type should be string array", ModuleWriteInterface),
	InvalidMappingContentKeySubType:    newWarnMessage("mapping content sub key type should be string", ModuleWriteInterface),
	InvalidMappingContentKeyValMissing: newWarnMessage("mapping content key value is empty array", ModuleWriteInterface),
	InvalidMappingTagsKeyType:          newWarnMessage("mapping tags key type should be string array", ModuleWriteInterface),
	InvalidMappingTagsKeySubType:       newWarnMessage("mapping tags sub key type should be string", ModuleWriteInterface),
	InvalidRequestBodyLength:           newWarnMessage("request body length more than 100M", ModuleWriteInterface),
	InvalidMappingDefaultType:          newWarnMessage("invalid mapping default type value", ModuleWriteInterface),
	InvalidRetryPara:                   newWarnMessage("invalid retry value", ModuleWriteInterface),
	InvalidXLogCompressType:            newWarnMessage("x-log-compresstype is err", ModuleWriteInterface),

	// network module error codes
	NoConnectionAvailable: newFatalMessage("no connections available, node: %v, %v", ModuleNetwork),
	NoNodeAvailable:       newFatalMessage("no node available, node: %v", ModuleNetwork),
	NodeConflict:          newWarnMessage("node conflict", ModuleNetwork),
	SelectClosedConn:      newWarnMessage("select data from closed connection. remote addr: %s; local addr: %s", ModuleNetwork),
	UnsupportedFlags:      newWarnMessage("handle data with unsupported flags(%d)", ModuleNetwork),
	InvalidHeaderSize:     newFatalMessage("expect read header with length %d, but %d", ModuleNetwork),
	InvalidHeader:         newFatalMessage("invalid version(%d), type(%d) of header", ModuleNetwork),
	DuplicateSession:      newNoticeMessage("add duplicate session with id %d", ModuleNetwork),
	InvalidDataSize:       newFatalMessage("expect write with data length %d, but %d", ModuleNetwork),
	TooManySessions:       newWarnMessage("accepted concurrent session exceeds the threshold(%d)", ModuleNetwork),
	ConnectionClosed:      newWarnMessage("multiplexed connection closed", ModuleNetwork),
	DuplicateEvent:        newFatalMessage("duplicate event for transition (%d, %d, %d)", ModuleNetwork),
	InvalidPublicKey:      newFatalMessage("invalid public key type, exp: *rsa.PublicKey; got: %s", ModuleUnknown),
	ShortPublicKey:        newFatalMessage("public key is too short, at least %d bit are required. got: %d bit", ModuleUnknown),
	UnsupportedSignAlgo:   newFatalMessage("unsupported signature algorithm: %s", ModuleUnknown),
	CertificateExpired:    newFatalMessage("certificate: %s expires on %s", ModuleUnknown),
	PoolClosed:            newWarnMessage("try get connection from a closed pool", ModuleNetwork),
	DuplicateConnection:   newWarnMessage("duplicate connection accept by server session", ModuleNetwork),
	NoReactorHandler:      newWarnMessage("handler of reactor for type %d is nil", ModuleNetwork),
	ResponserClosed:       newWarnMessage("apply on the closed responser", ModuleNetwork),
	InvalidAddress:        newNoticeMessage("invalid address: %s", ModuleNetwork),
	BadListen:             newNoticeMessage("bad practice to listen on %s", ModuleNetwork),
	FailedConvertToCodec:  newWarnMessage("failed to convert to Codec, give type: %s", ModuleNetwork),
	OpenSessionTimeout:    newWarnMessage("failed to open session: timeout", ModuleNetwork),
	SessionSelectTimeout:  newWarnMessage("select timeout in %s seconds", ModuleNetwork),
	RemoteError:           newWarnMessage("remote error: %v", ModuleNetwork),
	DataACKTimeout:        newWarnMessage("wait data ack signal timeout", ModuleNetwork),
	InvalidTLSConfig:      newWarnMessage("tsl configuration is not enabled or invalid", ModuleNetwork),

	// query engine error codes
	PipelineExecuting:            newNoticeMessage("pipeline executor is executing with %v and %v", ModuleQueryEngine),
	LogicPlanNotInit:             newWarnMessage("failed to unmarshal logical plan: %v, because it was not initialized", ModuleQueryEngine),
	NotSupportUnmarshal:          newWarnMessage("%s does not support unmarshal", ModuleQueryEngine),
	ProcessorNotFound:            newWarnMessage("processor wasn't found", ModuleQueryEngine),
	MissInputProcessor:           newWarnMessage("output(%d) of processor(%s) has no input processor", ModuleQueryEngine),
	MissOutputProcessor:          newWarnMessage("input(%d) of processor(%s) has no output processor", ModuleQueryEngine),
	MissInputTransform:           newWarnMessage("no input transform for plan", ModuleQueryEngine),
	CyclicVertex:                 newWarnMessage("a cyclic vertex(%d) found", ModuleQueryEngine),
	CyclicGraph:                  newWarnMessage("cyclic graph found", ModuleQueryEngine),
	UnsupportedLogicalPlan:       newWarnMessage("unsupported logical plan %v, can't build processor from it", ModuleQueryEngine),
	UnsupportedDataType:          newWarnMessage("unsupported (%s) iterator type: (%s)", ModuleQueryEngine),
	LogicalPlanBuildFail:         newWarnMessage("logical plan build failed: %s", ModuleQueryEngine),
	CreatePipelineExecutorFail:   newWarnMessage("create pipeline executor raise panic: %s", ModuleQueryEngine),
	LogicalPlainBuildFailInShard: newWarnMessage("logical plain build fail in shard: %v", ModuleQueryEngine),
	SchemaNotAligned:             newWarnMessage("input and output schemas art not aligned: %s", ModuleQueryEngine),
	NoFieldSelected:              newWarnMessage("no field selected: %s", ModuleQueryEngine),
	NoDimSelected:                newWarnMessage("no dim selected: %s", ModuleQueryEngine),
	NoColValToColumnFunc:         newWarnMessage("no func to transform colval into column: %s", ModuleQueryEngine),
	InvalidQuerySchema:           newWarnMessage("invalid query schema", ModuleQueryEngine),
	InvalidQueryStat:             newWarnMessage("invalid query stat", ModuleQueryEngine),
	ErrQueryNotFound:             newWarnMessage("no such query id: %d", ModuleQueryEngine),
	ErrQueryKilled:               newWarnMessage("query(%d) killed", ModuleQueryEngine),

	// store engine error codes
	CreateIndexFailPointRowType:        newFatalMessage("create index failed due to rows are not belong to type PointRow", ModuleIndex),
	InvalidDataDir:                     newWarnMessage("Data.Dir must be specified", ModuleUnknown),
	InvalidMetaDir:                     newWarnMessage("Meta.Dir must be specified", ModuleUnknown),
	InvalidImmTableMaxMemoryPercentage: newWarnMessage("imm-table-max-memory-percentage must be greater than 0", ModuleUnknown),
	InvalidMaxConcurrentCompactions:    newWarnMessage("max-concurrent-compactions must be greater than 0", ModuleUnknown),
	InvalidMaxFullCompactions:          newWarnMessage("max-full-compactions must be greater than 0", ModuleUnknown),
	InvalidShardMutableSizeLimit:       newWarnMessage("shard-mutable-size-limit must be greater than 0", ModuleUnknown),
	InvalidNodeMutableSizeLimit:        newWarnMessage("node-mutable-size-limit must be greater than shard-mutable-size-limit", ModuleUnknown),
	UnrecognizedEngine:                 newWarnMessage("unrecognized engine %s", ModuleUnknown),
	ProcessCompactLogFailed:            newFatalMessage("process compact log file failed, logDir=%s, errInfo=%s", ModuleCompact),
	RecoverFileFailed:                  newFatalMessage("recover file failed, shardDir %s", ModuleCompact),
	NotAllTsspFileOpenSuccess:          newFatalMessage("not all tssp file open success, totalCnt=%d, errCnt=%d", ModuleTssp),
	NotAllTsspFileLoadSuccess:          newFatalMessage("not all tssp file load success, totalCnt=%d, errCnt=%d", ModuleTssp),
	LoadFilesFailed:                    newFatalMessage("table store loadFiles failed", ModuleTssp),
	WriteFileFailed:                    newFatalMessage("table store write file failed", ModuleTssp),
	RemoveFileFailed:                   newFatalMessage("table store remove file failed", ModuleTssp),
	RenameFileFailed:                   newFatalMessage("table store rename file failed", ModuleTssp),
	CreateFileFailed:                   newFatalMessage("table store create file failed", ModuleTssp),
	ReadFileFailed:                     newFatalMessage("table store read file failed", ModuleTssp),
	OpenFileFailed:                     newFatalMessage("table store open file failed", ModuleTssp),
	MapFileFailed:                      newFatalMessage("table store mmap file failed", ModuleTssp),
	CloseFileFailed:                    newFatalMessage("table store close file failed", ModuleTssp),
	CompactPanicFail:                   newFatalMessage("compact fail", ModuleTssp),
	ErrShardClosed:                     newFatalMessage("shard closed %v", ModuleTssp),
	DBPTClosed:                         newWarnMessage("DBPT is being closing or closed", ModuleTssp),
	ShardNotFound:                      newWarnMessage("shard not found %v", ModuleTssp),
	IndexNotFound:                      newWarnMessage("shard index not exist db %s ,pt %v ,index %v", ModuleTssp),
	FailedToDecodeFloatArray:           newFatalMessage("failed to decode float array. exp length: %d, got: %d", ModuleStorageEngine),
	InvalidFloatBuffer:                 newFatalMessage("invalid input float encoded data, type = %v", ModuleStorageEngine),
	MemUsageExceeded:                   newFatalMessage("mem usage exceeded threshold %d", ModuleStorageEngine),

	// wal error codes
	ReadWalFileFailed:         newWarnMessage("read wal file failed", ModuleWal),
	DecompressWalRecordFailed: newWarnMessage("decompress wal record failed, name: %s, error: %s", ModuleWal),
	WalRecordHeaderCorrupted:  newWarnMessage("wal record header is corrupt", ModuleWal),
	WalRecordUnmarshalFailed:  newWarnMessage("wal record unmarshal failed, sid=%d, err=%s", ModuleWal),

	// merge out of order
	SeriesIdIsZero:     newFatalMessage("invalid record, series id is 0. file: %s", ModuleMerge),
	DiffLengthOfColVal: newFatalMessage("the number of ColVals is different", ModuleMerge),
	DiffSchemaType:     newFatalMessage("the schema type are different. name=%s, type=%d, %d", ModuleMerge),
	MergeCanceled:      newNoticeMessage("canceled", ModuleMerge),

	// query engine error codes
	UnsupportedExprType:            newWarnMessage("unsupported expr type of fill processor", ModuleQueryEngine),
	UnsupportedToFillPrevious:      newFatalMessage("the data type is not supported to fill previous: %s", ModuleQueryEngine),
	UnsupportedConditionInFullJoin: newWarnMessage("unsupported condition in full join", ModuleQueryEngine),
	UnsupportedHoltWinterInit:      newWarnMessage("unsupported holt_winters init", ModuleQueryEngine),
	BucketLacks:                    newWarnMessage("get resources out of time: bucket lacks of resources", ModuleQueryEngine),
	ShardBucketLacks:               newWarnMessage("get shard resources out of time: bucket lacks of resources", ModuleQueryEngine),
	SeriesBucketLacks:              newWarnMessage("get series resources out of time: bucket lacks of resources", ModuleQueryEngine),
	SortTransformRunningErr:        newWarnMessage("SortTransform run error", ModuleQueryEngine),
	HashMergeTransformRunningErr:   newWarnMessage("HashMergeTransform run error", ModuleQueryEngine),
	HashAggTransformRunningErr:     newWarnMessage("HashAggTransform work error", ModuleQueryEngine),
	InvalidIncQueryScrollID:        newFatalMessage("the format of the scroll_id inc query should be queryId-iterID. scroll_id=%s.", ModuleQueryEngine),
	FailedGetNodeMaxIterNum:        newFatalMessage("failed to get the max iter num for the inc query, queryID=%s. [Node]", ModuleQueryEngine),
	FailedPutGlobalMaxIterNum:      newFatalMessage("failed to put the max iter num for the inc query, queryID=%s. [Global]", ModuleQueryEngine),
	FailedGetGlobalMaxIterNum:      newFatalMessage("failed to get the max iter num for the inc query, queryID=%s. [Global]", ModuleQueryEngine),
	FailedRetryInvalidCache:        newFatalMessage("cache is invalid, failed to retry", ModuleQueryEngine),
	FailedGetIncAggItem:            newFatalMessage("IncAggTransform can not get the aggItem, queryId %s, iterID %d.", ModuleQueryEngine),
	InvalidIncQueryTimeDuration:    newFatalMessage("inc query should contain the start and end time. start=%d, end=%d", ModuleQueryEngine),
	InvalidIncAggItem:              newFatalMessage("the type of IncAggEntry element must be the IncAggItem", ModuleQueryEngine),
	ErrIncAggIterID:                newFatalMessage("the iterID is not equal to the expected. actual: %d, expected: %d", ModuleQueryEngine),
	ErrInputTimeExceedTimeRange:    newFatalMessage("input time exceeds the query time range. start=%d, end=%d, time=%d", ModuleQueryEngine),
	FailedPutNodeMaxIterNum:        newFatalMessage("failed to put the max iter num for the inc query, queryID=%s. [Node]", ModuleQueryEngine),

	// query interface error codes
	ReverseValueIllegal:     newWarnMessage("reverse value is illegal", ModuleQueryInterface),
	TimeoutMsValueIllegal:   newWarnMessage("timeout_ms value is illegal", ModuleQueryInterface),
	TimeoutMsRangeIllegal:   newWarnMessage("the valid range for timeout_ms is [%d, %d]", ModuleQueryInterface),
	FromValueIllegal:        newWarnMessage("from value is illegal", ModuleQueryInterface),
	ToValueIllegal:          newWarnMessage("to value is illegal", ModuleQueryInterface),
	FromValueLargerThanTo:   newWarnMessage("from value cannot be lower than to value", ModuleQueryInterface),
	FromValueLowerThanMin:   newWarnMessage("from value is lower than min value [%d]", ModuleQueryInterface),
	ToValueLargerThanMax:    newWarnMessage("to value is larger than max value [%d]", ModuleQueryInterface),
	ScrollIdRangeInvalid:    newWarnMessage("the valid range for scroll_id is [%d, %d]", ModuleQueryInterface),
	LimitValueIllegal:       newWarnMessage("limit value is illegal", ModuleQueryInterface),
	LimitValueLargerThanMax: newWarnMessage("limit is larger than max value [%d]", ModuleQueryInterface),
	LimitValueLowerThanMin:  newWarnMessage("limit is lower than min value [%d]", ModuleQueryInterface),
	HighlightValueIllegal:   newWarnMessage("highlight value is illegal", ModuleQueryInterface),
	SqlValueIllegal:         newWarnMessage("sql value is illegal", ModuleQueryInterface),
	TooLongQuery:            newWarnMessage("too long query, maximum length for query is %v", ModuleQueryInterface),
	WrongScrollId:           newWarnMessage("wrong scroll_id", ModuleQueryInterface),
	ScrollIdIllegal:         newWarnMessage("scroll_id value is illegal", ModuleQueryInterface),
	SetValueFailed:          newWarnMessage("set value failed", ModuleQueryInterface),

	// meta error codes
	InvalidTagKey:           newWarnMessage(`tag key can't be time, measurement is '%s'`, ModuleMeta),
	ConflictWithRep:         newWarnMessage("current feature conflicts with replication", ModuleMeta),
	ReplicaNumberNotEqual:   newWarnMessage("replication number of retention policy is not equal to database", ModuleMeta),
	ReplicaNumberNotSupport: newWarnMessage("replication number large than 2 is not support", ModuleMeta),
	ReplicaNodeNumIncorrect: newWarnMessage("node num %d is not an integer multiple of replicaN %d", ModuleMeta),
	FieldTypeConflict:       newWarnMessage(`field type conflict: input field "%s" on measurement "%s" is type %s, already exists as type %s`, ModuleMeta),
	DatabaseNotFound:        newWarnMessage("database not found: %s", ModuleMeta),
	DataNodeNotFound:        newWarnMessage("dataNode(id=%d,host=%s) not found", ModuleMeta),
	DataNoAlive:             newWarnMessage("dataNode(id=%d,host=%s) is not alive", ModuleMeta),
	ShardMetaNotFound:       newWarnMessage("shard(id=%d) meta not found", ModuleMeta),
	DataIsOlder:             newWarnMessage("current data is older than remote", ModuleMeta),
	DatabaseIsBeingDelete:   newWarnMessage("database(%s) is being delete", ModuleMeta),
	MetaIsNotLeader:         newWarnMessage("node is not the leader", ModuleMeta),
	RaftIsNotOpen:           newWarnMessage("raft is not open", ModuleMeta),
	ShardKeyConflict:        newWarnMessage("shard key conflict", ModuleMeta),
	ErrMeasurementNotFound:  newWarnMessage("measurement not found", ModuleMeta),
	PtNotFound:              newWarnMessage("pt not found", ModuleMeta),
	StreamHasExist:          newWarnMessage("stream has been existed", ModuleMeta),
	StreamNotFound:          newWarnMessage("stream not found", ModuleMeta),
	DataNodeSplitBrain:      newWarnMessage("data node split brain", ModuleMeta),
	OlderEvent:              newWarnMessage("older event", ModuleMeta),
	RpIsBeingDelete:         newWarnMessage("retention policy is being delete", ModuleMeta),
	ShardIsBeingDelete:      newWarnMessage("shard is being delete", ModuleMeta),
	MstIsBeingDelete:        newWarnMessage("measurement is being delete", ModuleMeta),

	NeedChangeStore:            newWarnMessage("need change store", ModuleHA),
	StateMachineIsNotRunning:   newWarnMessage("state machine is not running", ModuleHA),
	ConflictWithEvent:          newWarnMessage("conflict with exist event", ModuleHA),
	EventIsInterrupted:         newWarnMessage("pt event is interrupted", ModuleHA),
	EventNotFound:              newWarnMessage("event is not found", ModuleHA),
	PtChanged:                  newWarnMessage("pt is changed", ModuleHA),
	OpIdIsInvalid:              newWarnMessage("event op id is invalid", ModuleHA),
	ClusterManagerIsNotRunning: newWarnMessage("cluster manager is stopped", ModuleHA),
	ErrMigrationRequestDB:      newWarnMessage("migration action, but db is empty", ModuleHA),
	ErrMigrationRequestPt:      newWarnMessage("migration action, but pt is nil", ModuleHA),
	PtIsAlreadyMigrating:       newWarnMessage("pt is already migrating", ModuleHA),
	InvalidMigrationType:       newWarnMessage("invalid migration type", ModuleHA),

	InvalidName:              newWarnMessage("invalid database name", ModuleMeta),
	DownSamplePolicyExists:   newWarnMessage("downSample policy has been existed, drop it first", ModuleMeta),
	DownSamplePolicyNotFound: newWarnMessage("downSample policy is not found", ModuleMeta),
	DownSampleIntervalCheck: newWarnMessage("higher level downSample intervals must be must be an integer "+
		"multiple of lower level downSample intervals", ModuleMeta),
	DownSampleIntervalLenCheck:         newWarnMessage("%s and %s interval lengths is not same", ModuleMeta),
	DownSampleParaError:                newWarnMessage("%s can not used for downSample, expected Call", ModuleMeta),
	DownSampleUnExpectedDataType:       newWarnMessage("%s type is unsupported for downSample", ModuleMeta),
	DownSampleAtLeastOneOpsForDataType: newWarnMessage("%s type must contain at least one operator", ModuleMeta),
	DownSampleUnsupportedAggOp:         newWarnMessage("%s is not supported for downSample", ModuleMeta),
	RpNotFound:                         newWarnMessage("retention policy is not found", ModuleMeta),
	UpdateShardIdentFail:               newWarnMessage("update shard ident fail", ModuleDownSample),
	EventSrcNodeSegregating:            newWarnMessage("event srcNode %d is Segregating", ModuleMeta),
	EventDstNodeSegregating:            newWarnMessage("event dstNode %d is Segregating", ModuleMeta),
	InValidNodeID:                      newWarnMessage("invalid node id: %d", ModuleMeta),
	InValidNodeType:                    newWarnMessage("invalid node type: %s", ModuleMeta),

	// http error codes
	HttpUnauthorized:          newWarnMessage("authorization failed", ModuleHTTP),
	HttpDatabaseNotFound:      newWarnMessage("write error: database not found!", ModuleHTTP),
	HttpForbidden:             newWarnMessage("user is required!", ModuleHTTP),
	HttpRequestEntityTooLarge: newWarnMessage("write error:StatusRequestEntityTooLarge", ModuleHTTP),
	HttpCpuOverLoad:           newWarnMessage("the CPU usage exceeds the threshold", ModuleHTTP),

	// meta-client error codes
	InvalidPwdLen:   newNoticeMessage("the password needs to be between %d and %d characters long", ModuleMetaClient),
	InvalidWeakPwd:  newNoticeMessage("Weak password! Please enter a complex one.", ModuleMetaClient),
	InvalidPwdLooks: newNoticeMessage("User passwords must not same with username or username's reverse.", ModuleMetaClient),
	InvalidPwdComplex: newNoticeMessage("The user password must contain more than 8 characters "+
		"and uppercase letters, lowercase letters, digits, and at least one of "+
		"the special characters.", ModuleMetaClient),
	InvalidUsernameLen: newNoticeMessage("the username needs to be between %d and %d characters long", ModuleMetaClient),

	// index error codes
	ConvertToBinaryExprFailed:  newWarnMessage("convert to BinaryExpr failed: expr %T is not *influxql.BinaryExpr", ModuleIndex),
	ErrQuerySeriesUpperBound:   newNoticeMessage("max-select-series limit exceeded: %d/%d", ModuleIndex),
	ErrTooSmallKeyCount:        newNoticeMessage("too small key count error", ModuleIndex),
	ErrTooSmallIndexKey:        newNoticeMessage("too small index key error", ModuleIndex),
	ErrGetNonExistingMark:      newFatalMessage("trying to get non existing mark %d, while size is %d", ModuleIndex),
	ErrMarkRangeInserting:      newFatalMessage("Intersecting mark ranges are not allowed", ModuleIndex),
	ErrUnknownOpInCondition:    newFatalMessage("Unexpected op type in KeyCondition RPNElement", ModuleIndex),
	ErrInvalidStackInCondition: newFatalMessage("Unexpected stack size in KeyCondition checkInRange", ModuleIndex),
	ErrRPNSetInNotCreated:      newFatalMessage("Set for IN is not created yet", ModuleIndex),
	ErrRPNIsNullForNOT:         newFatalMessage("rpn stack should not be null for NOT", ModuleIndex),
	ErrRPNIsNullForAnd:         newFatalMessage("rpn stack should not be null for AND", ModuleIndex),
	ErrRPNIsNullForOR:          newFatalMessage("rpn stack should not be null for OR", ModuleIndex),
	ErrRPNElemSchema:           newFatalMessage("rpn element should be in schema", ModuleIndex),
	ErrRPNElemNum:              newFatalMessage("rpn element should consist of three elements", ModuleIndex),
	ErrRPNElemOp:               newFatalMessage("rpn operator should be influxql.Token", ModuleIndex),
	ErrRPNOp:                   newFatalMessage("unsupported the rpn op: %d", ModuleIndex),
	ErrRPNExpr:                 newFatalMessage("unknown the rpn expr: %v", ModuleIndex),
	ErrRPNElement:              newFatalMessage("unknown the rpn element: %v", ModuleIndex),
	ErrCoarseIndexFragment:     newFatalMessage("property coarse_index_fragment should be greater than 1", ModuleIndex),
	ErrQuerySchemaUpperBound:   newNoticeMessage("max-select-schema limit exceeded: %d/%d", ModuleQueryEngine),

	// monitoring and statistics
	WatchFileTimeout: newWarnMessage("watch file timeout", ModuleStat),

	// castor error codes
	DtypeNotSupport:          newNoticeMessage("only support integer\\float type", ModuleCastor),
	DtypeNotMatch:            newNoticeMessage("dtype type not match, expect:%v, got:%v", ModuleCastor),
	NumOfFieldNotEqual:       newNoticeMessage("number of field not equal between input and output", ModuleCastor),
	TimestampNotFound:        newNoticeMessage("timestamp not found in response", ModuleCastor),
	TypeAssertFail:           newNoticeMessage("type assert fail, expect %v", ModuleCastor),
	FailToProcessData:        newFatalMessage("fail to process batch point, quantity:%d", ModuleCastor),
	FailToFillUpConnPool:     newFatalMessage("fail to fill up connection pool", ModuleCastor),
	ClientQueueClosed:        newFatalMessage("client queue closed", ModuleCastor),
	NoAvailableClient:        newWarnMessage("no available client to send data", ModuleCastor),
	ConnectionBroken:         newWarnMessage("found connection broken", ModuleCastor),
	ResponseTimeout:          newWarnMessage("data response timeout", ModuleCastor),
	FailToConnectToPyworker:  newFatalMessage("fail to connect to pyworker, err:%v", ModuleCastor),
	UnknownDataMessage:       newFatalMessage("receive unknown message", ModuleCastor),
	UnknownDataMessageType:   newFatalMessage("receive other msgType, expect %s, got:%s", ModuleCastor),
	MessageNotFound:          newNoticeMessage("%s not found", ModuleCastor),
	InvalidAddr:              newNoticeMessage("not valid pyworker addr, expect ip format", ModuleCastor),
	InvalidPort:              newNoticeMessage("port must >= 0", ModuleCastor),
	AlgoConfNotFound:         newNoticeMessage("algorithm configuration file not found", ModuleCastor),
	AlgoNotFound:             newNoticeMessage("algorithm not found", ModuleCastor),
	AlgoTypeNotFound:         newNoticeMessage("algorithm type not found", ModuleCastor),
	InvalidResultWaitTimeout: newNoticeMessage("result-wait-timeout must >= 0", ModuleCastor),
	InvalidPoolSize:          newNoticeMessage("connect-pool-size must >= 1", ModuleCastor),
	ServiceNotEnable:         newNoticeMessage("service not enabled", ModuleCastor),
	ServiceNotAlive:          newNoticeMessage("service not alive", ModuleCastor),
	ResponseIncomplete:       newNoticeMessage("timeout, response incomplete, want:%d, got:%d", ModuleCastor),
	OnlySupportSingleField:   newNoticeMessage("only support 1 field", ModuleCastor),
	InvalidArgsNum:           newNoticeMessage("invalid number of arguments for %s, expected %d, got %d", ModuleCastor),
	DataTooMuch:              newNoticeMessage("too much data, maximum:%d, got:%d", ModuleCastor),
	FieldTypeNotEqual:        newNoticeMessage("field type not equal", ModuleCastor),
	FieldInfoNotFound:        newNoticeMessage("field info not found", ModuleCastor),
	FieldNotFound:            newNoticeMessage("field not found", ModuleCastor),
	MultiFieldIndex:          newNoticeMessage("multiple field index", ModuleCastor),
	EmptyData:                newNoticeMessage("empty input data", ModuleCastor),
	TaskQueueFull:            newNoticeMessage("task queue full", ModuleCastor),
	ExceedRetryChance:        newNoticeMessage("exceed retry chance", ModuleCastor),
	InvalidHaPolicy:          newNoticeMessage("HaPolicy should in (write-available-first, shared-storage, replication)", ModuleCastor),
	UnknownErr:               newNoticeMessage("unknown error", ModuleCastor),
}
