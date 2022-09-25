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

	ErrUnmarshalPoints: newWarnMessage("unmarshal points error, err: %s", ModuleWrite),
	ErrWriteReadonly:   newWarnMessage("this node is readonly status", ModuleWrite),
	DuplicateField:     newWarnMessage("duplicate field: %s", ModuleWrite),
	EngineClosed:       newWarnMessage("engine is closed", ModuleWrite),

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
	SessionSelectTimeout:  newWarnMessage("select timeout in %d seconds", ModuleNetwork),
	RemoteError:           newWarnMessage("remote error: %v", ModuleNetwork),
	DataACKTimeout:        newWarnMessage("wait data ack signal timeout", ModuleNetwork),
	InvalidTLSConfig:      newWarnMessage("tsl configuration is not enabled or invalid", ModuleNetwork),

	// query engine error codes
	PipelineExecuting:          newNoticeMessage("pipeline executor is executing with %v and %v", ModuleQueryEngine),
	LogicPlanNotInit:           newWarnMessage("failed to unmarshal logical plan: %v, because it was not initialized", ModuleQueryEngine),
	NotSupportUnmarshal:        newWarnMessage("%s does not support unmarshal", ModuleQueryEngine),
	ProcessorNotFound:          newWarnMessage("processor wasn't found", ModuleQueryEngine),
	MissInputProcessor:         newWarnMessage("output(%d) of processor(%s) has no input processor", ModuleQueryEngine),
	MissOutputProcessor:        newWarnMessage("input(%d) of processor(%s) has no output processor", ModuleQueryEngine),
	MissInputTransform:         newWarnMessage("no input transform for plan", ModuleQueryEngine),
	CyclicVertex:               newWarnMessage("a cyclic vertex(%d) found", ModuleQueryEngine),
	CyclicGraph:                newWarnMessage("cyclic graph found", ModuleQueryEngine),
	UnsupportedLogicalPlan:     newWarnMessage("unsupported logical plan %v, can't build processor from it", ModuleQueryEngine),
	UnsupportedDataType:        newWarnMessage("unsupported (%s) iterator type: (%s)", ModuleQueryEngine),
	LogicalPlanBuildFail:       newWarnMessage("logical plan build failed: %s", ModuleQueryEngine),
	CreatePipelineExecutorFail: newWarnMessage("create pipeline executor raise panic: %s", ModuleQueryEngine),

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

	// wal error codes
	ReadWalFileFailed:         newWarnMessage("read wal file failed", ModuleWal),
	DecompressWalRecordFailed: newWarnMessage("decompress wal record failed", ModuleWal),
	WalRecordHeaderCorrupted:  newWarnMessage("wal record header is corrupt", ModuleWal),
	WalRecordUnmarshalFailed:  newWarnMessage("wal record unmarshal failed", ModuleWal),

	// merge out of order
	SeriesIdIsZero:     newFatalMessage("invalid record, series id is 0. file: %s", ModuleMerge),
	DiffLengthOfColVal: newFatalMessage("the number of ColVals is different", ModuleMerge),
	DiffSchemaType:     newFatalMessage("the schema type are different. name=%s, type=%d, %d", ModuleMerge),
	MergeCanceled:      newNoticeMessage("canceled", ModuleMerge),

	// query engine error codes
	UnsupportedExprType:       newWarnMessage("unsupported expr type of fill processor", ModuleQueryEngine),
	UnsupportedToFillPrevious: newFatalMessage("the data type is not supported to fill previous: %s", ModuleQueryEngine),
	BucketLacks:               newWarnMessage("get resources out of time: bucket lacks of resources", ModuleQueryEngine),

	// meta error codes
	FieldTypeConflict:        newWarnMessage(`field type conflict: input field "%s" on measurement "%s" is type %s, already exists as type %s`, ModuleMeta),
	DatabaseNotFound:         newWarnMessage("database not found: %s", ModuleMeta),
	DataNodeNotFound:         newWarnMessage("dataNode(id=%d,host=%s) not found", ModuleMeta),
	DataNoAlive:              newWarnMessage("dataNode(id=%d,host=%s) is not alive", ModuleMeta),
	ShardMetaNotFound:        newWarnMessage("shard(id=%d) meta not found", ModuleMeta),
	DataIsOlder:              newWarnMessage("current data is older than remote", ModuleMeta),
	DatabaseIsBeingDelete:    newWarnMessage("database(%s) is being delete", ModuleMeta),
	MetaIsNotLeader:          newWarnMessage("node is not the leader", ModuleMeta),
	RaftIsNotOpen:            newWarnMessage("raft is not open", ModuleMeta),
	ShardKeyConflict:         newWarnMessage("shard key conflict", ModuleMeta),
	ErrMeasurementNotFound:   newWarnMessage("measurement not found", ModuleMeta),
	PtNotFound:               newWarnMessage("pt not found", ModuleMeta),
	NeedChangeStore:          newWarnMessage("need change store", ModuleHA),
	StateMachineIsNotRunning: newWarnMessage("state machine is not running", ModuleHA),
	ConflictWithEvent:        newWarnMessage("conflict with exist event", ModuleHA),
	EventIsInterrupted:       newWarnMessage("pt event is interrupted", ModuleHA),
	EventNotFound:            newWarnMessage("event is not found", ModuleHA),
	PtChanged:                newWarnMessage("pt is changed", ModuleHA),
	OpIdIsInvalid:            newWarnMessage("event op id is invalid", ModuleHA),

	// http error codes
	HttpUnauthorized:          newWarnMessage("authorization failed", ModuleHTTP),
	HttpDatabaseNotFound:      newWarnMessage("write error: database not found!", ModuleHTTP),
	HttpForbidden:             newWarnMessage("user is required!", ModuleHTTP),
	HttpRequestEntityTooLarge: newWarnMessage("write error:StatusRequestEntityTooLarge", ModuleHTTP),

	// meta-client error codes
	InvalidPwdLen:   newNoticeMessage("the password needs to be between %d and %d characters long", ModuleMetaClient),
	InvalidWeakPwd:  newNoticeMessage("Weak password! Please enter a complex one.", ModuleMetaClient),
	InvalidPwdLooks: newNoticeMessage("User passwords must not same with username or username's reverse.", ModuleMetaClient),
	InvalidPwdComplex: newNoticeMessage("The user password must contain more than 8 characters "+
		"and uppercase letters, lowercase letters, digits, and at least one of "+
		"the special characters.", ModuleMetaClient),
	InvalidUsernameLen: newNoticeMessage("the username needs to be between %d and %d characters long", ModuleMetaClient),

	// index error codes
	ConvertToBinaryExprFailed: newWarnMessage("convert to BinaryExpr failed: expr %T is not *influxql.BinaryExpr", ModuleIndex),

	// monitoring and statistics
	WatchFileTimeout: newWarnMessage("watch file timeout", ModuleStat),
}
