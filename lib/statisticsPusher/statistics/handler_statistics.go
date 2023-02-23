/*
Copyright (c) 2018 InfluxData
This code is originally from: This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/httpd/handler.go

2022.01.23 use the http statistics
Huawei Cloud Computing Technologies Co., Ltd.
*/

package statistics

import (
	"sync/atomic"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics/opsStat"
)

// Statistics keeps statistics related to the Handler
type HandlerStatistics struct {
	Requests                     int64
	QueryStmtCount               int64
	Query400ErrorStmtCount       int64 // client error
	QueryErrorStmtCount          int64
	QueryRequests                int64
	WriteRequests                int64
	Write400ErrRequests          int64
	Write500ErrRequests          int64
	PingRequests                 int64
	StatusRequests               int64
	WriteRequestBytesReceived    int64
	WriteRequestBytesIn          int64
	QueryRequestBytesTransmitted int64
	PointsWrittenOK              int64
	FieldsWritten                int64
	PointsWrittenDropped         int64
	PointsWrittenFail            int64
	AuthenticationFailures       int64
	RequestDuration              int64
	WriteRequestParseDuration    int64
	QueryRequestDuration         int64
	WriteRequestDuration         int64
	ActiveRequests               int64
	ActiveWriteRequests          int64
	ActiveQueryRequests          int64
	ClientErrors                 int64
	ServerErrors                 int64
	RecoveredPanics              int64
	WriteScheduleUnMarshalDns    int64
	WriteCreateMstDuration       int64
	WriteUpdateSchemaDuration    int64
	WriteCreateSgDuration        int64
	WriteUnmarshalSkDuration     int64
	WriteStoresDuration          int64
	WriteUpdateIndexDuration     int64
	WriteMapRowsDuration         int64
	WriteStreamRoutineDuration   int64
}

const (
	statRequest                      = "req"                     // Number of HTTP requests served.
	statQueryRequest                 = "queryReq"                // Number of query requests served.
	statQueryStmtCount               = "queryStmtCount"          // Number of query stmt served.
	Query400ErrorStmtCount           = "query400ErrorStmtCount"  // Number of query stmt occur 400(client) error.
	statQueryErrorStmtCount          = "queryErrorStmtCount"     // Number of query stmt occur not 400 error.
	statWriteRequest                 = "writeReq"                // Number of write requests serverd.
	statWrite400ErrRequest           = "write400ErrReq"          // Number of write 400 requests occur error.
	statWrite500ErrRequest           = "write500ErrReq"          // Number of write 500 requests occur error.
	statPingRequest                  = "pingReq"                 // Number of ping requests served.
	statStatusRequest                = "statusReq"               // Number of status requests served.
	statWriteRequestBytesIn          = "writeReqBytesIn"         // Sum of all bytes in write requests.
	statWriteRequestBytesReceived    = "writeReqBytes"           // Sum of all bytes in write requests written success.
	statQueryRequestBytesTransmitted = "queryRespBytes"          // Sum of all bytes returned in query reponses.
	statPointsWrittenOK              = "pointsWrittenOK"         // Number of points written OK.
	statFieldsWritten                = "fieldsWritten"           // Number of fields written.
	statPointsWrittenDropped         = "pointsWrittenDropped"    // Number of points dropped by the storage engine.
	statPointsWrittenFail            = "pointsWrittenFail"       // Number of points that failed to be written.
	statAuthFail                     = "authFail"                // Number of authentication failures.
	statRequestDuration              = "reqDurationNs"           // Number of (wall-time) nanoseconds spent inside requests.
	statWriteRequestParseDuration    = "writeReqParseDurationNs" // Number of (wall-time) nanoseconds spent parse write requests.
	statQueryRequestDuration         = "queryReqDurationNs"      // Number of (wall-time) nanoseconds spent inside query requests.
	statWriteRequestDuration         = "writeReqDurationNs"      // Number of (wall-time) nanoseconds spent inside write requests.
	statRequestsActive               = "reqActive"               // Number of currently active requests.
	statWriteRequestsActive          = "writeReqActive"          // Number of currently active write requests.
	statQueryRequestsActive          = "queryReqActive"          // Number of currently active query requests.
	statClientError                  = "clientError"             // Number of HTTP responses due to client error.
	statServerError                  = "serverError"             // Number of HTTP responses due to server error.
	statRecoveredPanics              = "recoveredPanics"         // Number of panics recovered by HTTP handler.
	statScheduleUnmarshalDns         = "scheduleUnmarshalDns"    // Schedule unmarshal durations
	statWriteCreateMstDuration       = "writeCreateMstDurationNs"
	statWriteUpdateSchemaDuration    = "writeUpdateSchemaDurationNs"
	statWriteCreateSgDuration        = "writeCreateSgDurationNs"
	statWriteUnmarshalSkDuration     = "writeUnmarshalSkDurationNs"
	statWriteWriteStoresDuration     = "writeStoresDurationNs"
	statWriteUpdateIndexDuration     = "WriteUpdateIndexDurationNs"
	statWriteMapRowsDuration         = "WriteMapRowsDurationNs"
	statWriteStreamRoutineDuration   = "WriteStreamRoutineDurationNs"
)

var HandlerStat = NewHandlerStatistics()
var HandlerTagMap map[string]string
var HandlerStatisticsName = "httpd"

func NewHandlerStatistics() *HandlerStatistics {
	return &HandlerStatistics{}
}

func InitHandlerStatistics(tags map[string]string) {
	HandlerStat = NewHandlerStatistics()
	HandlerTagMap = tags
}

func CollectHandlerStatistics(buffer []byte) ([]byte, error) {
	perfValueMap := genHandlerValueMap()

	buffer = AddPointToBuffer(HandlerStatisticsName, HandlerTagMap, perfValueMap, buffer)
	return buffer, nil
}

func genHandlerValueMap() map[string]interface{} {
	perfValueMap := map[string]interface{}{
		statRequest:                      atomic.LoadInt64(&HandlerStat.Requests),
		statQueryStmtCount:               atomic.LoadInt64(&HandlerStat.QueryStmtCount),
		Query400ErrorStmtCount:           atomic.LoadInt64(&HandlerStat.Query400ErrorStmtCount),
		statQueryErrorStmtCount:          atomic.LoadInt64(&HandlerStat.QueryErrorStmtCount),
		statQueryRequest:                 atomic.LoadInt64(&HandlerStat.QueryRequests),
		statWriteRequest:                 atomic.LoadInt64(&HandlerStat.WriteRequests),
		statWrite400ErrRequest:           atomic.LoadInt64(&HandlerStat.Write400ErrRequests),
		statWrite500ErrRequest:           atomic.LoadInt64(&HandlerStat.Write500ErrRequests),
		statPingRequest:                  atomic.LoadInt64(&HandlerStat.PingRequests),
		statStatusRequest:                atomic.LoadInt64(&HandlerStat.StatusRequests),
		statWriteRequestBytesIn:          atomic.LoadInt64(&HandlerStat.WriteRequestBytesIn),
		statWriteRequestBytesReceived:    atomic.LoadInt64(&HandlerStat.WriteRequestBytesReceived),
		statQueryRequestBytesTransmitted: atomic.LoadInt64(&HandlerStat.QueryRequestBytesTransmitted),
		statPointsWrittenOK:              atomic.LoadInt64(&HandlerStat.PointsWrittenOK),
		statFieldsWritten:                atomic.LoadInt64(&HandlerStat.FieldsWritten),
		statPointsWrittenDropped:         atomic.LoadInt64(&HandlerStat.PointsWrittenDropped),
		statPointsWrittenFail:            atomic.LoadInt64(&HandlerStat.PointsWrittenFail),
		statAuthFail:                     atomic.LoadInt64(&HandlerStat.AuthenticationFailures),
		statRequestDuration:              atomic.LoadInt64(&HandlerStat.RequestDuration),
		statWriteRequestParseDuration:    atomic.LoadInt64(&HandlerStat.WriteRequestParseDuration),
		statQueryRequestDuration:         atomic.LoadInt64(&HandlerStat.QueryRequestDuration),
		statWriteRequestDuration:         atomic.LoadInt64(&HandlerStat.WriteRequestDuration),
		statRequestsActive:               atomic.LoadInt64(&HandlerStat.ActiveRequests),
		statWriteRequestsActive:          atomic.LoadInt64(&HandlerStat.ActiveWriteRequests),
		statQueryRequestsActive:          atomic.LoadInt64(&HandlerStat.ActiveQueryRequests),
		statClientError:                  atomic.LoadInt64(&HandlerStat.ClientErrors),
		statServerError:                  atomic.LoadInt64(&HandlerStat.ServerErrors),
		statRecoveredPanics:              atomic.LoadInt64(&HandlerStat.RecoveredPanics),
		statScheduleUnmarshalDns:         atomic.LoadInt64(&HandlerStat.WriteScheduleUnMarshalDns),
		statWriteCreateMstDuration:       atomic.LoadInt64(&HandlerStat.WriteCreateMstDuration),
		statWriteUpdateSchemaDuration:    atomic.LoadInt64(&HandlerStat.WriteUpdateSchemaDuration),
		statWriteCreateSgDuration:        atomic.LoadInt64(&HandlerStat.WriteCreateSgDuration),
		statWriteUnmarshalSkDuration:     atomic.LoadInt64(&HandlerStat.WriteUnmarshalSkDuration),
		statWriteWriteStoresDuration:     atomic.LoadInt64(&HandlerStat.WriteStoresDuration),
		statWriteUpdateIndexDuration:     atomic.LoadInt64(&HandlerStat.WriteUpdateIndexDuration),
		statWriteMapRowsDuration:         atomic.LoadInt64(&HandlerStat.WriteMapRowsDuration),
		statWriteStreamRoutineDuration:   atomic.LoadInt64(&HandlerStat.WriteStreamRoutineDuration),
	}

	return perfValueMap
}

func CollectOpsHandlerStatistics() []opsStat.OpsStatistic {
	return []opsStat.OpsStatistic{{
		Name:   "httpd",
		Tags:   HandlerTagMap,
		Values: genHandlerValueMap(),
	},
	}
}
