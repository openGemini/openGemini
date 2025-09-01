/*
Copyright (c) 2018 InfluxData
This code is originally from: This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/services/httpd/handler.go

2022.01.23 use the http statistics
Huawei Cloud Computing Technologies Co., Ltd.
*/

package statistics

var handler = &Handler{}

func init() {
	NewCollector().Register(handler)
}

func NewHandler() *Handler {
	handler.enabled = true
	return handler
}

type Handler struct {
	BaseCollector

	Requests                     *ItemInt64 `name:"req"`                     // Number of HTTP requests served.
	QueryStmtCount               *ItemInt64 `name:"queryReq"`                // Number of query requests served.
	Query400ErrorStmtCount       *ItemInt64 `name:"queryStmtCount"`          // Number of query stmt served.
	QueryErrorStmtCount          *ItemInt64 `name:"query400ErrorStmtCount"`  // Number of query stmt occur 400(client) error.
	QueryRequests                *ItemInt64 `name:"queryErrorStmtCount"`     // Number of query stmt occur not 400 error.
	WriteRequests                *ItemInt64 `name:"writeReq"`                // Number of write requests serverd.
	Write400ErrRequests          *ItemInt64 `name:"write400ErrReq"`          // Number of write 400 requests occur error.
	Write500ErrRequests          *ItemInt64 `name:"write500ErrReq"`          // Number of write 500 requests occur error.
	PingRequests                 *ItemInt64 `name:"pingReq"`                 // Number of ping requests served.
	StatusRequests               *ItemInt64 `name:"statusReq"`               // Number of status requests served.
	WriteRequestBytesReceived    *ItemInt64 `name:"writeReqBytesIn"`         // Sum of all bytes in write requests.
	WriteRequestBytesIn          *ItemInt64 `name:"writeReqBytes"`           // Sum of all bytes in write requests written success.
	QueryRequestBytesTransmitted *ItemInt64 `name:"queryRespBytes"`          // Sum of all bytes returned in query reponses.
	PointsWrittenOK              *ItemInt64 `name:"pointsWrittenOK"`         // Number of points written OK.
	FieldsWritten                *ItemInt64 `name:"fieldsWritten"`           // Number of fields written.
	PointsWrittenDropped         *ItemInt64 `name:"pointsWrittenDropped"`    // Number of points dropped by the storage engine.
	PointsWrittenFail            *ItemInt64 `name:"pointsWrittenFail"`       // Number of points that failed to be written.
	AuthenticationFailures       *ItemInt64 `name:"authFail"`                // Number of authentication failures.
	RequestDuration              *ItemInt64 `name:"reqDurationNs"`           // Number of (wall-time) nanoseconds spent inside requests.
	WriteRequestParseDuration    *ItemInt64 `name:"writeReqParseDurationNs"` // Number of (wall-time) nanoseconds spent parse write requests.
	QueryRequestDuration         *ItemInt64 `name:"queryReqDurationNs"`      // Number of (wall-time) nanoseconds spent inside query requests.
	WriteRequestDuration         *ItemInt64 `name:"writeReqDurationNs"`      // Number of (wall-time) nanoseconds spent inside write requests.
	ActiveRequests               *ItemInt64 `name:"reqActive"`               // Number of currently active requests.
	ActiveWriteRequests          *ItemInt64 `name:"writeReqActive"`          // Number of currently active write requests.
	ActiveQueryRequests          *ItemInt64 `name:"queryReqActive"`          // Number of currently active query requests.
	ClientErrors                 *ItemInt64 `name:"clientError"`             // Number of HTTP responses due to client error.
	ServerErrors                 *ItemInt64 `name:"serverError"`             // Number of HTTP responses due to server error.
	RecoveredPanics              *ItemInt64 `name:"recoveredPanics"`         // Number of panics recovered by HTTP handler.
	WriteScheduleUnMarshalDns    *ItemInt64 `name:"scheduleUnmarshalDns"`    // Schedule unmarshal durations
	WriteCreateMstDuration       *ItemInt64 `name:"writeCreateMstDurationNs"`
	WriteUpdateSchemaDuration    *ItemInt64 `name:"writeUpdateSchemaDurationNs"`
	WriteCreateSgDuration        *ItemInt64 `name:"writeCreateSgDurationNs"`
	WriteUnmarshalSkDuration     *ItemInt64 `name:"writeUnmarshalSkDurationNs"`
	WriteStoresDuration          *ItemInt64 `name:"writeStoresDurationNs"`
	WriteUpdateIndexDuration     *ItemInt64 `name:"WriteUpdateIndexDurationNs"`
	WriteMapRowsDuration         *ItemInt64 `name:"WriteMapRowsDurationNs"`
	WriteStreamRoutineDuration   *ItemInt64 `name:"WriteStreamRoutineDurationNs"`
	ConnectionNums               *ItemInt64 `name:"connectionNums"` // Number of current connections"storeQueryReq"`
}

func (s *Handler) MeasurementName() string {
	return "httpd"
}
