package metrics

var ComIndexRegistry = map[string]*Metric{
	"httpd": {
		HelpMap: map[string]string{
			"fieldsWritten":       "The number of field columns written",
			"writeReqBytes":       "Total amount of data written successfully (as Byte)",
			"writeReqBytesIn":     "The total amount of write data (including write failures and write successes)",
			"writeReq":            "The cumulative number of write requests received by the serverThe cumulative number of write requests received by the server",
			"writeReqActive":      "The number of currently active write requests",
			"writeReqDurationNs":  "Cumulative write data delay (NS)",
			"pointsWrittenOK":     "Number of successful bars written",
			"queryReq":            "Number of client query requests received",
			"queryReqDurationNs":  "Cumulative query latency (NS)",
			"queryReqActive":      "The number of currently active query requests",
			"queryErrorStmtCount": "The number of query statements that failed the query",
			"write400ErrReq":      "The cumulative number of errors caused by problems such as not finding the database, insufficient permissions, and so on",
			"write500ErrReq":      "The cumulative number of write data errors within the server",
			"queryStmtCount":      "Total number of query statements",
		},
		Labels: []string{"app", "hostname"},
	},
	"performance": {
		HelpMap: map[string]string{
			"WriteFieldsCount":        "The total number of indicator columns (Field) read on the ts-store node",
			"writeActiveRequests":     "On the ts-store node, the number of tasks currently being written",
			"writeRowsBatch":          "On the ts-store node, the number of batch writes measures the frequency of data writes.",
			"WriteStorageDurationNs":  "The total delay of the entire writing process on the ts-store node",
			"WriteGetTokenDurationNs": "On the ts-sotre node, when writing data, the cumulative delay in the flow control logic waiting for resource allocation (ns)",
			"WriteIndexDurationNs":    "On the ts-store node, the cumulative delay in creating an index when writing data (ns)",
			"WriteRowsDurationNs":     "On the ts-store node, the cumulative delay of writing to the cache (the granularity is coarser than WriteMstInfoNs)",
			"WriteWalDurationNs":      "Accumulated delay of writing to WAL (ns)",
			"WriteRowsCount":          "On the ts-store node, the total number of rows of data written",
		},
		Labels: []string{"app", "hostname"},
	},
	"io": {
		HelpMap: map[string]string{
			"snapshotBytes": "Snapshot file size (cumulative, calculated in Byte)",
		},
		Labels: []string{"app", "hostname"},
	},
	"executor": {
		HelpMap: map[string]string{
			"exec_run_time_count": "The total number of executor executions of the query plan",
			"exec_abort_count":    "The total number of query plan executor terminations",
			"exec_failed_count":   "The total number of executor execution failures for the query plan",
			"exec_timeout_count":  "The total number of executor timeouts for the query plan",
			"source_rows_sum":     "The total number of rows of entry data when the executor is executed",
			"sink_rows_sum":       "The total number of rows of export data when the executor is executed",
		},
		Labels: []string{"app", "hostname"},
	},
	"system": {
		HelpMap: map[string]string{
			"CpuUsage":     "cpu utilization",
			"MemUsage":     "Node memory usage",
			"CpuNum":       "Number of node CPU cores",
			"MemSize":      "Node memory capacity",
			"DiskSize":     "Node main disk capacity",
			"IndexUsed":    "Index data uses disk capacity",
			"AuxDiskSize":  "Auxiliary disk capacity. WAL files are generally stored on different disks to improve data security. See the ts-monitor.conf aux-disk-path configuration item.",
			"StoreStatus":  "Store process status, 1 (running) 0 (killed)",
			"DiskUsage":    "Node primary disk space usage",
			"AuxDiskUsage": "Secondary disk space usage",
			"MemInUse":     "Used memory size",
			"SqlStatus":    "SQL process status, 1 (running) 0 (killed)",
			"MetaStatus":   "Meta process status, 1 (running) 0 (killed)",
			"Uptime":       "Node running time (s)",
			"NumGC":        "Number of GCs completed",
		},
		Labels: []string{"app", "hostname"},
	},
	"runtime": {
		HelpMap: map[string]string{
			"NumGC":        "Number of GCs completed",
			"NumGoroutine": "Number of Goroutines generated",
		},
		Labels: []string{"app", "hostname"},
	},
	"spdy": {
		HelpMap: map[string]string{
			"connTotal": "Total number of connections (internal)",
		},
		Labels: []string{"app", "hostname", "link", "remote_addr"},
	},
	"measurement_metric": {
		HelpMap: map[string]string{
			"SeriesCount": "Number of timelines",
		},
		Labels: []string{"app", "hostname"},
	},
	"cluster_metric": {
		HelpMap: map[string]string{
			"DBCount":       "Total number of DBs created in the cluster",
			"MstCount": "The total number of tables in all DBs in the cluster",
		},
		Labels: []string{"app", "hostname"},
	},
	"filestat_level": {
		HelpMap: map[string]string{
			"FileCount": "Number of data files for the table",
		},
		Labels: []string{"app", "database", "hostname", "level"},
	},
	"sql_slow_queries": {
		HelpMap: map[string]string{
			"totalDuration": "total duration",
		},
		Labels: []string{"app", "hostname"},
	},
	"errno": {
		HelpMap: map[string]string{
			"value": "Error code",
		},
		Labels: []string{"app", "errno", "hostname", "module"},
	},
}
