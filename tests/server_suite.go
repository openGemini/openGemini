package tests

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxdb/blob/1.7/tests/server_suite.go

2022.01.23 changed
modified some query response exp, like 'hot durition'
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

var tests Tests

// Load all shared tests
func init() {
	tests = make(map[string]Test)

	tests["database_commands"] = Test{
		queries: []*Query{
			&Query{
				name:    "create database should succeed",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create database with retention duration should succeed",
				command: `CREATE DATABASE db0_r WITH DURATION 24h REPLICATION 1 NAME db0_r_policy`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create database with retention policy should be error with invalid name",
				command: `CREATE DATABASE db1 WITH NAME "."`,
				exp:     `{"results":[{"statement_id":0,"error":"invalid name"}]}`,
				once:    true,
			},
			&Query{
				name:    "create database should error with some unquoted names",
				command: `CREATE DATABASE 0xdb0`,
				exp:     `{"error":"error parsing query: syntax error: unexpected DURATIONVAL, expecting IDENT"}`,
			},
			&Query{
				name:    "create database should error with invalid characters",
				command: `CREATE DATABASE "."`,
				exp:     `{"results":[{"statement_id":0,"error":"invalid name"}]}`,
			},
			&Query{
				name:    "create database with retention duration should error with bad retention duration",
				command: `CREATE DATABASE db0 WITH DURATION xyz`,
				exp:     `{"error":"error parsing query: syntax error: unexpected IDENT, expecting DURATIONVAL"}`,
			},
			&Query{
				name:    "create database with retention replication should error with bad retention replication number",
				command: `CREATE DATABASE db0 WITH REPLICATION xyz`,
				exp:     `{"error":"error parsing query: syntax error: unexpected IDENT, expecting INTEGER"}`,
			},
			&Query{
				name:    "create database with retention name should error with missing retention name",
				command: `CREATE DATABASE db0 WITH NAME`,
				exp:     `{"error":"error parsing query: syntax error: unexpected $end, expecting IDENT"}`,
			},
			&Query{
				name:    "show database should succeed",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db0_r"]]}]}]}`,
			},
			&Query{
				name:    "create database should not error with existing database",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create database should create non-existing database",
				command: `CREATE DATABASE db1`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create database with retention duration should error if retention policy is different",
				command: `CREATE DATABASE db1 WITH DURATION 24h`,
				exp:     `{"results":[{"statement_id":0,"error":"retention policy conflicts with an existing policy"}]}`,
			},
			&Query{
				name:    "create database should error with bad retention duration",
				command: `CREATE DATABASE db1 WITH DURATION xyz`,
				exp:     `{"error":"error parsing query: syntax error: unexpected IDENT, expecting DURATIONVAL"}`,
			},
			&Query{
				name:    "show database should succeed",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db0"],["db0_r"],["db1"]]}]}]}`,
			},
			&Query{
				name:    "drop database db0 should succeed",
				command: `DROP DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database db0_r should succeed",
				command: `DROP DATABASE db0_r`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database db1 should succeed",
				command: `DROP DATABASE db1`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "drop database should not error if it does not exists",
				command: `DROP DATABASE db1`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "drop database should not error with non-existing database db1",
				command: `DROP DATABASE db1`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "show database should have no results",
				command: `SHOW DATABASES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"]}]}]}`,
			},
			&Query{
				name:    "create database with shard group duration should succeed",
				command: `CREATE DATABASE db0 WITH SHARD DURATION 61m`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create database with shard group duration and duration should succeed",
				command: `CREATE DATABASE db1 WITH DURATION 60m SHARD DURATION 30m`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
		},
	}

	tests["measurement_commands"] = Test{
		queries: []*Query{
			&Query{
				name:    "create database should succeed",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create measurement cpu",
				params:  url.Values{"db": []string{"db0"}},
				command: `CREATE MEASUREMENT cpu`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "retry create measurement cpu",
				params:  url.Values{"db": []string{"db0"}},
				command: `CREATE MEASUREMENT cpu`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create measurement cpu with shardkey",
				params:  url.Values{"db": []string{"db0"}},
				command: `CREATE MEASUREMENT cpu WITH SHARDKEY hostname`,
				exp:     `{"results":[{"statement_id":0,"error":"measurement already exists"}]}`,
				once:    true,
			},
			&Query{
				name:    "show measurements",
				params:  url.Values{"db": []string{"db0"}},
				command: `SHOW MEASUREMENTS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["cpu"]]}]}]}`,
				once:    true,
			},
			&Query{
				name:    "create measurement cpu2 with shardkey",
				params:  url.Values{"db": []string{"db0"}},
				command: `CREATE MEASUREMENT cpu2 WITH SHARDKEY hostname`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show measurements",
				params:  url.Values{"db": []string{"db0"}},
				command: `SHOW MEASUREMENTS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["cpu"],["cpu2"]]}]}]}`,
				once:    true,
			},
			&Query{
				name:    "drop measurement cpu2",
				params:  url.Values{"db": []string{"db0"}},
				command: `DROP MEASUREMENT cpu2`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show measurements",
				params:  url.Values{"db": []string{"db0"}},
				command: `SHOW MEASUREMENTS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"measurements","columns":["name"],"values":[["cpu"]]}]}]}`,
				once:    true,
			},
		},
	}

	tests["drop_and_recreate_database"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Drop database after data write",
				command: `DROP DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "Recreate database",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "Recreate retention policy",
				command: `CREATE RETENTION POLICY rp0 ON db0 DURATION 365d REPLICATION 1 DEFAULT`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "Show measurements after recreate",
				command: `SHOW MEASUREMENTS`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data after recreate",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"error":"measurement not found"}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["drop_database_isolated"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Query data from 1st database",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data from 1st database with GROUP BY *",
				command: `SELECT * FROM cpu GROUP BY *`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"serverA","region":"uswest"},"columns":["time","val"],"values":[["2000-01-01T00:00:00Z",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop other database",
				command: `DROP DATABASE db1`,
				once:    true,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "Query data from 1st database and ensure it's still there",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Query data from 1st database and ensure it's still there with GROUP BY *",
				command: `SELECT * FROM cpu GROUP BY *`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"serverA","region":"uswest"},"columns":["time","val"],"values":[["2000-01-01T00:00:00Z",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["delete_series_time"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=100 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-03T00:00:00Z").UnixNano())},
			&Write{db: "db1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Delete series",
				command: `DELETE FROM cpu WHERE time < '2000-01-03T00:00:00Z'`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Show series still exists",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure last point still exists",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-03T00:00:00Z","serverA","uswest",200]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure data wasn't deleted from other database.",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db1"}},
			},
			&Query{
				name:    "Delete remaining instances of series",
				command: `DELETE FROM cpu WHERE time < '2000-01-04T00:00:00Z'`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Show series should now be empty",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["delete_series_time_tag_filter"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=100 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:00Z").UnixNano())},
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2000-01-03T00:00:00Z").UnixNano())},
			&Write{db: "db1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"],["cpu,host=serverB,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Delete series",
				command: `DELETE FROM cpu WHERE host = 'serverA' AND time < '2000-01-03T00:00:00Z'`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Show series still exists",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"],["cpu,host=serverB,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure last point still exists",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverB","uswest",23.2],["2000-01-03T00:00:00Z","serverA","uswest",200]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure other points are deleted",
				command: `SELECT COUNT(val) FROM cpu WHERE "host" = 'serverA'`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure data wasn't deleted from other database.",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db1"}},
			},
		},
	}

	tests["drop_and_recreate_series"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
			&Write{db: "db1", data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series after data write",
				command: `DROP SERIES FROM cpu`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Show series is gone",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Make sure data wasn't deleted from other database.",
				command: `SELECT * FROM cpu`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","region","val"],"values":[["2000-01-01T00:00:00Z","serverA","uswest",23.2]]}]}]}`,
				params:  url.Values{"db": []string{"db1"}},
			},
		},
	}
	tests["drop_and_recreate_series_retest"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present again after re-write",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["cpu,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["drop_series_from_regex"] = Test{
		db: "db0",
		rp: "rp0",
		writes: Writes{
			&Write{data: strings.Join([]string{
				fmt.Sprintf(`a,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
				fmt.Sprintf(`aa,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
				fmt.Sprintf(`b,host=serverA,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
				fmt.Sprintf(`c,host=serverA,region=uswest val=30.2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			}, "\n")},
		},
		queries: []*Query{
			&Query{
				name:    "Show series is present",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["a,host=serverA,region=uswest"],["aa,host=serverA,region=uswest"],["b,host=serverA,region=uswest"],["c,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series after data write",
				command: `DROP SERIES FROM /a.*/`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "Show series is gone",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["b,host=serverA,region=uswest"],["c,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series from regex that matches no measurements",
				command: `DROP SERIES FROM /a.*/`,
				exp:     `{"results":[{"statement_id":0}]}`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "make sure DROP SERIES doesn't delete anything when regex doesn't match",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["b,host=serverA,region=uswest"],["c,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series with WHERE field should error",
				command: `DROP SERIES FROM c WHERE val > 50.0`,
				exp:     `{"results":[{"statement_id":0,"error":"shard 1: fields not supported in WHERE clause during deletion"}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "make sure DROP SERIES with field in WHERE didn't delete data",
				command: `SHOW SERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["b,host=serverA,region=uswest"],["c,host=serverA,region=uswest"]]}]}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "Drop series with WHERE time should error",
				command: `DROP SERIES FROM c WHERE time > now() - 1d`,
				exp:     `{"results":[{"statement_id":0,"error":"DROP SERIES doesn't support time in WHERE clause"}]}`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["retention_policy_commands"] = Test{
		db: "db0",
		queries: []*Query{
			&Query{
				name:    "create retention policy with invalid name should return an error",
				command: `CREATE RETENTION POLICY "." ON db0 DURATION 1d REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0,"error":"invalid name"}]}`,
				once:    true,
			},
			&Query{
				name:    "create retention policy should succeed",
				command: `CREATE RETENTION POLICY rp0 ON db0 DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should succeed",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","default"],"values":[["rp0","1h0m0s","1h0m0s",1,false]]}]}]}`,
			},
			&Query{
				name:    "alter retention policy should succeed",
				command: `ALTER RETENTION POLICY rp0 ON db0 DURATION 2h REPLICATION 3 DEFAULT`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should have new altered information",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","default"],"values":[["rp0","2h0m0s","1h0m0s",3,true]]}]}]}`,
			},
			&Query{
				name:    "show retention policy should still show policy",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","default"],"values":[["rp0","2h0m0s","1h0m0s",3,true]]}]}]}`,
			},
			&Query{
				name:    "create a second non-default retention policy",
				command: `CREATE RETENTION POLICY rp2 ON db0 DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should show both",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","default"],"values":[["rp0","2h0m0s","1h0m0s",3,true],["rp2","1h0m0s","1h0m0s",1,false]]}]}]}`,
			},
			&Query{
				name:    "dropping non-default retention policy succeed",
				command: `DROP RETENTION POLICY rp2 ON db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create a third non-default retention policy",
				command: `CREATE RETENTION POLICY rp3 ON db0 DURATION 1h REPLICATION 1 SHARD DURATION 30m`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "create retention policy with default on",
				command: `CREATE RETENTION POLICY rp3 ON db0 DURATION 1h REPLICATION 1 SHARD DURATION 30m DEFAULT`,
				exp:     `{"results":[{"statement_id":0,"error":"retention policy conflicts with an existing policy"}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should show both with custom shard",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","default"],"values":[["rp0","2h0m0s","1h0m0s",3,true],["rp3","1h0m0s","1h0m0s",1,false]]}]}]}`,
			},
			&Query{
				name:    "dropping non-default custom shard retention policy succeed",
				command: `DROP RETENTION POLICY rp3 ON db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy should show just default",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","default"],"values":[["rp0","2h0m0s","1h0m0s",3,true]]}]}]}`,
			},
			&Query{
				name:    "Ensure retention policy with unacceptable retention cannot be created",
				command: `CREATE RETENTION POLICY rp4 ON db0 DURATION 1s REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0,"error":"retention policy duration must be at least 1h0m0s"}]}`,
				once:    true,
			},
			&Query{
				name:    "Check error when deleting retention policy on non-existent database",
				command: `DROP RETENTION POLICY rp1 ON mydatabase`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "Ensure retention policy for non existing db is not created",
				command: `CREATE RETENTION POLICY rp0 ON nodb DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0,"error":"database not found: nodb"}]}`,
				once:    true,
			},
			&Query{
				name:    "drop rp0",
				command: `DROP RETENTION POLICY rp0 ON db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			// INF Shard Group Duration will normalize to the Retention Policy Duration Default
			&Query{
				name:    "create retention policy with inf shard group duration",
				command: `CREATE RETENTION POLICY rpinf ON db0 DURATION INF REPLICATION 1 SHARD DURATION 0s`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			// 0s Shard Group Duration will normalize to the Replication Policy Duration
			&Query{
				name:    "create retention policy with 0s shard group duration",
				command: `CREATE RETENTION POLICY rpzero ON db0 DURATION 1h REPLICATION 1 SHARD DURATION 0s`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			// 1s Shard Group Duration will normalize to the MinDefaultRetentionPolicyDuration
			&Query{
				name:    "create retention policy with 1s shard group duration",
				command: `CREATE RETENTION POLICY rponesecond ON db0 DURATION 2h REPLICATION 1 SHARD DURATION 1s`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policy: validate normalized shard group durations are working",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","replicaN","default"],"values":[["rpinf","0s","168h0m0s",1,false],["rpzero","1h0m0s","1h0m0s",1,false],["rponesecond","2h0m0s","1h0m0s",1,false]]}]}]}`,
			},
		},
	}

	tests["retention_policy_auto_create"] = Test{
		queries: []*Query{
			&Query{
				name:    "create database should succeed",
				command: `CREATE DATABASE db0`,
				exp:     `{"results":[{"statement_id":0}]}`,
				once:    true,
			},
			&Query{
				name:    "show retention policies should return auto-created policy",
				command: `SHOW RETENTION POLICIES ON db0`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","hot duration","warm duration","index duration","replicaN","default"],"values":[["autogen","0s","168h0m0s","0s","0s","168h0m0s",1,true]]}]}]}`,
			},
		},
	}

	tests["consistency_check"] = Test{
		db: "db0",
		rp: "rp0",
		queries: []*Query{
			&Query{
				name:    "select *",
				command: `select * from db0.rp0.cpu`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "count(*)",
				command: `select count(*) from db0.rp0.cpu`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "mean(*)",
				command: `select mean(*) from db0.rp0.cpu`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "sum(*)",
				command: `select sum(*) from db0.rp0.cpu`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min(*)",
				command: `select min(*) from db0.rp0.cpu`,
				params:  url.Values{"db": []string{"db0"}},
				once:    true,
			},
			&Query{
				name:    "first(*)",
				command: `select first(*) from db0.rp0.cpu`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last(*)",
				command: `select last(*) from db0.rp0.cpu`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "percentile(*, 50)",
				command: `select percentile(*, 50) from db0.rp0.cpu`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "count(*) group by time",
				command: `select count(*) from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "count(*) group by *",
				command: `select count(*) from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "count(*) group by time, *",
				command: `select count(*) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},

			&Query{
				name:    "mean(*) group by time",
				command: `select mean(*) from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "mean(*) group by *",
				command: `select mean(*) from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "mean(*) group by time, *",
				command: `select mean(*) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "sum(*) group by time",
				command: `select sum(*) from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "sum(*) group by *",
				command: `select sum(*) from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "sum(*) group by time, *",
				command: `select sum(*) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min(*) group by time",
				command: `select min(*) from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min(*) group by *",
				command: `select min(*) from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min(*) group by time, *",
				command: `select min(*) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max(*) group by time",
				command: `select max(*) from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max(*) group by *",
				command: `select max(*) from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max(*) group by time, *",
				command: `select max(*) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first(*) group by time",
				command: `select first(*) from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first(*) group by *",
				command: `select first(*) from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first(*) group by time, *",
				command: `select first(*) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last(*) group by time",
				command: `select last(*) from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last(*) group by *",
				command: `select last(*) from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last(*) group by time, *",
				command: `select last(*) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "percentile(*, 10) group by time",
				command: `select percentile(*, 10) from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
				skip:    true,
			},
			&Query{
				name:    "percentile(*, 50) group by *",
				command: `select percentile(*, 50) from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "percentile(*, 90) group by time, *",
				command: `select percentile(*, 90) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min(actConn),* group by time",
				command: `select min(actConn),* from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min(totalConn),* group by *",
				command: `select min(totalConn),* from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			//bugfix
			&Query{
				name:    "select min(totalConn),* from db0.rp0.cpu group by *",
				command: `select min(used),*  from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max(actConn),* group by time",
				command: `select max(actConn),* from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max(totalConn),* group by *",
				command: `select max(totalConn),* from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max(used),* group by time, *",
				command: `select max(*) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first(totalConn),* group by time",
				command: `select first(totalConn),* from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first(used),* group by *",
				command: `select first(used),* from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first(vname),* group by time, *",
				command: `select first(vname),* from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last(used),* group by *",
				command: `select last(used),* from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last(vname),* group by time, *",
				command: `select last(vname),* from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last(vname),* group by time, *",
				command: `select percentile(actConn, 0.1),* from db0.rp0.cpu group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
				skip:    true,
			},
			&Query{
				name:    "percentile(actConn, 50),* group by *",
				command: `select percentile(actConn, 50),* from db0.rp0.cpu group by *`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "percentile(totalConn, 99.99) group by time, *",
				command: `select percentile(totalConn, 99.99) from db0.rp0.cpu group by time(5m),*`,
				params:  url.Values{"db": []string{"db0"}},
				skip:    true, //1.0 has tail Partial=true
			},
			&Query{
				name:    "limit 1",
				command: `select * from db0.rp0.cpu limit 1`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "limit 1 offset 1",
				command: `select * from db0.rp0.cpu limit 1 offset 1`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "limit 100 offset 1000",
				command: `select * from db0.rp0.cpu limit 100 offset 1000`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "limit 100 offset 4000",
				command: `select * from db0.rp0.cpu limit 100 offset 4000`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "limit 100 offset 4000 group by lisnId",
				command: `select * from db0.rp0.cpu group by lisnId limit 100 offset 4000`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "limit 100 offset 4000 group by lisnId hostId",
				command: `select * from db0.rp0.cpu group by lisnId,hostId limit 100 offset 4000`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "actConn limit 100 offset 4000 group by lisnId hostId",
				command: `select actConn from db0.rp0.cpu group by lisnId,hostId limit 100 offset 4000`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "actConn used limit 100 offset 4000 group by lisnId hostId",
				command: `select actConn,used from db0.rp0.cpu group by lisnId,hostId limit 100 offset 4000`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "limit 100 offset 4000 group by lisnId hostId group by time",
				command: `select * from db0.rp0.cpu group by lisnId,hostId,time limit 100 offset 4000`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "limit 100 offset 4000 group by lisnId hostId group by time 5m",
				command: `select * from db0.rp0.cpu group by lisnId,hostId，time(5m) limit 100 offset 4000`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "sum * group by lisnId",
				command: `select sum(*) from db0.rp0.cpu group by lisnId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "sum * group by lisnId hostId",
				command: `select sum(*) from db0.rp0.cpu group by lisnId,hostId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "sum * group by lisnId hostId time",
				command: `select sum(*) from db0.rp0.cpu group by lisnId,hostId,time`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "sum * group by lisnId hostId time(5m)",
				command: `select sum(*) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "mean * group by lisnId",
				command: `select mean(*) from db0.rp0.cpu group by lisnId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "mean * group by lisnId hostId",
				command: `select mean(*) from db0.rp0.cpu group by lisnId,hostId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "mean * group by lisnId hostId time",
				command: `select mean(*) from db0.rp0.cpu group by lisnId,hostId,time`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "mean * group by lisnId hostId time(5m)",
				command: `select mean(*) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min * group by lisnId",
				command: `select min(*) from db0.rp0.cpu group by lisnId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min * group by lisnId hostId",
				command: `select min(*) from db0.rp0.cpu group by lisnId,hostId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min * group by lisnId hostId time",
				command: `select min(*) from db0.rp0.cpu group by lisnId,hostId,time`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min * group by lisnId hostId",
				command: `select min(*) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max * group by lisnId",
				command: `select max(*) from db0.rp0.cpu group by lisnId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max * group by lisnId hostId",
				command: `select max(*) from db0.rp0.cpu group by lisnId,hostId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max * group by lisnId hostId time",
				command: `select max(*) from db0.rp0.cpu group by lisnId,hostId,time`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "max * group by lisnId hostId time(5m)",
				command: `select max(*) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "count * group by lisnId",
				command: `select count(*) from db0.rp0.cpu group by lisnId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "count * group by lisnId hostId",
				command: `select count(*) from db0.rp0.cpu group by lisnId,hostId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "count * group by lisnId hostId time",
				command: `select count(*) from db0.rp0.cpu group by lisnId,hostId,time`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "count * group by lisnId hostId time(5m)",
				command: `select count(*) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first * group by lisnId",
				command: `select first(*) from db0.rp0.cpu group by lisnId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first * group by lisnId hostId",
				command: `select first(*) from db0.rp0.cpu group by lisnId,hostId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first * group by lisnId hostId time",
				command: `select first(*) from db0.rp0.cpu group by lisnId,hostId,time`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "first * group by lisnId hostId time(5m)",
				command: `select first(*) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last * group by lisnId",
				command: `select last(*) from db0.rp0.cpu group by lisnId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last * group by lisnId hostId",
				command: `select last(*) from db0.rp0.cpu group by lisnId,hostId`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last * group by lisnId hostId time",
				command: `select last(*) from db0.rp0.cpu group by lisnId,hostId,time`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last * group by lisnId hostId time(5m)",
				command: `select last(*) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			//belows are multiple SQL
			&Query{
				name:    "min actConn max actConn group by lisnId hostId time(5m)",
				command: `select min(actConn),max(actConn) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last actConn max totalConn group by lisnId hostId time(5m)",
				command: `select last(actConn),max(totalConn) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last actConn mean totalConn max used first vname group by lisnId hostId time(5m)",
				command: `select last(actConn),mean(totalConn),max(used),first(vname) from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last actConn mean totalConn max used first vname elb_name used group by lisnId hostId time(5m)",
				command: `select last(actConn),mean(totalConn),max(used),first(vname),elb_name,used from db0.rp0.cpu group by lisnId,hostId,time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "min actConn max actConn where group by time(5m)",
				command: `select min(actConn),max(actConn) from db0.rp0.cpu where lisnId='lisnId-3' and hostId='cn-north-2'group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last actConn max totalConn where group by time(5m)",
				command: `select last(actConn),max(totalConn) from db0.rp0.cpu where lisnId='lisnId-3' and hostId='cn-north-2'group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
			&Query{
				name:    "last actConn mean totalConn max used first vname where group by time(5m)",
				command: `select last(actConn),mean(totalConn),max(used),first(vname) from db0.rp0.cpu from db0.rp0.cpu where lisnId='lisnId-3' and hostId='cn-north-2'group by time(5m)`,
				params:  url.Values{"db": []string{"db0"}},
			},
		},
	}

	tests["config_command"] = Test{
		db: "db0",
		rp: "rp0",
		queries: []*Query{
			&Query{
				name:    "show configs",
				command: `SHOW CONFIGS`,
				skip:    true,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["component","instance","name","value"],"values":[["sql","127.0.0.1:8086","common.cluster-id",""],["sql","127.0.0.1:8086","common.cpu-allocation-ratio",1],["sql","127.0.0.1:8086","common.cpu-num",0],["sql","127.0.0.1:8086","common.crypto-config",""],["sql","127.0.0.1:8086","common.executor-memory-size-limit",0],["sql","127.0.0.1:8086","common.executor-memory-wait-time","0s"],["sql","127.0.0.1:8086","common.ha-policy","write-available-first"],["sql","127.0.0.1:8086","common.ignore-empty-tag",false],["sql","127.0.0.1:8086","common.memory-size",0],["sql","127.0.0.1:8086","common.meta-join",["127.0.0.1:8092","127.0.0.2:8092","127.0.0.3:8092"]],["sql","127.0.0.1:8086","common.read-stop",false],["sql","127.0.0.1:8086","common.report-enable",true],["sql","127.0.0.1:8086","common.select-hash-algorithm","ver03"],["sql","127.0.0.1:8086","common.write-stop",false],["sql","127.0.0.1:8086","continuous-query.enabled",false],["sql","127.0.0.1:8086","continuous-query.max-process-CQ-number",1],["sql","127.0.0.1:8086","continuous-query.run-interval","1s"],["sql","127.0.0.1:8086","coordinator.force-broadcast-query",false],["sql","127.0.0.1:8086","coordinator.log-queries-after","0s"],["sql","127.0.0.1:8086","coordinator.max-concurrent-queries",0],["sql","127.0.0.1:8086","coordinator.max-query-mem",0],["sql","127.0.0.1:8086","coordinator.meta-executor-write-timeout","5s"],["sql","127.0.0.1:8086","coordinator.query-limit-flag",false],["sql","127.0.0.1:8086","coordinator.query-limit-interval-time",10],["sql","127.0.0.1:8086","coordinator.query-limit-level",0],["sql","127.0.0.1:8086","coordinator.query-time-compare-enabled",true],["sql","127.0.0.1:8086","coordinator.query-timeout","0s"],["sql","127.0.0.1:8086","coordinator.rp-limit",100],["sql","127.0.0.1:8086","coordinator.shard-mapper-timeout","10s"],["sql","127.0.0.1:8086","coordinator.shard-tier","warm"],["sql","127.0.0.1:8086","coordinator.shard-writer-timeout","10s"],["sql","127.0.0.1:8086","coordinator.tag-limit",0],["sql","127.0.0.1:8086","coordinator.time-range-limit",null],["sql","127.0.0.1:8086","coordinator.write-timeout","10s"],["sql","127.0.0.1:8086","http.access-log-path",""],["sql","127.0.0.1:8086","http.access-log-status-filters",null],["sql","127.0.0.1:8086","http.auth-enabled",false],["sql","127.0.0.1:8086","http.bind-address","127.0.0.1:8086"],["sql","127.0.0.1:8086","http.bind-socket","/var/run/tssql.sock"],["sql","127.0.0.1:8086","http.chunk-reader-parallel",2],["sql","127.0.0.1:8086","http.debug-pprof-enabled",false],["sql","127.0.0.1:8086","http.domain",""],["sql","127.0.0.1:8086","http.enqueued-query-timeout","5m0s"],["sql","127.0.0.1:8086","http.enqueued-write-timeout","30s"],["sql","127.0.0.1:8086","http.flight-address","127.0.0.1:8087"],["sql","127.0.0.1:8086","http.flight-auth-enabled",false],["sql","127.0.0.1:8086","http.flight-ch-factor",2],["sql","127.0.0.1:8086","http.flight-enabled",false],["sql","127.0.0.1:8086","http.flux-enabled",false],["sql","127.0.0.1:8086","http.flux-log-enabled",false],["sql","127.0.0.1:8086","http.https-certificate","/etc/ssl/influxdb.pem"],["sql","127.0.0.1:8086","http.https-enabled",false],["sql","127.0.0.1:8086","http.https-private-key",""],["sql","127.0.0.1:8086","http.log-enabled",true],["sql","127.0.0.1:8086","http.max-body-size",25000000],["sql","127.0.0.1:8086","http.max-concurrent-query-limit",2],["sql","127.0.0.1:8086","http.max-concurrent-write-limit",8],["sql","127.0.0.1:8086","http.max-connection-limit",250],["sql","127.0.0.1:8086","http.max-enqueued-query-limit",248],["sql","127.0.0.1:8086","http.max-enqueued-write-limit",242],["sql","127.0.0.1:8086","http.max-row-limit",1000000],["sql","127.0.0.1:8086","http.parallel-query-in-batch-enabled",true],["sql","127.0.0.1:8086","http.pprof-enabled",true],["sql","127.0.0.1:8086","http.query-memory-limit-enabled",true],["sql","127.0.0.1:8086","http.query-request-rate-limit",0],["sql","127.0.0.1:8086","http.read-block-size",65536],["sql","127.0.0.1:8086","http.realm","InfluxDB"],["sql","127.0.0.1:8086","http.shared-secret",""],["sql","127.0.0.1:8086","http.slow-query-time","10s"],["sql","127.0.0.1:8086","http.suppress-write-log",false],["sql","127.0.0.1:8086","http.time-filter-protection",false],["sql","127.0.0.1:8086","http.unix-socket-enabled",false],["sql","127.0.0.1:8086","http.unix-socket-group",null],["sql","127.0.0.1:8086","http.unix-socket-permissions","0777"],["sql","127.0.0.1:8086","http.weakpwd-path",""],["sql","127.0.0.1:8086","http.white_list",""],["sql","127.0.0.1:8086","http.write-request-rate-limit",0],["sql","127.0.0.1:8086","http.write-tracing",false],["sql","127.0.0.1:8086","logging.app","sql"],["sql","127.0.0.1:8086","logging.compress-enabled",true],["sql","127.0.0.1:8086","logging.format","auto"],["sql","127.0.0.1:8086","logging.level","info"],["sql","127.0.0.1:8086","logging.max-age",7],["sql","127.0.0.1:8086","logging.max-num",16],["sql","127.0.0.1:8086","logging.max-size",64],["sql","127.0.0.1:8086","logging.path","/tmp/openGemini/logs/1"],["sql","127.0.0.1:8086","spdy.byte-buffer-pool-default-size",0],["sql","127.0.0.1:8086","spdy.compress-enable",false],["sql","127.0.0.1:8086","spdy.concurrent-accept-session",0],["sql","127.0.0.1:8086","spdy.conn-pool-size",0],["sql","127.0.0.1:8086","spdy.data-ack-timeout","0s"],["sql","127.0.0.1:8086","spdy.open-session-timeout","0s"],["sql","127.0.0.1:8086","spdy.recv-window-size",0],["sql","127.0.0.1:8086","spdy.session-select-timeout","0s"],["sql","127.0.0.1:8086","spdy.tcp-dial-timeout","0s"],["sql","127.0.0.1:8086","spdy.tls-ca-root",""],["sql","127.0.0.1:8086","spdy.tls-certificate",""],["sql","127.0.0.1:8086","spdy.tls-client-auth",false],["sql","127.0.0.1:8086","spdy.tls-client-certificate",""],["sql","127.0.0.1:8086","spdy.tls-client-private-key",""],["sql","127.0.0.1:8086","spdy.tls-enable",false],["sql","127.0.0.1:8086","spdy.tls-insecure-skip-verify",false],["sql","127.0.0.1:8086","spdy.tls-private-key",""],["sql","127.0.0.1:8086","spdy.tls-server-name",""],["sql","127.0.0.1:8086","subscriber.enabled",true],["sql","127.0.0.1:8086","subscriber.http-timeout","30s"],["sql","127.0.0.1:8086","subscriber.https-certificate",""],["sql","127.0.0.1:8086","subscriber.insecure-skip-verify",false],["sql","127.0.0.1:8086","subscriber.write-buffer-size",100],["sql","127.0.0.1:8086","subscriber.write-concurrency",4]]}]}]}`,
			},
			&Query{
				name:    "set config",
				command: `SET CONFIG sql logging.level = debug`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "show configs",
				command: `SHOW CONFIGS`,
				skip:    true,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["component","instance","name","value"],"values":[["sql","127.0.0.1:8086","common.cluster-id",""],["sql","127.0.0.1:8086","common.cpu-allocation-ratio",1],["sql","127.0.0.1:8086","common.cpu-num",0],["sql","127.0.0.1:8086","common.crypto-config",""],["sql","127.0.0.1:8086","common.executor-memory-size-limit",0],["sql","127.0.0.1:8086","common.executor-memory-wait-time","0s"],["sql","127.0.0.1:8086","common.ha-policy","write-available-first"],["sql","127.0.0.1:8086","common.ignore-empty-tag",false],["sql","127.0.0.1:8086","common.memory-size",0],["sql","127.0.0.1:8086","common.meta-join",["127.0.0.1:8092","127.0.0.2:8092","127.0.0.3:8092"]],["sql","127.0.0.1:8086","common.read-stop",false],["sql","127.0.0.1:8086","common.report-enable",true],["sql","127.0.0.1:8086","common.select-hash-algorithm","ver03"],["sql","127.0.0.1:8086","common.write-stop",false],["sql","127.0.0.1:8086","continuous-query.enabled",false],["sql","127.0.0.1:8086","continuous-query.max-process-CQ-number",1],["sql","127.0.0.1:8086","continuous-query.run-interval","1s"],["sql","127.0.0.1:8086","coordinator.force-broadcast-query",false],["sql","127.0.0.1:8086","coordinator.log-queries-after","0s"],["sql","127.0.0.1:8086","coordinator.max-concurrent-queries",0],["sql","127.0.0.1:8086","coordinator.max-query-mem",0],["sql","127.0.0.1:8086","coordinator.meta-executor-write-timeout","5s"],["sql","127.0.0.1:8086","coordinator.query-limit-flag",false],["sql","127.0.0.1:8086","coordinator.query-limit-interval-time",10],["sql","127.0.0.1:8086","coordinator.query-limit-level",0],["sql","127.0.0.1:8086","coordinator.query-time-compare-enabled",true],["sql","127.0.0.1:8086","coordinator.query-timeout","0s"],["sql","127.0.0.1:8086","coordinator.rp-limit",100],["sql","127.0.0.1:8086","coordinator.shard-mapper-timeout","10s"],["sql","127.0.0.1:8086","coordinator.shard-tier","warm"],["sql","127.0.0.1:8086","coordinator.shard-writer-timeout","10s"],["sql","127.0.0.1:8086","coordinator.tag-limit",0],["sql","127.0.0.1:8086","coordinator.time-range-limit",null],["sql","127.0.0.1:8086","coordinator.write-timeout","10s"],["sql","127.0.0.1:8086","http.access-log-path",""],["sql","127.0.0.1:8086","http.access-log-status-filters",null],["sql","127.0.0.1:8086","http.auth-enabled",false],["sql","127.0.0.1:8086","http.bind-address","127.0.0.1:8086"],["sql","127.0.0.1:8086","http.bind-socket","/var/run/tssql.sock"],["sql","127.0.0.1:8086","http.chunk-reader-parallel",2],["sql","127.0.0.1:8086","http.debug-pprof-enabled",false],["sql","127.0.0.1:8086","http.domain",""],["sql","127.0.0.1:8086","http.enqueued-query-timeout","5m0s"],["sql","127.0.0.1:8086","http.enqueued-write-timeout","30s"],["sql","127.0.0.1:8086","http.flight-address","127.0.0.1:8087"],["sql","127.0.0.1:8086","http.flight-auth-enabled",false],["sql","127.0.0.1:8086","http.flight-ch-factor",2],["sql","127.0.0.1:8086","http.flight-enabled",false],["sql","127.0.0.1:8086","http.flux-enabled",false],["sql","127.0.0.1:8086","http.flux-log-enabled",false],["sql","127.0.0.1:8086","http.https-certificate","/etc/ssl/influxdb.pem"],["sql","127.0.0.1:8086","http.https-enabled",false],["sql","127.0.0.1:8086","http.https-private-key",""],["sql","127.0.0.1:8086","http.log-enabled",true],["sql","127.0.0.1:8086","http.max-body-size",25000000],["sql","127.0.0.1:8086","http.max-concurrent-query-limit",2],["sql","127.0.0.1:8086","http.max-concurrent-write-limit",8],["sql","127.0.0.1:8086","http.max-connection-limit",250],["sql","127.0.0.1:8086","http.max-enqueued-query-limit",248],["sql","127.0.0.1:8086","http.max-enqueued-write-limit",242],["sql","127.0.0.1:8086","http.max-row-limit",1000000],["sql","127.0.0.1:8086","http.parallel-query-in-batch-enabled",true],["sql","127.0.0.1:8086","http.pprof-enabled",true],["sql","127.0.0.1:8086","http.query-memory-limit-enabled",true],["sql","127.0.0.1:8086","http.query-request-rate-limit",0],["sql","127.0.0.1:8086","http.read-block-size",65536],["sql","127.0.0.1:8086","http.realm","InfluxDB"],["sql","127.0.0.1:8086","http.shared-secret",""],["sql","127.0.0.1:8086","http.slow-query-time","10s"],["sql","127.0.0.1:8086","http.suppress-write-log",false],["sql","127.0.0.1:8086","http.time-filter-protection",false],["sql","127.0.0.1:8086","http.unix-socket-enabled",false],["sql","127.0.0.1:8086","http.unix-socket-group",null],["sql","127.0.0.1:8086","http.unix-socket-permissions","0777"],["sql","127.0.0.1:8086","http.weakpwd-path",""],["sql","127.0.0.1:8086","http.white_list",""],["sql","127.0.0.1:8086","http.write-request-rate-limit",0],["sql","127.0.0.1:8086","http.write-tracing",false],["sql","127.0.0.1:8086","logging.app","sql"],["sql","127.0.0.1:8086","logging.compress-enabled",true],["sql","127.0.0.1:8086","logging.format","auto"],["sql","127.0.0.1:8086","logging.level","debug"],["sql","127.0.0.1:8086","logging.max-age",7],["sql","127.0.0.1:8086","logging.max-num",16],["sql","127.0.0.1:8086","logging.max-size",64],["sql","127.0.0.1:8086","logging.path","/tmp/openGemini/logs/1"],["sql","127.0.0.1:8086","spdy.byte-buffer-pool-default-size",0],["sql","127.0.0.1:8086","spdy.compress-enable",false],["sql","127.0.0.1:8086","spdy.concurrent-accept-session",0],["sql","127.0.0.1:8086","spdy.conn-pool-size",0],["sql","127.0.0.1:8086","spdy.data-ack-timeout","0s"],["sql","127.0.0.1:8086","spdy.open-session-timeout","0s"],["sql","127.0.0.1:8086","spdy.recv-window-size",0],["sql","127.0.0.1:8086","spdy.session-select-timeout","0s"],["sql","127.0.0.1:8086","spdy.tcp-dial-timeout","0s"],["sql","127.0.0.1:8086","spdy.tls-ca-root",""],["sql","127.0.0.1:8086","spdy.tls-certificate",""],["sql","127.0.0.1:8086","spdy.tls-client-auth",false],["sql","127.0.0.1:8086","spdy.tls-client-certificate",""],["sql","127.0.0.1:8086","spdy.tls-client-private-key",""],["sql","127.0.0.1:8086","spdy.tls-enable",false],["sql","127.0.0.1:8086","spdy.tls-insecure-skip-verify",false],["sql","127.0.0.1:8086","spdy.tls-private-key",""],["sql","127.0.0.1:8086","spdy.tls-server-name",""],["sql","127.0.0.1:8086","subscriber.enabled",true],["sql","127.0.0.1:8086","subscriber.http-timeout","30s"],["sql","127.0.0.1:8086","subscriber.https-certificate",""],["sql","127.0.0.1:8086","subscriber.insecure-skip-verify",false],["sql","127.0.0.1:8086","subscriber.write-buffer-size",100],["sql","127.0.0.1:8086","subscriber.write-concurrency",4]]}]}]}`,
			},
		},
	}

	tests["cluster_status"] = Test{
		db: "db0",
		rp: "rp0",
		queries: []*Query{
			&Query{
				name:    "show cluster",
				command: `SHOW CLUSTER`,
				skip:    true,
				exp:     ``,
			},
			&Query{
				name:    "show cluster where nodeID=1",
				command: `SHOW CLUSTER where nodeID=1`,
				skip:    true,
				exp:     ``,
			},
			&Query{
				name:    "show cluster where nodeType=data",
				command: `SHOW CLUSTER where nodeType=data`,
				skip:    true,
				exp:     ``,
			},
			&Query{
				name:    "show cluster where nodeType=meta and nodeID = 1",
				command: `SHOW CLUSTER where nodeType=meta and nodeID = 1`,
				skip:    true,
				exp:     ``,
			},
		},
	}
}

func (tests Tests) load(t *testing.T, key string) Test {
	test, ok := tests[key]
	if !ok {
		t.Fatalf("no test %q", key)
	}

	return test.duplicate()
}
