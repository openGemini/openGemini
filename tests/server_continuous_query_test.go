package tests

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type CQTests map[string]Test

var continuousQueryTests CQTests

// Load all continuous query tests
func init() {
	continuousQueryTests = make(map[string]Test)

	continuousQueryTests["continuous_query_commands"] = Test{
		queries: []*Query{
			&Query{
				name:    "create continuous query cq0_1 should succeed",
				command: `CREATE CONTINUOUS QUERY "cq0_1" ON "db0" RESAMPLE EVERY 1h FOR 90m BEGIN SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m) END`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create continuous query the same name cq0_1 and the same query should ignore",
				command: `create continuous query "cq0_1" on "db0" resample every 1h for 90m begin select mean("passengers") into "average_passengers" from "bus_data" group by time(30m) end`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create continuous query cq1_1 should succeed",
				command: `CREATE CONTINUOUS QUERY "cq1_1" ON "db1" RESAMPLE EVERY 1h FOR 90m BEGIN SELECT min("passengers") INTO "min_passengers" FROM "bus_data" GROUP BY time(15m) END`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create continuous query cq2_1 should succeed",
				command: `CREATE CONTINUOUS QUERY "cq2_1" ON "db2" RESAMPLE EVERY 1h FOR 90m BEGIN SELECT min("passengers") INTO "min_passengers" FROM "bus_data" GROUP BY time(15m) END`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create continuous query cq2_1 should return conflict name error",
				command: `CREATE CONTINUOUS QUERY "cq2_1" ON "db0" RESAMPLE EVERY 1h FOR 90m BEGIN SELECT min("passengers") INTO "min_passengers" FROM "bus_data" GROUP BY time(15m) END`,
				exp:     `{"results":[{"statement_id":0,"error":"continuous query name already exists"}]}`,
			},
			&Query{
				name:    "drop continuous query cq2_1 should succeed",
				command: `DROP CONTINUOUS QUERY "cq2_1" ON "db2"`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "show continuous query should succeed",
				command: `SHOW CONTINUOUS QUERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"db0","columns":["name","query"],"values":[["cq0_1","CREATE CONTINUOUS QUERY cq0_1 ON db0 RESAMPLE EVERY 1h FOR 90m BEGIN SELECT mean(passengers) INTO db0.autogen.average_passengers FROM db0.autogen.bus_data GROUP BY time(30m) END"]]},{"name":"db1","columns":["name","query"],"values":[["cq1_1","CREATE CONTINUOUS QUERY cq1_1 ON db1 RESAMPLE EVERY 1h FOR 90m BEGIN SELECT min(passengers) INTO db1.autogen.min_passengers FROM db1.autogen.bus_data GROUP BY time(15m) END"]]},{"name":"db2","columns":["name","query"]}]}]}`,
			},
		},
	}

	continuousQueryTests["run_continuous_queries"] = Test{
		queries: []*Query{
			&Query{
				name:    `create a new retention policy for CQ to write into`,
				command: `CREATE RETENTION POLICY rp1 ON db0 DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create continuous query with backreference",
				command: `CREATE CONTINUOUS QUERY cq1 ON db0 BEGIN SELECT min(value) INTO db0.rp1.min_value FROM cpu GROUP BY time(5s) END`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    `create another retention policy for CQ to write into`,
				command: `CREATE RETENTION POLICY rp2 ON db0 DURATION 1h REPLICATION 1`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create continuous query with backreference and group by time",
				command: `CREATE CONTINUOUS QUERY "cq2" ON db0 BEGIN SELECT max(value) INTO db0.rp2.max_value FROM cpu GROUP BY time(5s) END`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    `show continuous queries`,
				command: `SHOW CONTINUOUS QUERIES`,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"db0","columns":["name","query"],"values":[["cq1","CREATE CONTINUOUS QUERY cq1 ON db0 BEGIN SELECT min(value) INTO db0.rp1.min_value FROM db0.rp0.cpu GROUP BY time(5s) END"],["cq2","CREATE CONTINUOUS QUERY cq2 ON db0 BEGIN SELECT max(value) INTO db0.rp2.max_value FROM db0.rp0.cpu GROUP BY time(5s) END"]]}]}]}`,
			},
		},
	}

}

func (continuousQueryTests CQTests) load(t *testing.T, key string) Test {
	test, ok := continuousQueryTests[key]
	if !ok {
		t.Fatalf("no test %q", key)
	}

	return test.duplicate()
}

func TestServer_ContinuousQueryCommand(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := continuousQueryTests.load(t, "continuous_query_commands")

	// Create all databases.
	_, err := s.CreateDatabase("db0")
	assert.NoError(t, err)

	_, err = s.CreateDatabase("db1")
	assert.NoError(t, err)

	_, err = s.CreateDatabase("db2")
	assert.NoError(t, err)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_ContinuousQueryService(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	// Create a database.
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	runTest := func(t *testing.T, test *Test) {
		for i, query := range test.queries {
			t.Run(query.name, func(t *testing.T) {
				if i == 0 {
					if err := test.init(s); err != nil {
						t.Fatalf("test init failed: %s", err)
					}
				}
				if query.skip {
					t.Skipf("SKIP:: %s", query.name)
				}
				if err := query.Execute(s); err != nil {
					t.Error(query.Error(err))
				} else if !query.success() {
					t.Error(query.failureMessage())
				}
			})
		}
	}

	interval := 5 * time.Second
	now := time.Now().Truncate(interval)
	time1 := now.Add(-interval)
	time2 := now.Add(-2 * interval)
	time3 := now.Add(-time.Hour)
	writes := []string{
		// points in the past
		fmt.Sprintf("cpu,host=server01 value=1 %d", time3.UnixNano()),
		// points in two intervals ago
		fmt.Sprintf("cpu,host=server01 value=2 %d", time2.Add(time.Second).UnixNano()),
		fmt.Sprintf("cpu,host=server01 value=3 %d", time2.Add(2*time.Second).UnixNano()),
		fmt.Sprintf("cpu,host=server01 value=4 %d", time2.Add(3*time.Second).UnixNano()),
		// points in one interval ago
		fmt.Sprintf("cpu,host=server01 value=5 %d", time1.Add(time.Second).UnixNano()),
		fmt.Sprintf("cpu,host=server01 value=6 %d", time1.Add(2*time.Second).UnixNano()),
		fmt.Sprintf("cpu,host=server01 value=7 %d", time1.Add(3*time.Second).UnixNano()),
		// points now
		fmt.Sprintf("cpu,host=server01 value=8 %d", now.Add(time.Second).UnixNano()),
		fmt.Sprintf("cpu,host=server01 value=9 %d", now.Add(2*time.Second).UnixNano()),
	}

	test1 := continuousQueryTests.load(t, "run_continuous_queries")

	test1.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	runTest(t, &test1)

	// check the CQ results.
	test2 := NewTest("db0", "rp1")
	test2.addQueries([]*Query{
		&Query{
			name:    "check results of cq1",
			command: `SELECT * FROM db0.rp1.min_value`,
			skip:    true, // when you want to run cq tests, remove this line
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"min_value","columns":["time","min"],"values":[["` + time1.UTC().Format(time.RFC3339) + `",5],["` + now.UTC().Format(time.RFC3339) + `",8]]}]}]}`,
		},
		&Query{
			name:    "check results of cq2",
			command: `SELECT * FROM db0.rp2.max_value`,
			skip:    true, // when you want to run cq tests, remove this line
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"max_value","columns":["time","max"],"values":[["` + time1.UTC().Format(time.RFC3339) + `",7],["` + now.UTC().Format(time.RFC3339) + `",9]]}]}]}`,
		},
	}...)

	// time.Sleep(3 * interval) // TODO: when you want to test service, uncomment this line.
	runTest(t, &test2)
}
