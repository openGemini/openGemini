package tests

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestServer_ContinuousQueryCommand(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "continuous_query_commands")

	// Create all databases.
	if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.CreateDatabase("db1"); err != nil {
		t.Fatal(err)
	}

	if _, err := s.CreateDatabase("db2"); err != nil {
		t.Fatal(err)
	}

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

	test1 := tests.load(t, "run_continuous_queries")

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
