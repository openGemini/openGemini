package tests

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/stretchr/testify/assert"
)

// Global server used by benchmarks
var benchServer Server
var benchCfgPath string

// Global server used by test stub
var testCfgPath string

type TestType struct {
	MovePT  bool
	HA      bool
	ReStart bool
	ReStore bool
}

var testType TestType

func killInfluxDB3(t *testing.T) error {
	command := `ps -ef |grep -w config/openGemini-3.conf | grep ts-store | grep -v grep | cut -c 9-15 | xargs kill -9`
	cmd := exec.Command("/bin/bash", "-c", command)

	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("kill ts-store 3 error :%s failed with error:%s\n", command, err.Error())
		t.Error(fmt.Sprintf("InitTestEnv kill ts-store 3 error command: %s - err: %s\n", command, err.Error()))
		return err
	} else {
		fmt.Printf("kill ts-store 3 suc:%s finished with output:\n%s\n", command, string(output))
		return nil
	}
}

func startInfluxDB3(t *testing.T) {
	_ = os.MkdirAll("/tmp/openGemini/logs/3", 600)
	confFile := "config/openGemini-3.conf "
	logFile := "/tmp/openGemini/logs/3/store_extra3.log"

	command := "cd ..; nohup build/ts-store -config " + confFile + ">" + logFile + " 2>&1 &"
	cmd := exec.Command("/bin/bash", "-c", command)

	output, err := cmd.Output()
	if err != nil {
		fmt.Printf("start ts-store 3 error:%s failed with error:%s\n", command, err.Error())
		t.Fatal(fmt.Sprintf("start ts-store 3 error command: %s - err: %s\n", command, err.Error()))
	} else {
		time.Sleep(2 * time.Second)
		fmt.Printf("start influxdb3 suc:%s finished with output:\n%s\n", command, string(output))
	}
}

func InitTestEnv(t *testing.T, bodyType string, body io.Reader) error {
	if testType.HA {
		if err := killInfluxDB3(t); err != nil {
			fmt.Printf("kill influxdb3 error:%s", err.Error())
			return err
		}
		time.Sleep(40 * time.Second)
		return nil
	} else if testType.ReStart {
		if err := killInfluxDB3(t); err != nil {
			return err
		}
		startInfluxDB3(t)
	} else if testType.MovePT {
		b := "http://127.0.0.1:8091" + bodyType
		fmt.Printf(" movePT %s \n", b)
		res, err := http.Post(b, "", body)
		if err != nil {
			fmt.Printf("InitTestEnv movePT error: %s, status: %s\n", err.Error(), res.Status)
			t.Error(fmt.Sprintf("InitTestEnv movePT error command: %s - err: %s", b, err.Error()))
			return err
		} else {
			fmt.Printf("movePT suc status: %s\n ", res.Status)
			time.Sleep(20 * time.Second)
		}
	} else if testType.ReStore {
		if err := killInfluxDB3(t); err != nil {
			return err
		}
		time.Sleep(45 * time.Second)
		startInfluxDB3(t)
	}

	return nil
}

func InitHaTestEnv(t *testing.T) {
	if testType.HA {
		if err := killInfluxDB3(t); err != nil {
			fmt.Printf("kill ts-store 3 error: %s\n", err.Error())
			t.Fatal(err.Error())
		}
	}
}

func ReleasTestEnv(t *testing.T, bodyType string, body io.Reader) error {
	if testType.HA {
		startInfluxDB3(t)
	} else if testType.ReStart {
		return nil
	} else if testType.MovePT {
		b := "http://127.0.0.1:8091" + bodyType
		fmt.Printf(" movePT %s \n", b)
		res, err := http.Post(b, "", body)
		if err != nil {
			fmt.Printf("ReleasTestEnv movePT error: %s, status: %s\n", err.Error(), res.Status)
			t.Error(fmt.Sprintf("ReleasTestEnv movePT error command: %s - err: %s", b, err.Error()))
			return err
		} else {
			fmt.Printf("release movePT suc status: %s\n ", res.Status)
			time.Sleep(20 * time.Second)
		}
	} else if testType.ReStore {
		return nil
	}

	return nil

}

func ReleaseHaTestEnv(t *testing.T) {
	if testType.HA {
		startInfluxDB3(t)
	}
}

func TestMain(m *testing.M) {
	flag.BoolVar(&verboseServerLogs, "vv", false, "Turn on very verbose server logging.")
	flag.BoolVar(&cleanupData, "clean", true, "Clean up test data on disk.")
	flag.Int64Var(&seed, "seed", 0, "Set specific seed controlling randomness.")
	flag.Parse()

	testType.HA = false

	for _, arg := range flag.Args() {
		if arg == "HA" {
			testType.HA = true
		}
	}

	var r int

	benchCfgPath = os.Getenv("BENCH_CFG_PATH")
	testCfgPath = os.Getenv("TEST_CFG_PATH")
	for _, indexType = range RegisteredIndexes() {

		// Setup benchmark server
		c := NewParseConfig(benchCfgPath)

		benchServer = OpenDefaultServer(c)

		// Run test suite.
		if testing.Verbose() {
			fmt.Printf("============= Running all tests for %q index =============\n", indexType)
		}
		if thisr := m.Run(); r == 0 {
			r = thisr // We'll always remember the first time r is non-zero
		}

		// Cleanup
		benchServer.Close()
		if testing.Verbose() {
			fmt.Println()
		}
	}

	os.Exit(r)
}

// Ensure that HTTP responses include the InfluxDB version.
func TestServer_HTTPResponseVersion(t *testing.T) {
	if RemoteEnabled() {
		t.Skip("Skipping.  Cannot change version of remote server")
	}

	version := "v1234"
	s := OpenServerWithVersion(NewParseConfig(testCfgPath), version)
	defer s.Close()

	resp, _ := http.Get(s.URL() + "/query")
	got := resp.Header.Get("X-TSDB-Version")
	if got != version {
		t.Errorf("Server responded with incorrect version, exp %s, got %s", version, got)
	}
}

// Ensure the database commands work.
func TestServer_DatabaseCommands(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := tests.load(t, "database_commands")

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}

			if strings.HasPrefix(query.command, "drop database") || strings.HasPrefix(query.command, "DROP DATABASE") {
				db := strings.Split(query.name, " ")[2]
				err := s.CheckDropDatabases([]string{db})
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// Ensure the measurement commands work.
func TestServer_MeasurementCommands(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := tests.load(t, "measurement_commands")

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Query_DropAndRecreateDatabase(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := tests.load(t, "drop_and_recreate_database")

	if err := s.CreateDatabaseAndRetentionPolicy(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}

	InitHaTestEnv(t)

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
			if strings.Contains(query.name, "Recreate database") {
				err := s.CheckDropDatabases([]string{"db0"})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
	ReleaseHaTestEnv(t)
}

func TestServer_Query_DropDatabaseIsolated(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "drop_database_isolated")

	if err := s.CreateDatabaseAndRetentionPolicy(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateDatabaseAndRetentionPolicy("db1", NewRetentionPolicySpec("rp1", 1, 0), true); err != nil {
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

// Ensure retention policy commands work.
func TestServer_RetentionPolicyCommands(t *testing.T) {
	t.Parallel()
	c := NewConfig()
	s := OpenServer(c)
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping. Cannot alter auto create rp remotely")
	}

	test := tests.load(t, "retention_policy_commands")

	// Create a database.
	if _, err := s.CreateDatabase(test.database()); err != nil {
		t.Fatal(err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the autocreation of retention policy works.
func TestServer_DatabaseRetentionPolicyAutoCreate(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := tests.load(t, "retention_policy_auto_create")

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_ShowDatabases_NoAuth(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	test := Test{
		queries: []*Query{
			&Query{
				name:    "create db1",
				command: "CREATE DATABASE db1",
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create db2",
				command: "CREATE DATABASE db2",
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "show dbs",
				command: "SHOW DATABASES",
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["db1"],["db2"]]}]}]}`,
			},
		},
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(fmt.Sprintf("command: %s - err: %s", query.command, query.Error(err)))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_ShowDatabases_WithAuth(t *testing.T) {
	t.Parallel()
	c := NewConfig()
	c.ingesterConfig.HTTP.AuthEnabled = true
	s := OpenServer(c)
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Cannot enable auth on remote server")
	}

	adminParams := map[string][]string{"u": []string{"admin"}, "p": []string{"admin"}}
	readerParams := map[string][]string{"u": []string{"reader"}, "p": []string{"r"}}
	writerParams := map[string][]string{"u": []string{"writer"}, "p": []string{"w"}}
	nobodyParams := map[string][]string{"u": []string{"nobody"}, "p": []string{"n"}}

	test := Test{
		queries: []*Query{
			&Query{
				name:    "create admin",
				command: `CREATE USER admin WITH PASSWORD 'admin' WITH ALL PRIVILEGES`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "create databases",
				command: "CREATE DATABASE dbR; CREATE DATABASE dbW",
				params:  adminParams,
				exp:     `{"results":[{"statement_id":0},{"statement_id":1}]}`,
			},
			&Query{
				name:    "show dbs as admin",
				command: "SHOW DATABASES",
				params:  adminParams,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["dbR"],["dbW"]]}]}]}`,
			},
			&Query{
				name:    "create users",
				command: `CREATE USER reader WITH PASSWORD 'r'; GRANT READ ON "dbR" TO "reader"; CREATE USER writer WITH PASSWORD 'w'; GRANT WRITE ON "dbW" TO "writer"; CREATE USER nobody WITH PASSWORD 'n'`,
				params:  adminParams,
				exp:     `{"results":[{"statement_id":0},{"statement_id":1},{"statement_id":2},{"statement_id":3},{"statement_id":4}]}`,
			},
			&Query{
				name:    "show dbs as reader",
				command: "SHOW DATABASES",
				params:  readerParams,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["dbR"]]}]}]}`,
			},
			&Query{
				name:    "show dbs as writer",
				command: "SHOW DATABASES",
				params:  writerParams,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"],"values":[["dbW"]]}]}]}`,
			},
			&Query{
				name:    "show dbs as nobody",
				command: "SHOW DATABASES",
				params:  nobodyParams,
				exp:     `{"results":[{"statement_id":0,"series":[{"name":"databases","columns":["name"]}]}]}`,
			},
		},
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if err := query.Execute(s); err != nil {
				t.Error(fmt.Sprintf("command: %s - err: %s", query.command, query.Error(err)))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_UserCommands(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	// Create a database.
	if _, err := s.CreateDatabase("db0"); err != nil {
		t.Fatal(err)
	}

	test := Test{
		queries: []*Query{
			&Query{
				name:    "show users, no actual users",
				command: `SHOW USERS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["user","admin","rwuser"]}]}]}`,
			},
			&Query{
				name:    `create user`,
				command: "CREATE USER jdoe WITH PASSWORD 'Jdoe@1337'",
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "show users, 1 existing user",
				command: `SHOW USERS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["user","admin","rwuser"],"values":[["jdoe",false,false]]}]}]}`,
			},
			&Query{
				name:    "grant all priviledges to jdoe",
				command: `GRANT ALL PRIVILEGES TO jdoe`,
				exp:     `{"results":[{"statement_id":0,"error":"forbidden to grant or revoke privileges, because only one admin is allowed for the database"}]}`,
			},
			&Query{
				name:    "show users, existing user as admin",
				command: `SHOW USERS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["user","admin","rwuser"],"values":[["jdoe",false,false]]}]}]}`,
			},
			&Query{
				name:    "grant DB privileges to user",
				command: `GRANT READ ON db0 TO jdoe`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "revoke all privileges",
				command: `REVOKE ALL PRIVILEGES FROM jdoe`,
				exp:     `{"results":[{"statement_id":0,"error":"forbidden to grant or revoke privileges, because only one admin is allowed for the database"}]}`,
			},
			&Query{
				name:    `bad create user request, invalid password`,
				command: "CREATE USER jdoe1 WITH PASSWORD '1337'",
				exp:     `{"results":[{"statement_id":0,"error":"the password needs to be between 8 and 256 characters long"}]}`,
			},
			&Query{
				name:    `bad create user request, invalid password`,
				command: "CREATE USER jdoe1 WITH PASSWORD 'Jdoe1337'",
				exp:     `{"results":[{"statement_id":0,"error":"The user password must contain more than 8 characters and uppercase letters, lowercase letters, digits, and at least one of the special characters."}]}`,
			},
			&Query{
				name:    "bad create user request",
				command: `CREATE USER 0xBAD WITH PASSWORD 'Jdoe@1337'`,
				exp:     `{"error":"error parsing query: syntax error: unexpected DURATIONVAL, expecting IDENT"}`,
			},
			&Query{
				name:    "bad create user request, no name",
				command: `CREATE USER WITH PASSWORD 'Jdoe@1337'`,
				exp:     `{"error":"error parsing query: syntax error: unexpected WITH, expecting IDENT"}`,
			},
			&Query{
				name:    "bad create user request, no password",
				command: `CREATE USER jdoe`,
				exp:     `{"error":"error parsing query: syntax error: unexpected $end, expecting WITH"}`,
			},
			&Query{
				name:    "drop user",
				command: `DROP USER jdoe`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
			&Query{
				name:    "make sure user was dropped",
				command: `SHOW USERS`,
				exp:     `{"results":[{"statement_id":0,"series":[{"columns":["user","admin","rwuser"]}]}]}`,
			},
			&Query{
				name:    "delete non existing user",
				command: `DROP USER noone`,
				exp:     `{"results":[{"statement_id":0,"error":"user not found"}]}`,
			},
		},
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if query.skip {
				t.Skipf("SKIP:: %s", query.name)
			}
			if err := query.Execute(s); err != nil {
				t.Error(fmt.Sprintf("command: %s - err: %s", query.command, query.Error(err)))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

// Ensure the server will write all points possible with exception to the field type conflict.
// This should return a partial write and a status of 400
func TestServer_Write_FieldTypeConflict(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	if res, err := s.Write("db0", "rp0", fmt.Sprintf("cpu value=1i %d", mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:01Z").UnixNano()), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-01-01T00:00:01Z",1]]}]}]}`; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	writes := []string{
		fmt.Sprintf("cpu value=2i %d", mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf("cpu value=3  %d", mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf("cpu value=4i %d\n", mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:04Z").UnixNano()),
	}
	res, err := s.Write("db0", "rp0", strings.Join(writes, "\n"), nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	wr, ok := err.(WriteError)
	if !ok {
		t.Fatalf("wrong error type %v", err)
	}
	if exp, got := http.StatusBadRequest, wr.StatusCode(); exp != got {
		t.Fatalf("unexpected status code\nexp: %d\ngot: %d\n", exp, got)
	}
	if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu`); err != nil {
		t.Fatal(err)
	} else if exp := `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2015-01-01T00:00:01Z",1],["2015-01-01T00:00:02Z",2],["2015-01-01T00:00:04Z",4]]}]}]}`; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server will write all points possible with exception to the field name conflict.
func TestServer_Write_TagKeyConflict(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	_, err := s.Write("db0", "rp0", fmt.Sprintf("mst,tag=1,time=12 f1=2 %d", mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:01Z").UnixNano()), nil)
	assert.EqualError(t, err, "invalid status code: code=500, body={\"error\":\"tag key can't be 'time'\"}\n")
}

// Ensure the server will write all points possible with exception to the field type conflict.
// This should return a partial write and a status of 400
func TestServer_Write_MultiField_FieldTypeConflict(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`monitor cmd="test",collectTime="now",cpu=60i,mem=70i,process=1i,startTime="now",stat="running",state="ok",user="root" 1629129600000000000`),
	}

	if res, err := s.Write("db0", "rp0", strings.Join(writes, "\n"), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	writes = []string{
		fmt.Sprintf(`monitor cmd=0i,collectTime="now",cpu=60i,mem=70i,process=1i,startTime=0i,stat=0i,state="ok",user=0i 1629129700000000000`),
	}
	res, err := s.Write("db0", "rp0", strings.Join(writes, "\n"), nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	wr, ok := err.(WriteError)
	if !ok {
		t.Fatalf("wrong error type %v", err)
	}
	if exp, got := http.StatusBadRequest, wr.StatusCode(); exp != got {
		t.Fatalf("unexpected status code\nexp: %d\ngot: %d\n", exp, got)
	}
	if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.monitor`); err != nil {
		t.Fatal(err)
	} else if exp := `{"results":[{"statement_id":0,"series":[{"name":"monitor","columns":["time","cmd","collectTime","cpu","mem","process","startTime","stat","state","user"],"values":[["2021-08-16T16:00:00Z","test","now",60,70,1,"now","running","ok","root"],["2021-08-16T16:01:40Z",null,"now",60,70,1,null,null,"ok",null]]}]}]}`; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Write_LineProtocol_Float(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=1.0 `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

}

func TestServer_Query_With_All_ShardKey(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseShardKey("db0", "tag1,tag2"); err != nil {
		t.Fatal(err)
	}

	now := now()
	if res, err := s.Write("db0", "autogen", `cpu,tag1=tv1,tag2=tv2 value=1.0 `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)
	if res, err := s.Query(`SELECT * FROM db0.autogen.cpu where tag1='tv1' AND tag2='tv2'`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","tag1","tag2","value"],"values":[["%s","tv1","tv2",1]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can create a single point via line protocol with bool type and read it back.
func TestServer_Write_LineProtocol_Bool(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=true `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",true]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

// Ensure the server can create a single point via line protocol with string type and read it back.
func TestServer_Write_LineProtocol_String(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `cpu,host=server01 value="disk mem" 1610467200000000000
cpu,host=server01 value="disk\ mem" 1610467300000000000
cpu,host=server01 value="disk\\ mem" 1610467400000000000
cpu,host=server01 value="disk\\\ mem" 1610467500000000000
cpu,host=server01 value="disk\\\\ mem" 1610467600000000000

cpu,host=server01 value="disk\ mem\ host " 1610467700000000000
cpu,host=server01 value="disk\ mem\\ host " 1610467800000000000
cpu,host=server01 value="disk\ mem\\\ host " 1610467900000000000
cpu,host=server01 value="disk\ mem\\\\ host " 1610468000000000000

cpu,host=server01 value="disk\\ mem\ host " 1610468100000000000
cpu,host=server01 value="disk\\ mem\\ host " 1610468200000000000

cpu,host=server01 value="disk\\\ mem\ host " 1610468300000000000
cpu,host=server01 value="disk\\\\ mem\ host " 1610468400000000000

cpu,host=server01 value="disk\" mem\\\"" 1610468500000000000
cpu,host=server01 value="disk\" mem\\\" host\\" 1610468600000000000
`},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SELECT * FROM db0.rp0.cpu",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","value"],"values":[["2021-01-12T16:00:00Z","server01","disk mem"],["2021-01-12T16:01:40Z","server01","disk\\ mem"],["2021-01-12T16:03:20Z","server01","disk\\ mem"],["2021-01-12T16:05:00Z","server01","disk\\\\ mem"],["2021-01-12T16:06:40Z","server01","disk\\\\ mem"],["2021-01-12T16:08:20Z","server01","disk\\ mem\\ host "],["2021-01-12T16:10:00Z","server01","disk\\ mem\\ host "],["2021-01-12T16:11:40Z","server01","disk\\ mem\\\\ host "],["2021-01-12T16:13:20Z","server01","disk\\ mem\\\\ host "],["2021-01-12T16:15:00Z","server01","disk\\ mem\\ host "],["2021-01-12T16:16:40Z","server01","disk\\ mem\\ host "],["2021-01-12T16:18:20Z","server01","disk\\\\ mem\\ host "],["2021-01-12T16:20:00Z","server01","disk\\\\ mem\\ host "],["2021-01-12T16:21:40Z","server01","disk\" mem\\\""],["2021-01-12T16:23:20Z","server01","disk\" mem\\\" host\\"]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can create a single point via line protocol with integer type and read it back.
func TestServer_Write_LineProtocol_Integer(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	if res, err := s.Write("db0", "rp0", `cpu,host=server01 value=100i `+strconv.FormatInt(now.UnixNano(), 10), nil); err != nil {
		t.Fatal(err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",100]]}]}]}`, now.Format(time.RFC3339Nano)); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Write_LineProtocol_Partial(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 1*time.Hour), true); err != nil {
		t.Fatal(err)
	}

	now := now()
	points := []string{
		"cpu,host=server01 value=100 " + strconv.FormatInt(now.UnixNano(), 10),
		"cpu,host=server01 value=NaN " + strconv.FormatInt(now.UnixNano(), 10),
		"cpu,host=server01 value=NaN " + strconv.FormatInt(now.UnixNano(), 10),
	}

	if res, err := s.Write("db0", "rp0", strings.Join(points, "\n"), nil); err == nil {
		t.Fatal("expected error. got nil", err)
	} else if exp := ``; exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	} else if exp := "invalid field value"; !strings.Contains(err.Error(), exp) {
		t.Fatalf("unexpected error: exp\nexp: %v\ngot: %v", exp, err)
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	// Verify the data was written.
	if res, err := s.Query(`SELECT * FROM db0.rp0.cpu GROUP BY *`); err != nil {
		t.Fatal(err)
	} else if exp := fmt.Sprintf(`{"results":[{"statement_id":0,"error":"measurement not found"}]}`); exp != res {
		t.Fatalf("unexpected results\nexp: %s\ngot: %s\n", exp, res)
	}
}

func TestServer_Query_DefaultDBAndRP(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`cpu value=1.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano())},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "default db and rp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "default rp exists",
			command: `show retention policies ON db0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["name","duration","shardGroupDuration","hot duration","warm duration","index duration","replicaN","default"],"values":[["autogen","0s","168h0m0s","0s","0s","168h0m0s",1,false],["rp0","0s","168h0m0s","0s","0s","168h0m0s",1,true]]}]}]}`,
		},
		&Query{
			name:    "default rp",
			command: `SELECT * FROM db0..cpu GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "default dp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM rp0.cpu GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T01:00:00Z",1]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		if strings.Contains(err.Error(), "point without tags is unsupported") {
			t.Log("fail ignore")
			return
		}
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can have a database with multiple measurements.
func TestServer_Query_Multiple_Measurements(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	// Make sure we do writes for measurements that will span across shards
	writes := []string{
		fmt.Sprintf("cpu,host=server01 value=100,core=4 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf("cpu1,host=server02 value=50,core=2 %d", mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "measurement in one shard but not another shouldn't panic server",
			command: `SELECT host,value  FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","value"],"values":[["2000-01-01T00:00:00Z","server01",100]]}]}]}`,
		},
		&Query{
			name:    "measurement in one shard but not another shouldn't panic server",
			command: `SELECT host,value  FROM db0.rp0.cpu GROUP BY host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","host","value"],"values":[["2000-01-01T00:00:00Z","server01",100]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server correctly supports data with identical tag values.
func TestServer_Query_IdenticalTagValues(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	writes := []string{
		fmt.Sprintf("cpu,t1=val1 value=1 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf("cpu,t2=val2 value=2 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf("cpu,t1=val2 value=3 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:02:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "measurements with identical tag values - SELECT *, no GROUP BY",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"t1":"","t2":"val2"},"columns":["time","value"],"values":[["2000-01-01T00:01:00Z",2]]},{"name":"cpu","tags":{"t1":"val1","t2":""},"columns":["time","value"],"values":[["2000-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"t1":"val2","t2":""},"columns":["time","value"],"values":[["2000-01-01T00:02:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "measurements with identical tag values - SELECT *, with GROUP BY",
			command: `SELECT value FROM db0.rp0.cpu GROUP BY t1,t2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"t1":"","t2":"val2"},"columns":["time","value"],"values":[["2000-01-01T00:01:00Z",2]]},{"name":"cpu","tags":{"t1":"val1","t2":""},"columns":["time","value"],"values":[["2000-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"t1":"val2","t2":""},"columns":["time","value"],"values":[["2000-01-01T00:02:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "measurements with identical tag values - SELECT value no GROUP BY",
			command: `SELECT value FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",1],["2000-01-01T00:01:00Z",2],["2000-01-01T00:02:00Z",3]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can handle a query that involves accessing no shards.
func TestServer_Query_NoShards(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `cpu,host=server01 value=1 ` + strconv.FormatInt(now.UnixNano(), 10)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "selecting value should succeed",
			command: `SELECT value FROM db0.rp0.cpu WHERE time < now() - 1d`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can query a non-existent field
func TestServer_Query_NonExistent(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `cpu,host=server01 value=1 1656554067000000000`},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "selecting value should succeed",
			command: `SELECT value FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2022-06-30T01:54:27Z",1]]}]}]}`,
		},
		&Query{
			name:    "selecting non-existent should succeed",
			command: `SELECT foo FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "selecting columns contains non-existent should succeed",
			command: `SELECT value, foo FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value","foo"],"values":[["2022-06-30T01:54:27Z",1,null]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can perform basic math
func TestServer_Query_Math(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()
	writes := []string{
		"float value=42 " + strconv.FormatInt(now.UnixNano(), 10),
		"integer value=42i " + strconv.FormatInt(now.UnixNano(), 10),
	}

	test := NewTest("db", "rp")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SELECT multiple of float value",
			command: `SELECT value * 2 from db.rp.float`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"float","columns":["time","value"],"values":[["%s",84]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT multiple of float value",
			command: `SELECT 2 * value from db.rp.float`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"float","columns":["time","value"],"values":[["%s",84]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT multiple of integer value",
			command: `SELECT value * 2 from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value"],"values":[["%s",84]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT float multiple of integer value",
			command: `SELECT value * 2.0 from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value"],"values":[["%s",84]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "SELECT square of float value",
			command: `SELECT value * value from db.rp.float`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"float","columns":["time","value_value"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
			skip:    true,
		},
		&Query{
			name:    "SELECT square of integer value",
			command: `SELECT value * value from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value_value"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
			skip:    true,
		},
		&Query{
			name:    "SELECT square of integer, float value",
			command: `SELECT value * value,value from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value_value","value"],"values":[["%s",1764,42]]}]}]}`, now.Format(time.RFC3339Nano)),
			skip:    true,
		},
		&Query{
			name:    "SELECT square of integer value with alias",
			command: `SELECT value * value as square from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","square"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
			skip:    true,
		},
		&Query{
			name:    "SELECT sum of aggregates",
			command: `SELECT max(value) + min(value) from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","max_min"],"values":[["1970-01-01T00:00:00Z",84]]}]}]}`),
		},
		&Query{
			name:    "SELECT square of enclosed integer value",
			command: `SELECT ((value) * (value)) from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value_value"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
			skip:    true,
		},
		&Query{
			name:    "SELECT square of enclosed integer value",
			command: `SELECT (value * value) from db.rp.integer`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"integer","columns":["time","value_value"],"values":[["%s",1764]]}]}]}`, now.Format(time.RFC3339Nano)),
			skip:    true,
		},
	}...)

	if err := test.init(s); err != nil {
		if strings.Contains(err.Error(), "point without tags is unsupported") {
			t.Log("fail ignore")
			return
		}
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can query with the count aggregate function
func TestServer_Query_Count(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	writes := []string{
		`cpu,host=server01 value=1.0 ` + strconv.FormatInt(now.UnixNano(), 10),
		`ram value1=1.0,value2=2.0 ` + strconv.FormatInt(now.UnixNano(), 10),
	}

	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	hour_ago := now.Add(-time.Hour).UTC()

	test.addQueries([]*Query{
		&Query{
			name:    "selecting count(value) should succeed",
			command: `SELECT count(value) FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "selecting count(value) with where time should return result",
			command: fmt.Sprintf(`SELECT count(value) FROM db0.rp0.cpu WHERE time >= '%s'`, hour_ago.Format(time.RFC3339Nano)),
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["%s",1]]}]}]}`, hour_ago.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting count(value) with filter that excludes all results should return 0",
			command: fmt.Sprintf(`SELECT count(value) FROM db0.rp0.cpu WHERE value=100 AND time >= '%s'`, hour_ago.Format(time.RFC3339Nano)),
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "selecting count(value1) with matching filter against value2 should return correct result",
			command: fmt.Sprintf(`SELECT count(value1) FROM db0.rp0.ram WHERE value2=2 AND time >= '%s'`, hour_ago.Format(time.RFC3339Nano)),
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"ram","columns":["time","count"],"values":[["%s",1]]}]}]}`, hour_ago.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting count(value1) with non-matching filter against value2 should return correct result",
			command: fmt.Sprintf(`SELECT count(value1) FROM db0.rp0.ram WHERE value2=3 AND time >= '%s'`, hour_ago.Format(time.RFC3339Nano)),
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "selecting count(*) should expand the wildcard",
			command: `SELECT count(*) FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_value"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "selecting count(2) should error",
			command: `SELECT count(2) FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"error":"expected field argument in count()"}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can limit concurrent series.
/*func TestServer_Query_MaxSelectSeriesN(t *testing.T) {
	t.Parallel()
	config := NewConfig()
	config.Coordinator.MaxSelectSeriesN = 3
	s := OpenServer(config)
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Cannot modify MaxSelectSeriesN remotely")
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `cpu,host=server01 value=1.0 0`},
		&Write{data: `cpu,host=server02 value=1.0 0`},
		&Write{data: `cpu,host=server03 value=1.0 0`},
		&Write{data: `cpu,host=server04 value=1.0 0`},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "exceeed max series",
			command: `SELECT COUNT(value) FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"error":"max-select-series limit exceeded: (4/3)"}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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
}*/

// Ensure the server can query with Now().
func TestServer_Query_Now(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `cpu,host=server01 value=1.0 ` + strconv.FormatInt(now.UnixNano(), 10)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "where with time < now() should work",
			command: `SELECT * FROM db0.rp0.cpu where time < now()`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","value"],"values":[["%s","server01",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "where with time < now() and GROUP BY * should work",
			command: `SELECT * FROM db0.rp0.cpu where time < now() GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "where with time > now() should return an empty result",
			command: `SELECT * FROM db0.rp0.cpu where time > now()`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "where with time > now() with GROUP BY * should return an empty result",
			command: `SELECT * FROM db0.rp0.cpu where time > now() GROUP BY *`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can query with epoch precisions.
func TestServer_Query_EpochPrecision(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: `cpu,host=server01 value=1.0 ` + strconv.FormatInt(now.UnixNano(), 10)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "nanosecond precision",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			params:  url.Values{"epoch": []string{"n"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()),
		},
		&Query{
			name:    "microsecond precision",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			params:  url.Values{"epoch": []string{"u"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Microsecond)),
		},
		&Query{
			name:    "millisecond precision",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			params:  url.Values{"epoch": []string{"ms"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Millisecond)),
		},
		&Query{
			name:    "second precision",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			params:  url.Values{"epoch": []string{"s"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Second)),
		},
		&Query{
			name:    "minute precision",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			params:  url.Values{"epoch": []string{"m"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Minute)),
		},
		&Query{
			name:    "hour precision",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			params:  url.Values{"epoch": []string{"h"}},
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[[%d,1]]}]}]}`, now.UnixNano()/int64(time.Hour)),
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server works with tag queries.
func TestServer_Query_Tags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()

	writes := []string{
		fmt.Sprintf("cpu,host=server01 value=100,core=4 %d", now.UnixNano()),
		fmt.Sprintf("cpu,host=server02 value=50,core=2 %d", now.Add(1).UnixNano()),

		fmt.Sprintf("cpu1,host=server01,region=us-west value=100 %d", mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf("cpu1,host=server02 value=200 %d", mustParseTime(time.RFC3339Nano, "2010-02-28T01:03:37.703820946Z").UnixNano()),
		fmt.Sprintf("cpu1,host=server03 value=300 %d", mustParseTime(time.RFC3339Nano, "2012-02-28T01:03:38.703820946Z").UnixNano()),

		fmt.Sprintf("cpu2,host=server01 value=100 %d", mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf("cpu2 value=200 %d", mustParseTime(time.RFC3339Nano, "2012-02-28T01:03:38.703820946Z").UnixNano()),

		fmt.Sprintf("cpu3,company=acme01 value=100 %d", mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf("cpu3 value=200 %d", mustParseTime(time.RFC3339Nano, "2012-02-28T01:03:38.703820946Z").UnixNano()),

		fmt.Sprintf("status_code,url=http://www.example.com value=404 %d", mustParseTime(time.RFC3339Nano, "2015-07-22T08:13:54.929026672Z").UnixNano()),
		fmt.Sprintf("status_code,url=https://influxdb.com value=418 %d", mustParseTime(time.RFC3339Nano, "2015-07-22T09:52:24.914395083Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "tag without field should return error",
			command: `SELECT host FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"error":"statement must have at least one field in select clause"}]}`,
			skip:    true, // FIXME(benbjohnson): tags should stream as values
		},
		&Query{
			name:    "field with tag should succeed",
			command: `SELECT host, value FROM db0.rp0.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","value"],"values":[["%s","server01",100],["%s","server02",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "field with tag and GROUP BY should succeed",
			command: `SELECT host, value FROM db0.rp0.cpu GROUP BY host`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","host","value"],"values":[["%s","server01",100]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","host","value"],"values":[["%s","server02",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "field with two tags should succeed",
			command: `SELECT host, value, core FROM db0.rp0.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","value","core"],"values":[["%s","server01",100,4],["%s","server02",50,2]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "field with two tags and GROUP BY should succeed",
			command: `SELECT host, value, core FROM db0.rp0.cpu GROUP BY host`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","host","value","core"],"values":[["%s","server01",100,4]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","host","value","core"],"values":[["%s","server02",50,2]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "select * with tags should succeed",
			command: `SELECT * FROM db0.rp0.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core","host","value"],"values":[["%s",4,"server01",100],["%s",2,"server02",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "select * with tags with GROUP BY * should succeed",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","core","value"],"values":[["%s",4,100]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","core","value"],"values":[["%s",2,50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "group by tag",
			command: `SELECT value FROM db0.rp0.cpu GROUP by host`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",100]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","value"],"values":[["%s",50]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "single field (EQ tag value1)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host = 'server01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (2 EQ tags)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host = 'server01' AND region = 'us-west'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (OR different tags)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host = 'server03' OR region = 'us-west'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (OR with non-existent tag value)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host = 'server01' OR host = 'server66'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (OR with all tag values)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host = 'server01' OR host = 'server02' OR host = 'server03'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (1 EQ and 1 NEQ tag)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host = 'server01' AND region != 'us-west'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "single field (EQ tag value2)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host = 'server02'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host != 'server01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1 AND NEQ tag value2)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host != 'server01' AND host != 'server02'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",300]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1 OR NEQ tag value2)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host != 'server01' OR host != 'server02'`, // Yes, this is always true, but that's the point.
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2010-02-28T01:03:37.703820946Z",200],["2012-02-28T01:03:38.703820946Z",300],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1 AND NEQ tag value2 AND NEQ tag value3)",
			command: `SELECT value FROM db0.rp0.cpu1 WHERE host != 'server01' AND host != 'server02' AND host != 'server03'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1, point without any tags)",
			command: `SELECT value FROM db0.rp0.cpu2 WHERE host != 'server01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu2","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",200]]}]}]}`,
		},
		&Query{
			name:    "single field (NEQ tag value1, point without any tags)",
			command: `SELECT value FROM db0.rp0.cpu3 WHERE company !~ /acme01/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu3","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",200]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "single field (regex tag match)",
			command: `SELECT value FROM db0.rp0.cpu3 WHERE company =~ /acme01/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu3","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "single field (regex tag match)",
			command: `SELECT value FROM db0.rp0.cpu3 WHERE company !~ /acme[23]/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu3","columns":["time","value"],"values":[["2012-02-28T01:03:38.703820946Z",200],["2015-02-28T01:03:36.703820946Z",100]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "single field (regex tag match with escaping)",
			command: `SELECT value FROM db0.rp0.status_code WHERE url !~ /https\:\/\/influxdb\.com/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"status_code","columns":["time","value"],"values":[["2015-07-22T08:13:54.929026672Z",404]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "single field (regex tag match with escaping)",
			command: `SELECT value FROM db0.rp0.status_code WHERE url =~ /https\:\/\/influxdb\.com/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"status_code","columns":["time","value"],"values":[["2015-07-22T09:52:24.914395083Z",418]]}]}]}`,
			skip:    true,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func generateBool(i int) bool {
	if i%2 == 0 {
		return true
	}
	return false
}

func generateFloat(i int) float64 {
	return float64(i)
}

func generateString(i int) string {
	return fmt.Sprintf("abc%d", i)
}

// Ensure the server will succeed and error for common scenarios.
func TestServer_Query_Common(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("cpu,host=server01 value=1 %s", strconv.FormatInt(now.UnixNano(), 10))},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "selecting a from a non-existent database should error",
			command: `SELECT value FROM db1.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"error":"database not found: db1"}]}`,
		},
		&Query{
			name:    "selecting a from a non-existent retention policy should error",
			command: `SELECT value FROM db0.rp1.cpu`,
			exp:     `{"results":[{"statement_id":0,"error":"retention policy not found: rp1"}]}`,
		},
		&Query{
			name:    "selecting a valid  measurement and field should succeed",
			command: `SELECT value FROM db0.rp0.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "explicitly selecting time and a valid measurement and field should succeed",
			command: `SELECT time,value FROM db0.rp0.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting a measurement that doesn't exist should result in empty set",
			command: `SELECT value FROM db0.rp0.idontexist`,
			exp:     `{"results":[{"statement_id":0,"error":"measurement not found"}]}`,
		},
		&Query{
			name:    "selecting a field that doesn't exist should result in empty set",
			command: `SELECT idontexist FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "selecting 2 fields that contain one non-existent",
			command: `SELECT idontexist, value  FROM db0.rp0.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","idontexist","value"],"values":[["%s",null,1]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting wildcard without specifying a database should error",
			command: `SELECT * FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"error":"database name required"}]}`,
		},
		&Query{
			name:    "selecting explicit field without specifying a database should error",
			command: `SELECT value FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"error":"database name required"}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Ensure the server can query two points.
func TestServer_Query_SelectTwoPoints(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("cpu value=100 %s\ncpu value=200 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10))},
	}

	test.addQueries(
		&Query{
			name:    "selecting two points should result in two points",
			command: `SELECT * FROM db0.rp0.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",100],["%s",200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "selecting two points with GROUP BY * should result in two points",
			command: `SELECT * FROM db0.rp0.cpu GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",100],["%s",200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
		},
	)

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

// Ensure the server can query two negative points.
func TestServer_Query_SelectTwoNegativePoints(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("cpu value=-100 %s\ncpu value=-200 %s", strconv.FormatInt(now.UnixNano(), 10), strconv.FormatInt(now.Add(1).UnixNano(), 10))},
	}

	test.addQueries(&Query{
		name:    "selecting two negative points should succeed",
		command: `SELECT * FROM db0.rp0.cpu`,
		exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",-100],["%s",-200]]}]}]}`, now.Format(time.RFC3339Nano), now.Add(1).Format(time.RFC3339Nano)),
	})

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

// Ensure the server can query with relative time.
func TestServer_Query_SelectRelativeTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	now := now()
	yesterday := yesterday()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("cpu,host=server01 value=100 %s\ncpu,host=server01 value=200 %s", strconv.FormatInt(yesterday.UnixNano(), 10), strconv.FormatInt(now.UnixNano(), 10))},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "single point with time pre-calculated for past time queries yesterday",
			command: `SELECT * FROM db0.rp0.cpu where time >= '` + yesterday.Add(-1*time.Minute).Format(time.RFC3339Nano) + `' GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",100],["%s",200]]}]}]}`, yesterday.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "single point with time pre-calculated for relative time queries now",
			command: `SELECT * FROM db0.rp0.cpu where time >= now() - 1m GROUP BY *`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["%s",200]]}]}]}`, now.Format(time.RFC3339Nano)),
		},
	}...)

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

func TestServer_Query_SelectGroupByTime_MultipleAggregates(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`test,t=a x=1i 1000000000
test,t=b y=1i 1000000000
test,t=a x=2i 2000000000
test,t=b y=2i 2000000000
test,t=a x=3i 3000000000
test,t=b y=3i 3000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "two aggregates with a group by host",
			command: `SELECT mean(x) as x, mean(y) as y from db0.rp0.test where time >= 1s and time < 4s group by t, time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"test","tags":{"t":"a"},"columns":["time","x","y"],"values":[["1970-01-01T00:00:01Z",1,null],["1970-01-01T00:00:02Z",2,null],["1970-01-01T00:00:03Z",3,null]]},{"name":"test","tags":{"t":"b"},"columns":["time","x","y"],"values":[["1970-01-01T00:00:01Z",null,1],["1970-01-01T00:00:02Z",null,2],["1970-01-01T00:00:03Z",null,3]]}]}]}`,
		},
	}...)

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

func TestServer_Query_MathWithFill(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf(`cpu value=15 1278010020000000000
`)},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "multiplication with fill previous",
			command: `SELECT 4*mean(value) FROM db0.rp0.cpu WHERE time >= '2010-07-01 18:47:00' AND time < '2010-07-01 18:48:30' GROUP BY time(30s) FILL(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean"],"values":[["2010-07-01T18:47:00Z",60],["2010-07-01T18:47:30Z",60],["2010-07-01T18:48:00Z",60]]}]}]}`,
		},
		&Query{
			name:    "multiplication of mode value with fill previous",
			command: `SELECT 4*mode(value) FROM db0.rp0.cpu WHERE time >= '2010-07-01 18:47:00' AND time < '2010-07-01 18:48:30' GROUP BY time(30s) FILL(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mode"],"values":[["2010-07-01T18:47:00Z",60],["2010-07-01T18:47:30Z",60],["2010-07-01T18:48:00Z",60]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_MergeMany(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	// set infinite retention policy as we are inserting data in the past and don't want retention policy enforcement to make this test racy
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")

	writes := []string{}
	for i := 1; i < 11; i++ {
		for j := 1; j < 5+i%3; j++ {
			data := fmt.Sprintf(`cpu,host=server_%d value=22 %d`, i, time.Unix(int64(j), int64(0)).UTC().UnixNano())
			writes = append(writes, data)
		}
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "GROUP by time",
			command: `SELECT count(value) FROM db0.rp0.cpu WHERE time >= '1970-01-01T00:00:01Z' AND time <= '1970-01-01T00:00:06Z' GROUP BY time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:01Z",10],["1970-01-01T00:00:02Z",10],["1970-01-01T00:00:03Z",10],["1970-01-01T00:00:04Z",10],["1970-01-01T00:00:05Z",7],["1970-01-01T00:00:06Z",3]]}]}]}`,
		},
		&Query{
			skip:    true,
			name:    "GROUP by tag - FIXME issue #2875",
			command: `SELECT count(value) FROM db0.rp0.cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:00Z' group by host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","count"],"values":[["2000-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","count"],"values":[["2000-01-01T00:00:00Z",1]]},{"name":"cpu","tags":{"host":"server03"},"columns":["time","count"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "GROUP by field",
			command: `SELECT count(value) FROM db0.rp0.cpu group by value`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"value":""},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",50]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Regex(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu1,host=server01 value=10 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf(`cpu2,host=server01 value=20 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf(`cpu3,host=server01 value=30 %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "default db and rp",
			command: `SELECT * FROM /cpu[13]/`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","host","value"],"values":[["2015-02-28T01:03:36.703820946Z","server01",10]]},{"name":"cpu3","columns":["time","host","value"],"values":[["2015-02-28T01:03:36.703820946Z","server01",30]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "default db and rp with GROUP BY *",
			command: `SELECT * FROM /cpu[13]/ GROUP BY *`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "specifying db and rp",
			command: `SELECT * FROM db0.rp0./cpu[13]/ GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "default db and specified rp",
			command: `SELECT * FROM rp0./cpu[13]/ GROUP BY *`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "specified db and default rp",
			command: `SELECT * FROM db0../cpu[13]/ GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","tags":{"host":"server01"},"columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "map field type with a regex source",
			command: `SELECT value FROM /cpu[13]/`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu1","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",10]]},{"name":"cpu3","columns":["time","value"],"values":[["2015-02-28T01:03:36.703820946Z",30]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Aggregates_Int(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`int value=45 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		// int64
		&Query{
			name:    "stddev with just one point - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT STDDEV(value) FROM int`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"int","columns":["time","stddev"],"values":[["1970-01-01T00:00:00Z",null]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Aggregates_IntMax(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intmax value=%s %d`, maxInt64(), mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intmax value=%s %d`, maxInt64(), mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "large mean and stddev - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEAN(value), STDDEV(value) FROM intmax`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmax","columns":["time","mean","stddev"],"values":[["1970-01-01T00:00:00Z",` + maxInt64() + `,0]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Aggregates_IntMany_NowTime(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intmany,host=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "mean and stddev - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEAN(value), STDDEV(value) FROM intmany WHERE time >= '2000-01-01' AND time < '2000-01-01T00:02:00Z' GROUP BY time(10m)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","mean","stddev"],"values":[["2000-01-01T00:00:00Z",5,2.138089935299395]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "first - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT FIRST(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "first - int - epoch ms",
			params:  url.Values{"db": []string{"db0"}, "epoch": []string{"ms"}},
			command: `SELECT FIRST(value) FROM intmany`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","first"],"values":[[%d,2]]}]}]}`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()/int64(time.Millisecond)),
		},
		&Query{
			name:    "last - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT LAST(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","last"],"values":[["2000-01-01T00:01:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "spread - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SPREAD(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","spread"],"values":[["1970-01-01T00:00:00Z",7]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "median - even count - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",4.5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "median - odd count - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM intmany where time < '2000-01-01T00:01:10Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "mode - single - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MODE(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","mode"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "mode - multiple - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MODE(value) FROM intmany where time < '2000-01-01T00:01:10Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","mode"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "distinct as call - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT(value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:00Z",4],["1970-01-01T00:00:00Z",5],["1970-01-01T00:00:00Z",7],["1970-01-01T00:00:00Z",9]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "distinct alt syntax - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT value FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:00Z",4],["1970-01-01T00:00:00Z",5],["1970-01-01T00:00:00Z",7],["1970-01-01T00:00:00Z",9]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "distinct select tag - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT(host) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"error":"statement must have at least one field in select clause"}]}`,
			skip:    true, // FIXME(benbjohnson): should be allowed, need to stream tag values
		},
		&Query{
			name:    "distinct alt select tag - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT host FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"error":"statement must have at least one field in select clause"}]}`,
			skip:    true, // FIXME(benbjohnson): should be allowed, need to stream tag values
		},
		&Query{
			name:    "count distinct - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT value) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "count distinct as call - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT(value)) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "count distinct select tag - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT host) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0]]}]}]}`,
			skip:    true, // FIXME(benbjohnson): stream tag values
		},
		&Query{
			name:    "count distinct as call select tag - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT host) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0]]}]}]}`,
			skip:    true, // FIXME(benbjohnson): stream tag values
		},
	}...)

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

func TestServer_Query_Aggregates_IntMany_GroupBy(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intmany,host=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "max order by time with time specified group by 10s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, max(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(10s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",4],["2000-01-01T00:00:20Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:40Z",5],["2000-01-01T00:00:50Z",5],["2000-01-01T00:01:00Z",7],["2000-01-01T00:01:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "max order by time without time specified group by 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",4],["2000-01-01T00:00:30Z",5],["2000-01-01T00:01:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "max order by time with time specified group by 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, max(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",4],["2000-01-01T00:00:30Z",5],["2000-01-01T00:01:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "min order by time without time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "min order by time with time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, min(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "first order by time without time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "first order by time with time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, first(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "last order by time without time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",4],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",5],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",9]]}]}]}`,
		},
		&Query{
			name:    "last order by time with time specified group by 15s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, last(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:14Z' group by time(15s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",4],["2000-01-01T00:00:15Z",4],["2000-01-01T00:00:30Z",5],["2000-01-01T00:00:45Z",5],["2000-01-01T00:01:00Z",9]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_IntMany_OrderByDesc(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intmany,host=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`intmany,host=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "aggregate order by time desc",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(value) FROM intmany where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:00Z' group by time(10s) order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","max"],"values":[["2000-01-01T00:01:00Z",7],["2000-01-01T00:00:50Z",5],["2000-01-01T00:00:40Z",5],["2000-01-01T00:00:30Z",4],["2000-01-01T00:00:20Z",4],["2000-01-01T00:00:10Z",4],["2000-01-01T00:00:00Z",2]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Aggregates_IntOverlap(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`intoverlap,region=us-east value=20 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intoverlap,region=us-east value=30 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`intoverlap,region=us-west value=100 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`intoverlap,region=us-east otherVal=20 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "aggregation with a null field value - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM intoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intoverlap","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]},{"name":"intoverlap","tags":{"region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		&Query{
			name:    "multiple aggregations - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value), MEAN(value) FROM intoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intoverlap","tags":{"region":"us-east"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",50,25]]},{"name":"intoverlap","tags":{"region":"us-west"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",100,100]]}]}]}`,
		},
		&Query{
			name:    "multiple aggregations with division - int FIXME issue #2879",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value), mean(value), sum(value) / mean(value) as div FROM intoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intoverlap","tags":{"region":"us-east"},"columns":["time","sum","mean","div"],"values":[["1970-01-01T00:00:00Z",50,25,2]]},{"name":"intoverlap","tags":{"region":"us-west"},"columns":["time","sum","mean","div"],"values":[["1970-01-01T00:00:00Z",100,100,1]]}]}]}`,
		},
		&Query{
			name:    "multiple first aggregations - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(value), first(otherVal) FROM intoverlap`,
			exp:     ``,
			skip:    true,
		},
		&Query{
			name:    "multiple last aggregations - int",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(value), last(otherVal) FROM intoverlap`,
			exp:     ``,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Aggregates_FloatSingle(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`floatsingle value=45.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "stddev with just one point - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT STDDEV(value) FROM floatsingle`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatsingle","columns":["time","stddev"],"values":[["1970-01-01T00:00:00Z",null]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Aggregates_FloatMany(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`floatmany,host=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`floatmany,host=server02 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`floatmany,host=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
			fmt.Sprintf(`floatmany,host=server04 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
			fmt.Sprintf(`floatmany,host=server05 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
			fmt.Sprintf(`floatmany,host=server06 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
			fmt.Sprintf(`floatmany,host=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
			fmt.Sprintf(`floatmany,host=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "mean and stddev - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEAN(value), STDDEV(value) FROM floatmany WHERE time >= '2000-01-01' AND time < '2000-01-01T00:02:00Z' GROUP BY time(10m)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","mean","stddev"],"values":[["2000-01-01T00:00:00Z",5,2.138089935299395]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "first - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT FIRST(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "last - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT LAST(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","last"],"values":[["2000-01-01T00:01:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "spread - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SPREAD(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","spread"],"values":[["1970-01-01T00:00:00Z",7]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "median - even count - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",4.5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "median - odd count - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM floatmany where time < '2000-01-01T00:01:10Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "mode - single - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MODE(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","mode"],"values":[["1970-01-01T00:00:00Z",4]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "mode - multiple - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MODE(value) FROM floatmany where time < '2000-01-01T00:00:10Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","mode"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "distinct as call - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:00Z",4],["1970-01-01T00:00:00Z",5],["1970-01-01T00:00:00Z",7],["1970-01-01T00:00:00Z",9]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "distinct alt syntax - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT value FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",2],["1970-01-01T00:00:00Z",4],["1970-01-01T00:00:00Z",5],["1970-01-01T00:00:00Z",7],["1970-01-01T00:00:00Z",9]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "distinct select tag - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT(host) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"error":"statement must have at least one field in select clause"}]}`,
			skip:    true, // FIXME(benbjohnson): show be allowed, stream tag values
		},
		&Query{
			name:    "distinct alt select tag - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT DISTINCT host FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"error":"statement must have at least one field in select clause"}]}`,
			skip:    true, // FIXME(benbjohnson): show be allowed, stream tag values
		},
		&Query{
			name:    "count distinct - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "count distinct as call - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT(value)) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "count distinct select tag - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT host) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0]]}]}]}`,
			skip:    true, // FIXME(benbjohnson): stream tag values
		},
		&Query{
			name:    "count distinct as call select tag - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT host) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0]]}]}]}`,
			skip:    true, // FIXME(benbjohnson): stream tag values
		},
	}...)

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

func TestServer_Query_Aggregates_FloatOverlap(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`floatoverlap,region=us-east value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`floatoverlap,region=us-east value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`floatoverlap,region=us-west value=100.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`floatoverlap,region=us-east otherVal=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "aggregation with no interval - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT count(value) FROM floatoverlap WHERE time = '2000-01-01 00:00:00'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","columns":["time","count"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "sum - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM floatoverlap WHERE time >= '2000-01-01 00:00:05' AND time <= '2000-01-01T00:00:10Z' GROUP BY time(10s), region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",null],["2000-01-01T00:00:10Z",30]]}]}]}`,
		},
		&Query{
			name:    "aggregation with a null field value - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM floatoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","tags":{"region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]},{"name":"floatoverlap","tags":{"region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		&Query{
			name:    "multiple aggregations - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value), MEAN(value) FROM floatoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","tags":{"region":"us-east"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",50,25]]},{"name":"floatoverlap","tags":{"region":"us-west"},"columns":["time","sum","mean"],"values":[["1970-01-01T00:00:00Z",100,100]]}]}]}`,
		},
		&Query{
			name:    "multiple aggregations with division - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) / mean(value) as div FROM floatoverlap GROUP BY region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatoverlap","tags":{"region":"us-east"},"columns":["time","div"],"values":[["1970-01-01T00:00:00Z",2]]},{"name":"floatoverlap","tags":{"region":"us-west"},"columns":["time","div"],"values":[["1970-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "multiple first aggregations - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(value), first(otherVal) FROM intoverlap`,
			exp:     ``,
			skip:    true,
		},
		&Query{
			name:    "multiple last aggregations - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(value), last(otherVal) FROM intoverlap`,
			exp:     ``,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Aggregates_GroupByOffset(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`offset,region=us-east,host=serverA value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`offset,region=us-east,host=serverB value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`offset,region=us-west,host=serverC value=100.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "group by offset - standard",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM "offset" WHERE time >= '1999-12-31T23:59:55Z' AND time < '2000-01-01T00:00:15Z' GROUP BY time(10s, 5s) FILL(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"offset","columns":["time","sum"],"values":[["1999-12-31T23:59:55Z",120],["2000-01-01T00:00:05Z",30]]}]}]}`,
		},
		&Query{
			name:    "group by offset - misaligned time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM "offset" WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:20Z' GROUP BY time(10s, 5s) FILL(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"offset","columns":["time","sum"],"values":[["1999-12-31T23:59:55Z",120],["2000-01-01T00:00:05Z",30],["2000-01-01T00:00:15Z",0]]}]}]}`,
		},
		&Query{
			name:    "group by offset - negative time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM "offset" WHERE time >= '1999-12-31T23:59:55Z' AND time < '2000-01-01T00:00:15Z' GROUP BY time(10s, -5s) FILL(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"offset","columns":["time","sum"],"values":[["1999-12-31T23:59:55Z",120],["2000-01-01T00:00:05Z",30]]}]}]}`,
		},
		&Query{
			name:    "group by offset - modulo",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM "offset" WHERE time >= '1999-12-31T23:59:55Z' AND time < '2000-01-01T00:00:15Z' GROUP BY time(10s, 35s) FILL(0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"offset","columns":["time","sum"],"values":[["1999-12-31T23:59:55Z",120],["2000-01-01T00:00:05Z",30]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_Load(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`load,region=us-east,host=serverA value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
			fmt.Sprintf(`load,region=us-east,host=serverB value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
			fmt.Sprintf(`load,region=us-west,host=serverC value=100.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "group by multiple dimensions",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM load GROUP BY region, host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"load","tags":{"host":"serverA","region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",20]]},{"name":"load","tags":{"host":"serverB","region":"us-east"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",30]]},{"name":"load","tags":{"host":"serverC","region":"us-west"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",100]]}]}]}`,
		},
		&Query{
			name:    "group by multiple dimensions",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value)*2 FROM load`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"load","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",300]]}]}]}`,
		},
		&Query{
			name:    "group by multiple dimensions",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value)/2 FROM load`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"load","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",75]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_CPU(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`cpu,region=uk,host=serverZ,service=redis value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
			fmt.Sprintf(`cpu,region=uk,host=serverZ,service=mysql value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "aggregation with WHERE and AND",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) FROM cpu WHERE region='uk' AND host='serverZ'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",50]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregates_String(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join([]string{
			fmt.Sprintf(`stringdata value="first" %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
			fmt.Sprintf(`stringdata value="last" %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:04Z").UnixNano()),
		}, "\n")},
	}

	test.addQueries([]*Query{
		// strings
		&Query{
			name:    "STDDEV on string data - string",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT STDDEV(value) FROM stringdata`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"stringdata","columns":["time","stddev"],"values":[["1970-01-01T00:00:00Z",null]]}]}]}`,
			skip:    true, // FIXME(benbjohnson): allow non-float var ref expr in cursor iterator
		},
		&Query{
			name:    "MEAN on string data - string",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEAN(value) FROM stringdata`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"stringdata","columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",0]]}]}]}`,
			skip:    true, // FIXME(benbjohnson): allow non-float var ref expr in cursor iterator
		},
		&Query{
			name:    "MEDIAN on string data - string",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEDIAN(value) FROM stringdata`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"stringdata","columns":["time","median"],"values":[["1970-01-01T00:00:00Z",null]]}]}]}`,
			skip:    true, // FIXME(benbjohnson): allow non-float var ref expr in cursor iterator
		},
		&Query{
			name:    "COUNT on string data - string",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(value) FROM stringdata`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"stringdata","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`,
			skip:    false, // FIXME(benbjohnson): allow non-float var ref expr in cursor iterator
		},
		&Query{
			name:    "FIRST on string data - string",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT FIRST(value) FROM stringdata`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"stringdata","columns":["time","first"],"values":[["2000-01-01T00:00:03Z","first"]]}]}]}`,
			skip:    false, // FIXME(benbjohnson): allow non-float var ref expr in cursor iterator
		},
		&Query{
			name:    "LAST on string data - string",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT LAST(value) FROM stringdata`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"stringdata","columns":["time","last"],"values":[["2000-01-01T00:00:04Z","last"]]}]}]}`,
			skip:    false, // FIXME(benbjohnson): allow non-float var ref expr in cursor iterator
		},
	}...)

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

func TestServer_Query_Aggregates_Math(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`network,host=server01,region=west,cores=1 rx=10i,tx=20i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`network,host=server02,region=west,cores=2 rx=40i,tx=50i,core=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`network,host=server03,region=east,cores=3 rx=40i,tx=55i,core=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`network,host=server04,region=east,cores=4 rx=40i,tx=60i,core=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`network,host=server05,region=west,cores=1 rx=50i,tx=70i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
		fmt.Sprintf(`network,host=server06,region=east,cores=2 rx=50i,tx=40i,core=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
		fmt.Sprintf(`network,host=server07,region=west,cores=3 rx=70i,tx=30i,core=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`network,host=server08,region=east,cores=4 rx=90i,tx=10i,core=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		fmt.Sprintf(`network,host=server09,region=east,cores=1 rx=5i,tx=4i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "add two selectors",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(DISTINCT(value)) FROM intmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"intmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "use math one two selectors separately",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(rx) * 1, min(rx) * 1 FROM network WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","max","min"],"values":[["2000-01-01T00:00:00Z",90,5]]}]}]}`,
		},
		&Query{
			name:    "math with a single selector",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(rx) * 1 FROM network WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","max"],"values":[["2000-01-01T00:01:10Z",90]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Aggregate_For_String_Functions(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=azhu age=12.3,height=70i,address="shenzhen",alive=TRUE 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=20.5,height=80i,address="shanghai",alive=FALSE 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang age=3.4,height=90i,address="beijin",alive=TRUE 1629129602000000000`),
		fmt.Sprintf(`mst,country=japan,name=ahui age=30,height=121i,address="guangzhou",alive=FALSE 1629129603000000000`),
		fmt.Sprintf(`mst,country=canada,name=aqiu age=35,height=138i,address="chengdu",alive=TRUE 1629129604000000000`),
		fmt.Sprintf(`mst,country=china,name=agang age=48.8,height=149i,address="wuhan" 1629129605000000000`),
		fmt.Sprintf(`mst,country=american,name=agan age=52.7,height=153i,alive=TRUE 1629129606000000000`),
		fmt.Sprintf(`mst,country=germany,name=alin age=28.3,address="anhui",alive=FALSE 1629129607000000000`),
		fmt.Sprintf(`mst,country=japan,name=ali height=179i,address="xian",alive=TRUE 1629129608000000000`),
		fmt.Sprintf(`mst,country=canada age=60.8,height=180i,address="hangzhou",alive=FALSE 1629129609000000000`),
		fmt.Sprintf(`mst,name=ahuang age=102,height=191i,address="nanjin",alive=TRUE 1629129610000000000`),
		fmt.Sprintf(`mst,country=china,name=ayin age=123,height=203i,address="zhengzhou",alive=FALSE 1629129611000000000`),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SELECT str(address, 'shanghai')",
			command: `SELECT str(address, 'shanghai') FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","str"],"values":[["2021-08-16T16:00:00Z",false],["2021-08-16T16:00:01Z",true],["2021-08-16T16:00:02Z",false],["2021-08-16T16:00:03Z",false],["2021-08-16T16:00:04Z",false],["2021-08-16T16:00:05Z",false],["2021-08-16T16:00:07Z",false],["2021-08-16T16:00:08Z",false],["2021-08-16T16:00:09Z",false],["2021-08-16T16:00:10Z",false],["2021-08-16T16:00:11Z",false]]}]}]}`,
		},
		&Query{
			name:    "SELECT str(address, 'sh') GROUP BY",
			command: `SELECT str(address, 'sh') FROM db0.rp0.mst GROUP BY country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":""},"columns":["time","str"],"values":[["2021-08-16T16:00:10Z",false]]},{"name":"mst","tags":{"country":"american"},"columns":["time","str"],"values":[["2021-08-16T16:00:01Z",true]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","str"],"values":[["2021-08-16T16:00:04Z",false],["2021-08-16T16:00:09Z",false]]},{"name":"mst","tags":{"country":"china"},"columns":["time","str"],"values":[["2021-08-16T16:00:00Z",true],["2021-08-16T16:00:05Z",false],["2021-08-16T16:00:11Z",false]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","str"],"values":[["2021-08-16T16:00:02Z",false],["2021-08-16T16:00:07Z",false]]},{"name":"mst","tags":{"country":"japan"},"columns":["time","str"],"values":[["2021-08-16T16:00:03Z",false],["2021-08-16T16:00:08Z",false]]}]}]}`,
		},
		&Query{
			name:    "SELECT strlen(address)",
			command: `SELECT strlen(address) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","strlen"],"values":[["2021-08-16T16:00:00Z",8],["2021-08-16T16:00:01Z",8],["2021-08-16T16:00:02Z",6],["2021-08-16T16:00:03Z",9],["2021-08-16T16:00:04Z",7],["2021-08-16T16:00:05Z",5],["2021-08-16T16:00:07Z",5],["2021-08-16T16:00:08Z",4],["2021-08-16T16:00:09Z",8],["2021-08-16T16:00:10Z",6],["2021-08-16T16:00:11Z",9]]}]}]}`,
		},
		&Query{
			name:    "SELECT strlen(address) GROUP BY",
			command: `SELECT strlen(address) FROM db0.rp0.mst GROUP BY country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":""},"columns":["time","strlen"],"values":[["2021-08-16T16:00:10Z",6]]},{"name":"mst","tags":{"country":"american"},"columns":["time","strlen"],"values":[["2021-08-16T16:00:01Z",8]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","strlen"],"values":[["2021-08-16T16:00:04Z",7],["2021-08-16T16:00:09Z",8]]},{"name":"mst","tags":{"country":"china"},"columns":["time","strlen"],"values":[["2021-08-16T16:00:00Z",8],["2021-08-16T16:00:05Z",5],["2021-08-16T16:00:11Z",9]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","strlen"],"values":[["2021-08-16T16:00:02Z",6],["2021-08-16T16:00:07Z",5]]},{"name":"mst","tags":{"country":"japan"},"columns":["time","strlen"],"values":[["2021-08-16T16:00:03Z",9],["2021-08-16T16:00:08Z",4]]}]}]}`,
		},
		&Query{
			name:    "SELECT substr(address, 1, 4)",
			command: `SELECT substr(address, 1, 4) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","substr"],"values":[["2021-08-16T16:00:00Z","henz"],["2021-08-16T16:00:01Z","hang"],["2021-08-16T16:00:02Z","eiji"],["2021-08-16T16:00:03Z","uang"],["2021-08-16T16:00:04Z","heng"],["2021-08-16T16:00:05Z","uhan"],["2021-08-16T16:00:07Z","nhui"],["2021-08-16T16:00:08Z","ian"],["2021-08-16T16:00:09Z","angz"],["2021-08-16T16:00:10Z","anji"],["2021-08-16T16:00:11Z","heng"]]}]}]}`,
		},
		&Query{
			name:    "SELECT substr(address, 1, 4) GROUP BY",
			command: `SELECT substr(address, 1, 4) FROM db0.rp0.mst GROUP BY country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":""},"columns":["time","substr"],"values":[["2021-08-16T16:00:10Z","anji"]]},{"name":"mst","tags":{"country":"american"},"columns":["time","substr"],"values":[["2021-08-16T16:00:01Z","hang"]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","substr"],"values":[["2021-08-16T16:00:04Z","heng"],["2021-08-16T16:00:09Z","angz"]]},{"name":"mst","tags":{"country":"china"},"columns":["time","substr"],"values":[["2021-08-16T16:00:00Z","henz"],["2021-08-16T16:00:05Z","uhan"],["2021-08-16T16:00:11Z","heng"]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","substr"],"values":[["2021-08-16T16:00:02Z","eiji"],["2021-08-16T16:00:07Z","nhui"]]},{"name":"mst","tags":{"country":"japan"},"columns":["time","substr"],"values":[["2021-08-16T16:00:03Z","uang"],["2021-08-16T16:00:08Z","ian"]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Sliding_Window_Aggregate(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=azhu age=12.3,height=70i,address="shenzhen",alive=TRUE 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=20.5,height=80i,address="shanghai",alive=FALSE 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang age=3.4,height=90i,address="beijin",alive=TRUE 1629129602000000000`),
		fmt.Sprintf(`mst,country=japan,name=ahui age=30,height=121i,address="guangzhou",alive=FALSE 1629129603000000000`),
		fmt.Sprintf(`mst,country=canada,name=aqiu age=35,height=138i,address="chengdu",alive=TRUE 1629129604000000000`),
		fmt.Sprintf(`mst,country=china,name=agang age=48.8,height=149i,address="wuhan" 1629129605000000000`),
		fmt.Sprintf(`mst,country=american,name=agan age=52.7,height=153i,alive=TRUE 1629129606000000000`),
		fmt.Sprintf(`mst,country=germany,name=alin age=28.3,address="anhui",alive=FALSE 1629129607000000000`),
		fmt.Sprintf(`mst,country=japan,name=ali height=179i,address="xian",alive=TRUE 1629129608000000000`),
		fmt.Sprintf(`mst,country=canada age=60.8,height=180i,address="hangzhou",alive=FALSE 1629129609000000000`),
		fmt.Sprintf(`mst,name=ahuang age=102,height=191i,address="nanjin",alive=TRUE 1629129610000000000`),
		fmt.Sprintf(`mst,country=china,name=ayin age=123,height=203i,address="zhengzhou",alive=FALSE 1629129611000000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SELECT sliding_window(max), group by time",
			command: `select sliding_window(max(*), 5) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sliding_window_age","sliding_window_alive","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",35,true,138],["2021-08-16T16:00:01Z",48.8,true,149],["2021-08-16T16:00:02Z",52.7,true,153],["2021-08-16T16:00:03Z",52.7,true,153],["2021-08-16T16:00:04Z",52.7,true,179],["2021-08-16T16:00:05Z",60.8,true,180],["2021-08-16T16:00:06Z",102,true,191]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(min), group by time",
			command: `select sliding_window(min(*), 5) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sliding_window_age","sliding_window_alive","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",3.4,false,70],["2021-08-16T16:00:01Z",3.4,false,80],["2021-08-16T16:00:02Z",3.4,false,90],["2021-08-16T16:00:03Z",28.3,false,121],["2021-08-16T16:00:04Z",28.3,false,138],["2021-08-16T16:00:05Z",28.3,false,149],["2021-08-16T16:00:06Z",28.3,false,153]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(count), group by time",
			command: `select sliding_window(count(*), 5) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sliding_window_address","sliding_window_age","sliding_window_alive","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",5,5,5,5],["2021-08-16T16:00:01Z",5,5,4,5],["2021-08-16T16:00:02Z",4,5,4,5],["2021-08-16T16:00:03Z",4,5,4,4],["2021-08-16T16:00:04Z",4,4,4,4],["2021-08-16T16:00:05Z",4,4,4,4],["2021-08-16T16:00:06Z",4,4,5,4]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(sum), group by time",
			command: `select sliding_window(sum(height), 5) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sliding_window"],"values":[["2021-08-16T16:00:00Z",499],["2021-08-16T16:00:01Z",578],["2021-08-16T16:00:02Z",651],["2021-08-16T16:00:03Z",561],["2021-08-16T16:00:04Z",619],["2021-08-16T16:00:05Z",661],["2021-08-16T16:00:06Z",703]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(mean), group by time",
			command: `select sliding_window(mean(height), 5) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sliding_window"],"values":[["2021-08-16T16:00:00Z",99.8],["2021-08-16T16:00:01Z",115.6],["2021-08-16T16:00:02Z",130.2],["2021-08-16T16:00:03Z",140.25],["2021-08-16T16:00:04Z",154.75],["2021-08-16T16:00:05Z",165.25],["2021-08-16T16:00:06Z",175.75]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(spread), group by time",
			command: `select sliding_window(spread(*), 5) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",31.6,68],["2021-08-16T16:00:01Z",45.4,69],["2021-08-16T16:00:02Z",49.300000000000004,63],["2021-08-16T16:00:03Z",24.400000000000002,32],["2021-08-16T16:00:04Z",24.400000000000002,41],["2021-08-16T16:00:05Z",32.5,31],["2021-08-16T16:00:06Z",73.7,38]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(mean), group by time, tag",
			command: `select sliding_window(mean(*), 8) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s),country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":""},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",null,null],["2021-08-16T16:00:01Z",null,null],["2021-08-16T16:00:02Z",null,null],["2021-08-16T16:00:03Z",102,191]]},{"name":"mst","tags":{"country":"american"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",36.6,116.5],["2021-08-16T16:00:01Z",36.6,116.5],["2021-08-16T16:00:02Z",52.7,153],["2021-08-16T16:00:03Z",52.7,153]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",35,138],["2021-08-16T16:00:01Z",35,138],["2021-08-16T16:00:02Z",47.9,159],["2021-08-16T16:00:03Z",47.9,159]]},{"name":"mst","tags":{"country":"china"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",30.549999999999997,109.5],["2021-08-16T16:00:01Z",48.8,149],["2021-08-16T16:00:02Z",48.8,149],["2021-08-16T16:00:03Z",48.8,149]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",15.85,90],["2021-08-16T16:00:01Z",15.85,90],["2021-08-16T16:00:02Z",15.85,90],["2021-08-16T16:00:03Z",28.3,null]]},{"name":"mst","tags":{"country":"japan"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",30,121],["2021-08-16T16:00:01Z",30,150],["2021-08-16T16:00:02Z",30,150],["2021-08-16T16:00:03Z",30,150]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(spread), group by time,tag",
			command: `select sliding_window(spread(*), 8) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s),country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":""},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",null,null],["2021-08-16T16:00:01Z",null,null],["2021-08-16T16:00:02Z",null,null],["2021-08-16T16:00:03Z",0,0]]},{"name":"mst","tags":{"country":"american"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",32.2,73],["2021-08-16T16:00:01Z",32.2,73],["2021-08-16T16:00:02Z",0,0],["2021-08-16T16:00:03Z",0,0]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",0,0],["2021-08-16T16:00:01Z",0,0],["2021-08-16T16:00:02Z",25.799999999999997,42],["2021-08-16T16:00:03Z",25.799999999999997,42]]},{"name":"mst","tags":{"country":"china"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",36.5,79],["2021-08-16T16:00:01Z",0,0],["2021-08-16T16:00:02Z",0,0],["2021-08-16T16:00:03Z",0,0]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",24.900000000000002,0],["2021-08-16T16:00:01Z",24.900000000000002,0],["2021-08-16T16:00:02Z",24.900000000000002,0],["2021-08-16T16:00:03Z",0,null]]},{"name":"mst","tags":{"country":"japan"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",0,0],["2021-08-16T16:00:01Z",0,58],["2021-08-16T16:00:02Z",0,58],["2021-08-16T16:00:03Z",0,58]]}]}]}`,
		},
		&Query{
			name:    "SELECT * FROM (sliding_window(mean), group by time, tag)",
			command: `select * from (select sliding_window(mean(*), 8) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s),country)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","country","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z","",null,null],["2021-08-16T16:00:01Z","",null,null],["2021-08-16T16:00:02Z","",null,null],["2021-08-16T16:00:03Z","",102,191],["2021-08-16T16:00:00Z","american",36.6,116.5],["2021-08-16T16:00:01Z","american",36.6,116.5],["2021-08-16T16:00:02Z","american",52.7,153],["2021-08-16T16:00:03Z","american",52.7,153],["2021-08-16T16:00:00Z","canada",35,138],["2021-08-16T16:00:01Z","canada",35,138],["2021-08-16T16:00:02Z","canada",47.9,159],["2021-08-16T16:00:03Z","canada",47.9,159],["2021-08-16T16:00:00Z","china",30.549999999999997,109.5],["2021-08-16T16:00:01Z","china",48.8,149],["2021-08-16T16:00:02Z","china",48.8,149],["2021-08-16T16:00:03Z","china",48.8,149],["2021-08-16T16:00:00Z","germany",15.85,90],["2021-08-16T16:00:01Z","germany",15.85,90],["2021-08-16T16:00:02Z","germany",15.85,90],["2021-08-16T16:00:03Z","germany",28.3,null],["2021-08-16T16:00:00Z","japan",30,121],["2021-08-16T16:00:01Z","japan",30,150],["2021-08-16T16:00:02Z","japan",30,150],["2021-08-16T16:00:03Z","japan",30,150]]}]}]}`,
		},
		&Query{
			name:    "SELECT * FROM (SELECT sliding_window(spread), group by time,tag)",
			command: `select * from (select sliding_window(spread(*), 8) from db0.rp0.mst where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s),country)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","country","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z","",null,null],["2021-08-16T16:00:01Z","",null,null],["2021-08-16T16:00:02Z","",null,null],["2021-08-16T16:00:03Z","",0,0],["2021-08-16T16:00:00Z","american",32.2,73],["2021-08-16T16:00:01Z","american",32.2,73],["2021-08-16T16:00:02Z","american",0,0],["2021-08-16T16:00:03Z","american",0,0],["2021-08-16T16:00:00Z","canada",0,0],["2021-08-16T16:00:01Z","canada",0,0],["2021-08-16T16:00:02Z","canada",25.799999999999997,42],["2021-08-16T16:00:03Z","canada",25.799999999999997,42],["2021-08-16T16:00:00Z","china",36.5,79],["2021-08-16T16:00:01Z","china",0,0],["2021-08-16T16:00:02Z","china",0,0],["2021-08-16T16:00:03Z","china",0,0],["2021-08-16T16:00:00Z","germany",24.900000000000002,0],["2021-08-16T16:00:01Z","germany",24.900000000000002,0],["2021-08-16T16:00:02Z","germany",24.900000000000002,0],["2021-08-16T16:00:03Z","germany",0,null],["2021-08-16T16:00:00Z","japan",0,0],["2021-08-16T16:00:01Z","japan",0,58],["2021-08-16T16:00:02Z","japan",0,58],["2021-08-16T16:00:03Z","japan",0,58]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(mean(*)), FROM (SELECT *) group by time)",
			command: `select sliding_window(mean(*), 8) from (select * from db0.rp0.mst) where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s),country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":""},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",null,null],["2021-08-16T16:00:01Z",null,null],["2021-08-16T16:00:02Z",null,null],["2021-08-16T16:00:03Z",102,191]]},{"name":"mst","tags":{"country":"american"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",36.6,116.5],["2021-08-16T16:00:01Z",36.6,116.5],["2021-08-16T16:00:02Z",52.7,153],["2021-08-16T16:00:03Z",52.7,153]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",35,138],["2021-08-16T16:00:01Z",35,138],["2021-08-16T16:00:02Z",47.9,159],["2021-08-16T16:00:03Z",47.9,159]]},{"name":"mst","tags":{"country":"china"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",30.549999999999997,109.5],["2021-08-16T16:00:01Z",48.8,149],["2021-08-16T16:00:02Z",48.8,149],["2021-08-16T16:00:03Z",48.8,149]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",15.85,90],["2021-08-16T16:00:01Z",15.85,90],["2021-08-16T16:00:02Z",15.85,90],["2021-08-16T16:00:03Z",28.3,null]]},{"name":"mst","tags":{"country":"japan"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",30,121],["2021-08-16T16:00:01Z",30,150],["2021-08-16T16:00:02Z",30,150],["2021-08-16T16:00:03Z",30,150]]}]}]}`,
		},
		&Query{
			name:    "SELECT sliding_window(spread), FROM (SELECT *) group by time)",
			command: `select sliding_window(spread(*), 8) from (select * from db0.rp0.mst) where time >= '2021-08-16T16:00:00Z' and time < '2021-08-16T16:00:11Z' group by time(1s),country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":""},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",null,null],["2021-08-16T16:00:01Z",null,null],["2021-08-16T16:00:02Z",null,null],["2021-08-16T16:00:03Z",0,0]]},{"name":"mst","tags":{"country":"american"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",32.2,73],["2021-08-16T16:00:01Z",32.2,73],["2021-08-16T16:00:02Z",0,0],["2021-08-16T16:00:03Z",0,0]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",0,0],["2021-08-16T16:00:01Z",0,0],["2021-08-16T16:00:02Z",25.799999999999997,42],["2021-08-16T16:00:03Z",25.799999999999997,42]]},{"name":"mst","tags":{"country":"china"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",36.5,79],["2021-08-16T16:00:01Z",0,0],["2021-08-16T16:00:02Z",0,0],["2021-08-16T16:00:03Z",0,0]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",24.900000000000002,0],["2021-08-16T16:00:01Z",24.900000000000002,0],["2021-08-16T16:00:02Z",24.900000000000002,0],["2021-08-16T16:00:03Z",0,null]]},{"name":"mst","tags":{"country":"japan"},"columns":["time","sliding_window_age","sliding_window_height"],"values":[["2021-08-16T16:00:00Z",0,0],["2021-08-16T16:00:01Z",0,58],["2021-08-16T16:00:02Z",0,58],["2021-08-16T16:00:03Z",0,58]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Null_Aggregate(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=azhu age=12.3,height=70i,address="shenzhen",alive=TRUE 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=20.5,height=80i,address="shanghai",alive=FALSE 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang age=3.4,height=90i,address="beijin",alive=TRUE 1629129602000000000`),
		fmt.Sprintf(`mst,country=japan,name=ahui age=30,height=121i,address="guangzhou",alive=FALSE 1629129603000000000`),
		fmt.Sprintf(`mst,country=canada,name=aqiu age=35,height=138i,address="chengdu",alive=TRUE 1629129604000000000`),
		fmt.Sprintf(`mst,country=china,name=agang age=48.8,height=149i,address="wuhan" 1629129605000000000`),
		fmt.Sprintf(`mst,country=american,name=agan age=52.7,height=153i,alive=TRUE 1629129606000000000`),
		fmt.Sprintf(`mst,country=germany,name=alin age=28.3,address="anhui",alive=FALSE 1629129607000000000`),
		fmt.Sprintf(`mst,country=japan,name=ali height=179i,address="xian",alive=TRUE 1629129608000000000`),
		fmt.Sprintf(`mst,country=canada age=60.8,height=180i,address="hangzhou",alive=FALSE 1629129609000000000`),
		fmt.Sprintf(`mst,name=ahuang age=102,height=191i,address="nanjin",alive=TRUE 1629129610000000000`),
		fmt.Sprintf(`mst,country=china,name=ayin age=123,height=203i,address="zhengzhou",alive=FALSE 1629129611000000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// BUG2021121601543
		&Query{
			name:    "SELECT top(age, 2), country",
			command: `SELECT top(age, 2), country FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","top","country"],"values":[["2021-08-16T16:00:10Z",102,null],["2021-08-16T16:00:11Z",123,"china"]]}]}]}`,
		},
		// BUG2021121601543
		&Query{
			name:    "SELECT top(age, 2), age - height",
			command: `SELECT top(age, 2), age - height AS value FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","top","value"],"values":[["2021-08-16T16:00:10Z",102,-89],["2021-08-16T16:00:11Z",123,-80]]}]}]}`,
			//skip:    true,
		},
		// BUG2021121601543
		&Query{
			name:    "SELECT max(age), age - height",
			command: `SELECT max(age), age - height AS value FROM db0.rp0.mst GROUP BY country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":""},"columns":["time","max","value"],"values":[["2021-08-16T16:00:10Z",102,-89]]},{"name":"mst","tags":{"country":"american"},"columns":["time","max","value"],"values":[["2021-08-16T16:00:06Z",52.7,-100.3]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","max","value"],"values":[["2021-08-16T16:00:09Z",60.8,-119.2]]},{"name":"mst","tags":{"country":"china"},"columns":["time","max","value"],"values":[["2021-08-16T16:00:11Z",123,-80]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","max","value"],"values":[["2021-08-16T16:00:07Z",28.3,null]]},{"name":"mst","tags":{"country":"japan"},"columns":["time","max","value"],"values":[["2021-08-16T16:00:03Z",30,-91]]}]}]}`,
			//skip:    true,
		},
		// BUG2021121702524
		&Query{
			name:    "SELECT BOTTOM(value, 2) FROM (SELECT BOTTOM(age, 3), age - height AS value FROM db0.rp0.mst)",
			command: `SELECT BOTTOM(value, 2) FROM (SELECT BOTTOM(age, 3), age - height AS value FROM db0.rp0.mst GROUP BY country) WHERE time >= '2021-08-16T16:00:00Z' AND time < '2021-08-16T16:00:11Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","bottom"],"values":[["2021-08-16T16:00:04Z",-103],["2021-08-16T16:00:09Z",-119.2]]}]}]}`,
			//skip:    true,
		},
		// BUG2021121702480
		&Query{
			name:    "SELECT LAST(*) group by time(12m) limit 5",
			command: `SELECT LAST(*) FROM db0.rp0.mst group by time(12m) order by time limit 5`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","last_address","last_age","last_alive","last_height"],"values":[["2021-08-16T16:00:00Z","zhengzhou",123,false,203],["2021-08-16T16:12:00Z",null,null,null,null],["2021-08-16T16:24:00Z",null,null,null,null],["2021-08-16T16:36:00Z",null,null,null,null],["2021-08-16T16:48:00Z",null,null,null,null]]}]}]}`,
			//skip:    true,
		},
		// BUG2021121702512
		&Query{
			name:    "SELECT FIRST(*) FROM (SELECT * FROM db0.rp0.mst)",
			command: `SELECT FIRST(*) FROM (SELECT * FROM db0.rp0.mst)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","first_address","first_age","first_alive","first_height"],"values":[["1970-01-01T00:00:00Z","shenzhen",12.3,true,70]]}]}]}`,
			//skip:    true,
		},
		&Query{
			name:    "SELECT DIFFERENCE(*) FROM db0.rp0.mst",
			command: `SELECT DIFFERENCE(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","difference_age","difference_height"],"values":[["2021-08-16T16:00:01Z",8.2,10],["2021-08-16T16:00:02Z",-17.1,10],["2021-08-16T16:00:03Z",26.6,31],["2021-08-16T16:00:04Z",5,17],["2021-08-16T16:00:05Z",13.799999999999997,11],["2021-08-16T16:00:06Z",3.9000000000000057,4],["2021-08-16T16:00:07Z",-24.400000000000002,null],["2021-08-16T16:00:08Z",null,26],["2021-08-16T16:00:09Z",32.5,1],["2021-08-16T16:00:10Z",41.2,11],["2021-08-16T16:00:11Z",21,12]]}]}]}`,
		},
		&Query{
			name:    "SELECT DIFFERENCE(* 'front') FROM db0.rp0.mst",
			command: `SELECT DIFFERENCE(*, 'front') FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","difference_age","difference_height"],"values":[["2021-08-16T16:00:01Z",-8.2,-10],["2021-08-16T16:00:02Z",17.1,-10],["2021-08-16T16:00:03Z",-26.6,-31],["2021-08-16T16:00:04Z",-5,-17],["2021-08-16T16:00:05Z",-13.799999999999997,-11],["2021-08-16T16:00:06Z",-3.9000000000000057,-4],["2021-08-16T16:00:07Z",24.400000000000002,null],["2021-08-16T16:00:08Z",null,-26],["2021-08-16T16:00:09Z",-32.5,-1],["2021-08-16T16:00:10Z",-41.2,-11],["2021-08-16T16:00:11Z",-21,-12]]}]}]}`,
		},
		&Query{
			name:    "SELECT DIFFERENCE(* 'behind') FROM db0.rp0.mst",
			command: `SELECT DIFFERENCE(*, 'behind') FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","difference_age","difference_height"],"values":[["2021-08-16T16:00:01Z",8.2,10],["2021-08-16T16:00:02Z",-17.1,10],["2021-08-16T16:00:03Z",26.6,31],["2021-08-16T16:00:04Z",5,17],["2021-08-16T16:00:05Z",13.799999999999997,11],["2021-08-16T16:00:06Z",3.9000000000000057,4],["2021-08-16T16:00:07Z",-24.400000000000002,null],["2021-08-16T16:00:08Z",null,26],["2021-08-16T16:00:09Z",32.5,1],["2021-08-16T16:00:10Z",41.2,11],["2021-08-16T16:00:11Z",21,12]]}]}]}`,
		},
		&Query{
			name:    "SELECT DIFFERENCE(* 'absolute') FROM db0.rp0.mst",
			command: `SELECT DIFFERENCE(*, 'absolute') FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","difference_age","difference_height"],"values":[["2021-08-16T16:00:01Z",8.2,10],["2021-08-16T16:00:02Z",17.1,10],["2021-08-16T16:00:03Z",26.6,31],["2021-08-16T16:00:04Z",5,17],["2021-08-16T16:00:05Z",13.799999999999997,11],["2021-08-16T16:00:06Z",3.9000000000000057,4],["2021-08-16T16:00:07Z",24.400000000000002,null],["2021-08-16T16:00:08Z",null,26],["2021-08-16T16:00:09Z",32.5,1],["2021-08-16T16:00:10Z",41.2,11],["2021-08-16T16:00:11Z",21,12]]}]}]}`,
		},
		&Query{
			name:    "select difference(age, 'front') from ( select * from db0.rp0.mst group by country) group by country order by time desc",
			command: `select difference(age, 'front') from ( select * from db0.rp0.mst group by country) group by country order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":"germany"},"columns":["time","difference"],"values":[["2021-08-16T16:00:02Z",24.900000000000002]]},{"name":"mst","tags":{"country":"china"},"columns":["time","difference"],"values":[["2021-08-16T16:00:05Z",74.2],["2021-08-16T16:00:00Z",36.5]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","difference"],"values":[["2021-08-16T16:00:04Z",25.799999999999997]]},{"name":"mst","tags":{"country":"american"},"columns":["time","difference"],"values":[["2021-08-16T16:00:01Z",32.2]]}]}]}`,
		},
		&Query{
			name:    "select difference(age, 'behind') from ( select * from db0.rp0.mst group by country) group by country order by time desc",
			command: `select difference(age, 'behind') from ( select * from db0.rp0.mst group by country) group by country order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":"germany"},"columns":["time","difference"],"values":[["2021-08-16T16:00:02Z",-24.900000000000002]]},{"name":"mst","tags":{"country":"china"},"columns":["time","difference"],"values":[["2021-08-16T16:00:05Z",-74.2],["2021-08-16T16:00:00Z",-36.5]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","difference"],"values":[["2021-08-16T16:00:04Z",-25.799999999999997]]},{"name":"mst","tags":{"country":"american"},"columns":["time","difference"],"values":[["2021-08-16T16:00:01Z",-32.2]]}]}]}`,
		},
		&Query{
			name:    "select difference(age, 'absolute') from ( select * from db0.rp0.mst group by country) group by country order by time desc",
			command: `select difference(age, 'absolute') from ( select * from db0.rp0.mst group by country) group by country order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":"germany"},"columns":["time","difference"],"values":[["2021-08-16T16:00:02Z",24.900000000000002]]},{"name":"mst","tags":{"country":"china"},"columns":["time","difference"],"values":[["2021-08-16T16:00:05Z",74.2],["2021-08-16T16:00:00Z",36.5]]},{"name":"mst","tags":{"country":"canada"},"columns":["time","difference"],"values":[["2021-08-16T16:00:04Z",25.799999999999997]]},{"name":"mst","tags":{"country":"american"},"columns":["time","difference"],"values":[["2021-08-16T16:00:01Z",32.2]]}]}]}`,
		},
		&Query{
			name:    "SELECT NON_NEGATIVE_DIFFERENCE(*) FROM db0.rp0.mst",
			command: `SELECT NON_NEGATIVE_DIFFERENCE(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","non_negative_difference_age","non_negative_difference_height"],"values":[["2021-08-16T16:00:01Z",8.2,10],["2021-08-16T16:00:02Z",null,10],["2021-08-16T16:00:03Z",26.6,31],["2021-08-16T16:00:04Z",5,17],["2021-08-16T16:00:05Z",13.799999999999997,11],["2021-08-16T16:00:06Z",3.9000000000000057,4],["2021-08-16T16:00:08Z",null,26],["2021-08-16T16:00:09Z",32.5,1],["2021-08-16T16:00:10Z",41.2,11],["2021-08-16T16:00:11Z",21,12]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "SELECT DERIVATIVE(*) FROM db0.rp0.mst",
			command: `SELECT DERIVATIVE(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","derivative_age","derivative_height"],"values":[["2021-08-16T16:00:01Z",8.2,10],["2021-08-16T16:00:02Z",-17.1,10],["2021-08-16T16:00:03Z",26.6,31],["2021-08-16T16:00:04Z",5,17],["2021-08-16T16:00:05Z",13.799999999999997,11],["2021-08-16T16:00:06Z",3.9000000000000057,4],["2021-08-16T16:00:07Z",-24.400000000000002,null],["2021-08-16T16:00:08Z",null,13],["2021-08-16T16:00:09Z",16.25,1],["2021-08-16T16:00:10Z",41.2,11],["2021-08-16T16:00:11Z",21,12]]}]}]}`,
		},
		&Query{
			name:    "SELECT NON_NEGATIVE_DERIVATIVE(*) FROM db0.rp0.mst",
			command: `SELECT NON_NEGATIVE_DERIVATIVE(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","non_negative_derivative_age","non_negative_derivative_height"],"values":[["2021-08-16T16:00:01Z",8.2,10],["2021-08-16T16:00:02Z",null,10],["2021-08-16T16:00:03Z",26.6,31],["2021-08-16T16:00:04Z",5,17],["2021-08-16T16:00:05Z",13.799999999999997,11],["2021-08-16T16:00:06Z",3.9000000000000057,4],["2021-08-16T16:00:08Z",null,13],["2021-08-16T16:00:09Z",16.25,1],["2021-08-16T16:00:10Z",41.2,11],["2021-08-16T16:00:11Z",21,12]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "SELECT ELAPSED(*) FROM db0.rp0.mst",
			command: `SELECT ELAPSED(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","elapsed_address","elapsed_age","elapsed_alive","elapsed_height"],"values":[["2021-08-16T16:00:01Z",1000000000,1000000000,1000000000,1000000000],["2021-08-16T16:00:02Z",1000000000,1000000000,1000000000,1000000000],["2021-08-16T16:00:03Z",1000000000,1000000000,1000000000,1000000000],["2021-08-16T16:00:04Z",1000000000,1000000000,1000000000,1000000000],["2021-08-16T16:00:05Z",1000000000,1000000000,null,1000000000],["2021-08-16T16:00:06Z",null,1000000000,2000000000,1000000000],["2021-08-16T16:00:07Z",2000000000,1000000000,1000000000,null],["2021-08-16T16:00:08Z",1000000000,null,1000000000,2000000000],["2021-08-16T16:00:09Z",1000000000,2000000000,1000000000,1000000000],["2021-08-16T16:00:10Z",1000000000,1000000000,1000000000,1000000000],["2021-08-16T16:00:11Z",1000000000,1000000000,1000000000,1000000000]]}]}]}`,
		},
		&Query{
			name:    "SELECT MOVING_AVERAGE(*, 2) FROM db0.rp0.mst",
			command: `SELECT MOVING_AVERAGE(* ,2) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","moving_average_age","moving_average_height"],"values":[["2021-08-16T16:00:01Z",16.4,75],["2021-08-16T16:00:02Z",11.949999999999998,85],["2021-08-16T16:00:03Z",16.699999999999996,105.5],["2021-08-16T16:00:04Z",32.5,129.5],["2021-08-16T16:00:05Z",41.9,143.5],["2021-08-16T16:00:06Z",50.75,151],["2021-08-16T16:00:07Z",40.5,null],["2021-08-16T16:00:08Z",null,166],["2021-08-16T16:00:09Z",44.55,179.5],["2021-08-16T16:00:10Z",81.4,185.5],["2021-08-16T16:00:11Z",112.5,197]]}]}]}`,
		},
		&Query{
			name:    "SELECT CUMULATIVE_SUM(*) FROM db0.rp0.mst",
			command: `SELECT CUMULATIVE_SUM(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","cumulative_sum_age","cumulative_sum_height"],"values":[["2021-08-16T16:00:00Z",12.3,70],["2021-08-16T16:00:01Z",32.8,150],["2021-08-16T16:00:02Z",36.199999999999996,240],["2021-08-16T16:00:03Z",66.19999999999999,361],["2021-08-16T16:00:04Z",101.19999999999999,499],["2021-08-16T16:00:05Z",150,648],["2021-08-16T16:00:06Z",202.7,801],["2021-08-16T16:00:07Z",231,null],["2021-08-16T16:00:08Z",null,980],["2021-08-16T16:00:09Z",291.8,1160],["2021-08-16T16:00:10Z",393.8,1351],["2021-08-16T16:00:11Z",516.8,1554]]}]}]}`,
		},
		&Query{
			name:    "SELECT INTEGRAL(*) FROM db0.rp0.mst",
			command: `SELECT INTEGRAL(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","integral_age","integral_height"],"values":[["1970-01-01T00:00:00Z",493.69999999999993,1583.5]]}]}]}`,
		},
		&Query{
			name:    "SELECT COUNT(TIME)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT COUNT(TIME) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",12]]}]}]}`,
		},
		&Query{
			name:    "SELECT /*+ Exact_Statistic_Query */ COUNT(TIME)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT /*+ Exact_Statistic_Query */ COUNT(TIME) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",12]]}]}]}`,
		},
		&Query{
			name:    "SELECT rate(*)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT rate(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","rate_age","rate_height"],"values":[["1970-01-01T00:00:00Z",10.063636363636364,12.090909090909092]]}]}]}`,
		},
		&Query{
			name:    "SELECT irate(*)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT irate(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","irate_age","irate_height"],"values":[["1970-01-01T00:00:00Z",21,12]]}]}]}`,
		},
		&Query{
			name:    "SELECT absent(*)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT absent(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","absent_address","absent_age","absent_alive","absent_height"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]}]}]}`,
		},
		&Query{
			name:    "SELECT spread(*)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT spread(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","spread_age","spread_height"],"values":[["1970-01-01T00:00:00Z",119.6,133]]}]}]}`,
		},
		&Query{
			name:    "SELECT stddev(*)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT stddev(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","stddev_age","stddev_height"],"values":[["1970-01-01T00:00:00Z",36.90143135927978,46.139117696572626]]}]}]}`,
		},
		&Query{
			name:    "SELECT median(*)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT median(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","median_age","median_height"],"values":[["1970-01-01T00:00:00Z",35,149]]}]}]}`,
		},
		&Query{
			name:    "SELECT mode(*)",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT mode(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","mode_address","mode_age","mode_alive","mode_height"],"values":[["1970-01-01T00:00:00Z","anhui",3.4,true,70]]}]}]}`,
		},
		&Query{
			name:    "SELECT count(*), group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT count(*) FROM db0.rp0.mst WHERE time >= 1629129600000000000 and time <= 1629129611000000000 group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","count_address","count_age","count_alive","count_height"],"values":[["2021-08-16T16:00:00Z",1,1,1,1],["2021-08-16T16:00:01Z",1,1,1,1],["2021-08-16T16:00:02Z",1,1,1,1],["2021-08-16T16:00:03Z",1,1,1,1],["2021-08-16T16:00:04Z",1,1,1,1],["2021-08-16T16:00:05Z",1,1,0,1],["2021-08-16T16:00:06Z",0,1,1,1],["2021-08-16T16:00:07Z",1,1,1,0],["2021-08-16T16:00:08Z",1,0,1,1],["2021-08-16T16:00:09Z",1,1,1,1],["2021-08-16T16:00:10Z",1,1,1,1],["2021-08-16T16:00:11Z",1,1,1,1]]}]}]}`,
		},
		&Query{
			name:    "SELECT last(*), group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT last(*) FROM db0.rp0.mst WHERE time >= 1629129600000000000 and time <= 1629129611000000000 group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","last_address","last_age","last_alive","last_height"],"values":[["2021-08-16T16:00:00Z","shenzhen",12.3,true,70],["2021-08-16T16:00:01Z","shanghai",20.5,false,80],["2021-08-16T16:00:02Z","beijin",3.4,true,90],["2021-08-16T16:00:03Z","guangzhou",30,false,121],["2021-08-16T16:00:04Z","chengdu",35,true,138],["2021-08-16T16:00:05Z","wuhan",48.8,null,149],["2021-08-16T16:00:06Z",null,52.7,true,153],["2021-08-16T16:00:07Z","anhui",28.3,false,null],["2021-08-16T16:00:08Z","xian",null,true,179],["2021-08-16T16:00:09Z","hangzhou",60.8,false,180],["2021-08-16T16:00:10Z","nanjin",102,true,191],["2021-08-16T16:00:11Z","zhengzhou",123,false,203]]}]}]}`,
		},
		&Query{
			name:    "SELECT /*+ specific_series */  *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT /*+ specific_series */  * FROM db0.rp0.mst WHERE country='china' and age=12.3 and "name"='azhu'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","address","age","alive","country","height","name"],"values":[["2021-08-16T16:00:00Z","shenzhen",12.3,true,"china",70,"azhu"]]}]}]}`,
		},
	}...)

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

func TestServer_Query_For_BugList(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,tag1=1 field1=1 1566786536000000000`),
		fmt.Sprintf(`cpu,tag1=2 field1=2 1566786537000000000`),
		fmt.Sprintf(`cpu,tag2=3 field1=3 1566786538000000000`),
		fmt.Sprintf(`cpu,tag2=4 field1=4 1566786539000000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// BUG2022010501886
		&Query{
			name:    "select * from db0.rp0.cpu",
			command: `SELECT * FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","field1","tag1","tag2"],"values":[["2019-08-26T02:28:56Z",1,"1",null],["2019-08-26T02:28:57Z",2,"2",null],["2019-08-26T02:28:58Z",3,null,"3"],["2019-08-26T02:28:59Z",4,null,"4"]]}]}]}`,
		},
		// BUG2022071101301
		&Query{
			name:    "select min(field1),field1 from db0.rp0.cpu",
			command: `SELECT MIN(field1),field1 FROM db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","field1"],"values":[["2019-08-26T02:28:56Z",1,1]]}]}]}`,
		},
		// BUG2022080600046
		&Query{
			name:    "SELECT TOP(field1, 2),* FROM db0.rp0.cpu LIMIT 2",
			command: `SELECT TOP(field1, 2),* FROM db0.rp0.cpu LIMIT 2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top","field1","tag1","tag2"],"values":[["2019-08-26T02:28:58Z",3,3,null,"3"],["2019-08-26T02:28:59Z",4,4,null,"4"]]}]}]}`,
		},
		// BUG2022120901869
		&Query{
			name:    "SELECT multi-agg(field1) FROM db0.rp0.cpu LIMIT 1",
			command: `select MOVING_AVERAGE(field2,2) as f8, NON_NEGATIVE_DERIVATIVE(field2) as f1, NON_NEGATIVE_DIFFERENCE(field2) as f2, CUMULATIVE_SUM(field2) as f3, CUMULATIVE_SUM(field2) as f4, DERIVATIVE(field2) as f5, DIFFERENCE(field2) as f6, ELAPSED(field2) as f7, MOVING_AVERAGE(field1,2) as ax, INTEGRAL(field2) as f9 from db0.rp0.cpu limit 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","f8","f1","f2","f3","f4","f5","f6","f7","ax","f9"],"values":[["2019-08-26T02:28:57Z",null,null,null,null,null,null,null,null,1.5,null]]}]}]}`,
		},
		&Query{
			name:    "SELECT multi-agg(*) FROM db0.rp0.cpu LIMIT 1",
			command: `select ax, ay from ( select ax , f3 from (select  NON_NEGATIVE_DERIVATIVE(field2) as f1, NON_NEGATIVE_DIFFERENCE(field2) as f2, CUMULATIVE_SUM(field2) as f3, CUMULATIVE_SUM(field2) as f4, DERIVATIVE(field2) as f5, DIFFERENCE(field2) as f6, ELAPSED(field2) as f7, MOVING_AVERAGE(field1,2) as ax, MOVING_AVERAGE(field2,2) as f8, INTEGRAL(field2) as f9 from db0.rp0.cpu limit 1))`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","ax","ay"],"values":[["2019-08-26T02:28:57Z",1.5,null]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Blank_Row(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,t=di f1=1,f2=3i 1579415564528082073`),
		fmt.Sprintf(`mst,t=di1 f1=-2,f2=-1i 1579415565558082073`),
		fmt.Sprintf(`mst,t=di f1=3 1579415566558082073`),
		fmt.Sprintf(`mst,t=di f1=2,f2=4i 1579415567558882073`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select non_negative_difference(*) from mst",
			command: `select non_negative_difference(*) from db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","non_negative_difference_f1","non_negative_difference_f2"],"values":[["2020-01-19T06:32:46.558082073Z",5,null],["2020-01-19T06:32:47.558882073Z",null,5]]}]}]}`,
		},
		&Query{
			name:    "non_negative_derivative(*) from mst",
			command: `select non_negative_derivative(*) from db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","non_negative_derivative_f1","non_negative_derivative_f2"],"values":[["2020-01-19T06:32:46.558082073Z",5,null],["2020-01-19T06:32:47.558882073Z",null,2.4990003998400643]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Fill_Bug_List(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,tag1=1 count=0 0`),
		fmt.Sprintf(`mst,tag1=2 count=4 4000000000`),
		fmt.Sprintf(`mst,tag1=3 count=2 2000000000`),
		fmt.Sprintf(`mst,tag1=4 count=0 0`),
		fmt.Sprintf(`mst,tag1=4 count=1 1000000000`),
		fmt.Sprintf(`mst,tag1=5 count=3 3000000000`),
		fmt.Sprintf(`mst,tag1=5 count=4 4000000000`),
		fmt.Sprintf(`mst,tag1=6 count=0 0`),
		fmt.Sprintf(`mst,tag1=6 count=4 4000000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select sum(count) from db0.rp0.mst group by time(1s),tag1",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(count) from db0.rp0.mst where time >= 0 and time < 5000000000 group by time(1s),tag1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"tag1":"1"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:04Z",null]]},{"name":"mst","tags":{"tag1":"2"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",null],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:04Z",4]]},{"name":"mst","tags":{"tag1":"3"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",null],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:02Z",2],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:04Z",null]]},{"name":"mst","tags":{"tag1":"4"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T00:00:01Z",1],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:04Z",null]]},{"name":"mst","tags":{"tag1":"5"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",null],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:03Z",3],["1970-01-01T00:00:04Z",4]]},{"name":"mst","tags":{"tag1":"6"},"columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:04Z",4]]}]}]}`,
		},
		&Query{
			name:    "select sum(count) from db0.rp0.mst group by time(1s),tag1 order by time desc",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(count) from db0.rp0.mst where time >= 0 and time < 5000000000 group by time(1s),tag1 order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"tag1":"6"},"columns":["time","sum"],"values":[["1970-01-01T00:00:04Z",4],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:00Z",0]]},{"name":"mst","tags":{"tag1":"5"},"columns":["time","sum"],"values":[["1970-01-01T00:00:04Z",4],["1970-01-01T00:00:03Z",3],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:00Z",null]]},{"name":"mst","tags":{"tag1":"4"},"columns":["time","sum"],"values":[["1970-01-01T00:00:04Z",null],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:01Z",1],["1970-01-01T00:00:00Z",0]]},{"name":"mst","tags":{"tag1":"3"},"columns":["time","sum"],"values":[["1970-01-01T00:00:04Z",null],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:02Z",2],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:00Z",null]]},{"name":"mst","tags":{"tag1":"2"},"columns":["time","sum"],"values":[["1970-01-01T00:00:04Z",4],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:00Z",null]]},{"name":"mst","tags":{"tag1":"1"},"columns":["time","sum"],"values":[["1970-01-01T00:00:04Z",null],["1970-01-01T00:00:03Z",null],["1970-01-01T00:00:02Z",null],["1970-01-01T00:00:01Z",null],["1970-01-01T00:00:00Z",0]]}]}]}`,
		},
	}...)

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

func TestServer_SubQuery_Top_Min(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=azhu age=1,height=11i 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=2,height=12i 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang age=3,height=13i 1629129602000000000`),
		fmt.Sprintf(`mst,country=china,name=azhu age=4,height=24i 1629129603000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=5,height=25i 1629129604000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang age=6,height=26i 1629129605000000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// BUG: BUG2022040700537
		&Query{
			name:    "min-top",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT min(value) FROM (SELECT top(age, 2), age - height AS value FROM db0.rp0.mst) GROUP BY country`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":"american"},"columns":["time","min"],"values":[["2021-08-16T16:00:04Z",-20]]},{"name":"mst","tags":{"country":"china"},"columns":["time","min"],"values":[["2021-08-16T16:00:03Z",-20]]},{"name":"mst","tags":{"country":"germany"},"columns":["time","min"],"values":[["2021-08-16T16:00:05Z",-20]]}]}]}`,
		},
	}...)

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

func TestServer_difference_derivative_time_duplicate(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=azhu age=1,height=11i 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=2,height=12i 1629129600000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang age=3,height=13i 1629129602000000000`),
		fmt.Sprintf(`mst,country=china,name=azhu age=4,height=24i 1629129602000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=5,height=25i 1629129604000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang age=6,height=26i 1629129604000000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// BUG2022071301614
		&Query{
			name:    "difference",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT difference(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","difference_age","difference_height"],"values":[["2021-08-16T16:00:02Z",2,2],["2021-08-16T16:00:04Z",2,12]]}]}]}`,
		},
		// BUG2022071301614
		&Query{
			name:    "derivative",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `SELECT derivative(*) FROM db0.rp0.mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","derivative_age","derivative_height"],"values":[["2021-08-16T16:00:02Z",1,1],["2021-08-16T16:00:04Z",1,6]]}]}]}`,
		},
	}...)

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

func TestServer_top_bottom_nul_column(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=azhu height=11i 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=2,height=12i 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang height=13i 1629129602000000000`),
		fmt.Sprintf(`mst,country=china,name=azhu age=4,height=24i 1629129603000000000`),
		fmt.Sprintf(`mst,country=american,name=alan age=5,height=25i 1629129604000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang height=26i 1629129605000000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "top",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select top(sum_age, 3) from (select sum(age) as sum_age, sum(height) as sum_height from db0.rp0.mst where time >= 1629129600000000000 and time <= 1629129605000000000 group by time(1s)) where time >= 1629129600000000000 and time <= 1629129605000000000`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","top"],"values":[["2021-08-16T16:00:01Z",2],["2021-08-16T16:00:03Z",4],["2021-08-16T16:00:04Z",5]]}]}]}`,
		},
		&Query{
			name:    "bottom",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select bottom(sum_age, 3) from (select sum(age) as sum_age, sum(height) as sum_height from db0.rp0.mst where time >= 1629129600000000000 and time <= 1629129605000000000 group by time(1s)) where time >= 1629129600000000000 and time <= 1629129605000000000`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","bottom"],"values":[["2021-08-16T16:00:01Z",2],["2021-08-16T16:00:03Z",4],["2021-08-16T16:00:04Z",5]]}]}]}`,
		},
	}...)

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

// TODO:partial Compare
func TestServer_Query_Complex_Aggregate(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	// set infinite retention policy as we are inserting data in the past and don't want retention policy enforcement to make this test racy
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")

	writes := []string{}
	for i := 0; i < 10; i++ {
		for j := 0; j < 2048; j++ {
			data := fmt.Sprintf(`cpu,region=region_%d,az=az_%d v1=%di,v2=%f,v3=%t,v4="%s" %d`,
				i, i, i*2048+j, generateFloat(i*2048+j), generateBool(i*2048+j), generateString(i*2048+j), time.Unix(int64(i*2048+j), int64(0)).UTC().UnixNano())
			writes = append(writes, data)
		}
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// 1. multi-column *
		&Query{
			name:    "count(time)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select count(time) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",20480]]}]}]}`,
		},
		&Query{
			name:    "exact count(time)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select /*+ Exact_Statistic_Query */ count(time) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",20480]]}]}]}`,
		},
		&Query{
			name:    "count(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select count(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",20480,20480,20480,20480]]}]}]}`,
		},
		&Query{
			name:    "mean(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select mean(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",10239.5,10239.5]]}]}]}`,
		},
		&Query{
			name:    "sum(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select sum(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",209704960,209704960]]}]}]}`,
		},
		&Query{
			name:    "min(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select min(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",0,0,false]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "max(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select max(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",20479,20479,true]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "first(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select first(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"]]}]}]}`,
		},
		&Query{
			name:    "last(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select last(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "spread(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select spread(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",20479,20479]]}]}]}`,
		},
		&Query{
			name:    "percentile(*, 50)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select percentile(*, 50) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",10239,10239]]}]}]}`,
		},
		&Query{
			name:    "median(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select median(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",10239.5,10239.5]]}]}]}`,
		},
		&Query{
			name:    "mode(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select mode(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"]]}]}]}`,
		},
		&Query{
			name:    "rate(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select rate(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]}]}]}`,
		},
		&Query{
			name:    "irate(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select irate(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]}]}]}`,
		},
		&Query{
			name:    "stddev(*)",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select stddev(*) from db0.rp0.cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",5912.211092307175,5912.211092307175]]}]}]}`,
		},

		// 2. multi-column
		&Query{
			name:    "count(time) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(time) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",3600],["1970-01-01T01:00:00Z",3600],["1970-01-01T02:00:00Z",3600],["1970-01-01T03:00:00Z",3600],["1970-01-01T04:00:00Z",3600],["1970-01-01T05:00:00Z",2480]]}]}]}`,
		},
		&Query{
			name:    "count(time) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(time) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048]]}]}]}`,
		},
		&Query{
			name:    "count(time) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(time) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",2048],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",1552],["1970-01-01T01:00:00Z",496],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",2048],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",1056],["1970-01-01T02:00:00Z",992],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",2048],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",560],["1970-01-01T03:00:00Z",1488],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",2048],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",64],["1970-01-01T04:00:00Z",1984],["1970-01-01T05:00:00Z",0]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",1616],["1970-01-01T05:00:00Z",432]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","count"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T01:00:00Z",0],["1970-01-01T02:00:00Z",0],["1970-01-01T03:00:00Z",0],["1970-01-01T04:00:00Z",0],["1970-01-01T05:00:00Z",2048]]}]}]}`,
		},
		&Query{
			name:    "count(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",3600,3600,3600,3600],["1970-01-01T01:00:00Z",3600,3600,3600,3600],["1970-01-01T02:00:00Z",3600,3600,3600,3600],["1970-01-01T03:00:00Z",3600,3600,3600,3600],["1970-01-01T04:00:00Z",3600,3600,3600,3600],["1970-01-01T05:00:00Z",2480,2480,2480,2480]]}]}]}`,
		},
		&Query{
			name:    "count(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048]]}]}]}`,
		},
		&Query{
			name:    "count(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select count(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,2048,2048],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",1552,1552,1552,1552],["1970-01-01T01:00:00Z",496,496,496,496],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",2048,2048,2048,2048],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",1056,1056,1056,1056],["1970-01-01T02:00:00Z",992,992,992,992],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",2048,2048,2048,2048],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",560,560,560,560],["1970-01-01T03:00:00Z",1488,1488,1488,1488],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",2048,2048,2048,2048],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",64,64,64,64],["1970-01-01T04:00:00Z",1984,1984,1984,1984],["1970-01-01T05:00:00Z",0,0,0,0]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",1616,1616,1616,1616],["1970-01-01T05:00:00Z",432,432,432,432]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","count_v1","count_v2","count_v3","count_v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,0],["1970-01-01T01:00:00Z",0,0,0,0],["1970-01-01T02:00:00Z",0,0,0,0],["1970-01-01T03:00:00Z",0,0,0,0],["1970-01-01T04:00:00Z",0,0,0,0],["1970-01-01T05:00:00Z",2048,2048,2048,2048]]}]}]}`,
		},

		&Query{
			name:    "mean(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",1799.5,1799.5],["1970-01-01T01:00:00Z",5399.5,5399.5],["1970-01-01T02:00:00Z",8999.5,8999.5],["1970-01-01T03:00:00Z",12599.5,12599.5],["1970-01-01T04:00:00Z",16199.5,16199.5],["1970-01-01T05:00:00Z",19239.5,19239.5]]}]}]}`,
		},
		&Query{
			name:    "mean(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",1023.5,1023.5]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",3071.5,3071.5]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",5119.5,5119.5]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",7167.5,7167.5]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",9215.5,9215.5]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",11263.5,11263.5]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",13311.5,13311.5]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",15359.5,15359.5]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",17407.5,17407.5]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",19455.5,19455.5]]}]}]}`,
		},
		&Query{
			name:    "mean(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mean(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",1023.5,1023.5],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",2823.5,2823.5],["1970-01-01T01:00:00Z",3847.5,3847.5],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",5119.5,5119.5],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",6671.5,6671.5],["1970-01-01T02:00:00Z",7695.5,7695.5],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",9215.5,9215.5],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",10519.5,10519.5],["1970-01-01T03:00:00Z",11543.5,11543.5],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",13311.5,13311.5],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",14367.5,14367.5],["1970-01-01T04:00:00Z",15391.5,15391.5],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",17191.5,17191.5],["1970-01-01T05:00:00Z",18215.5,18215.5]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","mean_v1","mean_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",19455.5,19455.5]]}]}]}`,
		},

		&Query{
			name:    "sum(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",6478200,6478200],["1970-01-01T01:00:00Z",19438200,19438200],["1970-01-01T02:00:00Z",32398200,32398200],["1970-01-01T03:00:00Z",45358200,45358200],["1970-01-01T04:00:00Z",58318200,58318200],["1970-01-01T05:00:00Z",47713960,47713960]]}]}]}`,
		},
		&Query{
			name:    "sum(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",2096128,2096128]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",6290432,6290432]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",10484736,10484736]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",14679040,14679040]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",18873344,18873344]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",23067648,23067648]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",27261952,27261952]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",31456256,31456256]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",35650560,35650560]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",39844864,39844864]]}]}]}`,
		},
		&Query{
			name:    "sum(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select sum(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",2096128,2096128],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",4382072,4382072],["1970-01-01T01:00:00Z",1908360,1908360],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",10484736,10484736],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",7045104,7045104],["1970-01-01T02:00:00Z",7633936,7633936],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",18873344,18873344],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",5890920,5890920],["1970-01-01T03:00:00Z",17176728,17176728],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",27261952,27261952],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",919520,919520],["1970-01-01T04:00:00Z",30536736,30536736],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",27781464,27781464],["1970-01-01T05:00:00Z",7869096,7869096]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","sum_v1","sum_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",39844864,39844864]]}]}]}`,
		},

		&Query{
			name:    "min(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",0,0,false],["1970-01-01T01:00:00Z",3600,3600,false],["1970-01-01T02:00:00Z",7200,7200,false],["1970-01-01T03:00:00Z",10800,10800,false],["1970-01-01T04:00:00Z",14400,14400,false],["1970-01-01T05:00:00Z",18000,18000,false]]}]}]}`,
		},
		&Query{
			name:    "min(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",0,0,false]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",2048,2048,false]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",4096,4096,false]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",6144,6144,false]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",8192,8192,false]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",10240,10240,false]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",12288,12288,false]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",14336,14336,false]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",16384,16384,false]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",18432,18432,false]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "min(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",0,0,false],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",2048,2048,false],["1970-01-01T01:00:00Z",3600,3600,false],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",4096,4096,false],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",6144,6144,false],["1970-01-01T02:00:00Z",7200,7200,false],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",8192,8192,false],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",10240,10240,false],["1970-01-01T03:00:00Z",10800,10800,false],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",12288,12288,false],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",14336,14336,false],["1970-01-01T04:00:00Z",14400,14400,false],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",16384,16384,false],["1970-01-01T05:00:00Z",18000,18000,false]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min_v1","min_v2","min_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",18432,18432,false]]}]}]}`,
		},

		&Query{
			name:    "max(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",3599,3599,true],["1970-01-01T01:00:00Z",7199,7199,true],["1970-01-01T02:00:00Z",10799,10799,true],["1970-01-01T03:00:00Z",14399,14399,true],["1970-01-01T04:00:00Z",17999,17999,true],["1970-01-01T05:00:00Z",20479,20479,true]]}]}]}`,
		},
		&Query{
			name:    "max(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",2047,2047,true]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",4095,4095,true]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",6143,6143,true]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",8191,8191,true]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",10239,10239,true]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",12287,12287,true]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",14335,14335,true]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",16383,16383,true]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",18431,18431,true]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",20479,20479,true]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "max(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",2047,2047,true],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",3599,3599,true],["1970-01-01T01:00:00Z",4095,4095,true],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",6143,6143,true],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",7199,7199,true],["1970-01-01T02:00:00Z",8191,8191,true],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",10239,10239,true],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",10799,10799,true],["1970-01-01T03:00:00Z",12287,12287,true],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",14335,14335,true],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",14399,14399,true],["1970-01-01T04:00:00Z",16383,16383,true],["1970-01-01T05:00:00Z",null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",17999,17999,true],["1970-01-01T05:00:00Z",18431,18431,true]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","max_v1","max_v2","max_v3"],"values":[["1970-01-01T00:00:00Z",null,null,null],["1970-01-01T01:00:00Z",null,null,null],["1970-01-01T02:00:00Z",null,null,null],["1970-01-01T03:00:00Z",null,null,null],["1970-01-01T04:00:00Z",null,null,null],["1970-01-01T05:00:00Z",20479,20479,true]]}]}]}`,
		},
		&Query{
			name:    "first(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select first(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"],["1970-01-01T01:00:00Z",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",18000,18000,true,"abc18000"]]}]}]}`,
		},
		&Query{
			name:    "first(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select first(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,true,"abc2048"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",4096,4096,true,"abc4096"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",6144,6144,true,"abc6144"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",8192,8192,true,"abc8192"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",10240,10240,true,"abc10240"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",12288,12288,true,"abc12288"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",14336,14336,true,"abc14336"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",16384,16384,true,"abc16384"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",18432,18432,true,"abc18432"]]}]}]}`,
		},
		&Query{
			name:    "first(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select first(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,true,"abc2048"],["1970-01-01T01:00:00Z",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",4096,4096,true,"abc4096"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",6144,6144,true,"abc6144"],["1970-01-01T02:00:00Z",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",8192,8192,true,"abc8192"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",10240,10240,true,"abc10240"],["1970-01-01T03:00:00Z",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",12288,12288,true,"abc12288"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",14336,14336,true,"abc14336"],["1970-01-01T04:00:00Z",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",16384,16384,true,"abc16384"],["1970-01-01T05:00:00Z",18000,18000,true,"abc18000"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","first_v1","first_v2","first_v3","first_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",18432,18432,true,"abc18432"]]}]}]}`,
		},

		&Query{
			name:    "last(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select last(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",3599,3599,false,"abc3599"],["1970-01-01T01:00:00Z",7199,7199,false,"abc7199"],["1970-01-01T02:00:00Z",10799,10799,false,"abc10799"],["1970-01-01T03:00:00Z",14399,14399,false,"abc14399"],["1970-01-01T04:00:00Z",17999,17999,false,"abc17999"],["1970-01-01T05:00:00Z",20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "last(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select last(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",2047,2047,false,"abc2047"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",4095,4095,false,"abc4095"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",6143,6143,false,"abc6143"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",8191,8191,false,"abc8191"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",10239,10239,false,"abc10239"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",12287,12287,false,"abc12287"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",14335,14335,false,"abc14335"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",16383,16383,false,"abc16383"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "last(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select last(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",2047,2047,false,"abc2047"],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",3599,3599,false,"abc3599"],["1970-01-01T01:00:00Z",4095,4095,false,"abc4095"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",6143,6143,false,"abc6143"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",7199,7199,false,"abc7199"],["1970-01-01T02:00:00Z",8191,8191,false,"abc8191"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",10239,10239,false,"abc10239"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",10799,10799,false,"abc10799"],["1970-01-01T03:00:00Z",12287,12287,false,"abc12287"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",14335,14335,false,"abc14335"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",14399,14399,false,"abc14399"],["1970-01-01T04:00:00Z",16383,16383,false,"abc16383"],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",17999,17999,false,"abc17999"],["1970-01-01T05:00:00Z",18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","last_v1","last_v2","last_v3","last_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "spread(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select spread(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",3599,3599],["1970-01-01T01:00:00Z",3599,3599],["1970-01-01T02:00:00Z",3599,3599],["1970-01-01T03:00:00Z",3599,3599],["1970-01-01T04:00:00Z",3599,3599],["1970-01-01T05:00:00Z",2479,2479]]}]}]}`,
		},
		&Query{
			name:    "spread(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select spread(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047]]}]}]}`,
		},
		&Query{
			name:    "spread(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select spread(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",2047,2047],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",1551,1551],["1970-01-01T01:00:00Z",495,495],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",2047,2047],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",1055,1055],["1970-01-01T02:00:00Z",991,991],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",2047,2047],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",559,559],["1970-01-01T03:00:00Z",1487,1487],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",2047,2047],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",63,63],["1970-01-01T04:00:00Z",1983,1983],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",1615,1615],["1970-01-01T05:00:00Z",431,431]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","spread_v1","spread_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",2047,2047]]}]}]}`,
		},
		&Query{
			name:    "percentile(*, 10) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select percentile(*, 10) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",359,359],["1970-01-01T01:00:00Z",3959,3959],["1970-01-01T02:00:00Z",7559,7559],["1970-01-01T03:00:00Z",11159,11159],["1970-01-01T04:00:00Z",14759,14759],["1970-01-01T05:00:00Z",18247,18247]]}]}]}`,
		},
		&Query{
			name:    "percentile(*, 50) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select percentile(*, 50) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",1023,1023]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",3071,3071]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",5119,5119]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",7167,7167]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",9215,9215]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",11263,11263]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",13311,13311]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",15359,15359]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",17407,17407]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",19455,19455]]}]}]}`,
		},
		&Query{
			name:    "percentile(*, 90) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select percentile(*, 90) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",1842,1842],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",3444,3444],["1970-01-01T01:00:00Z",4045,4045],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",5938,5938],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",7093,7093],["1970-01-01T02:00:00Z",8092,8092],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",10034,10034],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",10743,10743],["1970-01-01T03:00:00Z",12138,12138],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",14130,14130],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",14393,14393],["1970-01-01T04:00:00Z",16185,16185],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",17837,17837],["1970-01-01T05:00:00Z",18388,18388]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",20274,20274]]}]}]}`,
		},
		&Query{
			name:    "median(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select median(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",1799.5,1799.5],["1970-01-01T01:00:00Z",5399.5,5399.5],["1970-01-01T02:00:00Z",8999.5,8999.5],["1970-01-01T03:00:00Z",12599.5,12599.5],["1970-01-01T04:00:00Z",16199.5,16199.5],["1970-01-01T05:00:00Z",19239.5,19239.5]]}]}]}`,
		},
		&Query{
			name:    "median(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select median(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",1023.5,1023.5]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",3071.5,3071.5]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",5119.5,5119.5]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",7167.5,7167.5]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",9215.5,9215.5]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",11263.5,11263.5]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",13311.5,13311.5]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",15359.5,15359.5]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",17407.5,17407.5]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",19455.5,19455.5]]}]}]}`,
		},
		&Query{
			name:    "median(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select median(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",1023.5,1023.5],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",2823.5,2823.5],["1970-01-01T01:00:00Z",3847.5,3847.5],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",5119.5,5119.5],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",6671.5,6671.5],["1970-01-01T02:00:00Z",7695.5,7695.5],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",9215.5,9215.5],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",10519.5,10519.5],["1970-01-01T03:00:00Z",11543.5,11543.5],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",13311.5,13311.5],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",14367.5,14367.5],["1970-01-01T04:00:00Z",15391.5,15391.5],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",17191.5,17191.5],["1970-01-01T05:00:00Z",18215.5,18215.5]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","median_v1","median_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",19455.5,19455.5]]}]}]}`,
		},
		&Query{
			name:    "mode(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mode(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"],["1970-01-01T01:00:00Z",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",7200,7200,true,"abc10000"],["1970-01-01T03:00:00Z",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",18000,18000,true,"abc18000"]]}]}]}`,
		},
		&Query{
			name:    "mode(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mode(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,true,"abc2048"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",4096,4096,true,"abc4096"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",6144,6144,true,"abc6144"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",8192,8192,true,"abc10000"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",10240,10240,true,"abc10240"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",12288,12288,true,"abc12288"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",14336,14336,true,"abc14336"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",16384,16384,true,"abc16384"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",18432,18432,true,"abc18432"]]}]}]}`,
		},
		&Query{
			name:    "mode(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select mode(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",0,0,true,"abc0"],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",2048,2048,true,"abc2048"],["1970-01-01T01:00:00Z",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",4096,4096,true,"abc4096"],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",6144,6144,true,"abc6144"],["1970-01-01T02:00:00Z",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",8192,8192,true,"abc10000"],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",10240,10240,true,"abc10240"],["1970-01-01T03:00:00Z",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",12288,12288,true,"abc12288"],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",14336,14336,true,"abc14336"],["1970-01-01T04:00:00Z",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",16384,16384,true,"abc16384"],["1970-01-01T05:00:00Z",18000,18000,true,"abc18000"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","mode_v1","mode_v2","mode_v3","mode_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",18432,18432,true,"abc18432"]]}]}]}`,
		},
		&Query{
			name:    "rate(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select rate(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",3600,3600],["1970-01-01T01:00:00Z",3600,3600],["1970-01-01T02:00:00Z",3600,3600],["1970-01-01T03:00:00Z",3600,3600],["1970-01-01T04:00:00Z",3600,3600],["1970-01-01T05:00:00Z",3600.0000000000005,3600.0000000000005]]}]}]}`,
		},
		&Query{
			name:    "rate(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select rate(*) from db0.rp0.cpu group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]}]}]}`,
		},
		&Query{
			name:    "rate(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select rate(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h), *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",3600.0000000000005,3600.0000000000005],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",3600,3600],["1970-01-01T01:00:00Z",3599.9999999999995,3599.9999999999995],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",3600.0000000000005,3600.0000000000005],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",3600,3600],["1970-01-01T02:00:00Z",3600,3600],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",3600.0000000000005,3600.0000000000005],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",3600.0000000000005,3600.0000000000005],["1970-01-01T03:00:00Z",3600,3600],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",3600.0000000000005,3600.0000000000005],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",3599.9999999999995,3599.9999999999995],["1970-01-01T04:00:00Z",3600.0000000000005,3600.0000000000005],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",3600,3600],["1970-01-01T05:00:00Z",3600,3600]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","rate_v1","rate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",3600.0000000000005,3600.0000000000005]]}]}]}`,
		},
		&Query{
			name:    "irate(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select irate(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",3600,3600],["1970-01-01T01:00:00Z",3600,3600],["1970-01-01T02:00:00Z",3600,3600],["1970-01-01T03:00:00Z",3600,3600],["1970-01-01T04:00:00Z",3600,3600],["1970-01-01T05:00:00Z",3600,3600]]}]}]}`,
		},
		&Query{
			name:    "irate(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select irate(*) from db0.rp0.cpu group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",1,1]]}]}]}`,
		},
		&Query{
			name:    "irate(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select irate(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h), *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",3600,3600],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",3600,3600],["1970-01-01T01:00:00Z",3600,3600],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",3600,3600],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",3600,3600],["1970-01-01T02:00:00Z",3600,3600],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",3600,3600],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",3600,3600],["1970-01-01T03:00:00Z",3600,3600],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",3600,3600],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",3600,3600],["1970-01-01T04:00:00Z",3600,3600],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",3600,3600],["1970-01-01T05:00:00Z",3600,3600]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","irate_v1","irate_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",3600,3600]]}]}]}`,
		},
		&Query{
			name:    "absent(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select absent(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1],["1970-01-01T01:00:00Z",1,1,1,1],["1970-01-01T02:00:00Z",1,1,1,1],["1970-01-01T03:00:00Z",1,1,1,1],["1970-01-01T04:00:00Z",1,1,1,1],["1970-01-01T05:00:00Z",1,1,1,1]]}]}]}`,
		},
		&Query{
			name:    "absent(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select absent(*) from db0.rp0.cpu group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1]]}]}]}`,
		},
		&Query{
			name:    "absent(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"2"}},
			command: `select absent(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h), *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",1,1,1,1],["1970-01-01T01:00:00Z",1,1,1,1],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",1,1,1,1],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",1,1,1,1],["1970-01-01T02:00:00Z",1,1,1,1],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",1,1,1,1],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",1,1,1,1],["1970-01-01T03:00:00Z",1,1,1,1],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",1,1,1,1],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",1,1,1,1],["1970-01-01T04:00:00Z",1,1,1,1],["1970-01-01T05:00:00Z",null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",1,1,1,1],["1970-01-01T05:00:00Z",1,1,1,1]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","absent_v1","absent_v2","absent_v3","absent_v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null],["1970-01-01T05:00:00Z",1,1,1,1]]}]}]}`,
		},
		&Query{
			name:    "stddev(*) group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select stddev(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",1039.3748120865737,1039.3748120865737],["1970-01-01T01:00:00Z",1039.3748120865741,1039.3748120865741],["1970-01-01T02:00:00Z",1039.374812086576,1039.374812086576],["1970-01-01T03:00:00Z",1039.3748120865687,1039.3748120865687],["1970-01-01T04:00:00Z",1039.3748120865687,1039.3748120865687],["1970-01-01T05:00:00Z",716.0586568152081,716.0586568152081]]}]}]}`,
		},
		&Query{
			name:    "stddev(*) group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select stddev(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]}]}]}`,
		},
		&Query{
			name:    "stddev(*) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select stddev(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",448.16812321568193,448.16812321568193],["1970-01-01T01:00:00Z",143.32713164877984,143.32713164877984],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",591.3509956024425,591.3509956024425],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",304.98524554475995,304.98524554475995],["1970-01-01T02:00:00Z",286.5100347282796,286.5100347282796],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",591.3509956024425,591.3509956024425],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",161.80234856144702,161.80234856144702],["1970-01-01T03:00:00Z",429.69291360226663,429.69291360226663],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",591.3509956024425,591.3509956024425],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",18.618986725025255,18.618986725025255],["1970-01-01T04:00:00Z",572.8757864202909,572.8757864202909],["1970-01-01T05:00:00Z",null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",466.64333274997085,466.64333274997085],["1970-01-01T05:00:00Z",124.85191228018863,124.85191228018863]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",null,null],["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null],["1970-01-01T04:00:00Z",null,null],["1970-01-01T05:00:00Z",591.3509956024425,591.3509956024425]]}]}]}`,
		},
		// 3. single column + aux + group by
		// TODO: The order of tag aux of TSDB2.0 is different TSDB1.0.
		&Query{
			name:    "min(v1),* group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(v1),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","az","region","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",0,"az_0","region_0",0,0,true,"abc0"],["1970-01-01T01:00:00Z",3600,"az_1","region_1",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",7200,"az_3","region_3",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",10800,"az_5","region_5",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",14400,"az_7","region_7",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",18000,"az_8","region_8",18000,18000,true,"abc18000"]]}]}]}`,
		},
		&Query{
			name:    "min(v2),* group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(v2),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,true,"abc0"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:34:08Z",2048,2048,2048,true,"abc2048"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T01:08:16Z",4096,4096,4096,true,"abc4096"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T01:42:24Z",6144,6144,6144,true,"abc6144"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T02:16:32Z",8192,8192,8192,true,"abc8192"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T02:50:40Z",10240,10240,10240,true,"abc10240"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T03:24:48Z",12288,12288,12288,true,"abc12288"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:56Z",14336,14336,14336,true,"abc14336"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T04:33:04Z",16384,16384,16384,true,"abc16384"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T05:07:12Z",18432,18432,18432,true,"abc18432"]]}]}]}`,
		},
		&Query{
			name:    "min(v3),* group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select min(v3),*  from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",false,1,1,false,"abc1"],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",false,2049,2049,false,"abc2049"],["1970-01-01T01:00:00Z",false,3601,3601,false,"abc3601"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",false,4097,4097,false,"abc4097"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",false,6145,6145,false,"abc6145"],["1970-01-01T02:00:00Z",false,7201,7201,false,"abc7201"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",false,8193,8193,false,"abc8193"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",false,10241,10241,false,"abc10241"],["1970-01-01T03:00:00Z",false,10801,10801,false,"abc10801"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",false,12289,12289,false,"abc12289"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",false,14337,14337,false,"abc14337"],["1970-01-01T04:00:00Z",false,14401,14401,false,"abc14401"],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",false,16385,16385,false,"abc16385"],["1970-01-01T05:00:00Z",false,18001,18001,false,"abc18001"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",false,18433,18433,false,"abc18433"]]}]}]}`,
			skip:    true,
		},

		&Query{
			name:    "max(v1),* group by time",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(v1),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max","az","region","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",3599,"az_1","region_1",3599,3599,false,"abc3599"],["1970-01-01T01:00:00Z",7199,"az_3","region_3",7199,7199,false,"abc7199"],["1970-01-01T02:00:00Z",10799,"az_5","region_5",10799,10799,false,"abc10799"],["1970-01-01T03:00:00Z",14399,"az_7","region_7",14399,14399,false,"abc14399"],["1970-01-01T04:00:00Z",17999,"az_8","region_8",17999,17999,false,"abc17999"],["1970-01-01T05:00:00Z",20479,"az_9","region_9",20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "max(v2),* group by *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(v2),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:34:07Z",2047,2047,2047,false,"abc2047"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T01:08:15Z",4095,4095,4095,false,"abc4095"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T01:42:23Z",6143,6143,6143,false,"abc6143"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T02:16:31Z",8191,8191,8191,false,"abc8191"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T02:50:39Z",10239,10239,10239,false,"abc10239"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T03:24:47Z",12287,12287,12287,false,"abc12287"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:55Z",14335,14335,14335,false,"abc14335"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T04:33:03Z",16383,16383,16383,false,"abc16383"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T05:07:11Z",18431,18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T05:41:19Z",20479,20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "max(v3),* group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"1"}},
			command: `select max(v3),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",true,0,0,true,"abc0"],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",true,2048,2048,true,"abc2048"],["1970-01-01T01:00:00Z",true,3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",true,4096,4096,true,"abc4096"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",true,6144,6144,true,"abc6144"],["1970-01-01T02:00:00Z",true,7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",true,8192,8192,true,"abc8192"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",true,10240,10240,true,"abc10240"],["1970-01-01T03:00:00Z",true,10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",true,12288,12288,true,"abc12288"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",true,14336,14336,true,"abc14336"],["1970-01-01T04:00:00Z",true,14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",true,16384,16384,true,"abc16384"],["1970-01-01T05:00:00Z",true,18000,18000,true,"abc18000"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","max","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",true,18432,18432,true,"abc18432"]]}]}]}`,
			skip:    true,
		},

		&Query{
			name:    "first(v2),* group by time",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select first(v2),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","first","az","region","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",0,"az_0","region_0",0,0,true,"abc0"],["1970-01-01T01:00:00Z",3600,"az_1","region_1",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",7200,"az_3","region_3",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",10800,"az_5","region_5",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",14400,"az_7","region_7",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",18000,"az_8","region_8",18000,18000,true,"abc18000"]]}]}]}`,
		},
		&Query{
			name:    "first(v3),* group by *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select first(v3),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",true,0,0,true,"abc0"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:34:08Z",true,2048,2048,true,"abc2048"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T01:08:16Z",true,4096,4096,true,"abc4096"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T01:42:24Z",true,6144,6144,true,"abc6144"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T02:16:32Z",true,8192,8192,true,"abc8192"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T02:50:40Z",true,10240,10240,true,"abc10240"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T03:24:48Z",true,12288,12288,true,"abc12288"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:56Z",true,14336,14336,true,"abc14336"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T04:33:04Z",true,16384,16384,true,"abc16384"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T05:07:12Z",true,18432,18432,true,"abc18432"]]}]}]}`,
		},
		&Query{
			name:    "first(v4),* group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select first(v4),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z","abc0",0,0,true,"abc0"],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z","abc2048",2048,2048,true,"abc2048"],["1970-01-01T01:00:00Z","abc3600",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z","abc4096",4096,4096,true,"abc4096"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z","abc6144",6144,6144,true,"abc6144"],["1970-01-01T02:00:00Z","abc7200",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z","abc8192",8192,8192,true,"abc8192"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z","abc10240",10240,10240,true,"abc10240"],["1970-01-01T03:00:00Z","abc10800",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z","abc12288",12288,12288,true,"abc12288"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z","abc14336",14336,14336,true,"abc14336"],["1970-01-01T04:00:00Z","abc14400",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z","abc16384",16384,16384,true,"abc16384"],["1970-01-01T05:00:00Z","abc18000",18000,18000,true,"abc18000"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","first","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z","abc18432",18432,18432,true,"abc18432"]]}]}]}`,
		},

		&Query{
			name:    "last(v2),* group by time",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select last(v2),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","last","az","region","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",3599,"az_1","region_1",3599,3599,false,"abc3599"],["1970-01-01T01:00:00Z",7199,"az_3","region_3",7199,7199,false,"abc7199"],["1970-01-01T02:00:00Z",10799,"az_5","region_5",10799,10799,false,"abc10799"],["1970-01-01T03:00:00Z",14399,"az_7","region_7",14399,14399,false,"abc14399"],["1970-01-01T04:00:00Z",17999,"az_8","region_8",17999,17999,false,"abc17999"],["1970-01-01T05:00:00Z",20479,"az_9","region_9",20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "last(v3),* group by *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select last(v3),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:34:07Z",false,2047,2047,false,"abc2047"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T01:08:15Z",false,4095,4095,false,"abc4095"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T01:42:23Z",false,6143,6143,false,"abc6143"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T02:16:31Z",false,8191,8191,false,"abc8191"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T02:50:39Z",false,10239,10239,false,"abc10239"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T03:24:47Z",false,12287,12287,false,"abc12287"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:55Z",false,14335,14335,false,"abc14335"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T04:33:03Z",false,16383,16383,false,"abc16383"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T05:07:11Z",false,18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T05:41:19Z",false,20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "last(v4),* group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select last(v4),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z","abc2047",2047,2047,false,"abc2047"],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z","abc3599",3599,3599,false,"abc3599"],["1970-01-01T01:00:00Z","abc4095",4095,4095,false,"abc4095"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z","abc6143",6143,6143,false,"abc6143"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z","abc7199",7199,7199,false,"abc7199"],["1970-01-01T02:00:00Z","abc8191",8191,8191,false,"abc8191"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z","abc10239",10239,10239,false,"abc10239"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z","abc10799",10799,10799,false,"abc10799"],["1970-01-01T03:00:00Z","abc12287",12287,12287,false,"abc12287"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z","abc14335",14335,14335,false,"abc14335"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z","abc14399",14399,14399,false,"abc14399"],["1970-01-01T04:00:00Z","abc16383",16383,16383,false,"abc16383"],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z","abc17999",17999,17999,false,"abc17999"],["1970-01-01T05:00:00Z","abc18431",18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","last","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z","abc20479",20479,20479,false,"abc20479"]]}]}]}`,
		},

		&Query{
			name:    "percentile(v1, 0.01),* group by time",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select percentile(v1, 0.01),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","percentile","az","region","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",0,"az_0","region_0",0,0,true,"abc0"],["1970-01-01T01:00:00Z",3600,"az_1","region_1",3600,3600,true,"abc3600"],["1970-01-01T02:00:00Z",7200,"az_3","region_3",7200,7200,true,"abc7200"],["1970-01-01T03:00:00Z",10800,"az_5","region_5",10800,10800,true,"abc10800"],["1970-01-01T04:00:00Z",14400,"az_7","region_7",14400,14400,true,"abc14400"],["1970-01-01T05:00:00Z",18000,"az_8","region_8",18000,18000,true,"abc18000"]]}]}]}`,
		},
		&Query{
			name:    "percentile(v1, 50),* group by *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select percentile(v1, 50),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:17:03Z",1023,1023,1023,false,"abc1023"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:51:11Z",3071,3071,3071,false,"abc3071"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T01:25:19Z",5119,5119,5119,false,"abc5119"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T01:59:27Z",7167,7167,7167,false,"abc7167"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T02:33:35Z",9215,9215,9215,false,"abc9215"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T03:07:43Z",11263,11263,11263,false,"abc11263"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T03:41:51Z",13311,13311,13311,false,"abc13311"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T04:15:59Z",15359,15359,15359,false,"abc15359"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T04:50:07Z",17407,17407,17407,false,"abc17407"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T05:24:15Z",19455,19455,19455,false,"abc19455"]]}]}]}`,
		},
		&Query{
			name:    "percentile(v2, 99.99),* group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select percentile(v2, 99.99),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",2047,2047,2047,false,"abc2047"],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",3599,3599,3599,false,"abc3599"],["1970-01-01T01:00:00Z",4095,4095,4095,false,"abc4095"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",6143,6143,6143,false,"abc6143"],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",7199,7199,7199,false,"abc7199"],["1970-01-01T02:00:00Z",8191,8191,8191,false,"abc8191"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",10239,10239,10239,false,"abc10239"],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",10799,10799,10799,false,"abc10799"],["1970-01-01T03:00:00Z",12287,12287,12287,false,"abc12287"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",14335,14335,14335,false,"abc14335"],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",14399,14399,14399,false,"abc14399"],["1970-01-01T04:00:00Z",16383,16383,16383,false,"abc16383"],["1970-01-01T05:00:00Z",null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",17999,17999,17999,false,"abc17999"],["1970-01-01T05:00:00Z",18431,18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","percentile","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,null,null],["1970-01-01T05:00:00Z",20479,20479,20479,false,"abc20479"]]}]}]}`,
		},

		&Query{
			name:    "top(v1, 3) group by time",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select top(v1, 3) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top"],"values":[["1970-01-01T00:59:57Z",3597],["1970-01-01T00:59:58Z",3598],["1970-01-01T00:59:59Z",3599],["1970-01-01T01:59:57Z",7197],["1970-01-01T01:59:58Z",7198],["1970-01-01T01:59:59Z",7199],["1970-01-01T02:59:57Z",10797],["1970-01-01T02:59:58Z",10798],["1970-01-01T02:59:59Z",10799],["1970-01-01T03:59:57Z",14397],["1970-01-01T03:59:58Z",14398],["1970-01-01T03:59:59Z",14399],["1970-01-01T04:59:57Z",17997],["1970-01-01T04:59:58Z",17998],["1970-01-01T04:59:59Z",17999],["1970-01-01T05:41:17Z",20477],["1970-01-01T05:41:18Z",20478],["1970-01-01T05:41:19Z",20479]]}]}]}`,
		},
		&Query{
			name:    "top(v2, 4),* group by *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select top(v2, 4),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T00:34:04Z",2044,2044,2044,true,"abc2044"],["1970-01-01T00:34:05Z",2045,2045,2045,false,"abc2045"],["1970-01-01T00:34:06Z",2046,2046,2046,true,"abc2046"],["1970-01-01T00:34:07Z",2047,2047,2047,false,"abc2047"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T01:08:12Z",4092,4092,4092,true,"abc4092"],["1970-01-01T01:08:13Z",4093,4093,4093,false,"abc4093"],["1970-01-01T01:08:14Z",4094,4094,4094,true,"abc4094"],["1970-01-01T01:08:15Z",4095,4095,4095,false,"abc4095"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T01:42:20Z",6140,6140,6140,true,"abc6140"],["1970-01-01T01:42:21Z",6141,6141,6141,false,"abc6141"],["1970-01-01T01:42:22Z",6142,6142,6142,true,"abc6142"],["1970-01-01T01:42:23Z",6143,6143,6143,false,"abc6143"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T02:16:28Z",8188,8188,8188,true,"abc8188"],["1970-01-01T02:16:29Z",8189,8189,8189,false,"abc8189"],["1970-01-01T02:16:30Z",8190,8190,8190,true,"abc8190"],["1970-01-01T02:16:31Z",8191,8191,8191,false,"abc8191"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T02:50:36Z",10236,10236,10236,true,"abc10236"],["1970-01-01T02:50:37Z",10237,10237,10237,false,"abc10237"],["1970-01-01T02:50:38Z",10238,10238,10238,true,"abc10238"],["1970-01-01T02:50:39Z",10239,10239,10239,false,"abc10239"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T03:24:44Z",12284,12284,12284,true,"abc12284"],["1970-01-01T03:24:45Z",12285,12285,12285,false,"abc12285"],["1970-01-01T03:24:46Z",12286,12286,12286,true,"abc12286"],["1970-01-01T03:24:47Z",12287,12287,12287,false,"abc12287"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:52Z",14332,14332,14332,true,"abc14332"],["1970-01-01T03:58:53Z",14333,14333,14333,false,"abc14333"],["1970-01-01T03:58:54Z",14334,14334,14334,true,"abc14334"],["1970-01-01T03:58:55Z",14335,14335,14335,false,"abc14335"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T04:33:00Z",16380,16380,16380,true,"abc16380"],["1970-01-01T04:33:01Z",16381,16381,16381,false,"abc16381"],["1970-01-01T04:33:02Z",16382,16382,16382,true,"abc16382"],["1970-01-01T04:33:03Z",16383,16383,16383,false,"abc16383"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T05:07:08Z",18428,18428,18428,true,"abc18428"],["1970-01-01T05:07:09Z",18429,18429,18429,false,"abc18429"],["1970-01-01T05:07:10Z",18430,18430,18430,true,"abc18430"],["1970-01-01T05:07:11Z",18431,18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T05:41:16Z",20476,20476,20476,true,"abc20476"],["1970-01-01T05:41:17Z",20477,20477,20477,false,"abc20477"],["1970-01-01T05:41:18Z",20478,20478,20478,true,"abc20478"],["1970-01-01T05:41:19Z",20479,20479,20479,false,"abc20479"]]}]}]}`,
		},
		&Query{
			name:    "top(v2, 5),* group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select top(v2, 5),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T00:34:03Z",2043,2043,2043,false,"abc2043"],["1970-01-01T00:34:04Z",2044,2044,2044,true,"abc2044"],["1970-01-01T00:34:05Z",2045,2045,2045,false,"abc2045"],["1970-01-01T00:34:06Z",2046,2046,2046,true,"abc2046"],["1970-01-01T00:34:07Z",2047,2047,2047,false,"abc2047"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T00:59:55Z",3595,3595,3595,false,"abc3595"],["1970-01-01T00:59:56Z",3596,3596,3596,true,"abc3596"],["1970-01-01T00:59:57Z",3597,3597,3597,false,"abc3597"],["1970-01-01T00:59:58Z",3598,3598,3598,true,"abc3598"],["1970-01-01T00:59:59Z",3599,3599,3599,false,"abc3599"],["1970-01-01T01:08:11Z",4091,4091,4091,false,"abc4091"],["1970-01-01T01:08:12Z",4092,4092,4092,true,"abc4092"],["1970-01-01T01:08:13Z",4093,4093,4093,false,"abc4093"],["1970-01-01T01:08:14Z",4094,4094,4094,true,"abc4094"],["1970-01-01T01:08:15Z",4095,4095,4095,false,"abc4095"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T01:42:19Z",6139,6139,6139,false,"abc6139"],["1970-01-01T01:42:20Z",6140,6140,6140,true,"abc6140"],["1970-01-01T01:42:21Z",6141,6141,6141,false,"abc6141"],["1970-01-01T01:42:22Z",6142,6142,6142,true,"abc6142"],["1970-01-01T01:42:23Z",6143,6143,6143,false,"abc6143"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T01:59:55Z",7195,7195,7195,false,"abc7195"],["1970-01-01T01:59:56Z",7196,7196,7196,true,"abc7196"],["1970-01-01T01:59:57Z",7197,7197,7197,false,"abc7197"],["1970-01-01T01:59:58Z",7198,7198,7198,true,"abc7198"],["1970-01-01T01:59:59Z",7199,7199,7199,false,"abc7199"],["1970-01-01T02:16:27Z",8187,8187,8187,false,"abc8187"],["1970-01-01T02:16:28Z",8188,8188,8188,true,"abc8188"],["1970-01-01T02:16:29Z",8189,8189,8189,false,"abc8189"],["1970-01-01T02:16:30Z",8190,8190,8190,true,"abc8190"],["1970-01-01T02:16:31Z",8191,8191,8191,false,"abc8191"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T02:50:35Z",10235,10235,10235,false,"abc10235"],["1970-01-01T02:50:36Z",10236,10236,10236,true,"abc10236"],["1970-01-01T02:50:37Z",10237,10237,10237,false,"abc10237"],["1970-01-01T02:50:38Z",10238,10238,10238,true,"abc10238"],["1970-01-01T02:50:39Z",10239,10239,10239,false,"abc10239"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T02:59:55Z",10795,10795,10795,false,"abc10795"],["1970-01-01T02:59:56Z",10796,10796,10796,true,"abc10796"],["1970-01-01T02:59:57Z",10797,10797,10797,false,"abc10797"],["1970-01-01T02:59:58Z",10798,10798,10798,true,"abc10798"],["1970-01-01T02:59:59Z",10799,10799,10799,false,"abc10799"],["1970-01-01T03:24:43Z",12283,12283,12283,false,"abc12283"],["1970-01-01T03:24:44Z",12284,12284,12284,true,"abc12284"],["1970-01-01T03:24:45Z",12285,12285,12285,false,"abc12285"],["1970-01-01T03:24:46Z",12286,12286,12286,true,"abc12286"],["1970-01-01T03:24:47Z",12287,12287,12287,false,"abc12287"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:51Z",14331,14331,14331,false,"abc14331"],["1970-01-01T03:58:52Z",14332,14332,14332,true,"abc14332"],["1970-01-01T03:58:53Z",14333,14333,14333,false,"abc14333"],["1970-01-01T03:58:54Z",14334,14334,14334,true,"abc14334"],["1970-01-01T03:58:55Z",14335,14335,14335,false,"abc14335"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T03:59:55Z",14395,14395,14395,false,"abc14395"],["1970-01-01T03:59:56Z",14396,14396,14396,true,"abc14396"],["1970-01-01T03:59:57Z",14397,14397,14397,false,"abc14397"],["1970-01-01T03:59:58Z",14398,14398,14398,true,"abc14398"],["1970-01-01T03:59:59Z",14399,14399,14399,false,"abc14399"],["1970-01-01T04:32:59Z",16379,16379,16379,false,"abc16379"],["1970-01-01T04:33:00Z",16380,16380,16380,true,"abc16380"],["1970-01-01T04:33:01Z",16381,16381,16381,false,"abc16381"],["1970-01-01T04:33:02Z",16382,16382,16382,true,"abc16382"],["1970-01-01T04:33:03Z",16383,16383,16383,false,"abc16383"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T04:59:55Z",17995,17995,17995,false,"abc17995"],["1970-01-01T04:59:56Z",17996,17996,17996,true,"abc17996"],["1970-01-01T04:59:57Z",17997,17997,17997,false,"abc17997"],["1970-01-01T04:59:58Z",17998,17998,17998,true,"abc17998"],["1970-01-01T04:59:59Z",17999,17999,17999,false,"abc17999"],["1970-01-01T05:07:07Z",18427,18427,18427,false,"abc18427"],["1970-01-01T05:07:08Z",18428,18428,18428,true,"abc18428"],["1970-01-01T05:07:09Z",18429,18429,18429,false,"abc18429"],["1970-01-01T05:07:10Z",18430,18430,18430,true,"abc18430"],["1970-01-01T05:07:11Z",18431,18431,18431,false,"abc18431"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","top","v1","v2","v3","v4"],"values":[["1970-01-01T05:41:15Z",20475,20475,20475,false,"abc20475"],["1970-01-01T05:41:16Z",20476,20476,20476,true,"abc20476"],["1970-01-01T05:41:17Z",20477,20477,20477,false,"abc20477"],["1970-01-01T05:41:18Z",20478,20478,20478,true,"abc20478"],["1970-01-01T05:41:19Z",20479,20479,20479,false,"abc20479"]]}]}]}`,
		},

		&Query{
			name:    "bottom(v1, 3) group by time",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select bottom(v1, 3) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom"],"values":[["1970-01-01T00:00:00Z",0],["1970-01-01T00:00:01Z",1],["1970-01-01T00:00:02Z",2],["1970-01-01T01:00:00Z",3600],["1970-01-01T01:00:01Z",3601],["1970-01-01T01:00:02Z",3602],["1970-01-01T02:00:00Z",7200],["1970-01-01T02:00:01Z",7201],["1970-01-01T02:00:02Z",7202],["1970-01-01T03:00:00Z",10800],["1970-01-01T03:00:01Z",10801],["1970-01-01T03:00:02Z",10802],["1970-01-01T04:00:00Z",14400],["1970-01-01T04:00:01Z",14401],["1970-01-01T04:00:02Z",14402],["1970-01-01T05:00:00Z",18000],["1970-01-01T05:00:01Z",18001],["1970-01-01T05:00:02Z",18002]]}]}]}`,
		},
		&Query{
			name:    "bottom(v2, 4),* group by *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select bottom(v2, 4),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,true,"abc0"],["1970-01-01T00:00:01Z",1,1,1,false,"abc1"],["1970-01-01T00:00:02Z",2,2,2,true,"abc2"],["1970-01-01T00:00:03Z",3,3,3,false,"abc3"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T00:34:08Z",2048,2048,2048,true,"abc2048"],["1970-01-01T00:34:09Z",2049,2049,2049,false,"abc2049"],["1970-01-01T00:34:10Z",2050,2050,2050,true,"abc2050"],["1970-01-01T00:34:11Z",2051,2051,2051,false,"abc2051"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T01:08:16Z",4096,4096,4096,true,"abc4096"],["1970-01-01T01:08:17Z",4097,4097,4097,false,"abc4097"],["1970-01-01T01:08:18Z",4098,4098,4098,true,"abc4098"],["1970-01-01T01:08:19Z",4099,4099,4099,false,"abc4099"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T01:42:24Z",6144,6144,6144,true,"abc6144"],["1970-01-01T01:42:25Z",6145,6145,6145,false,"abc6145"],["1970-01-01T01:42:26Z",6146,6146,6146,true,"abc6146"],["1970-01-01T01:42:27Z",6147,6147,6147,false,"abc6147"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T02:16:32Z",8192,8192,8192,true,"abc8192"],["1970-01-01T02:16:33Z",8193,8193,8193,false,"abc8193"],["1970-01-01T02:16:34Z",8194,8194,8194,true,"abc8194"],["1970-01-01T02:16:35Z",8195,8195,8195,false,"abc8195"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T02:50:40Z",10240,10240,10240,true,"abc10240"],["1970-01-01T02:50:41Z",10241,10241,10241,false,"abc10241"],["1970-01-01T02:50:42Z",10242,10242,10242,true,"abc10242"],["1970-01-01T02:50:43Z",10243,10243,10243,false,"abc10243"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T03:24:48Z",12288,12288,12288,true,"abc12288"],["1970-01-01T03:24:49Z",12289,12289,12289,false,"abc12289"],["1970-01-01T03:24:50Z",12290,12290,12290,true,"abc12290"],["1970-01-01T03:24:51Z",12291,12291,12291,false,"abc12291"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:56Z",14336,14336,14336,true,"abc14336"],["1970-01-01T03:58:57Z",14337,14337,14337,false,"abc14337"],["1970-01-01T03:58:58Z",14338,14338,14338,true,"abc14338"],["1970-01-01T03:58:59Z",14339,14339,14339,false,"abc14339"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T04:33:04Z",16384,16384,16384,true,"abc16384"],["1970-01-01T04:33:05Z",16385,16385,16385,false,"abc16385"],["1970-01-01T04:33:06Z",16386,16386,16386,true,"abc16386"],["1970-01-01T04:33:07Z",16387,16387,16387,false,"abc16387"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T05:07:12Z",18432,18432,18432,true,"abc18432"],["1970-01-01T05:07:13Z",18433,18433,18433,false,"abc18433"],["1970-01-01T05:07:14Z",18434,18434,18434,true,"abc18434"],["1970-01-01T05:07:15Z",18435,18435,18435,false,"abc18435"]]}]}]}`,
		},
		&Query{
			name:    "bottom(v2, 5),* group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select bottom(v2, 5),* from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T00:00:00Z",0,0,0,true,"abc0"],["1970-01-01T00:00:01Z",1,1,1,false,"abc1"],["1970-01-01T00:00:02Z",2,2,2,true,"abc2"],["1970-01-01T00:00:03Z",3,3,3,false,"abc3"],["1970-01-01T00:00:04Z",4,4,4,true,"abc4"]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T00:34:08Z",2048,2048,2048,true,"abc2048"],["1970-01-01T00:34:09Z",2049,2049,2049,false,"abc2049"],["1970-01-01T00:34:10Z",2050,2050,2050,true,"abc2050"],["1970-01-01T00:34:11Z",2051,2051,2051,false,"abc2051"],["1970-01-01T00:34:12Z",2052,2052,2052,true,"abc2052"],["1970-01-01T01:00:00Z",3600,3600,3600,true,"abc3600"],["1970-01-01T01:00:01Z",3601,3601,3601,false,"abc3601"],["1970-01-01T01:00:02Z",3602,3602,3602,true,"abc3602"],["1970-01-01T01:00:03Z",3603,3603,3603,false,"abc3603"],["1970-01-01T01:00:04Z",3604,3604,3604,true,"abc3604"]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T01:08:16Z",4096,4096,4096,true,"abc4096"],["1970-01-01T01:08:17Z",4097,4097,4097,false,"abc4097"],["1970-01-01T01:08:18Z",4098,4098,4098,true,"abc4098"],["1970-01-01T01:08:19Z",4099,4099,4099,false,"abc4099"],["1970-01-01T01:08:20Z",4100,4100,4100,true,"abc4100"]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T01:42:24Z",6144,6144,6144,true,"abc6144"],["1970-01-01T01:42:25Z",6145,6145,6145,false,"abc6145"],["1970-01-01T01:42:26Z",6146,6146,6146,true,"abc6146"],["1970-01-01T01:42:27Z",6147,6147,6147,false,"abc6147"],["1970-01-01T01:42:28Z",6148,6148,6148,true,"abc6148"],["1970-01-01T02:00:00Z",7200,7200,7200,true,"abc7200"],["1970-01-01T02:00:01Z",7201,7201,7201,false,"abc7201"],["1970-01-01T02:00:02Z",7202,7202,7202,true,"abc7202"],["1970-01-01T02:00:03Z",7203,7203,7203,false,"abc7203"],["1970-01-01T02:00:04Z",7204,7204,7204,true,"abc7204"]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T02:16:32Z",8192,8192,8192,true,"abc8192"],["1970-01-01T02:16:33Z",8193,8193,8193,false,"abc8193"],["1970-01-01T02:16:34Z",8194,8194,8194,true,"abc8194"],["1970-01-01T02:16:35Z",8195,8195,8195,false,"abc8195"],["1970-01-01T02:16:36Z",8196,8196,8196,true,"abc8196"]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T02:50:40Z",10240,10240,10240,true,"abc10240"],["1970-01-01T02:50:41Z",10241,10241,10241,false,"abc10241"],["1970-01-01T02:50:42Z",10242,10242,10242,true,"abc10242"],["1970-01-01T02:50:43Z",10243,10243,10243,false,"abc10243"],["1970-01-01T02:50:44Z",10244,10244,10244,true,"abc10244"],["1970-01-01T03:00:00Z",10800,10800,10800,true,"abc10800"],["1970-01-01T03:00:01Z",10801,10801,10801,false,"abc10801"],["1970-01-01T03:00:02Z",10802,10802,10802,true,"abc10802"],["1970-01-01T03:00:03Z",10803,10803,10803,false,"abc10803"],["1970-01-01T03:00:04Z",10804,10804,10804,true,"abc10804"]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T03:24:48Z",12288,12288,12288,true,"abc12288"],["1970-01-01T03:24:49Z",12289,12289,12289,false,"abc12289"],["1970-01-01T03:24:50Z",12290,12290,12290,true,"abc12290"],["1970-01-01T03:24:51Z",12291,12291,12291,false,"abc12291"],["1970-01-01T03:24:52Z",12292,12292,12292,true,"abc12292"]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T03:58:56Z",14336,14336,14336,true,"abc14336"],["1970-01-01T03:58:57Z",14337,14337,14337,false,"abc14337"],["1970-01-01T03:58:58Z",14338,14338,14338,true,"abc14338"],["1970-01-01T03:58:59Z",14339,14339,14339,false,"abc14339"],["1970-01-01T03:59:00Z",14340,14340,14340,true,"abc14340"],["1970-01-01T04:00:00Z",14400,14400,14400,true,"abc14400"],["1970-01-01T04:00:01Z",14401,14401,14401,false,"abc14401"],["1970-01-01T04:00:02Z",14402,14402,14402,true,"abc14402"],["1970-01-01T04:00:03Z",14403,14403,14403,false,"abc14403"],["1970-01-01T04:00:04Z",14404,14404,14404,true,"abc14404"]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T04:33:04Z",16384,16384,16384,true,"abc16384"],["1970-01-01T04:33:05Z",16385,16385,16385,false,"abc16385"],["1970-01-01T04:33:06Z",16386,16386,16386,true,"abc16386"],["1970-01-01T04:33:07Z",16387,16387,16387,false,"abc16387"],["1970-01-01T04:33:08Z",16388,16388,16388,true,"abc16388"],["1970-01-01T05:00:00Z",18000,18000,18000,true,"abc18000"],["1970-01-01T05:00:01Z",18001,18001,18001,false,"abc18001"],["1970-01-01T05:00:02Z",18002,18002,18002,true,"abc18002"],["1970-01-01T05:00:03Z",18003,18003,18003,false,"abc18003"],["1970-01-01T05:00:04Z",18004,18004,18004,true,"abc18004"]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","bottom","v1","v2","v3","v4"],"values":[["1970-01-01T05:07:12Z",18432,18432,18432,true,"abc18432"],["1970-01-01T05:07:13Z",18433,18433,18433,false,"abc18433"],["1970-01-01T05:07:14Z",18434,18434,18434,true,"abc18434"],["1970-01-01T05:07:15Z",18435,18435,18435,false,"abc18435"],["1970-01-01T05:07:16Z",18436,18436,18436,true,"abc18436"]]}]}]}`,
		},
		&Query{
			name:    "distinct(v3)",
			command: `select distinct(v3) from db0.rp0.cpu`,
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]}]}]}`,
		},
		&Query{
			name:    "distinct(v3) group by *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select distinct(v3) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]}]}]}`,
		},
		&Query{
			name:    "distinct(v3) group by time, *",
			params:  url.Values{"inner_chunk_size": []string{"10"}},
			command: `select distinct(v3) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","distinct"],"values":[["1970-01-01T00:00:00Z",true],["1970-01-01T00:00:00Z",false],["1970-01-01T01:00:00Z",true],["1970-01-01T01:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","distinct"],"values":[["1970-01-01T01:00:00Z",true],["1970-01-01T01:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","distinct"],"values":[["1970-01-01T01:00:00Z",true],["1970-01-01T01:00:00Z",false],["1970-01-01T02:00:00Z",true],["1970-01-01T02:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","distinct"],"values":[["1970-01-01T02:00:00Z",true],["1970-01-01T02:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","distinct"],"values":[["1970-01-01T02:00:00Z",true],["1970-01-01T02:00:00Z",false],["1970-01-01T03:00:00Z",true],["1970-01-01T03:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","distinct"],"values":[["1970-01-01T03:00:00Z",true],["1970-01-01T03:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","distinct"],"values":[["1970-01-01T03:00:00Z",true],["1970-01-01T03:00:00Z",false],["1970-01-01T04:00:00Z",true],["1970-01-01T04:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","distinct"],"values":[["1970-01-01T04:00:00Z",true],["1970-01-01T04:00:00Z",false],["1970-01-01T05:00:00Z",true],["1970-01-01T05:00:00Z",false]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","distinct"],"values":[["1970-01-01T05:00:00Z",true],["1970-01-01T05:00:00Z",false]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),spread(v2),stddev(v1) group by *",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),spread(v2),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,2047,2096128,2048,1023.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",2048,4095,6290432,2048,3071.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",4096,6143,10484736,2048,5119.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",6144,8191,14679040,2048,7167.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",8192,10239,18873344,2048,9215.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",10240,12287,23067648,2048,11263.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",12288,14335,27261952,2048,13311.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",14336,16383,31456256,2048,15359.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",16384,18431,35650560,2048,17407.5,2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",18432,20479,39844864,2048,19455.5,2047,591.3509956024425]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),spread(v2),stddev(v1) group by time",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),spread(v2),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,3599,6478200,3600,1799.5,3599,1039.3748120865737],["1970-01-01T01:00:00Z",3600,7199,19438200,3600,5399.5,3599,1039.3748120865741],["1970-01-01T02:00:00Z",7200,10799,32398200,3600,8999.5,3599,1039.374812086576],["1970-01-01T03:00:00Z",10800,14399,45358200,3600,12599.5,3599,1039.3748120865687],["1970-01-01T04:00:00Z",14400,17999,58318200,3600,16199.5,3599,1039.3748120865687],["1970-01-01T05:00:00Z",18000,20479,47713960,2480,19239.5,2479,716.0586568152081]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),spread(v2),stddev(v1) group by time,*",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),spread(v2),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,2047,2096128,2048,1023.5,2047,591.3509956024425],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",2048,3599,4382072,1552,2823.5,1551,448.16812321568193],["1970-01-01T01:00:00Z",3600,4095,1908360,496,3847.5,495,143.32713164877984],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null],["1970-01-01T01:00:00Z",4096,6143,10484736,2048,5119.5,2047,591.3509956024425],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null],["1970-01-01T01:00:00Z",6144,7199,7045104,1056,6671.5,1055,304.98524554475995],["1970-01-01T02:00:00Z",7200,8191,7633936,992,7695.5,991,286.5100347282796],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null],["1970-01-01T02:00:00Z",8192,10239,18873344,2048,9215.5,2047,591.3509956024425],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null],["1970-01-01T02:00:00Z",10240,10799,5890920,560,10519.5,559,161.80234856144702],["1970-01-01T03:00:00Z",10800,12287,17176728,1488,11543.5,1487,429.69291360226663],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null],["1970-01-01T03:00:00Z",12288,14335,27261952,2048,13311.5,2047,591.3509956024425],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null],["1970-01-01T03:00:00Z",14336,14399,919520,64,14367.5,63,18.618986725025255],["1970-01-01T04:00:00Z",14400,16383,30536736,1984,15391.5,1983,572.8757864202909],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null],["1970-01-01T04:00:00Z",16384,17999,27781464,1616,17191.5,1615,466.64333274997085],["1970-01-01T05:00:00Z",18000,18431,7869096,432,18215.5,431,124.85191228018863]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min","max","sum","count","mean","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null],["1970-01-01T05:00:00Z",18432,20479,39844864,2048,19455.5,2047,591.3509956024425]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by *",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,2047,2096128,2048,1023.5,true,"abc2047",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",2048,4095,6290432,2048,3071.5,true,"abc4095",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",4096,6143,10484736,2048,5119.5,true,"abc6143",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",6144,8191,14679040,2048,7167.5,true,"abc8191",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",8192,10239,18873344,2048,9215.5,true,"abc10239",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",10240,12287,23067648,2048,11263.5,true,"abc12287",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",12288,14335,27261952,2048,13311.5,true,"abc14335",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",14336,16383,31456256,2048,15359.5,true,"abc16383",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",16384,18431,35650560,2048,17407.5,true,"abc18431",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",18432,20479,39844864,2048,19455.5,true,"abc20479",2047,591.3509956024425]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by time",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,3599,6478200,3600,1799.5,true,"abc3599",3599,1039.3748120865737],["1970-01-01T01:00:00Z",3600,7199,19438200,3600,5399.5,true,"abc7199",3599,1039.3748120865741],["1970-01-01T02:00:00Z",7200,10799,32398200,3600,8999.5,true,"abc10799",3599,1039.374812086576],["1970-01-01T03:00:00Z",10800,14399,45358200,3600,12599.5,true,"abc14399",3599,1039.3748120865687],["1970-01-01T04:00:00Z",14400,17999,58318200,3600,16199.5,true,"abc17999",3599,1039.3748120865687],["1970-01-01T05:00:00Z",18000,20479,47713960,2480,19239.5,true,"abc20479",2479,716.0586568152081]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by time,*",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),*`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,2047,2096128,2048,1023.5,true,"abc2047",2047,591.3509956024425],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",2048,3599,4382072,1552,2823.5,true,"abc3599",1551,448.16812321568193],["1970-01-01T01:00:00Z",3600,4095,1908360,496,3847.5,true,"abc4095",495,143.32713164877984],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T01:00:00Z",4096,6143,10484736,2048,5119.5,true,"abc6143",2047,591.3509956024425],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T01:00:00Z",6144,7199,7045104,1056,6671.5,true,"abc7199",1055,304.98524554475995],["1970-01-01T02:00:00Z",7200,8191,7633936,992,7695.5,true,"abc8191",991,286.5100347282796],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",8192,10239,18873344,2048,9215.5,true,"abc10239",2047,591.3509956024425],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",10240,10799,5890920,560,10519.5,true,"abc10799",559,161.80234856144702],["1970-01-01T03:00:00Z",10800,12287,17176728,1488,11543.5,true,"abc12287",1487,429.69291360226663],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_6","region":"region_6"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T03:00:00Z",12288,14335,27261952,2048,13311.5,true,"abc14335",2047,591.3509956024425],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_7","region":"region_7"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T03:00:00Z",14336,14399,919520,64,14367.5,true,"abc14399",63,18.618986725025255],["1970-01-01T04:00:00Z",14400,16383,30536736,1984,15391.5,true,"abc16383",1983,572.8757864202909],["1970-01-01T05:00:00Z",null,null,null,0,null,null,null,null,null]]},{"name":"cpu","tags":{"az":"az_8","region":"region_8"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T04:00:00Z",16384,17999,27781464,1616,17191.5,true,"abc17999",1615,466.64333274997085],["1970-01-01T05:00:00Z",18000,18431,7869096,432,18215.5,true,"abc18431",431,124.85191228018863]]},{"name":"cpu","tags":{"az":"az_9","region":"region_9"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T04:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T05:00:00Z",18432,20479,39844864,2048,19455.5,true,"abc20479",2047,591.3509956024425]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by * limit 3",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * limit 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,2047,2096128,2048,1023.5,true,"abc2047",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",2048,4095,6290432,2048,3071.5,true,"abc4095",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",4096,6143,10484736,2048,5119.5,true,"abc6143",2047,591.3509956024425]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by time limit 3",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) limit 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,3599,6478200,3600,1799.5,true,"abc3599",3599,1039.3748120865737],["1970-01-01T01:00:00Z",3600,7199,19438200,3600,5399.5,true,"abc7199",3599,1039.3748120865741],["1970-01-01T02:00:00Z",7200,10799,32398200,3600,8999.5,true,"abc10799",3599,1039.374812086576]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by time,* limit 3",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* limit 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",0,2047,2096128,2048,1023.5,true,"abc2047",2047,591.3509956024425],["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by * limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",2048,4095,6290432,2048,3071.5,true,"abc4095",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",4096,6143,10484736,2048,5119.5,true,"abc6143",2047,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T00:00:00Z",6144,8191,14679040,2048,7167.5,true,"abc8191",2047,591.3509956024425]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by time limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T01:00:00Z",3600,7199,19438200,3600,5399.5,true,"abc7199",3599,1039.3748120865741],["1970-01-01T02:00:00Z",7200,10799,32398200,3600,8999.5,true,"abc10799",3599,1039.374812086576],["1970-01-01T03:00:00Z",10800,14399,45358200,3600,12599.5,true,"abc14399",3599,1039.3748120865687]]}]}]}`,
		},
		&Query{
			name:    "min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) group by time,* limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select min(v1),max(v2),sum(v1),count(v2),mean(v1),first(v3),last(v4),spread(v1),stddev(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","min","max","sum","count","mean","first","last","spread","stddev"],"values":[["1970-01-01T01:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T02:00:00Z",null,null,null,0,null,null,null,null,null],["1970-01-01T03:00:00Z",null,null,null,0,null,null,null,null,null]]}]}]}`,
		},
		&Query{
			name:    "stddev(*) group by * limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select stddev(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T00:00:00Z",591.3509956024425,591.3509956024425]]}]}]}`,
		},
		&Query{
			name:    "stddev(*) group by time limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select stddev(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h) limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T01:00:00Z",1039.3748120865741,1039.3748120865741],["1970-01-01T02:00:00Z",1039.374812086576,1039.374812086576],["1970-01-01T03:00:00Z",1039.3748120865687,1039.3748120865687]]}]}]}`,
		},
		&Query{
			name:    "stddev(*) group by time,* limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select stddev(*) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by time(1h),* limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","stddev_v1","stddev_v2"],"values":[["1970-01-01T01:00:00Z",null,null],["1970-01-01T02:00:00Z",null,null],["1970-01-01T03:00:00Z",null,null]]}]}]}`,
		},
		&Query{
			name:    "percentile group by time,* limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select percentile(v1, 50) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","percentile"],"values":[["1970-01-01T00:51:11Z",3071]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","percentile"],"values":[["1970-01-01T01:25:19Z",5119]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","percentile"],"values":[["1970-01-01T01:59:27Z",7167]]}]}]}`,
		},
		&Query{
			name:    "top group by time,* limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select top(v1, 3) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","top"],"values":[["1970-01-01T01:08:13Z",4093],["1970-01-01T01:08:14Z",4094],["1970-01-01T01:08:15Z",4095]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","top"],"values":[["1970-01-01T01:42:21Z",6141],["1970-01-01T01:42:22Z",6142],["1970-01-01T01:42:23Z",6143]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","top"],"values":[["1970-01-01T02:16:29Z",8189],["1970-01-01T02:16:30Z",8190],["1970-01-01T02:16:31Z",8191]]}]}]}`,
		},
		&Query{
			name:    "bottom group by time,* limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select bottom(v1, 3) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","bottom"],"values":[["1970-01-01T00:34:08Z",2048],["1970-01-01T00:34:09Z",2049],["1970-01-01T00:34:10Z",2050]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","bottom"],"values":[["1970-01-01T01:08:16Z",4096],["1970-01-01T01:08:17Z",4097],["1970-01-01T01:08:18Z",4098]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","bottom"],"values":[["1970-01-01T01:42:24Z",6144],["1970-01-01T01:42:25Z",6145],["1970-01-01T01:42:26Z",6146]]}]}]}`,
		},
		&Query{
			name:    "difference group by * limit 3 offset 1",
			params:  url.Values{"inner_chunk_size": []string{"5"}},
			command: `select difference(v1) from db0.rp0.cpu where time >= '1970-01-01T00:00:00Z' AND time <= '1970-01-01T05:41:19Z' group by * limit 3 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","difference"],"values":[["1970-01-01T00:00:02Z",1],["1970-01-01T00:00:03Z",1],["1970-01-01T00:00:04Z",1]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Null_Group(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	// set infinite retention policy as we are inserting data in the past and don't want retention policy enforcement to make this test racy
	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")

	// Insert data into the measurement, and the filed value of some groups is completely empty.
	writes := []string{}
	i := 0
	for i = 0; i < 6; i++ {
		if i < 2 {
			for j := 0; j < 2048; j++ {
				data := fmt.Sprintf(`cpu,region=region_%d,az=az_%d v1=%di,v2=%f,v3=%t,v4="%s" %d`,
					i, i, i*2048+j, generateFloat(i*2048+j), generateBool(i*2048+j), generateString(i*2048+j), time.Unix(int64(i*2048+j), int64(0)).UTC().UnixNano())
				writes = append(writes, data)
			}
		} else if i == 2 { //v1=null
			for j := 0; j < 2048; j++ {
				data := fmt.Sprintf(`cpu,region=region_%d,az=az_%d v2=%f,v3=%t,v4="%s" %d`,
					i, i, generateFloat(i*2048+j), generateBool(i*2048+j), generateString(i*2048+j), time.Unix(int64(i*2048+j), int64(0)).UTC().UnixNano())
				writes = append(writes, data)
			}
		} else if i == 3 { //v2=null
			for j := 0; j < 2048; j++ {
				data := fmt.Sprintf(`cpu,region=region_%d,az=az_%d v1=%di,v3=%t,v4="%s" %d`,
					i, i, i*2048+j, generateBool(i*2048+j), generateString(i*2048+j), time.Unix(int64(i*2048+j), int64(0)).UTC().UnixNano())
				writes = append(writes, data)
			}
		} else if i == 4 { //v3=null
			for j := 0; j < 2048; j++ {
				data := fmt.Sprintf(`cpu,region=region_%d,az=az_%d v1=%di,v2=%f,v4="%s" %d`,
					i, i, i*2048+j, generateFloat(i*2048+j), generateString(i*2048+j), time.Unix(int64(i*2048+j), int64(0)).UTC().UnixNano())
				writes = append(writes, data)
			}
		} else if i == 5 { //v4=null
			for j := 0; j < 2048; j++ {
				data := fmt.Sprintf(`cpu,region=region_%d,az=az_%d v1=%di,v2=%f,v3=%t %d`,
					i, i, i*2048+j, generateFloat(i*2048+j), generateBool(i*2048+j), time.Unix(int64(i*2048+j), int64(0)).UTC().UnixNano())
				writes = append(writes, data)
			}
		}

	}
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		&Query{ //BUG:BUG2022060902489
			name:    "percentile with group by * : innerChunkSize=1",
			params:  url.Values{"db": []string{"db0"}, "inner_chunk_size": []string{"1"}},
			command: `SELECT percentile(*,95) FROM cpu group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",1945,1945]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",3993,3993]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,6041]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",8089,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",10137,10137]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",12185,12185]]}]}]}`,
		},
		&Query{ //BUG:BUG2022060902489
			name:    "percentile with group by * : innerChunkSize=1024",
			params:  url.Values{"db": []string{"db0"}, "inner_chunk_size": []string{"1024"}},
			command: `SELECT percentile(*,95) FROM cpu group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"az":"az_0","region":"region_0"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",1945,1945]]},{"name":"cpu","tags":{"az":"az_1","region":"region_1"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",3993,3993]]},{"name":"cpu","tags":{"az":"az_2","region":"region_2"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",null,6041]]},{"name":"cpu","tags":{"az":"az_3","region":"region_3"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",8089,null]]},{"name":"cpu","tags":{"az":"az_4","region":"region_4"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",10137,10137]]},{"name":"cpu","tags":{"az":"az_5","region":"region_5"},"columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",12185,12185]]}]}]}`,
		},
		&Query{ //BUG:BUG2022060902489
			name:    "percentile : innerChunkSize=1",
			params:  url.Values{"db": []string{"db0"}, "inner_chunk_size": []string{"1"}},
			command: `SELECT percentile(*,95) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",11775,11775]]}]}]}`,
		},
		&Query{ //BUG:BUG2022060902489
			name:    "percentile : innerChunkSize=1024",
			params:  url.Values{"db": []string{"db0"}, "inner_chunk_size": []string{"1024"}},
			command: `SELECT percentile(*,95) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","percentile_v1","percentile_v2"],"values":[["1970-01-01T00:00:00Z",11775,11775]]}]}]}`,
		},
	}...,
	)
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
func TestServer_Query_AggregateSelectors(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`network,host=server01,region=west,cores=1 rx=10i,tx=20i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`network,host=server02,region=west,cores=2 rx=40i,tx=50i,core=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`network,host=server03,region=east,cores=3 rx=40i,tx=55i,core=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`network,host=server04,region=east,cores=4 rx=40i,tx=60i,core=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`network,host=server05,region=west,cores=1 rx=50i,tx=70i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
		fmt.Sprintf(`network,host=server06,region=east,cores=2 rx=50i,tx=40i,core=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
		fmt.Sprintf(`network,host=server07,region=west,cores=3 rx=70i,tx=30i,core=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`network,host=server08,region=east,cores=4 rx=90i,tx=10i,core=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		fmt.Sprintf(`network,host=server09,region=east,cores=1 rx=5i,tx=4i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "baseline",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM network`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","core","cores","host","region","rx","tx"],"values":[["2000-01-01T00:00:00Z",2,"1","server01","west",10,20],["2000-01-01T00:00:10Z",3,"2","server02","west",40,50],["2000-01-01T00:00:20Z",4,"3","server03","east",40,55],["2000-01-01T00:00:30Z",1,"4","server04","east",40,60],["2000-01-01T00:00:40Z",2,"1","server05","west",50,70],["2000-01-01T00:00:50Z",3,"2","server06","east",50,40],["2000-01-01T00:01:00Z",4,"3","server07","west",70,30],["2000-01-01T00:01:10Z",1,"4","server08","east",90,10],["2000-01-01T00:01:20Z",2,"1","server09","east",5,4]]}]}]}`,
		},
		&Query{
			name:    "max - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",90]]}]}]}`,
		},
		&Query{
			name:    "max - baseline 30s - epoch ms",
			params:  url.Values{"db": []string{"db0"}, "epoch": []string{"ms"}},
			command: `SELECT max(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp: fmt.Sprintf(
				`{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","max"],"values":[[%d,40],[%d,50],[%d,90]]}]}]}`,
				mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()/int64(time.Millisecond),
				mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()/int64(time.Millisecond),
				mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()/int64(time.Millisecond),
			),
		},
		&Query{
			name:    "max - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, max(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","max"],"values":[["2000-01-01T00:00:00Z",50,40],["2000-01-01T00:00:30Z",70,50],["2000-01-01T00:01:00Z",10,90]]}]}]}`,
		},
		&Query{
			name:    "max - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, max(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",90]]}]}]}`,
		},
		&Query{
			name:    "max - time and tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, tx, max(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","max"],"values":[["2000-01-01T00:00:00Z",50,40],["2000-01-01T00:00:30Z",70,50],["2000-01-01T00:01:00Z",10,90]]}]}]}`,
		},
		&Query{
			name:    "min - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:30Z",40],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "min - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, min(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","min"],"values":[["2000-01-01T00:00:00Z",20,10],["2000-01-01T00:00:30Z",60,40],["2000-01-01T00:01:00Z",4,5]]}]}]}`,
		},
		&Query{
			name:    "min - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, min(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:30Z",40],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "min - time and tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, tx, min(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","min"],"values":[["2000-01-01T00:00:00Z",20,10],["2000-01-01T00:00:30Z",60,40],["2000-01-01T00:01:00Z",4,5]]}]}]}`,
		},
		&Query{
			name:    "max,min - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(rx), min(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","max","min"],"values":[["2000-01-01T00:00:00Z",40,10],["2000-01-01T00:00:30Z",50,40],["2000-01-01T00:01:00Z",90,5]]}]}]}`,
		},
		&Query{
			name:    "first - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:30Z",40],["2000-01-01T00:01:00Z",70]]}]}]}`,
		},
		&Query{
			name:    "first - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, tx, first(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","first"],"values":[["2000-01-01T00:00:00Z",20,10],["2000-01-01T00:00:30Z",60,40],["2000-01-01T00:01:00Z",30,70]]}]}]}`,
		},
		&Query{
			name:    "first - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, first(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:30Z",40],["2000-01-01T00:01:00Z",70]]}]}]}`,
		},
		&Query{
			name:    "first - time and tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, tx, first(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","first"],"values":[["2000-01-01T00:00:00Z",20,10],["2000-01-01T00:00:30Z",60,40],["2000-01-01T00:01:00Z",30,70]]}]}]}`,
		},
		&Query{
			name:    "last - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "last - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, last(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","last"],"values":[["2000-01-01T00:00:00Z",55,40],["2000-01-01T00:00:30Z",40,50],["2000-01-01T00:01:00Z",4,5]]}]}]}`,
		},
		&Query{
			name:    "last - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, last(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",5]]}]}]}`,
		},
		&Query{
			name:    "last - time and tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, tx, last(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","last"],"values":[["2000-01-01T00:00:00Z",55,40],["2000-01-01T00:00:30Z",40,50],["2000-01-01T00:01:00Z",4,5]]}]}]}`,
		},
		&Query{
			name:    "count - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT count(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","count"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:30Z",3],["2000-01-01T00:01:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "count - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, count(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","count"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:30Z",3],["2000-01-01T00:01:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "count - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, count(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "distinct - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT distinct(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","distinct"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70],["2000-01-01T00:01:00Z",90],["2000-01-01T00:01:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "distinct - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, distinct(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","distinct"],"values":[["2000-01-01T00:00:00Z",10],["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70],["2000-01-01T00:01:00Z",90],["2000-01-01T00:01:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "distinct - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, distinct(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"aggregate function distinct() cannot be combined with other functions or fields"}]}`,
			skip:    true,
		},
		&Query{
			name:    "mean - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",30],["2000-01-01T00:00:30Z",46.666666666666664],["2000-01-01T00:01:00Z",55]]}]}]}`,
		},
		&Query{
			name:    "mean - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, mean(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",30],["2000-01-01T00:00:30Z",46.666666666666664],["2000-01-01T00:01:00Z",55]]}]}]}`,
		},
		&Query{
			name:    "mean - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, mean(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "median - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT median(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","median"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "median - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, median(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","median"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "median - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, median(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
			skip:    true,
		},
		&Query{
			name:    "mode - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mode(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","mode"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "mode - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, mode(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","mode"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "mode - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, mode(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
			skip:    true,
		},
		&Query{
			name:    "spread - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT spread(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","spread"],"values":[["2000-01-01T00:00:00Z",30],["2000-01-01T00:00:30Z",10],["2000-01-01T00:01:00Z",85]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "spread - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, spread(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","spread"],"values":[["2000-01-01T00:00:00Z",30],["2000-01-01T00:00:30Z",10],["2000-01-01T00:01:00Z",85]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "spread - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, spread(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
		},
		&Query{
			name:    "stddev - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT stddev(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","stddev"],"values":[["2000-01-01T00:00:00Z",17.320508075688775],["2000-01-01T00:00:30Z",5.773502691896258],["2000-01-01T00:01:00Z",44.44097208657794]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "stddev - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, stddev(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","stddev"],"values":[["2000-01-01T00:00:00Z",17.320508075688775],["2000-01-01T00:00:30Z",5.773502691896258],["2000-01-01T00:01:00Z",44.44097208657794]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "stddev - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, stddev(rx) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"error":"mixing aggregate and non-aggregate queries is not supported"}]}`,
			skip:    true,
		},
		&Query{
			name:    "percentile - baseline 30s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT percentile(rx, 75) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","percentile"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "percentile - time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT time, percentile(rx, 75) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","percentile"],"values":[["2000-01-01T00:00:00Z",40],["2000-01-01T00:00:30Z",50],["2000-01-01T00:01:00Z",70]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "percentile - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT tx, percentile(rx, 75) FROM network where time >= '2000-01-01T00:00:00Z' AND time <= '2000-01-01T00:01:29Z' group by time(30s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","tx","percentile"],"values":[["2000-01-01T00:00:00Z",50,40],["2000-01-01T00:00:30Z",70,50],["2000-01-01T00:01:00Z",30,70]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_ExactTimeRange(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu value=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00.000000000Z").UnixNano()),
		fmt.Sprintf(`cpu value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00.000000001Z").UnixNano()),
		fmt.Sprintf(`cpu value=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00.000000002Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "query point at exactly one time - rfc3339nano",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu WHERE time = '2000-01-01T00:00:00.000000001Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00.000000001Z",2]]}]}]}`,
		},
		&Query{
			name:    "query point at exactly one time - timestamp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu WHERE time = 946684800000000001`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00.000000001Z",2]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Selectors(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`network,host=server01,region=west,cores=1 rx=10i,tx=20i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`network,host=server02,region=west,cores=2 rx=40i,tx=50i,core=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`network,host=server03,region=east,cores=3 rx=40i,tx=55i,core=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`network,host=server04,region=east,cores=4 rx=40i,tx=60i,core=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`network,host=server05,region=west,cores=1 rx=50i,tx=70i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
		fmt.Sprintf(`network,host=server06,region=east,cores=2 rx=50i,tx=40i,core=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:50Z").UnixNano()),
		fmt.Sprintf(`network,host=server07,region=west,cores=3 rx=70i,tx=30i,core=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`network,host=server08,region=east,cores=4 rx=90i,tx=10i,core=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:10Z").UnixNano()),
		fmt.Sprintf(`network,host=server09,region=east,cores=1 rx=5i,tx=4i,core=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "max - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(tx) FROM network`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","max"],"values":[["2000-01-01T00:00:40Z",70]]}]}]}`,
		},
		&Query{
			name:    "min - tx",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(tx) FROM network`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","min"],"values":[["2000-01-01T00:01:20Z",4]]}]}]}`,
		},
		&Query{
			name:    "first",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(tx) FROM network`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",20]]}]}]}`,
		},
		&Query{
			name:    "last",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(tx) FROM network`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","last"],"values":[["2000-01-01T00:01:20Z",4]]}]}]}`,
		},
		&Query{
			name:    "percentile",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT percentile(tx, 50) FROM network`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"network","columns":["time","percentile"],"values":[["2000-01-01T00:00:50Z",40]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_TopBottomInt(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		// cpu data with overlapping duplicate values
		// hour 0
		fmt.Sprintf(`cpu,host=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02 value=3.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		// hour 1
		fmt.Sprintf(`cpu,host=server04 value=3.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server05 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:10Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server06 value=6.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:20Z").UnixNano()),
		// hour 2
		fmt.Sprintf(`cpu,host=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:10Z").UnixNano()),

		// memory data
		// hour 0
		fmt.Sprintf(`memory,host=a,service=redis value=1000i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`memory,host=b,service=mysql value=2000i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`memory,host=b,service=redis value=1500i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		// hour 1
		fmt.Sprintf(`memory,host=a,service=redis value=1001i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`memory,host=b,service=mysql value=2001i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`memory,host=b,service=redis value=1501i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		// hour 2
		fmt.Sprintf(`memory,host=a,service=redis value=1002i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
		fmt.Sprintf(`memory,host=b,service=mysql value=2002i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
		fmt.Sprintf(`memory,host=b,service=redis value=1502i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "top - cpu",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, 1) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top"],"values":[["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - cpu",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, 1) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "top - cpu - 2 values",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, 2) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top"],"values":[["2000-01-01T01:00:10Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - cpu - 2 values",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, 2) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3]]}]}]}`,
		},
		&Query{
			name:    "top - cpu - 3 values - sorts on tie properly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, 3) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top"],"values":[["2000-01-01T01:00:10Z",7],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - cpu - 3 values - sorts on tie properly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, 3) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3],["2000-01-01T01:00:00Z",3]]}]}]}`,
		},
		&Query{
			name:    "top - cpu - with tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, host, 2) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top","host"],"values":[["2000-01-01T01:00:10Z",7,"server05"],["2000-01-01T02:00:10Z",9,"server08"]]}]}]}`,
		},
		&Query{
			name:    "bottom - cpu - with tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, host, 2) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom","host"],"values":[["2000-01-01T00:00:00Z",2,"server01"],["2000-01-01T00:00:10Z",3,"server02"]]}]}]}`,
		},
		&Query{
			name:    "top - cpu - 3 values with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, 3) FROM cpu limit 2`,
			exp:     `{"results":[{"statement_id":0,"error":"limit (3) in top function can not be larger than the LIMIT (2) in the select statement"}]}`,
		},
		&Query{
			name:    "bottom - cpu - 3 values with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, 3) FROM cpu limit 2`,
			exp:     `{"results":[{"statement_id":0,"error":"limit (3) in bottom function can not be larger than the LIMIT (2) in the select statement"}]}`,
		},
		&Query{
			name:    "top - cpu - hourly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, 1) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top"],"values":[["2000-01-01T00:00:20Z",4],["2000-01-01T01:00:10Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - cpu - hourly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, 1) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T01:00:00Z",3],["2000-01-01T02:00:00Z",7]]}]}]}`,
		},
		&Query{
			name:    "top - cpu - 2 values hourly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, 2) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top"],"values":[["2000-01-01T00:00:10Z",3],["2000-01-01T00:00:20Z",4],["2000-01-01T01:00:10Z",7],["2000-01-01T01:00:20Z",6],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - cpu - 2 values hourly",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, 2) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3],["2000-01-01T01:00:00Z",3],["2000-01-01T01:00:20Z",6],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "top - cpu - 3 values hourly - validates that a bucket can have less than limit if no values exist in that time bucket",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, 3) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3],["2000-01-01T00:00:20Z",4],["2000-01-01T01:00:00Z",3],["2000-01-01T01:00:10Z",7],["2000-01-01T01:00:20Z",6],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "bottom - cpu - 3 values hourly - validates that a bucket can have less than limit if no values exist in that time bucket",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, 3) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T02:00:10Z' group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:10Z",3],["2000-01-01T00:00:20Z",4],["2000-01-01T01:00:00Z",3],["2000-01-01T01:00:10Z",7],["2000-01-01T01:00:20Z",6],["2000-01-01T02:00:00Z",7],["2000-01-01T02:00:10Z",9]]}]}]}`,
		},
		&Query{
			name:    "top - memory - 2 values, two tags",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, 2), host, service FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","top","host","service"],"values":[["2000-01-01T01:00:00Z",2001,"b","mysql"],["2000-01-01T02:00:00Z",2002,"b","mysql"]]}]}]}`,
		},
		&Query{
			name:    "bottom - memory - 2 values, two tags",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, 2), host, service FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","bottom","host","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","redis"],["2000-01-01T01:00:00Z",1001,"a","redis"]]}]}]}`,
		},
		&Query{
			name:    "top - memory - host tag with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, host, 2) FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","top","host"],"values":[["2000-01-01T02:00:00Z",2002,"b"],["2000-01-01T02:00:00Z",1002,"a"]]}]}]}`,
		},
		&Query{
			name:    "bottom - memory - host tag with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, host, 2) FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","bottom","host"],"values":[["2000-01-01T00:00:00Z",1000,"a"],["2000-01-01T00:00:00Z",1500,"b"]]}]}]}`,
		},
		&Query{
			name:    "top - memory - host tag with limit 2, service tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, host, 2), service FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","top","host","service"],"values":[["2000-01-01T02:00:00Z",2002,"b","mysql"],["2000-01-01T02:00:00Z",1002,"a","redis"]]}]}]}`,
		},
		&Query{
			name:    "bottom - memory - host tag with limit 2, service tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, host, 2), service FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","bottom","host","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","redis"],["2000-01-01T00:00:00Z",1500,"b","redis"]]}]}]}`,
		},
		&Query{
			name:    "top - memory - service tag with limit 2, host tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, service, 2), host FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","top","service","host"],"values":[["2000-01-01T02:00:00Z",2002,"mysql","b"],["2000-01-01T02:00:00Z",1502,"redis","b"]]}]}]}`,
		},
		&Query{
			name:    "bottom - memory - service tag with limit 2, host tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, service, 2), host FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","bottom","service","host"],"values":[["2000-01-01T00:00:00Z",1000,"redis","a"],["2000-01-01T00:00:00Z",2000,"mysql","b"]]}]}]}`,
		},
		&Query{
			name:    "top - memory - host and service tag with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, host, service, 2) FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","top","host","service"],"values":[["2000-01-01T02:00:00Z",2002,"b","mysql"],["2000-01-01T02:00:00Z",1502,"b","redis"]]}]}]}`,
		},
		&Query{
			name:    "bottom - memory - host and service tag with limit 2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, host, service, 2) FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","bottom","host","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","redis"],["2000-01-01T00:00:00Z",1500,"b","redis"]]}]}]}`,
		},
		&Query{
			name:    "top - memory - host tag with limit 2 with service tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, host, 2), service FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","top","host","service"],"values":[["2000-01-01T02:00:00Z",2002,"b","mysql"],["2000-01-01T02:00:00Z",1002,"a","redis"]]}]}]}`,
		},
		&Query{
			name:    "bottom - memory - host tag with limit 2 with service tag in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, host, 2), service FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","bottom","host","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","redis"],["2000-01-01T00:00:00Z",1500,"b","redis"]]}]}]}`,
		},
		&Query{
			name:    "top - memory - host and service tag with limit 3",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT TOP(value, host, service, 3) FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","top","host","service"],"values":[["2000-01-01T02:00:00Z",2002,"b","mysql"],["2000-01-01T02:00:00Z",1502,"b","redis"],["2000-01-01T02:00:00Z",1002,"a","redis"]]}]}]}`,
		},
		&Query{
			name:    "bottom - memory - host and service tag with limit 3",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT BOTTOM(value, host, service, 3) FROM memory`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"memory","columns":["time","bottom","host","service"],"values":[["2000-01-01T00:00:00Z",1000,"a","redis"],["2000-01-01T00:00:00Z",1500,"b","redis"],["2000-01-01T00:00:00Z",2000,"b","mysql"]]}]}]}`,
		},

		// TODO
		// - Test that specifiying fields or tags in the function will rewrite the query to expand them to the fields
		// - Test that a field can be used in the top function
		// - Test that asking for a field will come back before a tag if they have the same name for a tag and a field
		// - Test that `select top(value, host, 2)` when there is only one value for `host` it will only bring back one value
		// - Test that `select top(value, host, 4) from foo where time > now() - 1d and time < now() group by time(1h)` and host is unique in some time buckets that it returns only the unique ones, and not always 4 values

	}...)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP: %s", query.name)
			}

			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_TopBottomWriteTags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=2.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02 value=3.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server03 value=4.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		// hour 1
		fmt.Sprintf(`cpu,host=server04 value=5.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server05 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:10Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server06 value=6.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T01:00:20Z").UnixNano()),
		// hour 2
		fmt.Sprintf(`cpu,host=server07 value=7.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server08 value=9.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T02:00:10Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "top - write - with tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT top(value, host, 2) INTO cpu_top FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"result","columns":["time","written"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "top - read results with tags",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu_top GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_top","tags":{"host":"server05"},"columns":["time","top"],"values":[["2000-01-01T01:00:10Z",7]]},{"name":"cpu_top","tags":{"host":"server08"},"columns":["time","top"],"values":[["2000-01-01T02:00:10Z",9]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "top - read results as fields",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu_top`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_top","columns":["time","host","top"],"values":[["2000-01-01T01:00:10Z","server05",7],["2000-01-01T02:00:10Z","server08",9]]}]}]}`,
			skip:    true,
		},
	}...)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			}
			if query.skip {
				t.Skipf("SKIP: %s", query.name)
			}

			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

// Test various aggregates when different series only have data for the same timestamp.
func TestServer_Query_Aggregates_IdenticalTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`series,host=a value=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,host=b value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,host=c value=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,host=d value=4 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,host=e value=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,host=f value=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,host=g value=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,host=h value=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`series,host=i value=5 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "last from multiple series with identical timestamp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT last(value) FROM "series"`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"series","columns":["time","last"],"values":[["2000-01-01T00:00:00Z",5]]}]}]}`,
			repeat:  100,
		},
		&Query{
			name:    "first from multiple series with identical timestamp",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT first(value) FROM "series"`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"series","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",5]]}]}]}`,
			repeat:  100,
		},
	}...)

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
			for n := 0; n <= query.repeat; n++ {
				if err := query.Execute(s); err != nil {
					t.Error(query.Error(err))
				} else if !query.success() {
					t.Error(query.failureMessage())
				}
			}
		})
	}
}

// This will test that when using a group by, that it observes the time you asked for
// but will only put the values in the bucket that match the time range
func TestServer_Query_GroupByTimeCutoffs(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01 value=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01 value=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:05Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01 value=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:08Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01 value=5i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:09Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01 value=6i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	test.addQueries([]*Query{
		&Query{
			name:    "sum all time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",21]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T00:00:10Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:05Z",12],["2000-01-01T00:00:10Z",6]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s missing first point",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM cpu where time >= '2000-01-01T00:00:01Z' and time <= '2000-01-01T00:00:10Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",2],["2000-01-01T00:00:05Z",12],["2000-01-01T00:00:10Z",6]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s missing first points (null for bucket)",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM cpu where time >= '2000-01-01T00:00:02Z' and time <= '2000-01-01T00:00:10Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",null],["2000-01-01T00:00:05Z",12],["2000-01-01T00:00:10Z",6]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s missing last point - 2 time intervals",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T00:00:09Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:05Z",12]]}]}]}`,
		},
		&Query{
			name:    "sum all time grouped by time 5s missing last 2 points - 2 time intervals",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM cpu where time >= '2000-01-01T00:00:00Z' and time <= '2000-01-01T00:00:08Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",3],["2000-01-01T00:00:05Z",7]]}]}]}`,
		},
	}...)

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

func TestServer_Query_MapType(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server02 speed=25 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "query value with a single measurement",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT value FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			name:    "query wildcard with a single measurement",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","value"],"values":[["2000-01-01T00:00:00Z","server01",2]]}]}]}`,
		},
		&Query{
			name:    "query value with multiple measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT value FROM cpu, gpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "query wildcard with multiple measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu, gpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","speed","value"],"values":[["2000-01-01T00:00:00Z","server01",null,2]]},{"name":"gpu","columns":["time","host","speed","value"],"values":[["2000-01-01T00:00:00Z","server02",25,null]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "query value with a regex measurement",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT value FROM /[cg]pu/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "query wildcard with a regex measurement",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM /[cg]pu/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host","speed","value"],"values":[["2000-01-01T00:00:00Z","server01",null,2]]},{"name":"gpu","columns":["time","host","speed","value"],"values":[["2000-01-01T00:00:00Z","server02",25,null]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Subqueries(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 usage_user=70i,usage_system=30i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01 usage_user=45i,usage_system=55i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01 usage_user=23i,usage_system=77i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02 usage_user=11i,usage_system=89i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02 usage_user=28i,usage_system=72i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02 usage_user=12i,usage_system=53i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT STDDEV(value) FROM stringdata`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"stringdata","columns":["time","stddev"],"values":[["1970-01-01T00:00:00Z",null]]}]}]}`,
			skip:    true, // FIXME(benbjohnson): allow non-float var ref expr in cursor iterator
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT value FROM (SELECT mean(usage_user) AS value FROM cpu) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",31.5]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(usage) FROM (SELECT 100 - usage_user AS usage FROM cpu) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",68.5]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT host FROM (SELECT min(usage_user), host FROM cpu) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host"],"values":[["2000-01-01T00:00:00Z","server02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT host FROM (SELECT min(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host"],"values":[["2000-01-01T00:00:00Z","server02"],["2000-01-01T00:00:20Z","server01"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT host FROM (SELECT min(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' GROUP BY host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","host"],"values":[["2000-01-01T00:00:20Z","server01"]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","host"],"values":[["2000-01-01T00:00:00Z","server02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(min) FROM (SELECT min(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",17]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(min) FROM (SELECT (min(usage_user)) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",17]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(min), host FROM (SELECT min(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max","host"],"values":[["2000-01-01T00:00:20Z",23,"server01"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean, host FROM (SELECT mean(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean","host"],"values":[["2000-01-01T00:00:00Z",46,"server01"],["2000-01-01T00:00:00Z",17,"server02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT host FROM (SELECT mean(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","host"],"values":[["2000-01-01T00:00:00Z","server01"],["2000-01-01T00:00:00Z","server02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max(usage_system) FROM (SELECT min(usage_user), usage_system FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",89]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(top), host FROM (SELECT top(usage_user, host, 2) FROM cpu) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","host"],"values":[["2000-01-01T00:00:10Z",28,"server02"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(top), host FROM (SELECT top(usage_user, 2), host FROM cpu) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","host"],"values":[["2000-01-01T00:00:10Z",45,"server01"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT count(host) FROM (SELECT top(usage_user, host, 2) FROM cpu) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["2000-01-01T00:00:00Z",2]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(derivative) FROM (SELECT derivative(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",-4.6]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(max) FROM (SELECT 100 - max(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",30]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(usage_system) FROM (SELECT max(usage_user), 100 - usage_system FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min"],"values":[["2000-01-01T00:00:10Z",28]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(value) FROM (SELECT max(usage_user), usage_user - usage_system AS value FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min"],"values":[["2000-01-01T00:00:10Z",-44]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(value) FROM (SELECT top(usage_user, 2), usage_user - usage_system AS value FROM cpu) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' GROUP BY host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","min"],"values":[["2000-01-01T00:00:10Z",-10]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","min"],"values":[["2000-01-01T00:00:10Z",-44]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT min(value) FROM (SELECT max(usage_user), usage_user - usage_system AS value FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND host = 'server01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",40]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT value FROM (SELECT max(usage_user), usage_user - usage_system AS value FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND value > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",40]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max FROM (SELECT max(usage_user) FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND host = 'server01'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max"],"values":[["2000-01-01T00:00:00Z",70]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(value) FROM (SELECT max(usage_user), usage_user - usage_system AS value FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND value > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",40]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(value) FROM (SELECT max(usage_user), usage_user - usage_system AS value FROM cpu GROUP BY host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z' AND host =~ /server/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",-2]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT top(usage_system, host, 2) FROM (SELECT min(usage_user), usage_system FROM cpu GROUP BY time(20s), host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","top","host"],"values":[["2000-01-01T00:00:00Z",89,"server02"],["2000-01-01T00:00:20Z",77,"server01"]]}]}]}`,
		},
		&Query{
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT bottom(usage_system, host, 2) FROM (SELECT max(usage_user), usage_system FROM cpu GROUP BY time(20s), host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:30Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","bottom","host"],"values":[["2000-01-01T00:00:00Z",30,"server01"],["2000-01-01T00:00:20Z",53,"server02"]]}]}]}`,
		},
	}...)
}

func TestServer_Query_SubqueryWithGroupBy(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01,region=uswest value=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=4i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=uswest value=5i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=uswest value=6i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=uswest value=7i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=uswest value=8i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=9i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=10i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=11i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=12i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=13i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=14i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=15i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=16i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "group by time(2s) - time(2s), host",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(mean) FROM (SELECT mean(value) FROM cpu GROUP BY time(2s), host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:04Z' GROUP BY time(2s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",7.5],["2000-01-01T00:00:02Z",9.5]]}]}]}`,
		},
		&Query{
			name:    "group by time(4s), host - time(2s), host",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(mean) FROM (SELECT mean(value) FROM cpu GROUP BY time(2s), host) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:04Z' GROUP BY time(4s), host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",6.5]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",10.5]]}]}]}`,
		},
		&Query{
			name:    "group by time(2s), host - time(2s), host, region",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(mean) FROM (SELECT mean(value) FROM cpu GROUP BY time(2s), host, region) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:04Z' GROUP BY time(2s), host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",5.5],["2000-01-01T00:00:02Z",7.5]]},{"name":"cpu","tags":{"host":"server02"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",9.5],["2000-01-01T00:00:02Z",11.5]]}]}]}`,
		},
	}...)

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

func TestServer_Query_SubqueryForLogicalOptimize(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=azhu no=1i,age=12.3,height=70i,address="shenzhen",alive=TRUE 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan no=2i,age=20.5,height=80i,address="shanghai",alive=FALSE 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang no=3i,age=3.4,height=90i,address="beijin",alive=TRUE 1629129602000000000`),
		fmt.Sprintf(`mst,country=japan,name=ahui no=4i,age=30,height=121i,address="guangzhou",alive=FALSE 1629129603000000000`),
		fmt.Sprintf(`mst,country=canada,name=aqiu no=5i,age=35,height=138i,address="chengdu",alive=TRUE 1629129604000000000`),
		fmt.Sprintf(`mst,country=china,name=agang no=6i,age=48.8,height=149i,address="wuhan" 1629129605000000000`),
		fmt.Sprintf(`mst,country=american,name=agan no=7i,age=52.7,height=153i,alive=TRUE 1629129606000000000`),
		fmt.Sprintf(`mst,country=germany,name=alin no=8i,age=28.3,address="anhui",alive=FALSE 1629129607000000000`),
		fmt.Sprintf(`mst,country=japan,name=ali no=9i,height=179i,address="xian",alive=TRUE 1629129608000000000`),
		fmt.Sprintf(`mst,country=canada no=10i,age=60.8,height=180i,address="hangzhou",alive=FALSE 1629129609000000000`),
		fmt.Sprintf(`mst,name=ahuang no=11i,age=102,height=191i,address="nanjin",alive=TRUE 1629129610000000000`),
		fmt.Sprintf(`mst,country=china,name=ayin no=12i,age=123,height=203i,address="zhengzhou",alive=FALSE 1629129611000000000`),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "sum no_height",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(sum_height) FROM (SELECT sum(height) as sum_height FROM (select * from mst where time >= 1629129600000000000 and time <= 1629129611000000000))`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",1554]]}]}]}`,
		},
		&Query{
			name:    "sum height_alias",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(sum_height) FROM (SELECT sum(a) as sum_height FROM (select height as a from mst where time >= 1629129600000000000 and time <= 1629129611000000000))`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",1554]]}]}]}`,
		},
		&Query{
			name:    "sum height_binary_alias",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(a) as sum_height, sum(a)+sum(b), mean(a) FROM (select height as a, age as b from mst where time >= 1629129600000000000 and time <= 1629129611000000000)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum_height","sum_sum","mean"],"values":[["2021-08-16T16:00:00Z",1554,2070.8,141.27272727272728]]}]}]}`,
		},
		&Query{
			name:    "sum sum_height, *",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(sum_height) FROM (SELECT sum(height) as sum_height FROM (select * from mst where time >= 1629129600000000000 and time <= 1629129611000000000)) where time >= 1629129600000000000 and time <= 1629129611000000000 group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum"],"values":[["2021-08-16T16:00:00Z",1554],["2021-08-16T16:00:01Z",null],["2021-08-16T16:00:02Z",null],["2021-08-16T16:00:03Z",null],["2021-08-16T16:00:04Z",null],["2021-08-16T16:00:05Z",null],["2021-08-16T16:00:06Z",null],["2021-08-16T16:00:07Z",null],["2021-08-16T16:00:08Z",null],["2021-08-16T16:00:09Z",null],["2021-08-16T16:00:10Z",null],["2021-08-16T16:00:11Z",null]]}]}]}`,
		},
		&Query{
			name:    "mean usage",
			params:  url.Values{"db": []string{"db0"}},
			command: `select mean(usage) from (select 100 - age as usage from mst where time >= 1629129600000000000 and time <= 1629129611000000000)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",53.01818181818182]]}]}]}`,
		},
		&Query{
			name:    "outer filter#1",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM (SELECT max(age), age-height AS value FROM mst GROUP BY country) WHERE time >= '2021-08-16T16:00:00Z' AND time < '2021-08-16T16:00:11Z' AND value < 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","country","max","value"],"values":[["2021-08-16T16:00:10Z","",102,-89],["2021-08-16T16:00:06Z","american",52.7,-100.3],["2021-08-16T16:00:09Z","canada",60.8,-119.2],["2021-08-16T16:00:05Z","china",48.8,-100.2],["2021-08-16T16:00:03Z","japan",30,-91]]}]}]}`,
		},
		&Query{
			name:    "outer filter#2",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT max FROM (SELECT max(age) FROM mst GROUP BY country) WHERE time >= '2021-08-16T16:00:00Z' AND time < '2021-08-16T16:00:11Z' AND country = 'china'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","max"],"values":[["2021-08-16T16:00:05Z",48.8]]}]}]}`,
		},
		&Query{
			name:    "outer filter#3",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(value) FROM (SELECT max(age), age - height AS value FROM mst GROUP BY country) WHERE time >= '2021-08-16T16:00:00Z' AND time < '2021-08-16T16:00:11Z' AND value < 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","mean"],"values":[["2021-08-16T16:00:00Z",-99.94]]}]}]}`,
		},
	}...)

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

func TestServer_Query_NewChunkTagCheckList(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mem,t1=10.172.161.227:31533\,10.172.161.227:31533 value=1 1625558240121000000`),
		fmt.Sprintf(`mem,t1=10.172.161.227:31533 value=1 1625558240122000000`),
		fmt.Sprintf(`mem,t1=10.172.161.227:31533\,10.172.161.227:31533\,10.172.161.227:31533 value=1 1625558240123000000`),
		fmt.Sprintf(`mst,country=china,name=azhu no=1i,age=12.3,height=70i,address="shenzhen",alive=TRUE 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan no=2i,age=20.5,height=80i,address="shanghai",alive=FALSE 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang no=3i,age=3.4,height=90i,address="beijin",alive=TRUE 1629129602000000000`),
		fmt.Sprintf(`mst,country=japan,name=ahui no=4i,age=30,height=121i,address="guangzhou",alive=FALSE 1629129603000000000`),
		fmt.Sprintf(`mst,country=canada,name=aqiu no=5i,age=35,height=138i,address="chengdu",alive=TRUE 1629129604000000000`),
		fmt.Sprintf(`mst,country=china,name=agang no=6i,age=48.8,height=149i,address="wuhan" 1629129605000000000`),
		fmt.Sprintf(`mst,country=american,name=agan no=7i,age=52.7,height=153i,alive=TRUE 1629129606000000000`),
		fmt.Sprintf(`mst,country=germany,name=alin no=8i,age=28.3,address="anhui",alive=FALSE 1629129607000000000`),
		fmt.Sprintf(`mst,country=japan,name=ali no=9i,height=179i,address="xian",alive=TRUE 1629129608000000000`),
		fmt.Sprintf(`mst,country=canada no=10i,age=60.8,height=180i,address="hangzhou",alive=FALSE 1629129609000000000`),
		fmt.Sprintf(`mst,name=ahuang no=11i,age=102,height=191i,address="nanjin",alive=TRUE 1629129610000000000`),
		fmt.Sprintf(`mst,country=china,name=ayin no=12i,age=123,height=203i,address="zhengzhou",alive=FALSE 1629129611000000000`),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select *  group by * ",
			params:  url.Values{"db": []string{"db0"}},
			command: `select * from mem group by *::tag`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mem","tags":{"t1":"10.172.161.227:31533"},"columns":["time","value"],"values":[["2021-07-06T07:57:20.122Z",1]]},{"name":"mem","tags":{"t1":"10.172.161.227:31533,10.172.161.227:31533"},"columns":["time","value"],"values":[["2021-07-06T07:57:20.121Z",1]]},{"name":"mem","tags":{"t1":"10.172.161.227:31533,10.172.161.227:31533,10.172.161.227:31533"},"columns":["time","value"],"values":[["2021-07-06T07:57:20.123Z",1]]}]}]}`,
		},
		&Query{
			name:    "select min(*) group by * ",
			params:  url.Values{"db": []string{"db0"}},
			command: `select min(*) from mst group by *::tag`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"country":"","name":"ahuang"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",102,true,191,11]]},{"name":"mst","tags":{"country":"american","name":"agan"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",52.7,true,153,7]]},{"name":"mst","tags":{"country":"american","name":"alan"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",20.5,false,80,2]]},{"name":"mst","tags":{"country":"canada","name":""},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",60.8,false,180,10]]},{"name":"mst","tags":{"country":"canada","name":"aqiu"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",35,true,138,5]]},{"name":"mst","tags":{"country":"china","name":"agang"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",48.8,null,149,6]]},{"name":"mst","tags":{"country":"china","name":"ayin"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",123,false,203,12]]},{"name":"mst","tags":{"country":"china","name":"azhu"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",12.3,true,70,1]]},{"name":"mst","tags":{"country":"germany","name":"alang"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",3.4,true,90,3]]},{"name":"mst","tags":{"country":"germany","name":"alin"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",28.3,false,null,8]]},{"name":"mst","tags":{"country":"japan","name":"ahui"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",30,false,121,4]]},{"name":"mst","tags":{"country":"japan","name":"ali"},"columns":["time","min_age","min_alive","min_height","min_no"],"values":[["1970-01-01T00:00:00Z",null,true,179,9]]}]}]}`,
		},
	}...)

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

func TestServer_Query_MultiMeasurements(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst1,country=china,name=ada age=15 1625558240121000000`),
		fmt.Sprintf(`mst1,country=china,name=billy age=27 1625558240122000000`),
		fmt.Sprintf(`mst1,country=china,name=demon age=57 1625558240123000000`),
		fmt.Sprintf(`mst1,country=China,name=king age=22 1625558240124000000`),
		fmt.Sprintf(`mst1,country=Egypt,name=chris age=31 1625558242121000000`),
		fmt.Sprintf(`mst1,country=Egypt,name=daisy age=40 1625558242122000000`),
		fmt.Sprintf(`mst1,country=France,name=paul age=45 1625558242123000000`),
		fmt.Sprintf(`mst1,country=Germany,name=frank age=35 1625558242124000000`),
		fmt.Sprintf(`mst1,country=Japan,name=jack age=21 1625558242125000000`),
		fmt.Sprintf(`mst,country=china,name=azhu no=1i,age=12.3,height=70i,address="shenzhen",alive=TRUE 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan no=2i,age=20.5,height=80i,address="shanghai",alive=FALSE 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang no=3i,age=3.4,height=90i,address="beijin",alive=TRUE 1629129602000000000`),
		fmt.Sprintf(`mst,country=japan,name=ahui no=4i,age=30,height=121i,address="guangzhou",alive=FALSE 1629129603000000000`),
		fmt.Sprintf(`mst,country=canada,name=aqiu no=5i,age=35,height=138i,address="chengdu",alive=TRUE 1629129604000000000`),
		fmt.Sprintf(`mst,country=china,name=agang no=6i,age=48.8,height=149i,address="wuhan" 1629129605000000000`),
		fmt.Sprintf(`mst,country=american,name=agan no=7i,age=52.7,height=153i,alive=TRUE 1629129606000000000`),
		fmt.Sprintf(`mst,country=germany,name=alin no=8i,age=28.3,address="anhui",alive=FALSE 1629129607000000000`),
		fmt.Sprintf(`mst,country=japan,name=ali no=9i,height=179i,address="xian",alive=TRUE 1629129608000000000`),
		fmt.Sprintf(`mst,country=canada no=10i,age=60.8,height=180i,address="hangzhou",alive=FALSE 1629129609000000000`),
		fmt.Sprintf(`mst,name=ahuang no=11i,age=102,height=191i,address="nanjin",alive=TRUE 1629129610000000000`),
		fmt.Sprintf(`mst,country=china,name=ayin no=12i,age=123,height=203i,address="zhengzhou",alive=FALSE 1629129611000000000`),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select * from measurements ",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM mst,mst1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst,mst1","columns":["time","address","age","alive","country","height","name","no"],"values":[["2021-07-06T07:57:20.121Z",null,15,null,"china",null,"ada",null],["2021-07-06T07:57:20.122Z",null,27,null,"china",null,"billy",null],["2021-07-06T07:57:20.123Z",null,57,null,"china",null,"demon",null],["2021-07-06T07:57:20.124Z",null,22,null,"China",null,"king",null],["2021-07-06T07:57:22.121Z",null,31,null,"Egypt",null,"chris",null],["2021-07-06T07:57:22.122Z",null,40,null,"Egypt",null,"daisy",null],["2021-07-06T07:57:22.123Z",null,45,null,"France",null,"paul",null],["2021-07-06T07:57:22.124Z",null,35,null,"Germany",null,"frank",null],["2021-07-06T07:57:22.125Z",null,21,null,"Japan",null,"jack",null],["2021-08-16T16:00:00Z","shenzhen",12.3,true,"china",70,"azhu",1],["2021-08-16T16:00:01Z","shanghai",20.5,false,"american",80,"alan",2],["2021-08-16T16:00:02Z","beijin",3.4,true,"germany",90,"alang",3],["2021-08-16T16:00:03Z","guangzhou",30,false,"japan",121,"ahui",4],["2021-08-16T16:00:04Z","chengdu",35,true,"canada",138,"aqiu",5],["2021-08-16T16:00:05Z","wuhan",48.8,null,"china",149,"agang",6],["2021-08-16T16:00:06Z",null,52.7,true,"american",153,"agan",7],["2021-08-16T16:00:07Z","anhui",28.3,false,"germany",null,"alin",8],["2021-08-16T16:00:08Z","xian",null,true,"japan",179,"ali",9],["2021-08-16T16:00:09Z","hangzhou",60.8,false,"canada",180,null,10],["2021-08-16T16:00:10Z","nanjin",102,true,null,191,"ahuang",11],["2021-08-16T16:00:11Z","zhengzhou",123,false,"china",203,"ayin",12]]}]}]}`,
		},
		&Query{
			name:    "select * from measurements where ",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM mst,mst1 where country='china'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst,mst1","columns":["time","address","age","alive","country","height","name","no"],"values":[["2021-07-06T07:57:20.121Z",null,15,null,"china",null,"ada",null],["2021-07-06T07:57:20.122Z",null,27,null,"china",null,"billy",null],["2021-07-06T07:57:20.123Z",null,57,null,"china",null,"demon",null],["2021-08-16T16:00:00Z","shenzhen",12.3,true,"china",70,"azhu",1],["2021-08-16T16:00:05Z","wuhan",48.8,null,"china",149,"agang",6],["2021-08-16T16:00:11Z","zhengzhou",123,false,"china",203,"ayin",12]]}]}]}`,
		},
		&Query{
			name:    "select field from measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT age FROM mst,mst1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst,mst1","columns":["time","age"],"values":[["2021-07-06T07:57:20.121Z",15],["2021-07-06T07:57:20.122Z",27],["2021-07-06T07:57:20.123Z",57],["2021-07-06T07:57:20.124Z",22],["2021-07-06T07:57:22.121Z",31],["2021-07-06T07:57:22.122Z",40],["2021-07-06T07:57:22.123Z",45],["2021-07-06T07:57:22.124Z",35],["2021-07-06T07:57:22.125Z",21],["2021-08-16T16:00:00Z",12.3],["2021-08-16T16:00:01Z",20.5],["2021-08-16T16:00:02Z",3.4],["2021-08-16T16:00:03Z",30],["2021-08-16T16:00:04Z",35],["2021-08-16T16:00:05Z",48.8],["2021-08-16T16:00:06Z",52.7],["2021-08-16T16:00:07Z",28.3],["2021-08-16T16:00:09Z",60.8],["2021-08-16T16:00:10Z",102],["2021-08-16T16:00:11Z",123]]}]}]}`,
		},
		&Query{
			name:    "select subqueries from measurements_1",
			params:  url.Values{"db": []string{"db0"}},
			command: `select sum(a),sum(b) from (select min(age) as a from mst1),(select sum(age) as b from mst1)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst1","columns":["time","sum","sum_1"],"values":[["1970-01-01T00:00:00Z",15,293]]}]}]}`,
		},
		&Query{
			name:    "select subqueries from measurements_2",
			params:  url.Values{"db": []string{"db0"}},
			command: `select sum(a)+sum(b) from (select sum(age) as a from mst1),(select sum(age) as b from mst1)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst1","columns":["time","sum_sum"],"values":[["1970-01-01T00:00:00Z",586]]}]}]}`,
		},
		&Query{
			name:    "select subqueries from measurements_3",
			params:  url.Values{"db": []string{"db0"}},
			command: `select sum(a),sum(b) from (select count(age) as a from mst where country='china' and time >= 1629129600000000000 and time <= 1629129611000000000 group by time(1s)),(select count(age) as b from mst where time >= 1629129600000000000 and time <= 1629129611000000000 group by time(1s))`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum","sum_1"],"values":[["1970-01-01T00:00:00Z",3,11]]}]}]}`,
		},
		&Query{
			name:    "select agg from measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT count(age) FROM mst,mst1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst,mst1","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",20]]}]}]}`,
		},
		&Query{
			name:    "select agg subqueries from measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `select a from (select sum(age) as a from mst1,mst group by country)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst,mst1","columns":["time","a"],"values":[["1970-01-01T00:00:00Z",102],["1970-01-01T00:00:00Z",22],["1970-01-01T00:00:00Z",71],["1970-01-01T00:00:00Z",45],["1970-01-01T00:00:00Z",35],["1970-01-01T00:00:00Z",21],["1970-01-01T00:00:00Z",73.2],["1970-01-01T00:00:00Z",95.8],["1970-01-01T00:00:00Z",283.1],["1970-01-01T00:00:00Z",31.7],["1970-01-01T00:00:00Z",30]]}]}]}`,
		},
		&Query{
			name:    "select * from regexe measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `select * from /mst.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst,mst1","columns":["time","address","age","alive","country","height","name","no"],"values":[["2021-07-06T07:57:20.121Z",null,15,null,"china",null,"ada",null],["2021-07-06T07:57:20.122Z",null,27,null,"china",null,"billy",null],["2021-07-06T07:57:20.123Z",null,57,null,"china",null,"demon",null],["2021-07-06T07:57:20.124Z",null,22,null,"China",null,"king",null],["2021-07-06T07:57:22.121Z",null,31,null,"Egypt",null,"chris",null],["2021-07-06T07:57:22.122Z",null,40,null,"Egypt",null,"daisy",null],["2021-07-06T07:57:22.123Z",null,45,null,"France",null,"paul",null],["2021-07-06T07:57:22.124Z",null,35,null,"Germany",null,"frank",null],["2021-07-06T07:57:22.125Z",null,21,null,"Japan",null,"jack",null],["2021-08-16T16:00:00Z","shenzhen",12.3,true,"china",70,"azhu",1],["2021-08-16T16:00:01Z","shanghai",20.5,false,"american",80,"alan",2],["2021-08-16T16:00:02Z","beijin",3.4,true,"germany",90,"alang",3],["2021-08-16T16:00:03Z","guangzhou",30,false,"japan",121,"ahui",4],["2021-08-16T16:00:04Z","chengdu",35,true,"canada",138,"aqiu",5],["2021-08-16T16:00:05Z","wuhan",48.8,null,"china",149,"agang",6],["2021-08-16T16:00:06Z",null,52.7,true,"american",153,"agan",7],["2021-08-16T16:00:07Z","anhui",28.3,false,"germany",null,"alin",8],["2021-08-16T16:00:08Z","xian",null,true,"japan",179,"ali",9],["2021-08-16T16:00:09Z","hangzhou",60.8,false,"canada",180,null,10],["2021-08-16T16:00:10Z","nanjin",102,true,null,191,"ahuang",11],["2021-08-16T16:00:11Z","zhengzhou",123,false,"china",203,"ayin",12]]}]}]}`,
		},
	}...)

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

func TestServer_Query_NilColumn(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=ada age=15,address="chengdu" 1625558240121000000`),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	writes2 := []string{
		fmt.Sprintf(`mst,country=china,name=ada age=15 1625558240121000000`),
		fmt.Sprintf(`mst,country=china,name=bcb height=10,age=15,address="chongqing" 1625558240122000000`),
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes2, "\n")},
	}
	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	test.addQueries([]*Query{
		&Query{
			name:    "select count(*) from measurements ",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT count(*) FROM mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","count_address","count_age","count_height"],"values":[["1970-01-01T00:00:00Z",1,2,1]]}]}]}`,
		},
	}...)

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

func TestServer_Query_MultipleFiles_NoCrossTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=China,name=ada age=15 1625558240121000000`),
		fmt.Sprintf(`mst,country=China,name=billy age=27 1625558240122000000`),
	}
	test := NewTest("db0", "rp0")
	test.writesArray = append(test.writesArray, Writes{
		&Write{data: strings.Join(writes, "\n")},
	})

	writes2 := []string{
		fmt.Sprintf(`mst,country=China,name=demon age=57 1625558242123000000`),
		fmt.Sprintf(`mst,country=China,name=king age=22 1625558242124000000`),
	}
	test.writesArray = append(test.writesArray, Writes{
		&Write{data: strings.Join(writes2, "\n")},
	})

	writes3 := []string{
		fmt.Sprintf(`mst,country=Egypt,name=chris age=31 1625558244121000000`),
		fmt.Sprintf(`mst,country=Egypt,name=daisy age=40 1625558244122000000`),
		fmt.Sprintf(`mst,country=France,name=paul age=45 1625558245123000000`),
		fmt.Sprintf(`mst,country=Germany,name=frank age=36 1625558245124000000`),
		fmt.Sprintf(`mst,country=Japan,name=jack age=21 1625558245125000000`),
	}
	test.writesArray = append(test.writesArray, Writes{
		&Write{data: strings.Join(writes3, "\n")},
	})

	test.addQueries([]*Query{
		&Query{
			name:    "select mean(*) from measurements group by time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(*) FROM mst where time >= 1625558240121000000 and time <= 1625558245125000000 group by time(1s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","mean_age"],"values":[["2021-07-06T07:57:20Z",21],["2021-07-06T07:57:21Z",null],["2021-07-06T07:57:22Z",39.5],["2021-07-06T07:57:23Z",null],["2021-07-06T07:57:24Z",35.5],["2021-07-06T07:57:25Z",34]]}]}]}`,
		},
	}...)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 0 {
				if err := test.initMultipleFiles(s); err != nil {
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

func TestServer_Query_OutOfOrder_Overlap_Column(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=china,name=azhu no=1i,age=12.3,height=70i,address="shenzhen",alive=TRUE 1629129600000000000`),
		fmt.Sprintf(`mst,country=american,name=alan no=2i,age=20.5,height=80i,address="shanghai",alive=FALSE 1629129601000000000`),
		fmt.Sprintf(`mst,country=germany,name=alang no=3i,age=3.4,height=90i,address="beijin",alive=TRUE 1629129602000000000`),
		fmt.Sprintf(`mst,country=japan,name=ahui no=4i,age=30,height=121i,address="guangzhou",alive=FALSE 1629129603000000000`),
		fmt.Sprintf(`mst,country=canada,name=aqiu no=5i,age=35,height=138i,address="chengdu",alive=TRUE 1629129604000000000`),
		fmt.Sprintf(`mst,country=china,name=agang no=6i,age=48.8,height=149i,address="wuhan" 1629129605000000000`),
		fmt.Sprintf(`mst,country=american,name=agan no=7i,age=52.7,height=153i,alive=TRUE 1629129606000000000`),
		fmt.Sprintf(`mst,country=germany,name=alin no=8i,age=28.3,address="anhui",alive=FALSE 1629129607000000000`),
		fmt.Sprintf(`mst,country=japan,name=ali no=9i,height=179i,address="xian",alive=TRUE 1629129608000000000`),
		fmt.Sprintf(`mst,country=canada no=10i,age=60.8,height=180i,address="hangzhou",alive=FALSE 1629129609000000000`),
		fmt.Sprintf(`mst,name=ahuang no=11i,age=102,height=191i,address="nanjin",alive=TRUE 1629129610000000000`),
		fmt.Sprintf(`mst,country=china,name=ayin no=12i,height=203i,address="zhengzhou",alive=FALSE 1629129611000000000`),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	if err := writeTestData(s, &test); err != nil {
		t.Fatalf("write data failed: %s", err)
	}

	writes2 := []string{
		fmt.Sprintf(`mst,country=china,name=ayin no=12i,age=1,height=20i,address="zhengzhou",alive=FALSE 1629129600000000000`),
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes2, "\n")},
	}
	if err := writeTestData(s, &test); err != nil {
		t.Fatalf("write data failed: %s", err)
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select last(height),age from mst",
			params:  url.Values{"db": []string{"db0"}},
			command: `select last(height),age from mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","last","age"],"values":[["2021-08-16T16:00:11Z",203,null]]}]}]}`,
		},
		&Query{
			name:    "select max(height),age from mst",
			params:  url.Values{"db": []string{"db0"}},
			command: `select max(height),age from mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","max","age"],"values":[["2021-08-16T16:00:11Z",203,null]]}]}]}`,
		},
	}...)

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

func TestServer_Query_PreAgg_StringAux_WithNullValue(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,host=server19900,region=tagval2_19900,core_tag=spring core="field-*_values _1_19900",tx=19900i,yx=19900,rx=True 1648190516508400896`),
		fmt.Sprintf(`mst,host=server19901,region=tagval2_19901,core_tag=summer core="field-*_values _1_19901",tx=19901i,yx=19901,rx=False 1648190516509400896`),
		fmt.Sprintf(`mst,host=server19902,region=tagval2_19902 yx=19902,rx=True 1648190516510400896`),
		fmt.Sprintf(`mst,host=server19903,region=tagval2_19903,core_tag=winter core="field-*_values _1_19903",tx=19903i,yx=19903,rx=False 1648190516511400896`),
		fmt.Sprintf(`mst,host=server19904,region=tagval2_19904,core_tag=spring core="field-*_values _1_19904",tx=19904i,yx=19904,rx=True 1648190516512400896`),
		fmt.Sprintf(`mst,host=server19905,region=tagval2_19905 yx=19905,rx=False 1648190516513400896`),
		fmt.Sprintf(`mst,host=server19906,region=tagval2_19906,core_tag=autumn core="field-*_values _1_19906",tx=19906i,yx=19906,rx=True 1648190516514400896`),
		fmt.Sprintf(`mst,host=server19907,region=tagval2_19907,core_tag=winter core="field-*_values _1_19907",tx=19907i,yx=19907,rx=False 1648190516515400896`),
		fmt.Sprintf(`mst,host=server19908,region=tagval2_19908 yx=19908,rx=True 1648190516516400896`),
		fmt.Sprintf(`mst,host=server19909,region=tagval2_19909,core_tag=summer core="field-*_values _1_19909",tx=19909i,yx=19909,rx=False 1648190516517400896`),
		fmt.Sprintf(`mst,host=server19910,region=tagval2_19910,core_tag=autumn core="field-*_values _1_19910",tx=19910i,yx=19910,rx=True 1648190516518400896`),
		fmt.Sprintf(`mst,host=server19911,region=tagval2_19911 yx=19911,rx=False 1648190516519400896`),
		fmt.Sprintf(`mst,host=server19912,region=tagval2_19912,core_tag=spring core="field-*_values _1_19912",tx=19912i,yx=19912,rx=True 1648190516520400896`),
		fmt.Sprintf(`mst,host=server19913,region=tagval2_19913,core_tag=summer core="field-*_values _1_19913",tx=19913i,yx=19913,rx=False 1648190516521400896`),
		fmt.Sprintf(`mst,host=server19914,region=tagval2_19914 yx=19914,rx=True 1648190516522400896`),
		fmt.Sprintf(`mst,host=server19915,region=tagval2_19915,core_tag=winter core="field-*_values _1_19915",tx=19915i,yx=19915,rx=False 1648190516523400896`),
		fmt.Sprintf(`mst,host=server19916,region=tagval2_19916,core_tag=spring core="field-*_values _1_19916",tx=19916i,yx=19916,rx=True 1648190516524400896`),
		fmt.Sprintf(`mst,host=server19917,region=tagval2_19917 yx=19917,rx=False 1648190516525400896`),
		fmt.Sprintf(`mst,host=server19918,region=tagval2_19918,core_tag=autumn core="field-*_values _1_19918",tx=19918i,yx=19918,rx=True 1648190516526400896`),
		fmt.Sprintf(`mst,host=server19919,region=tagval2_19919,core_tag=winter core="field-*_values _1_19919",tx=19919i,yx=19919,rx=False 1648190516527400896`),
		fmt.Sprintf(`mst,host=server19920,region=tagval2_19920 yx=19920,rx=True 1648190516528400896`),
		fmt.Sprintf(`mst,host=server19921,region=tagval2_19921,core_tag=summer core="field-*_values _1_19921",tx=19921i,yx=19921,rx=False 1648190516529400896`),
		fmt.Sprintf(`mst,host=server19922,region=tagval2_19922,core_tag=autumn core="field-*_values _1_19922",tx=19922i,yx=19922,rx=True 1648190516530400896`),
		fmt.Sprintf(`mst,host=server19923,region=tagval2_19923 yx=19923,rx=False 1648190516531400896`),
		fmt.Sprintf(`mst,host=server19924,region=tagval2_19924,core_tag=spring core="field-*_values _1_19924",tx=19924i,yx=19924,rx=True 1648190516532400896`),
		fmt.Sprintf(`mst,host=server19925,region=tagval2_19925,core_tag=summer core="field-*_values _1_19925",tx=19925i,yx=19925,rx=False 1648190516533400896`),
		fmt.Sprintf(`mst,host=server19926,region=tagval2_19926 yx=19926,rx=True 1648190516534400896`),
		fmt.Sprintf(`mst,host=server19927,region=tagval2_19927,core_tag=winter core="field-*_values _1_19927",tx=19927i,yx=19927,rx=False 1648190516535400896`),
		fmt.Sprintf(`mst,host=server19928,region=tagval2_19928,core_tag=spring core="field-*_values _1_19928",tx=19928i,yx=19928,rx=True 1648190516536400896`),
		fmt.Sprintf(`mst,host=server19929,region=tagval2_19929 yx=19929,rx=False 1648190516537400896`),
		fmt.Sprintf(`mst,host=server19930,region=tagval2_19930,core_tag=autumn core="field-*_values _1_19930",tx=19930i,yx=19930,rx=True 1648190516538400896`),
		fmt.Sprintf(`mst,host=server19931,region=tagval2_19931,core_tag=winter core="field-*_values _1_19931",tx=19931i,yx=19931,rx=False 1648190516539400896`),
		fmt.Sprintf(`mst,host=server19932,region=tagval2_19932 yx=19932,rx=True 1648190516540400896`),
		fmt.Sprintf(`mst,host=server19933,region=tagval2_19933,core_tag=summer core="field-*_values _1_19933",tx=19933i,yx=19933,rx=False 1648190516541400896`),
		fmt.Sprintf(`mst,host=server19934,region=tagval2_19934,core_tag=autumn core="field-*_values _1_19934",tx=19934i,yx=19934,rx=True 1648190516542400896`),
		fmt.Sprintf(`mst,host=server19935,region=tagval2_19935 yx=19935,rx=False 1648190516543400896`),
		fmt.Sprintf(`mst,host=server19936,region=tagval2_19936,core_tag=spring core="field-*_values _1_19936",tx=19936i,yx=19936,rx=True 1648190516544400896`),
		fmt.Sprintf(`mst,host=server19937,region=tagval2_19937,core_tag=summer core="field-*_values _1_19937",tx=19937i,yx=19937,rx=False 1648190516545400896`),
		fmt.Sprintf(`mst,host=server19938,region=tagval2_19938 yx=19938,rx=True 1648190516546400896`),
		fmt.Sprintf(`mst,host=server19939,region=tagval2_19939,core_tag=winter core="field-*_values _1_19939",tx=19939i,yx=19939,rx=False 1648190516547400896`),
		fmt.Sprintf(`mst,host=server19940,region=tagval2_19940,core_tag=spring core="field-*_values _1_19940",tx=19940i,yx=19940,rx=True 1648190516548400896`),
		fmt.Sprintf(`mst,host=server19941,region=tagval2_19941 yx=19941,rx=False 1648190516549400896`),
		fmt.Sprintf(`mst,host=server19942,region=tagval2_19942,core_tag=autumn core="field-*_values _1_19942",tx=19942i,yx=19942,rx=True 1648190516550400896`),
		fmt.Sprintf(`mst,host=server19943,region=tagval2_19943,core_tag=winter core="field-*_values _1_19943",tx=19943i,yx=19943,rx=False 1648190516551400896`),
		fmt.Sprintf(`mst,host=server19944,region=tagval2_19944 yx=19944,rx=True 1648190516552400896`),
		fmt.Sprintf(`mst,host=server19945,region=tagval2_19945,core_tag=summer core="field-*_values _1_19945",tx=19945i,yx=19945,rx=False 1648190516553400896`),
		fmt.Sprintf(`mst,host=server19946,region=tagval2_19946,core_tag=autumn core="field-*_values _1_19946",tx=19946i,yx=19946,rx=True 1648190516554400896`),
		fmt.Sprintf(`mst,host=server19947,region=tagval2_19947 yx=19947,rx=False 1648190516555400896`),
		fmt.Sprintf(`mst,host=server19948,region=tagval2_19948,core_tag=spring core="field-*_values _1_19948",tx=19948i,yx=19948,rx=True 1648190516556400896`),
		fmt.Sprintf(`mst,host=server19949,region=tagval2_19949,core_tag=summer core="field-*_values _1_19949",tx=19949i,yx=19949,rx=False 1648190516557400896`),
		fmt.Sprintf(`mst,host=server19950,region=tagval2_19950 yx=19950,rx=True 1648190516558400896`),
		fmt.Sprintf(`mst,host=server19951,region=tagval2_19951,core_tag=winter core="field-*_values _1_19951",tx=19951i,yx=19951,rx=False 1648190516559400896`),
		fmt.Sprintf(`mst,host=server19952,region=tagval2_19952,core_tag=spring core="field-*_values _1_19952",tx=19952i,yx=19952,rx=True 1648190516560400896`),
		fmt.Sprintf(`mst,host=server19953,region=tagval2_19953 yx=19953,rx=False 1648190516561400896`),
		fmt.Sprintf(`mst,host=server19954,region=tagval2_19954,core_tag=autumn core="field-*_values _1_19954",tx=19954i,yx=19954,rx=True 1648190516562400896`),
		fmt.Sprintf(`mst,host=server19955,region=tagval2_19955,core_tag=winter core="field-*_values _1_19955",tx=19955i,yx=19955,rx=False 1648190516563400896`),
		fmt.Sprintf(`mst,host=server19956,region=tagval2_19956 yx=19956,rx=True 1648190516564400896`),
		fmt.Sprintf(`mst,host=server19957,region=tagval2_19957,core_tag=summer core="field-*_values _1_19957",tx=19957i,yx=19957,rx=False 1648190516565400896`),
		fmt.Sprintf(`mst,host=server19958,region=tagval2_19958,core_tag=autumn core="field-*_values _1_19958",tx=19958i,yx=19958,rx=True 1648190516566400896`),
		fmt.Sprintf(`mst,host=server19959,region=tagval2_19959 yx=19959,rx=False 1648190516567400896`),
		fmt.Sprintf(`mst,host=server19960,region=tagval2_19960,core_tag=spring core="field-*_values _1_19960",tx=19960i,yx=19960,rx=True 1648190516568400896`),
		fmt.Sprintf(`mst,host=server19961,region=tagval2_19961,core_tag=summer core="field-*_values _1_19961",tx=19961i,yx=19961,rx=False 1648190516569400896`),
		fmt.Sprintf(`mst,host=server19962,region=tagval2_19962 yx=19962,rx=True 1648190516570400896`),
		fmt.Sprintf(`mst,host=server19963,region=tagval2_19963,core_tag=winter core="field-*_values _1_19963",tx=19963i,yx=19963,rx=False 1648190516571400896`),
		fmt.Sprintf(`mst,host=server19964,region=tagval2_19964,core_tag=spring core="field-*_values _1_19964",tx=19964i,yx=19964,rx=True 1648190516572400896`),
		fmt.Sprintf(`mst,host=server19965,region=tagval2_19965 yx=19965,rx=False 1648190516573400896`),
		fmt.Sprintf(`mst,host=server19966,region=tagval2_19966,core_tag=autumn core="field-*_values _1_19966",tx=19966i,yx=19966,rx=True 1648190516574400896`),
		fmt.Sprintf(`mst,host=server19967,region=tagval2_19967,core_tag=winter core="field-*_values _1_19967",tx=19967i,yx=19967,rx=False 1648190516575400896`),
		fmt.Sprintf(`mst,host=server19968,region=tagval2_19968 yx=19968,rx=True 1648190516576400896`),
		fmt.Sprintf(`mst,host=server19969,region=tagval2_19969,core_tag=summer core="field-*_values _1_19969",tx=19969i,yx=19969,rx=False 1648190516577400896`),
		fmt.Sprintf(`mst,host=server19970,region=tagval2_19970,core_tag=autumn core="field-*_values _1_19970",tx=19970i,yx=19970,rx=True 1648190516578400896`),
		fmt.Sprintf(`mst,host=server19971,region=tagval2_19971 yx=19971,rx=False 1648190516579400896`),
		fmt.Sprintf(`mst,host=server19972,region=tagval2_19972,core_tag=spring core="field-*_values _1_19972",tx=19972i,yx=19972,rx=True 1648190516580400896`),
		fmt.Sprintf(`mst,host=server19973,region=tagval2_19973,core_tag=summer core="field-*_values _1_19973",tx=19973i,yx=19973,rx=False 1648190516581400896`),
		fmt.Sprintf(`mst,host=server19974,region=tagval2_19974 yx=19974,rx=True 1648190516582400896`),
		fmt.Sprintf(`mst,host=server19975,region=tagval2_19975,core_tag=winter core="field-*_values _1_19975",tx=19975i,yx=19975,rx=False 1648190516583400896`),
		fmt.Sprintf(`mst,host=server19976,region=tagval2_19976,core_tag=spring core="field-*_values _1_19976",tx=19976i,yx=19976,rx=True 1648190516584400896`),
		fmt.Sprintf(`mst,host=server19977,region=tagval2_19977 yx=19977,rx=False 1648190516585400896`),
		fmt.Sprintf(`mst,host=server19978,region=tagval2_19978,core_tag=autumn core="field-*_values _1_19978",tx=19978i,yx=19978,rx=True 1648190516586400896`),
		fmt.Sprintf(`mst,host=server19979,region=tagval2_19979,core_tag=winter core="field-*_values _1_19979",tx=19979i,yx=19979,rx=False 1648190516587400896`),
		fmt.Sprintf(`mst,host=server19980,region=tagval2_19980 yx=19980,rx=True 1648190516588400896`),
		fmt.Sprintf(`mst,host=server19981,region=tagval2_19981,core_tag=summer core="field-*_values _1_19981",tx=19981i,yx=19981,rx=False 1648190516589400896`),
		fmt.Sprintf(`mst,host=server19982,region=tagval2_19982,core_tag=autumn core="field-*_values _1_19982",tx=19982i,yx=19982,rx=True 1648190516590400896`),
		fmt.Sprintf(`mst,host=server19983,region=tagval2_19983 yx=19983,rx=False 1648190516591400896`),
		fmt.Sprintf(`mst,host=server19984,region=tagval2_19984,core_tag=spring core="field-*_values _1_19984",tx=19984i,yx=19984,rx=True 1648190516592400896`),
		fmt.Sprintf(`mst,host=server19985,region=tagval2_19985,core_tag=summer core="field-*_values _1_19985",tx=19985i,yx=19985,rx=False 1648190516593400896`),
		fmt.Sprintf(`mst,host=server19986,region=tagval2_19986 yx=19986,rx=True 1648190516594400896`),
		fmt.Sprintf(`mst,host=server19987,region=tagval2_19987,core_tag=winter core="field-*_values _1_19987",tx=19987i,yx=19987,rx=False 1648190516595400896`),
		fmt.Sprintf(`mst,host=server19988,region=tagval2_19988,core_tag=spring core="field-*_values _1_19988",tx=19988i,yx=19988,rx=True 1648190516596400896`),
		fmt.Sprintf(`mst,host=server19989,region=tagval2_19989 yx=19989,rx=False 1648190516597400896`),
		fmt.Sprintf(`mst,host=server19990,region=tagval2_19990,core_tag=autumn core="field-*_values _1_19990",tx=19990i,yx=19990,rx=True 1648190516598400896`),
		fmt.Sprintf(`mst,host=server19991,region=tagval2_19991,core_tag=winter core="field-*_values _1_19991",tx=19991i,yx=19991,rx=False 1648190516599400896`),
		fmt.Sprintf(`mst,host=server19992,region=tagval2_19992 yx=19992,rx=True 1648190516600400896`),
		fmt.Sprintf(`mst,host=server19993,region=tagval2_19993,core_tag=summer core="field-*_values _1_19993",tx=19993i,yx=19993,rx=False 1648190516601400896`),
		fmt.Sprintf(`mst,host=server19994,region=tagval2_19994,core_tag=autumn core="field-*_values _1_19994",tx=19994i,yx=19994,rx=True 1648190516602400896`),
		fmt.Sprintf(`mst,host=server19995,region=tagval2_19995 yx=19995,rx=False 1648190516603400896`),
		fmt.Sprintf(`mst,host=server19996,region=tagval2_19996,core_tag=spring core="field-*_values _1_19996",tx=19996i,yx=19996,rx=True 1648190516604400896`),
		fmt.Sprintf(`mst,host=server19997,region=tagval2_19997,core_tag=summer core="field-*_values _1_19997",tx=19997i,yx=19997,rx=False 1648190516605400896`),
		fmt.Sprintf(`mst,host=server19998,region=tagval2_19998 yx=19998,rx=True 1648190516606400896`),
		fmt.Sprintf(`mst,host=server19999,region=tagval2_19999,core_tag=winter core="field-*_values _1_19999",tx=19999i,yx=19999,rx=False 1648190516607400896`),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)

	test.addQueries([]*Query{
		&Query{
			name:    "select max(yx),core,tx,rx from mst",
			params:  url.Values{"db": []string{"db0"}},
			command: `select max(yx),core,tx,rx from mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","max","core","tx","rx"],"values":[["2022-03-25T06:41:56.607400896Z",19999,"field-*_values _1_19999",19999,false]]}]}]}`,
		},
	}...)

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

func TestServer_Query_PreAgg_OutOfOrderData(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := Test{
		queries: []*Query{
			&Query{
				name:    "create database with shard group duration and index duration should succeed",
				command: `CREATE DATABASE db3 WITH SHARD DURATION 12h index duration 24h name rp3`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
		},
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

	test.writes = Writes{
		&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.1,no=2i,alive=TRUE %d`, mustParseTime(time.RFC3339Nano, "2021-11-26T13:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=23,no=3i,alive=FALSE %d`, mustParseTime(time.RFC3339Nano, "2021-11-26T14:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=23,no=4i %d`, mustParseTime(time.RFC3339Nano, "2021-11-25T13:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.4,no=5i,alive=FALSE %d`, mustParseTime(time.RFC3339Nano, "2021-11-25T14:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=105,no=6i,alive=TRUE %d`, mustParseTime(time.RFC3339Nano, "2021-11-27T09:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=200,no=7i,alive=FALSE %d`, mustParseTime(time.RFC3339Nano, "2021-11-25T10:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=100,no=8i,alive=TRUE %d`, mustParseTime(time.RFC3339Nano, "2021-11-26T09:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=200,no=9i,alive=TRUE %d`, mustParseTime(time.RFC3339Nano, "2021-11-26T10:00:00Z").UnixNano())},
	}
	test.db = "db3"
	test.rp = "rp3"
	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	test.queries = []*Query{
		&Query{
			name:    "select count(time) should success",
			command: `select count(time) from db3.rp3.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",8]]}]}]}`),
		},
		&Query{
			name:    "select count(*)",
			command: `select count(*) from db3.rp3.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","count_alive","count_no","count_val"],"values":[["1970-01-01T00:00:00Z",7,8,8]]}]}]}`),
		},
		&Query{
			name:    "select min(val)",
			command: `select min(val) from db3.rp3.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min"],"values":[["2021-11-25T13:00:00Z",23]]}]}]}`),
		},
		&Query{
			name:    "select min(val),host,region,aliv",
			command: `select min(val),host,region,alive from db3.rp3.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","min","host","region","alive"],"values":[["2021-11-25T13:00:00Z",23,"serverB","uswest",null]]}]}]}`),
		},
		&Query{
			name:    "select max(val)",
			command: `select max(val) from db3.rp3.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","max"],"values":[["2021-11-25T10:00:00Z",200]]}]}]}`),
		},
		&Query{
			name:    "select first(val)",
			command: `select first(val) from db3.rp3.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","first"],"values":[["2021-11-25T10:00:00Z",200]]}]}]}`),
		},
		&Query{
			name:    "select last(val)",
			command: `select last(val) from db3.rp3.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","last"],"values":[["2021-11-27T09:00:00Z",105]]}]}]}`),
		},
		&Query{
			name:    "select sum(val)",
			command: `select sum(val) from db3.rp3.cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",697.5]]}]}]}`),
		},
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

func TestServer_Query_PreAgg_WithEmptyData(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,k=1 f1=1,f2=1,f3=1i,f4=true    1629129601000000000`),
		fmt.Sprintf(`mst,k=1 f1=2                       1629129602000000000`),
		fmt.Sprintf(`mst,k=1 f1=3                       1629129603000000000`),
		fmt.Sprintf(`mst,k=1 f1=4,f2=4,f3=4i,f4=false   1629129604000000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "max float",
			params:  url.Values{"db": []string{"db0"}},
			command: `select max(f2) from mst where time>=1629129602000000000 and time < 1629129603000000000`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "max int",
			params:  url.Values{"db": []string{"db0"}},
			command: `select max(f3) from mst where time>=1629129602000000000 and time < 1629129603000000000`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "max bool",
			params:  url.Values{"db": []string{"db0"}},
			command: `select max(f4) from mst where time>=1629129602000000000 and time < 1629129603000000000`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "min float",
			params:  url.Values{"db": []string{"db0"}},
			command: `select min(f2) from mst where time>=1629129602000000000 and time < 1629129603000000000`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "min int",
			params:  url.Values{"db": []string{"db0"}},
			command: `select min(f3) from mst where time>=1629129602000000000 and time < 1629129603000000000`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "min bool",
			params:  url.Values{"db": []string{"db0"}},
			command: `select min(f4) from mst where time>=1629129602000000000 and time < 1629129603000000000`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
	}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})

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

func TestServer_Query_PreAgg_Filter(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`mst,country=China,city=Chengdu age=1 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Chengdu age=2 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Hangzhou age=3 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Hangzhou age=4 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Nanjing age=5 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Nanjing age=6 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Beijing age=7 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Beijing age=8 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Shanghai age=9 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Shanghai age=10 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Guangzhou age=11 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Guangzhou age=12 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Shenzheng age=13 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Shenzheng age=14 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Yantai age=15 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Yantai age=16 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Heilongjiang age=17 1625558240120000000`),
		fmt.Sprintf(`mst,country=China,city=Heilongjiang age=18 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Changsha age=19 1625558240119000000`),
		fmt.Sprintf(`mst,country=China,city=Changsha age=20 1625558240130000000`),
		fmt.Sprintf(`mst,country=China,city=Xizhang age=21 1625558240119000000`),
		fmt.Sprintf(`mst,country=China,city=Xizhang age=22 1625558240130000000`),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "max age group by *",
			params:  url.Values{"db": []string{"db0"}},
			command: `select max(age) from mst where time >=1625558240120000000 and time < 1625558240130000000 group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"city":"Beijing","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",7]]},{"name":"mst","tags":{"city":"Chengdu","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Guangzhou","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",11]]},{"name":"mst","tags":{"city":"Hangzhou","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",3]]},{"name":"mst","tags":{"city":"Heilongjiang","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",17]]},{"name":"mst","tags":{"city":"Nanjing","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",5]]},{"name":"mst","tags":{"city":"Shanghai","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",9]]},{"name":"mst","tags":{"city":"Shenzheng","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",13]]},{"name":"mst","tags":{"city":"Yantai","country":"China"},"columns":["time","max"],"values":[["2021-07-06T07:57:20.12Z",15]]}]}]}`,
		},
		&Query{
			name:    "min age group by *",
			params:  url.Values{"db": []string{"db0"}},
			command: `select min(age) from mst where time >=1625558240120000000 and time < 1625558240130000000 group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"city":"Beijing","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",7]]},{"name":"mst","tags":{"city":"Chengdu","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Guangzhou","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",11]]},{"name":"mst","tags":{"city":"Hangzhou","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",3]]},{"name":"mst","tags":{"city":"Heilongjiang","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",17]]},{"name":"mst","tags":{"city":"Nanjing","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",5]]},{"name":"mst","tags":{"city":"Shanghai","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",9]]},{"name":"mst","tags":{"city":"Shenzheng","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",13]]},{"name":"mst","tags":{"city":"Yantai","country":"China"},"columns":["time","min"],"values":[["2021-07-06T07:57:20.12Z",15]]}]}]}`,
		},
		&Query{
			name:    "first age group by *",
			params:  url.Values{"db": []string{"db0"}},
			command: `select first(age) from mst where time >=1625558240120000000 and time < 1625558240130000000 group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"city":"Beijing","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",7]]},{"name":"mst","tags":{"city":"Chengdu","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Guangzhou","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",11]]},{"name":"mst","tags":{"city":"Hangzhou","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",3]]},{"name":"mst","tags":{"city":"Heilongjiang","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",17]]},{"name":"mst","tags":{"city":"Nanjing","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",5]]},{"name":"mst","tags":{"city":"Shanghai","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",9]]},{"name":"mst","tags":{"city":"Shenzheng","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",13]]},{"name":"mst","tags":{"city":"Yantai","country":"China"},"columns":["time","first"],"values":[["2021-07-06T07:57:20.12Z",15]]}]}]}`,
		},
		&Query{
			name:    "last age group by *",
			params:  url.Values{"db": []string{"db0"}},
			command: `select last(age) from mst where time >=1625558240120000000 and time < 1625558240130000000 group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"city":"Beijing","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",7]]},{"name":"mst","tags":{"city":"Chengdu","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Guangzhou","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",11]]},{"name":"mst","tags":{"city":"Hangzhou","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",3]]},{"name":"mst","tags":{"city":"Heilongjiang","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",17]]},{"name":"mst","tags":{"city":"Nanjing","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",5]]},{"name":"mst","tags":{"city":"Shanghai","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",9]]},{"name":"mst","tags":{"city":"Shenzheng","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",13]]},{"name":"mst","tags":{"city":"Yantai","country":"China"},"columns":["time","last"],"values":[["2021-07-06T07:57:20.12Z",15]]}]}]}`,
		},
		&Query{
			name:    "count age group by *",
			params:  url.Values{"db": []string{"db0"}},
			command: `select count(age) from mst where time >=1625558240120000000 and time < 1625558240130000000 group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"city":"Beijing","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Chengdu","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Guangzhou","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Hangzhou","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Heilongjiang","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Nanjing","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Shanghai","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Shenzheng","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Yantai","country":"China"},"columns":["time","count"],"values":[["2021-07-06T07:57:20.12Z",1]]}]}]}`,
		},
		&Query{
			name:    "sum age group by *",
			params:  url.Values{"db": []string{"db0"}},
			command: `select sum(age) from mst where time >=1625558240120000000 and time < 1625558240130000000 group by *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"city":"Beijing","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",7]]},{"name":"mst","tags":{"city":"Chengdu","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",1]]},{"name":"mst","tags":{"city":"Guangzhou","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",11]]},{"name":"mst","tags":{"city":"Hangzhou","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",3]]},{"name":"mst","tags":{"city":"Heilongjiang","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",17]]},{"name":"mst","tags":{"city":"Nanjing","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",5]]},{"name":"mst","tags":{"city":"Shanghai","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",9]]},{"name":"mst","tags":{"city":"Shenzheng","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",13]]},{"name":"mst","tags":{"city":"Yantai","country":"China"},"columns":["time","sum"],"values":[["2021-07-06T07:57:20.12Z",15]]}]}]}`,
		},
	}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})

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

func TestServer_Query_Aggregates_FloatMany_New(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = make(Writes, 0, 10)
	var index int64
	for j := 0; j < cap(test.writes); j++ {
		writes := make([]string, 0, 10000)
		for i := 0; i < cap(writes); i++ {
			writes = append(writes, fmt.Sprintf(`floatmany,host=server%d value=%d %d`, j, i, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()+index*1000000000))
			index++
		}
		test.writes = append(test.writes, &Write{data: strings.Join(writes, "\n")})
	}

	test.addQueries([]*Query{
		&Query{
			name:    "count - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT COUNT(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","count"],"values":[["1970-01-01T00:00:00Z",100000]]}]}]}`,
		},
		&Query{
			name:    "first - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT FIRST(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","first"],"values":[["2000-01-01T00:00:00Z",0]]}]}]}`,
		},
		&Query{
			name:    "last - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT LAST(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","last"],"values":[["2000-01-02T03:46:39Z",9999]]}]}]}`,
		},
		&Query{
			name:    "sum - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT SUM(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","sum"],"values":[["1970-01-01T00:00:00Z",499950000]]}]}]}`,
		},
		&Query{
			name:    "max - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MAX(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","max"],"values":[["2000-01-01T02:46:39Z",9999]]}]}]}`,
		},
		&Query{
			name:    "min - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MIN(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","min"],"values":[["2000-01-01T00:00:00Z",0]]}]}]}`,
		},
		&Query{
			name:    "mean - float",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT MEAN(value) FROM floatmany`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"floatmany","columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",4999.5]]}]}]}`,
		},
	}...)

	funcCmp := func(exp, act string) bool {
		if strings.Contains(exp, "4999.") && strings.Contains(act, "4999.") {
			return true
		}
		return false
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

			if query.name == "mean - float" {
				query.Execute(s)
			}

			if err := query.Execute(s); err != nil {
				t.Error(query.Error(err))
			} else if query.name == "mean - float" {
				if !funcCmp(query.exp, query.act) {
					t.Error(query.failureMessage())
				}
			} else if !query.success() {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_Query_SubqueryMath(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf("m0 f2=4,f3=2 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf("m0 f1=5,f3=8 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf("m0 f1=5,f2=3,f3=6 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "SumThreeValues",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum FROM (SELECT f1 + f2 + f3 AS sum FROM m0)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m0","columns":["time","sum"],"values":[["2000-01-01T00:00:00Z",null],["2000-01-01T00:00:10Z",null],["2000-01-01T00:00:20Z",14]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Query_PercentileDerivative(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`counter value=12 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`counter value=34 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`counter value=78 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`counter value=89 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`counter value=101 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "nth percentile of derivative",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT percentile(derivative, 95) FROM (SELECT derivative(value, 1s) FROM counter) WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:00:50Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"counter","columns":["time","percentile"],"values":[["2000-01-01T00:00:20Z",4.4]]}]}]}`,
			skip:    true,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Query_UnderscoreMeasurement(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`_cpu value=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "select underscore with underscore prefix",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM _cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"_cpu","columns":["time","value"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Write_Precision(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []struct {
		write  string
		params url.Values
	}{
		{
			write: fmt.Sprintf("cpu_n0_precision value=1 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").UnixNano()),
		},
		{
			write:  fmt.Sprintf("cpu_n1_precision value=1.1 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").UnixNano()),
			params: url.Values{"precision": []string{"n"}},
		},
		{
			write:  fmt.Sprintf("cpu_u_precision value=100 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Microsecond).UnixNano()/int64(time.Microsecond)),
			params: url.Values{"precision": []string{"u"}},
		},
		{
			write:  fmt.Sprintf("cpu_ms_precision value=200 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Millisecond).UnixNano()/int64(time.Millisecond)),
			params: url.Values{"precision": []string{"ms"}},
		},
		{
			write:  fmt.Sprintf("cpu_s_precision value=300 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Second).UnixNano()/int64(time.Second)),
			params: url.Values{"precision": []string{"s"}},
		},
		{
			write:  fmt.Sprintf("cpu_m_precision value=400 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Minute).UnixNano()/int64(time.Minute)),
			params: url.Values{"precision": []string{"m"}},
		},
		{
			write:  fmt.Sprintf("cpu_h_precision value=500 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T12:34:56.789012345Z").Truncate(time.Hour).UnixNano()/int64(time.Hour)),
			params: url.Values{"precision": []string{"h"}},
		},
	}

	test := NewTest("db0", "rp0")

	test.addQueries([]*Query{
		&Query{
			name:    "point with nanosecond precision time - no precision specified on write",
			command: `SELECT * FROM cpu_n0_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_n0_precision","columns":["time","value"],"values":[["2000-01-01T12:34:56.789012345Z",1]]}]}]}`,
		},
		&Query{
			name:    "point with nanosecond precision time",
			command: `SELECT * FROM cpu_n1_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_n1_precision","columns":["time","value"],"values":[["2000-01-01T12:34:56.789012345Z",1.1]]}]}]}`,
		},
		&Query{
			name:    "point with microsecond precision time",
			command: `SELECT * FROM cpu_u_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_u_precision","columns":["time","value"],"values":[["2000-01-01T12:34:56.789012Z",100]]}]}]}`,
		},
		&Query{
			name:    "point with millisecond precision time",
			command: `SELECT * FROM cpu_ms_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_ms_precision","columns":["time","value"],"values":[["2000-01-01T12:34:56.789Z",200]]}]}]}`,
		},
		&Query{
			name:    "point with second precision time",
			command: `SELECT * FROM cpu_s_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_s_precision","columns":["time","value"],"values":[["2000-01-01T12:34:56Z",300]]}]}]}`,
		},
		&Query{
			name:    "point with minute precision time",
			command: `SELECT * FROM cpu_m_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_m_precision","columns":["time","value"],"values":[["2000-01-01T12:34:00Z",400]]}]}]}`,
		},
		&Query{
			name:    "point with hour precision time",
			command: `SELECT * FROM cpu_h_precision`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu_h_precision","columns":["time","value"],"values":[["2000-01-01T12:00:00Z",500]]}]}]}`,
		},
	}...)

	// we are doing writes that require parameter changes, so we are fighting the test harness a little to make this happen properly
	for _, w := range writes {
		test.writes = Writes{
			&Write{data: w.write},
		}
		test.params = w.params
		test.initialized = false
		if err := test.init(s); err != nil {
			t.Fatalf("test init failed: %s", err)
		}
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Query_Wildcards(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`wildcard,region=us-east value=10 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-east valx=20 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-east value=30,valx=40 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),

		fmt.Sprintf(`wgroup,region=us-east value=10.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`wgroup,region=us-east value=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`wgroup,region=us-west value=30.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),

		fmt.Sprintf(`m1,region=us-east value=10.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`m2,host=server01 field=20.0 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "wildcard",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","region","value","valx"],"values":[["2000-01-01T00:00:00Z","us-east",10,null],["2000-01-01T00:00:10Z","us-east",null,20],["2000-01-01T00:00:20Z","us-east",30,40]]}]}]}`,
		},
		&Query{
			name:    "wildcard with group by",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","tags":{"region":"us-east"},"columns":["time","value","valx"],"values":[["2000-01-01T00:00:00Z",10,null],["2000-01-01T00:00:10Z",null,20],["2000-01-01T00:00:20Z",30,40]]}]}]}`,
		},
		&Query{
			name:    "GROUP BY queries",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(value) FROM wgroup GROUP BY *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wgroup","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",15]]},{"name":"wgroup","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["1970-01-01T00:00:00Z",30]]}]}]}`,
		},
		&Query{
			name:    "GROUP BY queries with time",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT mean(value) FROM wgroup WHERE time >= '2000-01-01T00:00:00Z' AND time < '2000-01-01T00:01:00Z' GROUP BY *,TIME(1m)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wgroup","tags":{"region":"us-east"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",15]]},{"name":"wgroup","tags":{"region":"us-west"},"columns":["time","mean"],"values":[["2000-01-01T00:00:00Z",30]]}]}]}`,
		},
		&Query{
			name:    "wildcard and field in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT value, * FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","value","region","value_1","valx"],"values":[["2000-01-01T00:00:00Z",10,"us-east",10,null],["2000-01-01T00:00:10Z",null,"us-east",null,20],["2000-01-01T00:00:20Z",30,"us-east",30,40]]}]}]}`,
		},
		&Query{
			name:    "field and wildcard in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT value, * FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","value","region","value_1","valx"],"values":[["2000-01-01T00:00:00Z",10,"us-east",10,null],["2000-01-01T00:00:10Z",null,"us-east",null,20],["2000-01-01T00:00:20Z",30,"us-east",30,40]]}]}]}`,
		},
		&Query{
			name:    "field and wildcard in group by",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard GROUP BY region, *`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","tags":{"region":"us-east"},"columns":["time","value","valx"],"values":[["2000-01-01T00:00:00Z",10,null],["2000-01-01T00:00:10Z",null,20],["2000-01-01T00:00:20Z",30,40]]}]}]}`,
		},
		&Query{
			name:    "wildcard and field in group by",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard GROUP BY *, region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","tags":{"region":"us-east"},"columns":["time","value","valx"],"values":[["2000-01-01T00:00:00Z",10,null],["2000-01-01T00:00:10Z",null,20],["2000-01-01T00:00:20Z",30,40]]}]}]}`,
		},
		&Query{
			name:    "wildcard with multiple measurements",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM m1, m2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m1","columns":["time","field","host","region","value"],"values":[["2000-01-01T00:00:00Z",null,null,"us-east",10]]},{"name":"m2","columns":["time","field","host","region","value"],"values":[["2000-01-01T00:00:01Z",20,"server01",null,null]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "wildcard with multiple measurements via regex",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM /^m.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m1","columns":["time","field","host","region","value"],"values":[["2000-01-01T00:00:00Z",null,null,"us-east",10]]},{"name":"m2","columns":["time","field","host","region","value"],"values":[["2000-01-01T00:00:01Z",20,"server01",null,null]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "wildcard with multiple measurements via regex and limit",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM db0../^m.*/ LIMIT 2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"m1","columns":["time","field","host","region","value"],"values":[["2000-01-01T00:00:00Z",null,null,"us-east",10]]},{"name":"m2","columns":["time","field","host","region","value"],"values":[["2000-01-01T00:00:01Z",20,"server01",null,null]]}]}]}`,
			skip:    true,
		},
	}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})

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

func TestServer_Query_WildcardExpansion(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`wildcard,region=us-east,host=A value=10,cpu=80 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-east,host=B value=20,cpu=90 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-west,host=B value=30,cpu=70 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`wildcard,region=us-east,host=A value=40,cpu=60 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),

		fmt.Sprintf(`dupnames,region=us-east,days=1 value=10,day=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`dupnames,region=us-east,days=2 value=20,day=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`dupnames,region=us-west,days=3 value=30,day=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "wildcard",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","cpu","host","region","value"],"values":[["2000-01-01T00:00:00Z",80,"A","us-east",10],["2000-01-01T00:00:10Z",90,"B","us-east",20],["2000-01-01T00:00:20Z",70,"B","us-west",30],["2000-01-01T00:00:30Z",60,"A","us-east",40]]}]}]}`,
		},
		&Query{
			name:    "no wildcard in select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT cpu, host, region, value  FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","cpu","host","region","value"],"values":[["2000-01-01T00:00:00Z",80,"A","us-east",10],["2000-01-01T00:00:10Z",90,"B","us-east",20],["2000-01-01T00:00:20Z",70,"B","us-west",30],["2000-01-01T00:00:30Z",60,"A","us-east",40]]}]}]}`,
		},
		&Query{
			name:    "no wildcard in select, preserve column order",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT host, cpu, region, value  FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","host","cpu","region","value"],"values":[["2000-01-01T00:00:00Z","A",80,"us-east",10],["2000-01-01T00:00:10Z","B",90,"us-east",20],["2000-01-01T00:00:20Z","B",70,"us-west",30],["2000-01-01T00:00:30Z","A",60,"us-east",40]]}]}]}`,
		},

		&Query{
			name:    "no wildcard with alias",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT cpu as c, host as h, region, value  FROM wildcard`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"wildcard","columns":["time","c","h","region","value"],"values":[["2000-01-01T00:00:00Z",80,"A","us-east",10],["2000-01-01T00:00:10Z",90,"B","us-east",20],["2000-01-01T00:00:20Z",70,"B","us-west",30],["2000-01-01T00:00:30Z",60,"A","us-east",40]]}]}]}`,
		},
		&Query{
			name:    "duplicate tag and field key",
			command: `SELECT * FROM dupnames`,
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"dupnames","columns":["time","day","days","region","value"],"values":[["2000-01-01T00:00:00Z",3,"1","us-east",10],["2000-01-01T00:00:10Z",2,"2","us-east",20],["2000-01-01T00:00:20Z",1,"3","us-west",30]]}]}]}`,
		},
	}...)

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

func TestServer_Query_AcrossShardsAndFields(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu load=100 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu load=200 %d`, mustParseTime(time.RFC3339Nano, "2010-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu core=4 %d`, mustParseTime(time.RFC3339Nano, "2015-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "two results for cpu",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT load FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","load"],"values":[["2000-01-01T00:00:00Z",100],["2010-01-01T00:00:00Z",200]]}]}]}`,
		},
		&Query{
			name:    "two results for cpu, multi-select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT core,load FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core","load"],"values":[["2000-01-01T00:00:00Z",null,100],["2010-01-01T00:00:00Z",null,200],["2015-01-01T00:00:00Z",4,null]]}]}]}`,
		},
		&Query{
			name:    "two results for cpu, wildcard select",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core","load"],"values":[["2000-01-01T00:00:00Z",null,100],["2010-01-01T00:00:00Z",null,200],["2015-01-01T00:00:00Z",4,null]]}]}]}`,
		},
		&Query{
			name:    "one result for core",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT core FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core"],"values":[["2015-01-01T00:00:00Z",4]]}]}]}`,
		},
		&Query{
			name:    "empty result set from non-existent field",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT foo FROM cpu`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "select existent and non-existent field",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT foo,load FROM cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","foo","load"],"values":[["2000-01-01T00:00:00Z",null,100],["2010-01-01T00:00:00Z",null,200]]}]}]}`),
		},
	}...)

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

func TestServer_Query_OrderedAcrossShards(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu value=7 %d`, mustParseTime(time.RFC3339Nano, "2010-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu value=14 %d`, mustParseTime(time.RFC3339Nano, "2010-01-08T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu value=28 %d`, mustParseTime(time.RFC3339Nano, "2010-01-15T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu value=56 %d`, mustParseTime(time.RFC3339Nano, "2010-01-22T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu value=112 %d`, mustParseTime(time.RFC3339Nano, "2010-01-29T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "derivative",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT derivative(value, 24h) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","derivative"],"values":[["2010-01-08T00:00:00Z",1],["2010-01-15T00:00:00Z",2],["2010-01-22T00:00:00Z",4],["2010-01-29T00:00:00Z",8]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "non_negative_derivative",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT non_negative_derivative(value, 24h) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","non_negative_derivative"],"values":[["2010-01-08T00:00:00Z",1],["2010-01-15T00:00:00Z",2],["2010-01-22T00:00:00Z",4],["2010-01-29T00:00:00Z",8]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "difference",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT difference(value) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","difference"],"values":[["2010-01-08T00:00:00Z",7],["2010-01-15T00:00:00Z",14],["2010-01-22T00:00:00Z",28],["2010-01-29T00:00:00Z",56]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "cumulative_sum",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT cumulative_sum(value) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","cumulative_sum"],"values":[["2010-01-01T00:00:00Z",7],["2010-01-08T00:00:00Z",21],["2010-01-15T00:00:00Z",49],["2010-01-22T00:00:00Z",105],["2010-01-29T00:00:00Z",217]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_Where_Fields(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu alert_id="alert",tenant_id="tenant",_cust="johnson brothers" %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),
		fmt.Sprintf(`cpu alert_id="alert",tenant_id="tenant",_cust="johnson brothers" %d`, mustParseTime(time.RFC3339Nano, "2015-02-28T01:03:36.703820946Z").UnixNano()),

		fmt.Sprintf(`cpu load=100.0,core=4 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu load=80.0,core=2 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:01:02Z").UnixNano()),

		fmt.Sprintf(`clicks local=true %d`, mustParseTime(time.RFC3339Nano, "2014-11-10T23:00:01Z").UnixNano()),
		fmt.Sprintf(`clicks local=false %d`, mustParseTime(time.RFC3339Nano, "2014-11-10T23:00:02Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		// non type specific
		&Query{
			name:    "missing measurement with group by",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT load from missing group by *`,
			exp:     `{"results":[{"statement_id":0,"error":"measurement not found"}]}`,
		},

		// string
		&Query{
			name:    "single string field",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id FROM cpu WHERE alert_id='alert'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","alert_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert"]]}]}]}`,
		},
		&Query{
			name:    "string AND query, all fields in SELECT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id,tenant_id,_cust FROM cpu WHERE alert_id='alert' AND tenant_id='tenant'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","alert_id","tenant_id","_cust"],"values":[["2015-02-28T01:03:36.703820946Z","alert","tenant","johnson brothers"]]}]}]}`,
		},
		&Query{
			name:    "string AND query, all fields in SELECT, one in parenthesis",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id,tenant_id FROM cpu WHERE alert_id='alert' AND (tenant_id='tenant')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","alert_id","tenant_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert","tenant"]]}]}]}`,
		},
		&Query{
			name:    "string underscored field",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id FROM cpu WHERE _cust='johnson brothers'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","alert_id"],"values":[["2015-02-28T01:03:36.703820946Z","alert"]]}]}]}`,
		},
		&Query{
			name:    "string no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT alert_id FROM cpu WHERE _cust='acme'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},

		// float64
		&Query{
			name:    "float64 GT no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load > 100`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "float64 GTE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load >= 100`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		&Query{
			name:    "float64 EQ match upper bound",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load = 100`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		&Query{
			name:    "float64 LTE match two",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load <= 100`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100],["2009-11-10T23:01:02Z",80]]}]}]}`,
		},
		&Query{
			name:    "float64 GT match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load > 99`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:00:02Z",100]]}]}]}`,
		},
		&Query{
			name:    "float64 EQ no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load = 99`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "float64 LT match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load < 99`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:01:02Z",80]]}]}]}`,
		},
		&Query{
			name:    "float64 LT no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load < 80`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "float64 NE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select load from cpu where load != 100`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","load"],"values":[["2009-11-10T23:01:02Z",80]]}]}]}`,
		},

		// int64
		&Query{
			name:    "int64 GT no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core > 4`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "int64 GTE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core >= 4`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core"],"values":[["2009-11-10T23:00:02Z",4]]}]}]}`,
		},
		&Query{
			name:    "int64 EQ match upper bound",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core = 4`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core"],"values":[["2009-11-10T23:00:02Z",4]]}]}]}`,
		},
		&Query{
			name:    "int64 LTE match two ",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core <= 4`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core"],"values":[["2009-11-10T23:00:02Z",4],["2009-11-10T23:01:02Z",2]]}]}]}`,
		},
		&Query{
			name:    "int64 GT match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core > 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core"],"values":[["2009-11-10T23:00:02Z",4]]}]}]}`,
		},
		&Query{
			name:    "int64 EQ no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core = 3`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "int64 LT match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core < 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core"],"values":[["2009-11-10T23:01:02Z",2]]}]}]}`,
		},
		&Query{
			name:    "int64 LT no match",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core < 2`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "int64 NE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select core from cpu where core != 4`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","core"],"values":[["2009-11-10T23:01:02Z",2]]}]}]}`,
		},

		// bool
		&Query{
			name:    "bool EQ match true",
			params:  url.Values{"db": []string{"db0"}},
			command: `select local from clicks where local = true`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"clicks","columns":["time","local"],"values":[["2014-11-10T23:00:01Z",true]]}]}]}`,
		},
		&Query{
			name:    "bool EQ match false",
			params:  url.Values{"db": []string{"db0"}},
			command: `select local from clicks where local = false`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"clicks","columns":["time","local"],"values":[["2014-11-10T23:00:02Z",false]]}]}]}`,
		},

		&Query{
			name:    "bool NE match one",
			params:  url.Values{"db": []string{"db0"}},
			command: `select local from clicks where local != true`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"clicks","columns":["time","local"],"values":[["2014-11-10T23:00:02Z",false]]}]}]}`,
		},
	}...)

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

func TestServer_Query_Where_With_Tags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`where_events,tennant=paul foo="bar" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`where_events,tennant=paul foo="baz" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
		fmt.Sprintf(`where_events,tennant=paul foo="bat" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:04Z").UnixNano()),
		fmt.Sprintf(`where_events,tennant=todd foo="bar" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:05Z").UnixNano()),
		fmt.Sprintf(`where_events,tennant=david foo="bap" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:06Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "tag field and time",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where (tennant = 'paul' OR tennant = 'david') AND time > 1s AND (foo = 'bar' OR foo = 'baz' OR foo = 'bap')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:06Z","bap"]]}]}]}`,
		},
		&Query{
			name:    "tag or field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where tennant = 'paul' OR foo = 'bar'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "field or tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where foo = 'bar' OR tennant = 'paul'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "tag or tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where tennant = 'todd' OR tennant = 'paul'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "field or field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where foo = 'bar' OR foo = 'baz'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "tag or parent field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where foo = 'bar' OR ((foo = 'baz'))`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "parent field or tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where ((foo = 'baz')) OR foo = 'bar'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "parent field or parent field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where ((foo = 'bar')) OR ((foo = 'baz'))`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "tag and field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where tennant = 'paul' AND foo = 'bar'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "field and tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where foo = 'bar' AND tennant = 'paul'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "field and field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where foo = 'bar' AND foo = 'bat'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "tag and parent field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where tennant = 'paul' AND ((foo = 'bar'))`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "parent field and tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where ((foo = 'bar')) AND tennant = 'paul'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "parent field and parent field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where ((foo = 'bar')) AND ((foo = 'bat'))`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},

		&Query{
			name:    "non-existant tag and field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where tenant != 'paul' AND foo = 'bar'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "non-existant tag or field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where tenant != 'paul' OR foo = 'bar'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:05Z","bar"]]}]}]}`,
		},
		&Query{
			name:    "where comparing tag and field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where tennant != foo`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"],["2009-11-10T23:00:06Z","bap"]]}]}]}`,
		},
		/*&Query{
			name:    "where comparing tag and tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from where_events where tennant = tennant`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"where_events","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z","bar"],["2009-11-10T23:00:03Z","baz"],["2009-11-10T23:00:04Z","bat"],["2009-11-10T23:00:05Z","bar"],["2009-11-10T23:00:06Z","bap"]]}]}]}`,
		},*/
	}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})
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

func TestServer_Query_With_EmptyTags(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu value=1 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01 value=2 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "where empty tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu where host = ''`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:02Z",1]]}]}]}`,
			skip:    true, //Issue #54
		},
		&Query{
			name:    "where not empty tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu where host != ''`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "where regex none",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu where host !~ /.*/`,
			exp:     `{"results":[{"statement_id":0}]}`,
			skip:    true,
		},
		&Query{
			name:    "where regex exact",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu where host =~ /^server01$/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "where regex exact (case insensitive)",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu where host =~ /(?i)^SeRvEr01$/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "where regex exact (not)",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu where host !~ /^server01$/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:02Z",1]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "where regex at least one char",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu where host =~ /.+/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "where regex not at least one char",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu where host !~ /.+/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:02Z",1]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "group by empty tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu group by host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":""},"columns":["time","value"],"values":[["2009-11-10T23:00:02Z",1]]},{"name":"cpu","tags":{"host":"server01"},"columns":["time","value"],"values":[["2009-11-10T23:00:03Z",2]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "group by missing tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from cpu group by region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"region":""},"columns":["time","value"],"values":[["2009-11-10T23:00:02Z",1],["2009-11-10T23:00:03Z",2]]}]}]}`,
			skip:    true,
		},
	}...)

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

func TestServer_Query_LimitAndOffset(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`limited,tennant=paul foo=2 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`limited,tennant=paul foo=3 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
		fmt.Sprintf(`limited,tennant=paul foo=4 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:04Z").UnixNano()),
		fmt.Sprintf(`limited,tennant=todd foo=5 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:05Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "limit on points",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from "limited" LIMIT 2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "limit higher than the number of data points",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from "limited" LIMIT 20`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","foo"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4],["2009-11-10T23:00:05Z",5]]}]}]}`,
		},
		&Query{
			name:    "limit and offset",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from "limited" LIMIT 2 OFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","foo"],"values":[["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4]]}]}]}`,
		},
		&Query{
			name:    "limit + offset equal to total number of points",
			params:  url.Values{"db": []string{"db0"}},
			command: `select foo from "limited" LIMIT 3 OFFSET 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","foo"],"values":[["2009-11-10T23:00:05Z",5]]}]}]}`,
		},
		&Query{
			name:    "limit - offset higher than number of points",
			command: `select foo from "limited" LIMIT 2 OFFSET 20`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit on points with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","mean"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit higher than the number of data points with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 20`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","mean"],"values":[["2009-11-10T23:00:02Z",2],["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4],["2009-11-10T23:00:05Z",5]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit and offset with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2 OFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","mean"],"values":[["2009-11-10T23:00:03Z",3],["2009-11-10T23:00:04Z",4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit + offset equal to the number of points with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 3 OFFSET 3`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","columns":["time","mean"],"values":[["2009-11-10T23:00:05Z",5]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit - offset higher than number of points with group by time",
			command: `select mean(foo) from "limited" WHERE time >= '2009-11-10T23:00:02Z' AND time < '2009-11-10T23:00:06Z' GROUP BY TIME(1s) LIMIT 2 OFFSET 20`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit - group by tennant",
			command: `select foo from "limited" group by tennant limit 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","tags":{"tennant":"paul"},"columns":["time","foo"],"values":[["2009-11-10T23:00:02Z",2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "limit and offset - group by tennant",
			command: `select foo from "limited" group by tennant limit 1 offset 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"limited","tags":{"tennant":"paul"},"columns":["time","foo"],"values":[["2009-11-10T23:00:03Z",3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

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

func TestServer_Query_Fill(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`fills val=3 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:02Z").UnixNano()),
		fmt.Sprintf(`fills val=5 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:03Z").UnixNano()),
		fmt.Sprintf(`fills val=4 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:06Z").UnixNano()),
		fmt.Sprintf(`fills val=10 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:16Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "fill with value",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(1)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",1],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with value, WHERE all values match condition",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' and val < 50 group by time(5s) FILL(1)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",1],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with value, WHERE no values match condition",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' and val > 50 group by time(5s) FILL(1)`,
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with previous",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",4],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with none, i.e. clear out nulls",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) FILL(none)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill defaults to null",
			command: `select mean(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",null],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill defaults to 0 for count",
			command: `select count(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","count"],"values":[["2009-11-10T23:00:00Z",2],["2009-11-10T23:00:05Z",1],["2009-11-10T23:00:10Z",0],["2009-11-10T23:00:15Z",1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill none drops 0s for count",
			command: `select count(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) fill(none)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","count"],"values":[["2009-11-10T23:00:00Z",2],["2009-11-10T23:00:05Z",1],["2009-11-10T23:00:15Z",1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill previous overwrites 0s for count",
			command: `select count(val) from fills where time >= '2009-11-10T23:00:00Z' and time < '2009-11-10T23:00:20Z' group by time(5s) fill(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","count"],"values":[["2009-11-10T23:00:00Z",2],["2009-11-10T23:00:05Z",1],["2009-11-10T23:00:10Z",1],["2009-11-10T23:00:15Z",1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "fill with implicit start time",
			command: `select mean(val) from fills where time < '2009-11-10T23:00:20Z' group by time(5s)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"fills","columns":["time","mean"],"values":[["2009-11-10T23:00:00Z",4],["2009-11-10T23:00:05Z",4],["2009-11-10T23:00:10Z",null],["2009-11-10T23:00:15Z",10]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

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

func TestServer_Query_By_Chunked_SingleMst(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`survey,country=China,name=ada      age=15,height=170i,sex=false,city="chengdu" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:20.121Z").UnixNano()),
		fmt.Sprintf(`survey,country=China,name=billy    age=27,height=165i,sex=false,city="shenzhen" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:20.122Z").UnixNano()),
		fmt.Sprintf(`survey,country=China,name=demon    age=57,height=150i,sex=false,city="shanghai" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:20.123Z").UnixNano()),
		fmt.Sprintf(`survey,country=China,name=king    age=22,height=167i,sex=false,city="beijing" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:20.124Z").UnixNano()),
		fmt.Sprintf(`survey,country=Egypt,name=chris   age=31,height=159i,sex=false,city="elilansa" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:22.121Z").UnixNano()),
		fmt.Sprintf(`survey,country=Egypt,name=daisy   age=40,height=178i,sex=true,city="gunilanduo" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:22.122Z").UnixNano()),
		fmt.Sprintf(`survey,country=France,name=paul   age=45,height=164i,sex=true,city="paris" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:22.123Z").UnixNano()),
		fmt.Sprintf(`survey,country=Germany,name=frank age=35,height=169i,sex=true,city="bakeli" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:22.124Z").UnixNano()),
		fmt.Sprintf(`survey,country=Japan,name=jack    age=21,height=190i,sex=true,city="dongjin" %d`, mustParseTime(time.RFC3339Nano, "2021-07-06T07:57:22.125Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "query with a single measurement by chunk size 1",
			params:  url.Values{"db": []string{"db0"}, "chunked": []string{"true"}, "chunk_size": []string{"1"}},
			command: `SELECT * FROM survey`,
			exp: `{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:20.121Z",15,"chengdu","China",170,"ada",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:20.122Z",27,"shenzhen","China",165,"billy",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:20.123Z",57,"shanghai","China",150,"demon",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:20.124Z",22,"beijing","China",167,"king",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:22.121Z",31,"elilansa","Egypt",159,"chris",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:22.122Z",40,"gunilanduo","Egypt",178,"daisy",true]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:22.123Z",45,"paris","France",164,"paul",true]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:22.124Z",35,"bakeli","Germany",169,"frank",true]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:22.125Z",21,"dongjin","Japan",190,"jack",true]]}]}]}`,
		},
		&Query{
			name:    "query with a single measurement by chunk size 4",
			params:  url.Values{"db": []string{"db0"}, "chunked": []string{"true"}, "chunk_size": []string{"4"}},
			command: `SELECT * FROM survey`,
			exp: `{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:20.121Z",15,"chengdu","China",170,"ada",false],["2021-07-06T07:57:20.122Z",27,"shenzhen","China",165,"billy",false],["2021-07-06T07:57:20.123Z",57,"shanghai","China",150,"demon",false],["2021-07-06T07:57:20.124Z",22,"beijing","China",167,"king",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:22.121Z",31,"elilansa","Egypt",159,"chris",false],["2021-07-06T07:57:22.122Z",40,"gunilanduo","Egypt",178,"daisy",true],["2021-07-06T07:57:22.123Z",45,"paris","France",164,"paul",true],["2021-07-06T07:57:22.124Z",35,"bakeli","Germany",169,"frank",true]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:22.125Z",21,"dongjin","Japan",190,"jack",true]]}]}]}`,
		},
		&Query{
			name:    "query with a single measurement by chunk size 3 and inner chunk size 4",
			params:  url.Values{"db": []string{"db0"}, "chunked": []string{"true"}, "chunk_size": []string{"3"}, "inner_chunk_size": []string{"4"}},
			command: `SELECT * FROM survey`,
			exp: `{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:20.121Z",15,"chengdu","China",170,"ada",false],["2021-07-06T07:57:20.122Z",27,"shenzhen","China",165,"billy",false],["2021-07-06T07:57:20.123Z",57,"shanghai","China",150,"demon",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:20.124Z",22,"beijing","China",167,"king",false],["2021-07-06T07:57:22.121Z",31,"elilansa","Egypt",159,"chris",false],["2021-07-06T07:57:22.122Z",40,"gunilanduo","Egypt",178,"daisy",true]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","columns":["time","age","city","country","height","name","sex"],"values":[["2021-07-06T07:57:22.123Z",45,"paris","France",164,"paul",true],["2021-07-06T07:57:22.124Z",35,"bakeli","Germany",169,"frank",true],["2021-07-06T07:57:22.125Z",21,"dongjin","Japan",190,"jack",true]]}]}]}`,
		},
		&Query{
			name:    "query with a single measurement by chunk size 1 and inner chunk size 3",
			params:  url.Values{"db": []string{"db0"}, "chunked": []string{"true"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"3"}},
			command: `SELECT * FROM survey group by country`,
			exp: `{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"China"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:20.121Z",15,"chengdu",170,"ada",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"China"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:20.122Z",27,"shenzhen",165,"billy",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"China"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:20.123Z",57,"shanghai",150,"demon",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"China"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:20.124Z",22,"beijing",167,"king",false]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Egypt"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.121Z",31,"elilansa",159,"chris",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Egypt"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.122Z",40,"gunilanduo",178,"daisy",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"France"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.123Z",45,"paris",164,"paul",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Germany"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.124Z",35,"bakeli",169,"frank",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Japan"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.125Z",21,"dongjin",190,"jack",true]]}]}]}`,
		},
		&Query{
			name:    "query with a single measurement by chunk size 3 and inner chunk size 3",
			params:  url.Values{"db": []string{"db0"}, "chunked": []string{"true"}, "chunk_size": []string{"3"}, "inner_chunk_size": []string{"3"}},
			command: `SELECT * FROM survey group by country`,
			exp: `{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"China"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:20.121Z",15,"chengdu",170,"ada",false],["2021-07-06T07:57:20.122Z",27,"shenzhen",165,"billy",false],["2021-07-06T07:57:20.123Z",57,"shanghai",150,"demon",false]],"partial":true}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"China"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:20.124Z",22,"beijing",167,"king",false]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Egypt"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.121Z",31,"elilansa",159,"chris",false],["2021-07-06T07:57:22.122Z",40,"gunilanduo",178,"daisy",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"France"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.123Z",45,"paris",164,"paul",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Germany"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.124Z",35,"bakeli",169,"frank",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Japan"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.125Z",21,"dongjin",190,"jack",true]]}]}]}`,
		},
		&Query{
			name:    "query with a single measurement by chunk size 5 and inner chunk size 3",
			params:  url.Values{"db": []string{"db0"}, "chunked": []string{"true"}, "chunk_size": []string{"5"}, "inner_chunk_size": []string{"3"}},
			command: `SELECT * FROM survey group by country`,
			exp: `{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"China"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:20.121Z",15,"chengdu",170,"ada",false],["2021-07-06T07:57:20.122Z",27,"shenzhen",165,"billy",false],["2021-07-06T07:57:20.123Z",57,"shanghai",150,"demon",false],["2021-07-06T07:57:20.124Z",22,"beijing",167,"king",false]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Egypt"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.121Z",31,"elilansa",159,"chris",false],["2021-07-06T07:57:22.122Z",40,"gunilanduo",178,"daisy",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"France"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.123Z",45,"paris",164,"paul",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Germany"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.124Z",35,"bakeli",169,"frank",true]]}],"partial":true}]}
{"results":[{"statement_id":0,"series":[{"name":"survey","tags":{"country":"Japan"},"columns":["time","age","city","height","name","sex"],"values":[["2021-07-06T07:57:22.125Z",21,"dongjin",190,"jack",true]]}]}]}`,
		},
	}...)

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

// FixMe: show measurements doesn't support regular expression
func TestServer_Query_ShowMeasurementExactCardinality(t *testing.T) {

	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server02,region=caeast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`other,host=server03,region=caeast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show measurement cardinality using FROM and regex`,
			command: "SHOW MEASUREMENT CARDINALITY FROM /[cg]pu/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement cardinality using FROM and regex - no matches`,
			command: "SHOW MEASUREMENT CARDINALITY FROM /.*zzzzz.*/",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement cardinality where tag matches regular expression`,
			command: "SHOW MEASUREMENT CARDINALITY WHERE region =~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement cardinality where tag does not match a regular expression`,
			command: "SHOW MEASUREMENT CARDINALITY WHERE region !~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement cardinality with time in WHERE clauses errors`,
			command: `SHOW MEASUREMENT CARDINALITY WHERE time > now() - 1h`,
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement exact cardinality`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement exact cardinality using FROM`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY FROM cpu",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement exact cardinality using FROM and regex`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY FROM /[cg]pu/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement exact cardinality using FROM and regex - no matches`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY FROM /.*zzzzz.*/",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement exact cardinality where tag matches regular expression`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY WHERE region =~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement exact cardinality where tag does not match a regular expression`,
			command: "SHOW MEASUREMENT EXACT CARDINALITY WHERE region !~ /ca.*/",
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show measurement exact cardinality with time in WHERE clauses errors`,
			command: `SHOW MEASUREMENT EXACT CARDINALITY WHERE time > now() - 1h`,
			exp:     `{"results":[{"statement_id":0,"error":"SHOW MEASUREMENT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
	}...)

	InitHaTestEnv(t)

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

	ReleaseHaTestEnv(t)
}

func TestServer_Query_ShowSeries(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02 value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=uswest value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `create measurement cpu`,
			command: "CREATE MEASUREMENT cpu",
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    `drop measurement cpu`,
			command: "DROP MEASUREMENT cpu",
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    `show series exact cardinality`,
			command: "SHOW SERIES EXACT CARDINALITY",
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[6]]}]}]}`,
		},
		&Query{
			name:    `show series cardinality`,
			command: "SHOW SERIES CARDINALITY",
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["startTime","endTime","count"],"values":[["2009-11-09T00:00:00Z","2009-11-16T00:00:00Z",6]]}]}]}`,
		},
	}...)

	InitHaTestEnv(t)

	for i, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if i == 2 {
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

	ReleaseHaTestEnv(t)
}

func TestServer_Query_ShowTagKeys(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server03,region=caeast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`disk,host=server03,region=caeast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show tag keys`,
			command: "SHOW TAG KEYS",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"disk","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"gpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag keys on db0`,
			command: "SHOW TAG KEYS ON db0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"disk","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"gpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
		},
		&Query{
			name:    "show tag keys from",
			command: "SHOW TAG KEYS FROM cpu",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag keys from regex",
			command: "SHOW TAG KEYS FROM /[cg]pu/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"gpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag keys measurement not found",
			command: "SHOW TAG KEYS FROM doesntexist",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag keys with time`,
			command: "SHOW TAG KEYS WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"disk","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"gpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag keys on db0 with time`,
			command: "SHOW TAG KEYS ON db0 WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"disk","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"gpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "show tag keys with time from",
			command: "SHOW TAG KEYS FROM cpu WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    "show tag keys with time from regex",
			command: "SHOW TAG KEYS FROM /[cg]pu/ WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"gpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    "show tag keys with time where",
			command: "SHOW TAG KEYS WHERE host = 'server03' AND time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["tagKey"],"values":[["host"],["region"]]},{"name":"gpu","columns":["tagKey"],"values":[["host"],["region"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    "show tag keys with time measurement not found",
			command: "SHOW TAG KEYS FROM doesntexist WHERE time > 0",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
	}...)

	InitHaTestEnv(t)

	var initialized bool
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			if !initialized {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
				initialized = true
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

	ReleaseHaTestEnv(t)
}

func TestServer_Query_ShowTagValues(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server03,region=caeast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`disk,host=server03,region=caeast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "show tag values with key",
			command: "SHOW TAG VALUES WITH KEY = host",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["host","server02"]]},{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"],["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with key regex",
			command: "SHOW TAG VALUES WITH KEY =~ /ho/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["host","server02"]]},{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"],["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where`,
			command: `SHOW TAG VALUES FROM cpu WITH KEY = host WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key regex and where`,
			command: `SHOW TAG VALUES FROM cpu WITH KEY =~ /ho/ WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where matches the regular expression`,
			command: `SHOW TAG VALUES WITH KEY = host WHERE region =~ /ca.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where does not match the regular expression`,
			command: `SHOW TAG VALUES WITH KEY = region WHERE host !~ /server0[12]/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["key","value"],"values":[["region","caeast"]]},{"name":"gpu","columns":["key","value"],"values":[["region","caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and where partially matches the regular expression`,
			command: `SHOW TAG VALUES WITH KEY = host WHERE region =~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["host","server02"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key and where partially does not match the regular expression`,
			command: `SHOW TAG VALUES WITH KEY = host WHERE region !~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"]]},{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key in and where does not match the regular expression`,
			command: `SHOW TAG VALUES FROM cpu WITH KEY IN (host, region) WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["region","uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key regex and where does not match the regular expression`,
			command: `SHOW TAG VALUES FROM cpu WITH KEY =~ /(host|region)/ WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["region","uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values with key and measurement matches regular expression`,
			command: `SHOW TAG VALUES FROM /[cg]pu/ WITH KEY = host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["host","server02"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"],["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with key where time",
			command: "SHOW TAG VALUES WITH KEY = host WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["host","server02"]]},{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"],["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    "show tag values with key regex where time",
			command: "SHOW TAG VALUES WITH KEY =~ /ho/ WHERE time > 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["host","server02"]]},{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"],["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key and where time`,
			command: `SHOW TAG VALUES FROM cpu WITH KEY = host WHERE region = 'uswest' AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key regex and where time`,
			command: `SHOW TAG VALUES FROM cpu WITH KEY =~ /ho/ WHERE region = 'uswest' AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key and where matches the regular expression where time`,
			command: `SHOW TAG VALUES WITH KEY = host WHERE region =~ /ca.*/ AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key and where does not match the regular expression where time`,
			command: `SHOW TAG VALUES WITH KEY = region WHERE host !~ /server0[12]/ AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["key","value"],"values":[["region","caeast"]]},{"name":"gpu","columns":["key","value"],"values":[["region","caeast"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key and where partially matches the regular expression where time`,
			command: `SHOW TAG VALUES WITH KEY = host WHERE region =~ /us/ AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["host","server02"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key and where partially does not match the regular expression where time`,
			command: `SHOW TAG VALUES WITH KEY = host WHERE region !~ /us/ AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"]]},{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key in and where does not match the regular expression where time`,
			command: `SHOW TAG VALUES FROM cpu WITH KEY IN (host, region) WHERE region = 'uswest' AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["region","uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key regex and where does not match the regular expression where time`,
			command: `SHOW TAG VALUES FROM cpu WITH KEY =~ /(host|region)/ WHERE region = 'uswest' AND time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["region","uswest"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values with key and measurement matches regular expression where time`,
			command: `SHOW TAG VALUES FROM /[cg]pu/ WITH KEY = host WHERE time > 0`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"],["host","server02"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"],["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    "show tag values with value filter",
			command: "SHOW TAG VALUES WITH KEY = host WHERE value = 'server03'",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server03"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    "show tag values with no matching value filter",
			command: "SHOW TAG VALUES WITH KEY = host WHERE value = 'no_such_value'",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with non-string value filter",
			command: "SHOW TAG VALUES WITH KEY = host WHERE value = 5000",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with limit 1 offset 2",
			command: "SHOW TAG VALUES FROM cpu WITH KEY = host limit 1 offset 2",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag values with limit 1 offset 0",
			command: "SHOW TAG VALUES WITH KEY = host limit 1 offset 0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["key","value"],"values":[["host","server01"]]},{"name":"disk","columns":["key","value"],"values":[["host","server03"]]},{"name":"gpu","columns":["key","value"],"values":[["host","server02"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	InitHaTestEnv(t)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})
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

	ReleaseHaTestEnv(t)
}

func TestServer_Query_ShowTagKeyCardinality(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server02,region=useast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server03,region=caeast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`disk,host=server03,region=caeast value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show tag key cardinality`,
			command: "SHOW TAG KEY CARDINALITY",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"disk","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag key cardinality on db0`,
			command: "SHOW TAG KEY CARDINALITY ON db0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"disk","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[2]]}]}]}`,
		},
		&Query{
			name:    "show tag key cardinality from",
			command: "SHOW TAG KEY CARDINALITY FROM cpu",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key cardinality from regex",
			command: "SHOW TAG KEY CARDINALITY FROM /[cg]pu/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key cardinality measurement not found",
			command: "SHOW TAG KEY CARDINALITY FROM doesntexist",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key cardinality with time in WHERE clause errors",
			command: "SHOW TAG KEY CARDINALITY FROM cpu WHERE time > now() - 1h",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW TAG KEY EXACT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag key exact cardinality`,
			command: "SHOW TAG KEY EXACT CARDINALITY",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"disk","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag key exact cardinality on db0`,
			command: "SHOW TAG KEY EXACT CARDINALITY ON db0",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"disk","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[2]]}]}]}`,
		},
		&Query{
			name:    "show tag key exact cardinality from",
			command: "SHOW TAG KEY EXACT CARDINALITY FROM cpu",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key exact cardinality from regex",
			command: "SHOW TAG KEY EXACT CARDINALITY FROM /[cg]pu/",
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key exact cardinality measurement not found",
			command: "SHOW TAG KEY EXACT CARDINALITY FROM doesntexist",
			exp:     `{"results":[{"statement_id":0}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    "show tag key exact cardinality with time in WHERE clause errors",
			command: "SHOW TAG KEY EXACT CARDINALITY FROM cpu WHERE time > now() - 1h",
			exp:     `{"results":[{"statement_id":0,"error":"SHOW TAG KEY EXACT CARDINALITY doesn't support time in WHERE clause"}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and where matches the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY WITH KEY = host WHERE region =~ /ca.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["count"],"values":[[1]]},{"name":"gpu","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and where does not match the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY WITH KEY = region WHERE host !~ /server0[12]/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["count"],"values":[[1]]},{"name":"gpu","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and where partially matches the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY WITH KEY = host WHERE region =~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values cardinality with key and where partially does not match the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY WITH KEY = host WHERE region !~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["count"],"values":[[1]]},{"name":"gpu","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values cardinality with key in and where does not match the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY FROM cpu WITH KEY IN (host, region) WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key regex and where does not match the regular expression`,
			command: `SHOW TAG VALUES CARDINALITY FROM cpu WITH KEY =~ /(host|region)/ WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values cardinality with key and measurement matches regular expression`,
			command: `SHOW TAG VALUES CARDINALITY FROM /[cg]pu/ WITH KEY = host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and where matches the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY WITH KEY = host WHERE region =~ /ca.*/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["count"],"values":[[1]]},{"name":"gpu","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and where does not match the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY WITH KEY = region WHERE host !~ /server0[12]/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["count"],"values":[[1]]},{"name":"gpu","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and where partially matches the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY WITH KEY = host WHERE region =~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values exact cardinality with key and where partially does not match the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY WITH KEY = host WHERE region !~ /us/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"disk","columns":["count"],"values":[[1]]},{"name":"gpu","columns":["count"],"values":[[1]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
			skip:    true,
		},
		&Query{
			name:    `show tag values exact cardinality with key in and where does not match the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY FROM cpu WITH KEY IN (host, region) WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key regex and where does not match the regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY FROM cpu WITH KEY =~ /(host|region)/ WHERE region = 'uswest'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show tag values exact cardinality with key and measurement matches regular expression`,
			command: `SHOW TAG VALUES EXACT CARDINALITY FROM /[cg]pu/ WITH KEY = host`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	InitHaTestEnv(t)

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

	ReleaseHaTestEnv(t)
}

func TestServer_Query_ShowFieldKeys(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 field1=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server01,region=useast field4=200,field5=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server03,region=caeast field6=200,field7=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`disk,host=server03,region=caeast field8=200,field9=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show field keys`,
			command: `SHOW FIELD KEYS`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["field1","float"],["field2","float"],["field3","float"]]},{"name":"disk","columns":["fieldKey","fieldType"],"values":[["field8","float"],["field9","float"]]},{"name":"gpu","columns":["fieldKey","fieldType"],"values":[["field4","float"],["field5","float"],["field6","float"],["field7","float"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field keys from measurement`,
			command: `SHOW FIELD KEYS FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["field1","float"],["field2","float"],["field3","float"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field keys measurement with regex`,
			command: `SHOW FIELD KEYS FROM /[cg]pu/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["fieldKey","fieldType"],"values":[["field1","float"],["field2","float"],["field3","float"]]},{"name":"gpu","columns":["fieldKey","fieldType"],"values":[["field4","float"],["field5","float"],["field6","float"],["field7","float"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	InitHaTestEnv(t)

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

	ReleaseHaTestEnv(t)
}

func TestServer_Query_ShowFieldKeyCardinality(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 field1=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server01,region=useast field4=200,field5=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`gpu,host=server03,region=caeast field6=200,field7=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`disk,host=server03,region=caeast field8=200,field9=300 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show field key cardinality`,
			command: `SHOW FIELD KEY CARDINALITY`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[3]]},{"name":"disk","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key cardinality from measurement`,
			command: `SHOW FIELD KEY CARDINALITY FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key cardinality measurement with regex`,
			command: `SHOW FIELD KEY CARDINALITY FROM /[cg]pu/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[3]]},{"name":"gpu","columns":["count"],"values":[[4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key exact cardinality`,
			command: `SHOW FIELD KEY EXACT CARDINALITY`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[3]]},{"name":"disk","columns":["count"],"values":[[2]]},{"name":"gpu","columns":["count"],"values":[[4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key exact cardinality from measurement`,
			command: `SHOW FIELD KEY EXACT CARDINALITY FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[3]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `show field key exact cardinality measurement with regex`,
			command: `SHOW FIELD KEY EXACT CARDINALITY FROM /[cg]pu/`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[3]]},{"name":"gpu","columns":["count"],"values":[[4]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

	InitHaTestEnv(t)

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

	ReleaseHaTestEnv(t)
}

func TestServer_Query_TagOrder(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server03 field1=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast field1=200,field2=300,field3=400 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `group by tag1,tag2`,
			command: `select * from cpu group by host,region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"server01","region":"useast"},"columns":["time","field1","field2","field3"],"values":[["2009-11-10T23:00:00Z",200,300,400]]},{"name":"cpu","tags":{"host":"server01","region":"uswest"},"columns":["time","field1","field2","field3"],"values":[["2009-11-10T23:00:00Z",200,300,400]]},{"name":"cpu","tags":{"host":"server02","region":"useast"},"columns":["time","field1","field2","field3"],"values":[["2009-11-10T23:00:00Z",200,300,400]]},{"name":"cpu","tags":{"host":"server03","region":""},"columns":["time","field1","field2","field3"],"values":[["2009-11-10T23:00:00Z",100,null,null]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
		&Query{
			name:    `group by tag2,tag1`,
			command: `select * from cpu group by region,region`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"region":""},"columns":["time","field1","field2","field3","host"],"values":[["2009-11-10T23:00:00Z",100,null,null,"server03"]]},{"name":"cpu","tags":{"region":"useast"},"columns":["time","field1","field2","field3","host"],"values":[["2009-11-10T23:00:00Z",200,300,400,"server01"],["2009-11-10T23:00:00Z",200,300,400,"server02"]]},{"name":"cpu","tags":{"region":"uswest"},"columns":["time","field1","field2","field3","host"],"values":[["2009-11-10T23:00:00Z",200,300,400,"server01"]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

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

func TestServer_Query_EvilIdentifiers(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("cpu select=1,in-bytes=2 %d", mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano())},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `query evil identifiers`,
			command: `SELECT "select", "in-bytes" FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","select","in-bytes"],"values":[["2000-01-01T00:00:00Z",1,2]]}]}]}`,
			params:  url.Values{"db": []string{"db0"}},
		},
	}...)

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

func TestServer_Query_OrderByTime(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server1 value=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server1 value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server1 value=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),

		fmt.Sprintf(`power,presence=true value=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`power,presence=true value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`power,presence=true value=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
		fmt.Sprintf(`power,presence=false value=4 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:04Z").UnixNano()),

		fmt.Sprintf(`mem,host=server1 free=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`mem,host=server1 free=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`mem,host=server2 used=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`mem,host=server2 used=4 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "order on points",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from "cpu" ORDER BY time DESC`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:03Z",3],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:01Z",1]]}]}]}`,
		},

		&Query{
			name:    "order desc with tags",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from "power" ORDER BY time DESC`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"power","columns":["time","value"],"values":[["2000-01-01T00:00:04Z",4],["2000-01-01T00:00:03Z",3],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:01Z",1]]}]}]}`,
		},

		&Query{
			name:    "order desc with sparse data",
			params:  url.Values{"db": []string{"db0"}},
			command: `select used, free from "mem" ORDER BY time DESC`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mem","columns":["time","used","free"],"values":[["2000-01-01T00:00:02Z",null,2],["2000-01-01T00:00:02Z",4,null],["2000-01-01T00:00:01Z",null,1],["2000-01-01T00:00:01Z",3,null]]}]}]}`,
		},

		&Query{
			name:    "order desc with an aggregate and sparse data",
			params:  url.Values{"db": []string{"db0"}},
			command: `select first("used") AS "used", first("free") AS "free" from "mem" WHERE time >= '2000-01-01T00:00:01Z' AND time <= '2000-01-01T00:00:02Z' GROUP BY host, time(1s) FILL(none) ORDER BY time DESC`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mem","tags":{"host":"server2"},"columns":["time","used","free"],"values":[["2000-01-01T00:00:02Z",4,null],["2000-01-01T00:00:01Z",3,null]]},{"name":"mem","tags":{"host":"server1"},"columns":["time","used","free"],"values":[["2000-01-01T00:00:02Z",null,2],["2000-01-01T00:00:01Z",null,1]]}]}]}`,
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Query_FieldWithMultiplePeriods(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu foo.bar.baz=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "baseline",
			params:  url.Values{"db": []string{"db0"}},
			command: `select * from cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","foo.bar.baz"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "select field with periods",
			params:  url.Values{"db": []string{"db0"}},
			command: `select "foo.bar.baz" from cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","foo.bar.baz"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Query_FieldWithMultiplePeriodsMeasurementPrefixMatch(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`foo foo.bar.baz=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "baseline",
			params:  url.Values{"db": []string{"db0"}},
			command: `select * from foo`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"foo","columns":["time","foo.bar.baz"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
		&Query{
			name:    "select field with periods",
			params:  url.Values{"db": []string{"db0"}},
			command: `select "foo.bar.baz" from foo`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"foo","columns":["time","foo.bar.baz"],"values":[["2000-01-01T00:00:00Z",1]]}]}]}`,
		},
	}...)

	for i, query := range test.queries {
		if i == 0 {
			if err := test.init(s); err != nil {
				t.Fatalf("test init failed: %s", err)
			}
		}
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Query_IntoTarget(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`foo value=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`foo value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:10Z").UnixNano()),
		fmt.Sprintf(`foo value=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:20Z").UnixNano()),
		fmt.Sprintf(`foo value=4 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:30Z").UnixNano()),
		fmt.Sprintf(`foo value=4,foobar=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:40Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "into",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * INTO baz FROM foo`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"result","columns":["time","written"],"values":[["1970-01-01T00:00:00Z",5]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "confirm results",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM baz`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"baz","columns":["time","foobar","value"],"values":[["2000-01-01T00:00:00Z",null,1],["2000-01-01T00:00:10Z",null,2],["2000-01-01T00:00:20Z",null,3],["2000-01-01T00:00:30Z",null,4],["2000-01-01T00:00:40Z",3,4]]}]}]}`,
			skip:    true,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Query_LargeTimestamp(t *testing.T) {
	t.Parallel()
	s := OpenDefaultServer(NewConfig())
	defer s.Close()

	if _, ok := s.(*RemoteServer); ok {
		t.Skip("Skipping.  Cannot restart remote server")
	}

	writes := []string{
		fmt.Sprintf(`cpu value=100 %d`, models.MaxNanoTime),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    `select value at max nano time`,
			params:  url.Values{"db": []string{"db0"}},
			command: fmt.Sprintf(`SELECT value FROM cpu WHERE time <= %d`, models.MaxNanoTime),
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["` + time.Unix(0, models.MaxNanoTime).UTC().Format(time.RFC3339Nano) + `",100]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	// Open a new server with the same configuration file.
	// This is to ensure the meta data was marshaled correctly.
	s2 := OpenServer((s.(*LocalServer)).Config)
	defer s2.(*LocalServer).Server.Close()

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_WhereTimeInclusive(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu value=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu value=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:03Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "all GTE/LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time >= '2000-01-01T00:00:01Z' and time <= '2000-01-01T00:00:03Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "all GTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time >= '2000-01-01T00:00:01Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "all LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time <= '2000-01-01T00:00:03Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "first GTE/LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time >= '2000-01-01T00:00:01Z' and time <= '2000-01-01T00:00:01Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",1]]}]}]}`,
		},
		&Query{
			name:    "last GTE/LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time >= '2000-01-01T00:00:03Z' and time <= '2000-01-01T00:00:03Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "before GTE/LTE",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time <= '2000-01-01T00:00:00Z'`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "all GT/LT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time > '2000-01-01T00:00:00Z' and time < '2000-01-01T00:00:04Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "first GT/LT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time > '2000-01-01T00:00:00Z' and time < '2000-01-01T00:00:02Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",1]]}]}]}`,
		},
		&Query{
			name:    "last GT/LT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time > '2000-01-01T00:00:02Z' and time < '2000-01-01T00:00:04Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "all GT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time > '2000-01-01T00:00:00Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "all LT",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * from cpu where time < '2000-01-01T00:00:04Z'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-01T00:00:01Z",1],["2000-01-01T00:00:02Z",2],["2000-01-01T00:00:03Z",3]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Query_ImplicitEndTime(t *testing.T) {
	t.Skip("flaky test")
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	past := now.Add(-10 * time.Second)
	future := now.Add(10 * time.Minute)
	writes := []string{
		fmt.Sprintf(`cpu value=1 %d`, past.UnixNano()),
		fmt.Sprintf(`cpu value=2 %d`, future.UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "raw query",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT * FROM cpu`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["%s",1],["%s",2]]}]}]}`, past.Format(time.RFC3339Nano), future.Format(time.RFC3339Nano)),
		},
		&Query{
			name:    "aggregate query",
			params:  url.Values{"db": []string{"db0"}},
			command: fmt.Sprintf(`SELECT mean(value) FROM cpu WHERE time > '%s' - 1m group by time(1h) FILL(none)`, now.Truncate(time.Minute).Format(time.RFC3339Nano)),
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","mean"],"values":[["%s",1]]}]}]}`, now.Truncate(time.Minute).Format(time.RFC3339Nano)),
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Query_Sample_Wildcard(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu float=1,int=1i,string="hello, world",bool=true %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "sample() with wildcard",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sample(*, 1) FROM cpu`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sample_bool","sample_float","sample_int","sample_string"],"values":[["2000-01-01T00:00:00Z",true,1,1,"hello, world"]]}]}]}`,
			skip:    true,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Query_Sample_LimitOffset(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu float=1,int=1i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu float=2,int=2i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:01:00Z").UnixNano()),
		fmt.Sprintf(`cpu float=3,int=3i %d`, mustParseTime(time.RFC3339Nano, "2000-01-01T00:02:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "sample() with limit 1",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sample(float, 3), int FROM cpu LIMIT 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sample","int"],"values":[["2000-01-01T00:00:00Z",1,1]]}]}]}`,
		},
		&Query{
			name:    "sample() with offset 1",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sample(float, 3), int FROM cpu OFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sample","int"],"values":[["2000-01-01T00:01:00Z",2,2],["2000-01-01T00:02:00Z",3,3]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "sample() with limit 1 offset 1",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sample(float, 3), int FROM cpu LIMIT 1 OFFSET 1`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sample","int"],"values":[["2000-01-01T00:01:00Z",2,2]]}]}]}`,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Validate that nested aggregates don't panic
func TestServer_NestedAggregateWithMathPanics(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		`cpu value=2i 120000000000`,
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "dividing by elapsed count should not panic",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) / elapsed(sum(value), 1m) FROM cpu WHERE time > 0 AND time < 10m group by time(1h)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum_elapsed"],"values":[["1970-01-01T00:00:00Z",null],["1970-01-01T00:01:00Z",null],["1970-01-01T00:02:00Z",null],["1970-01-01T00:03:00Z",null],["1970-01-01T00:04:00Z",null],["1970-01-01T00:05:00Z",null],["1970-01-01T00:06:00Z",null],["1970-01-01T00:07:00Z",null],["1970-01-01T00:08:00Z",null],["1970-01-01T00:09:00Z",null]]}]}]}`,
			skip:    true,
		},
		&Query{
			name:    "dividing by elapsed count with fill previous should not panic",
			params:  url.Values{"db": []string{"db0"}},
			command: `SELECT sum(value) / elapsed(sum(value), 1m) FROM cpu WHERE time > 0 AND time < 10m group by time(1h) FILL(previous)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","sum_elapsed"],"values":[["1970-01-01T00:00:00Z",null],["1970-01-01T00:01:00Z",null],["1970-01-01T00:02:00Z",null],["1970-01-01T00:03:00Z",2],["1970-01-01T00:04:00Z",2],["1970-01-01T00:05:00Z",2],["1970-01-01T00:06:00Z",2],["1970-01-01T00:07:00Z",2],["1970-01-01T00:08:00Z",2],["1970-01-01T00:09:00Z",2]]}]}]}`,
			skip:    true,
		},
	}...)

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

// Validate that nested aggregates don't panic
func TestServer_Query_SelectRelativeTime1(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("cpu,region=region_0,az=az_0 v1=0i,v2=0.000000,v3=true 1610380800000000000\n" +
			"cpu,region=region_0,az=az_0 v1=1i,v2=1.000000,v3=false 1610467200000000000\n" +
			"cpu,region=region_0,az=az_0 v1=2i,v2=2.000000,v3=true 1610553600000000000\n" +
			"cpu,region=region_0,az=az_0 v1=3i,v2=3.000000,v3=false 1610640000000000000")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "single point with time pre-calculated for past time queries yesterday",
			command: `select v3 from db0.rp0.cpu where time>='2021-01-12T16:00:00Z'`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","v3"],"values":[["2021-01-12T16:00:00Z",false],["2021-01-13T16:00:00Z",true],["2021-01-14T16:00:00Z",false]]}]}]}`),
		},
	}...)

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

func TestServer_Write_OutOfOrder(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := Test{
		queries: []*Query{
			&Query{
				name:    "create database with shard group duration and index duration should succeed",
				command: `CREATE DATABASE db3 WITH SHARD DURATION 12h index duration 24h name rp3`,
				exp:     `{"results":[{"statement_id":0}]}`,
			},
		},
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

	test.writes = Writes{
		&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.1 %d`, mustParseTime(time.RFC3339Nano, "2021-11-26T13:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=23.2 %d`, mustParseTime(time.RFC3339Nano, "2021-11-26T14:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=23.3 %d`, mustParseTime(time.RFC3339Nano, "2021-11-25T13:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=23.4 %d`, mustParseTime(time.RFC3339Nano, "2021-11-25T14:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=105 %d`, mustParseTime(time.RFC3339Nano, "2021-11-27T09:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=106 %d`, mustParseTime(time.RFC3339Nano, "2021-11-27T10:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverA,region=uswest val=100 %d`, mustParseTime(time.RFC3339Nano, "2021-11-26T09:00:00Z").UnixNano())},
		&Write{data: fmt.Sprintf(`cpu,host=serverB,region=uswest val=200 %d`, mustParseTime(time.RFC3339Nano, "2021-11-26T10:00:00Z").UnixNano())},
	}
	test.db = "db3"
	test.rp = "rp3"
	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	test.queries = []*Query{
		&Query{
			name:    "select val from in date 2021-11-26 should success",
			command: `select val from db3.rp3.cpu where time>='2021-11-26T00:00:00Z' and time<='2021-11-26T23:00:00Z' and "host"='serverB'`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","val"],"values":[["2021-11-26T10:00:00Z",200],["2021-11-26T14:00:00Z",23.2]]}]}]}`),
		},
		&Query{
			name:    "select val from in date 2021-11-27 should success",
			command: `select val from db3.rp3.cpu where time>='2021-11-27T00:00:00Z' and time<='2021-11-27T23:00:00Z' and "host"='serverB'`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","val"],"values":[["2021-11-27T10:00:00Z",106]]}]}]}`),
		},
		&Query{
			name:    "select val from 25 to 26 should success",
			command: `select val from db3.rp3.cpu where time>='2021-11-25T00:00:00Z' and time<='2021-11-26T23:00:00Z' and "host"='serverB'`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","val"],"values":[["2021-11-25T13:00:00Z",23.3],["2021-11-26T10:00:00Z",200],["2021-11-26T14:00:00Z",23.2]]}]}]}`),
		},
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

func TestServer_Query_OutOfOrder(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	// step1: write data
	writes := []string{
		fmt.Sprintf(`cpu,host=server1 value=1 %d`, mustParseTime(time.RFC3339Nano, "2000-01-03T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server1 value=2 %d`, mustParseTime(time.RFC3339Nano, "2000-01-03T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server1 value=3 %d`, mustParseTime(time.RFC3339Nano, "2000-01-03T00:00:03Z").UnixNano()),
	}
	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}
	if err := writeTestData(s, &test); err != nil {
		t.Fatalf("write data failed: %s", err)
	}

	// step2: write out of order data
	writes1 := []string{
		fmt.Sprintf(`cpu,host=server1 value=11 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server1 value=22 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server1 value=33 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:03Z").UnixNano()),
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes1, "\n")},
	}
	if err := writeTestData(s, &test); err != nil {
		t.Fatalf("write data failed: %s", err)
	}

	// step3: write new field value with same series and timestamp
	writes2 := []string{
		fmt.Sprintf(`cpu,host=server1 value=111 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:01Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server1 value=222 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:02Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server1 value=333 %d`, mustParseTime(time.RFC3339Nano, "2000-01-02T00:00:03Z").UnixNano()),
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes2, "\n")},
	}
	if err := writeTestData(s, &test); err != nil {
		t.Fatalf("write data failed: %s", err)
	}

	test.addQueries([]*Query{
		&Query{
			name:    "out of order query",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from "cpu"`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-02T00:00:01Z",111],["2000-01-02T00:00:02Z",222],["2000-01-02T00:00:03Z",333],["2000-01-03T00:00:01Z",1],["2000-01-03T00:00:02Z",2],["2000-01-03T00:00:03Z",3]]}]}]}`,
		},
		&Query{
			name:    "out of order query desc",
			params:  url.Values{"db": []string{"db0"}},
			command: `select value from "cpu" order by time desc`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2000-01-03T00:00:03Z",3],["2000-01-03T00:00:02Z",2],["2000-01-03T00:00:01Z",1],["2000-01-02T00:00:03Z",333],["2000-01-02T00:00:02Z",222],["2000-01-02T00:00:01Z",111]]}]}]}`,
		},
	}...)

	for _, query := range test.queries {
		if query.skip {
			t.Logf("SKIP:: %s", query.name)
			continue
		}
		if err := query.Execute(s); err != nil {
			t.Error(query.Error(err))
		} else if !query.success() {
			t.Error(query.failureMessage())
		}
	}
}

func TestServer_Query_FullSeries(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=101 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=102 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=103 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "full series and no result",
			params:  url.Values{"db": []string{"db0"}},
			command: `select /*+ full_series */ value from cpu where (host = 'server05' AND region = 'uswest')`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "full series and single tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select /*+ full_series */ value from cpu where (host = 'server01')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:00Z",100]]}]}]}`,
		},
		&Query{
			name:    "full series normal",
			params:  url.Values{"db": []string{"db0"}},
			command: `select /*+ full_series */ value from cpu where (host = 'server01' AND region = 'uswest')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:00Z",101]]}]}]}`,
		},
		&Query{
			name:    "full series or field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select /*+ full_series */ value from cpu where (host = 'server01' AND region = 'uswest' OR value > 99)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:00Z",101]]}]}]}`,
		}}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})
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

func TestServer_Query_SpecificSeries(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	writes := []string{
		fmt.Sprintf(`cpu,host=server01 value=100 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=uswest value=101 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server01,region=useast value=102 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=useast value=103 %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "specific series and no result",
			params:  url.Values{"db": []string{"db0"}},
			command: `select /*+ specific_series */ value from cpu where (host = 'server05' AND region = 'uswest')`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "specific series and single tag",
			params:  url.Values{"db": []string{"db0"}},
			command: `select /*+ specific_series */ value from cpu where (host = 'server01')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:00Z",100],["2009-11-10T23:00:00Z",101],["2009-11-10T23:00:00Z",102]]}]}]}`,
		},
		&Query{
			name:    "specific series normal",
			params:  url.Values{"db": []string{"db0"}},
			command: `select /*+ specific_series */ value from cpu where (host = 'server01' AND region = 'uswest')`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:00Z",101]]}]}]}`,
		},
		&Query{
			name:    "specific series or field",
			params:  url.Values{"db": []string{"db0"}},
			command: `select /*+ specific_series */ value from cpu where (host = 'server01' AND region = 'uswest' OR value > 99)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","value"],"values":[["2009-11-10T23:00:00Z",101]]}]}]}`,
		}}...)

	var once sync.Once
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
			once.Do(func() {
				if err := test.init(s); err != nil {
					t.Fatalf("test init failed: %s", err)
				}
			})
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

func TestServer_HintQuery_FilterNullColumn(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("mst,tk1=tv1 f1=0i 1610380800000000000\n" +
			"mst,tk1=tv2 f1=1i    1610467200000000000\n" +
			"mst,tk2=tv3 f2=false 1610553600000000000\n" +
			"mst,tk2=tv4 f2=true  1610640000000000000")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "show series",
			params:  url.Values{"db": []string{"db0"}},
			command: `show series`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["mst,tk1=tv1"],["mst,tk1=tv2"],["mst,tk2=tv3"],["mst,tk2=tv4"]]}]}]}`),
		},
		&Query{
			name:    "single field with tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Filter_Null_Column */ f1,*::tag from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","f1","tk1"],"values":[["2021-01-11T16:00:00Z",0,"tv1"],["2021-01-12T16:00:00Z",1,"tv2"]]}]}]}`),
		},
		&Query{
			name:    "single field group by tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Filter_Null_Column */ f1,*::tag from mst group by *::tag`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"tk1":"tv1","tk2":""},"columns":["time","f1"],"values":[["2021-01-11T16:00:00Z",0]]},{"name":"mst","tags":{"tk1":"tv2","tk2":""},"columns":["time","f1"],"values":[["2021-01-12T16:00:00Z",1]]}]}]}`),
		},
		&Query{
			name:    "single field with tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Filter_Null_Column */ f2,*::tag from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","f2","tk2"],"values":[["2021-01-13T16:00:00Z",false,"tv3"],["2021-01-14T16:00:00Z",true,"tv4"]]}]}]}`),
		},
		&Query{
			name:    "single field group tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Filter_Null_Column */ f2,*::tag from mst group by *::tag`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"tk1":"","tk2":"tv3"},"columns":["time","f2"],"values":[["2021-01-13T16:00:00Z",false]]},{"name":"mst","tags":{"tk1":"","tk2":"tv4"},"columns":["time","f2"],"values":[["2021-01-14T16:00:00Z",true]]}]}]}`),
		},
	}...)

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

func TestServer_HintQuery_ManyNullColumns(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("mst,tk1=tv1 f1=0i 1610380800000000000\n" +
			"mst,tk1=tv2 f1=1i    1610467200000000000\n" +
			"mst,tk2=tv3 f2=2i 1610553600000000000\n" +
			"mst,tk3=tv4 f3=true  1610640000000000000")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "show series",
			params:  url.Values{"db": []string{"db0"}},
			command: `show series`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"columns":["key"],"values":[["mst,tk1=tv1"],["mst,tk1=tv2"],["mst,tk2=tv3"],["mst,tk3=tv4"]]}]}]}`),
		},
		&Query{
			name:    "single field f1 with tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Filter_Null_Column */ f1,*::tag from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","f1","tk1"],"values":[["2021-01-11T16:00:00Z",0,"tv1"],["2021-01-12T16:00:00Z",1,"tv2"]]}]}]}`),
		},
		&Query{
			name:    "single field f1 group by tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Filter_Null_Column */ f1,*::tag from mst group by *::tag`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"tk1":"tv1","tk2":"","tk3":""},"columns":["time","f1"],"values":[["2021-01-11T16:00:00Z",0]]},{"name":"mst","tags":{"tk1":"tv2","tk2":"","tk3":""},"columns":["time","f1"],"values":[["2021-01-12T16:00:00Z",1]]}]}]}`),
		},
		&Query{
			name:    "single field f2 with tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Filter_Null_Column */ f2,*::tag from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","f2","tk2"],"values":[["2021-01-13T16:00:00Z",2,"tv3"]]}]}]}`),
		},
		&Query{
			name:    "single field f2 group tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Filter_Null_Column */ f2,*::tag from mst group by *::tag`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","tags":{"tk1":"","tk2":"tv3","tk3":""},"columns":["time","f2"],"values":[["2021-01-13T16:00:00Z",2]]}]}]}`),
		},
		&Query{
			name:    "exact count query",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Exact_Statistic_Query */ count(*) from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","count_f1","count_f2","count_f3"],"values":[["1970-01-01T00:00:00Z",2,1,1]]}]}]}`),
		},
		&Query{
			name:    "exact first query",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Exact_Statistic_Query */ first(*) from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","first_f1","first_f2","first_f3"],"values":[["1970-01-01T00:00:00Z",0,2,true]]}]}]}`),
		},
	}...)

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

func TestServer_HintLimit(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("mst,tk1=tv9 f1=9i 1610380800000000000\n" +
			"mst,tk1=tv2 f1=2i    1610380800000000000\n" +
			"mst,tk1=tv3 f1=3i 1610380800000000000\n" +
			"mst,tk1=tv4 f1=4i  1610380800000000000\n" +
			"mst,tk1=tv5 f1=5i 1610380800000000000\n" +
			"mst,tk1=tv6 f1=6i 1610380800000000000\n" +
			"mst,tk1=tv7 f1=7i 1610380800000000000\n" +
			"mst,tk1=tv8 f1=8i 1610380800000000000\n" +
			"mst,tk1=tv1 f1=1i 1610380800000000000\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "exact limit * query",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Exact_Statistic_Query */ * from mst limit 1`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","f1","tk1"],"values":[["2021-01-11T16:00:00Z",1,"tv1"]]}]}]}`),
		},
		&Query{
			name:    "exact limit field query",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select /*+ Exact_Statistic_Query */ f1 from mst limit 1`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","f1"],"values":[["2021-01-11T16:00:00Z",1]]}]}]}`),
		},
	}...)

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
func TestServer_FullJoin(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("mst,tk1=tv1 f1=1i 1610380800000000000\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "full join on one tag",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select m1.f1, m2.f1 from (select f1 from mst) as m1 full join (select f1 from mst) as m2 on (m1.tk1 = m2.tk1) group by tk1`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"m1,m2","tags":{"tk1":"tv1"},"columns":["time","m1.f1","m2.f1"],"values":[["2021-01-11T16:00:00Z",1,1]]}]}]}`),
		},
	}...)

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
func TestServer_Write_Compatible(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")

	type TestCase struct {
		name      string
		writes    []string
		skip      bool
		expectErr error
	}
	var testCases = []TestCase{
		{
			name: "duplicated fields",
			writes: []string{
				fmt.Sprintf(`mst,tk1=tv1 f1=0,f1=2 %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T22:00:00Z").UnixNano()),
			},
			expectErr: nil,
		},
		{
			name: "duplicated time",
			writes: []string{
				fmt.Sprintf(`mst,tk1=tv1 f1=3,time=1,f2=2,time=2 %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T22:01:00Z").UnixNano()),
			},
			expectErr: nil,
		},
		{
			name: "duplicated field, diffrent type",
			writes: []string{
				fmt.Sprintf(`mst,tk1=tv1 f1=4,f1="foo" %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T22:02:00Z").UnixNano()),
			},
			expectErr: fmt.Errorf("partial write: conflict field type: f1 dropped=1"),
		},
		{
			name: "duplicated field, diffrent type",
			writes: []string{
				fmt.Sprintf(`mst,tk1=tv1 f1="bar",f1=5 %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T22:03:00Z").UnixNano()),
			},
			expectErr: fmt.Errorf("partial write: conflict field type: f1 dropped=1"),
		},
		{
			name: "duplicated tag",
			writes: []string{
				fmt.Sprintf(`mst,tk1=tv1,tk1=tv2 f1=6 %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T22:05:00Z").UnixNano()),
			},
			expectErr: fmt.Errorf("duplicate tag tk1"),
		},
		{
			name: "time tag",
			writes: []string{
				fmt.Sprintf(`mst,tk1=tv1,time=123 f1=6 %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T22:05:00Z").UnixNano()),
			},
			skip:      true,
			expectErr: fmt.Errorf("not support time tag"),
		},
		{
			name: "normal",
			writes: []string{
				fmt.Sprintf(`mst,tk3=tv4 f3=99 %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T23:00:00Z").UnixNano()),
			},
			expectErr: nil,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip {
				t.Skipf("SKIP:: %s", tt.name)
			}
			test.initialized = false
			test.writes = Writes{
				&Write{data: strings.Join(tt.writes, "\n")},
			}
			err := test.init(s)
			if tt.expectErr == nil && err == nil {
				return
			} else if tt.expectErr != nil && err != nil {
				if !strings.Contains(err.Error(), tt.expectErr.Error()) {
					t.Fatal(err)
				}
			} else if tt.expectErr == nil && err != nil {
				t.Fatal(err)
			} else if tt.expectErr != nil && err == nil {
				t.Fatal("should be error:", tt.expectErr.Error())
			}
		})
	}

	test.addQueries([]*Query{
		{
			name:    "select count(*) from mst",
			params:  url.Values{"db": []string{"db0"}},
			command: `select count(*) from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","count_f1","count_f2","count_f3"],"values":[["1970-01-01T00:00:00Z",2,1,1]]}]}]}`),
		},
		{
			name:    "select * from mst",
			params:  url.Values{"db": []string{"db0"}},
			command: `select * from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","f1","f2","f3","tk1","tk3"],"values":[["2022-06-10T22:00:00Z",2,null,null,"tv1",null],["2022-06-10T22:01:00Z",3,2,null,"tv1",null],["2022-06-10T23:00:00Z",null,null,99,null,"tv4"]]}]}]}`),
		},
	}...)

	_, _ = http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)
	time.Sleep(time.Second / 10)

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_DuplicateField(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")

	writes := []string{
		fmt.Sprintf(`mst,tk1=tv1 f1=0,f1=2 %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T22:00:00Z").UnixNano()),
		fmt.Sprintf(`mst,tk3=tv4 f3=99 %d`, mustParseTime(time.RFC3339Nano, "2022-06-10T23:00:00Z").UnixNano()),
	}
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	test.addQueries([]*Query{
		{
			name:    "select count(*) from mst",
			params:  url.Values{"db": []string{"db0"}},
			command: `select count(*) from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","count_f1","count_f3"],"values":[["1970-01-01T00:00:00Z",1,1]]}]}]}`),
		},
		{
			name:    "select * from mst",
			params:  url.Values{"db": []string{"db0"}},
			command: `select * from mst`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","f1","f3","tk1","tk3"],"values":[["2022-06-10T22:00:00Z",2,null,"tv1",null],["2022-06-10T23:00:00Z",null,99,null,"tv4"]]}]}]}`),
		},
	}...)
	_ = test.init(s)
	_, _ = http.Post(s.URL()+"/debug/ctrl?mod=flush", "", nil)
	time.Sleep(time.Second / 10)
	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

func TestServer_Field_Not_In_Condition(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("mst,tk1=tv9 f1=9i 1610380800000000000\n" +
			"mst,tk1=tv2 f1=2i    1610380800000000000\n" +
			"mst,tk1=tv3 f1=3i 1610380800000000000\n" +
			"mst,tk1=tv4 f1=4i  1610380800000000000\n" +
			"mst,tk1=tv5 f1=5i 1610380800000000000\n" +
			"mst,tk1=tv6 f1=6i 1610380800000000000\n" +
			"mst,tk1=tv7 f1=7i 1610380800000000000\n" +
			"mst,tk1=tv8 f1=8i 1610380800000000000\n" +
			"mst,tk1=tv1 f1=1i 1610380800000000000\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "field condition exists",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select sum(*) from mst where f1= 2`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum_f1"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`),
		},
		&Query{
			name:    "field condition not exist",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select sum(*) from mst where f2=3`,
			exp:     fmt.Sprintf(`{"results":[{"statement_id":0}]}`),
		},
	}...)

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

func RegisteredIndexes() []string {
	a := make([]string, 0, 1)
	a = append(a, "tsi")
	return a
}

func Test_killInfluxDB3(t *testing.T) {
	type args struct {
		t *testing.T
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := killInfluxDB3(tt.args.t); (err != nil) != tt.wantErr {
				t.Errorf("killInfluxDB3() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_startInfluxDB3(t *testing.T) {
	type args struct {
		t *testing.T
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startInfluxDB3(tt.args.t)
		})
	}
}

func TestInitTestEnv(t *testing.T) {
	type args struct {
		t        *testing.T
		bodyType string
		body     io.Reader
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InitTestEnv(tt.args.t, tt.args.bodyType, tt.args.body); (err != nil) != tt.wantErr {
				t.Errorf("InitTestEnv() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInitHaTestEnv(t *testing.T) {
	type args struct {
		t *testing.T
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitHaTestEnv(tt.args.t)
		})
	}
}

func TestReleasTestEnv(t *testing.T) {
	type args struct {
		t        *testing.T
		bodyType string
		body     io.Reader
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ReleasTestEnv(tt.args.t, tt.args.bodyType, tt.args.body); (err != nil) != tt.wantErr {
				t.Errorf("ReleasTestEnv() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestReleaseHaTestEnv(t *testing.T) {
	type args struct {
		t *testing.T
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ReleaseHaTestEnv(tt.args.t)
		})
	}
}

func Test_generateBool(t *testing.T) {
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateBool(tt.args.i); got != tt.want {
				t.Errorf("generateBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_generateFloat(t *testing.T) {
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateFloat(tt.args.i); got != tt.want {
				t.Errorf("generateFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_generateString(t *testing.T) {
	type args struct {
		i int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := generateString(tt.args.i); got != tt.want {
				t.Errorf("generateString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRegisteredIndexes(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RegisteredIndexes(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RegisteredIndexes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func holtWinterResultCompare(dst []byte, exp []byte) bool {
	sampleRegex := regexp.MustCompile(`[1-9]\d*\.\d+`)
	dstNumsIndexs := sampleRegex.FindAllIndex(dst, len(dst))
	expNumsIndexs := sampleRegex.FindAllIndex(exp, len(exp))
	if len(dstNumsIndexs) != len(expNumsIndexs) {
		return false
	}
	var dstNums []float64
	var expNums []float64
	var dLast []byte
	var eLast []byte
	dStart := 0
	eStart := 0
	for i, dIndex := range dstNumsIndexs {
		if len(dIndex) != 2 || len(expNumsIndexs[i]) != 2 {
			return false
		}
		dnum, _ := strconv.ParseFloat(string(dst[dIndex[0]:dIndex[1]]), 64)
		dstNums = append(dstNums, dnum)
		enum, _ := strconv.ParseFloat(string(exp[expNumsIndexs[i][0]:expNumsIndexs[i][1]]), 64)
		expNums = append(expNums, enum)
		dLast = append(dLast, dst[dStart:dIndex[0]]...)
		dStart = dIndex[1]
		eLast = append(eLast, exp[eStart:expNumsIndexs[i][0]]...)
		eStart = expNumsIndexs[i][1]
	}
	dLast = append(dLast, dst[dStart:]...)
	eLast = append(eLast, exp[eStart:]...)
	for i, dnum := range dstNums {
		if math.Abs(dnum-expNums[i]) >= 0.0001 {
			return false
		}
	}
	return bytes.Equal(dLast, eLast)
}

func TestServer_HoltWinters(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("cpu,host=server1 value=10 1597042800000000000\n" +
			"cpu,host=server2 value=22 1597043400000000000\n" +
			"cpu,host=server3 value=37 1597044000000000000\n" +
			"cpu,host=server4 value=15 1597044600000000000\n" +
			"cpu,host=server5 value=48 1597045200000000000\n" +
			"cpu,host=server6 value=27 1597045800000000000\n" +
			"cpu,host=server7 value=80 1597046400000000000\n" +
			"cpu,host=server8 value=69 1597047000000000000\n" +
			"cpu,host=server9 value=39 1597047600000000000\n" +
			"cpu,host=server10 value=57 1597048200000000000\n" +
			"cpu,host=server11 value=25 1597048800000000000\n" +
			"cpu,host=server12 value=98 1597049400000000000\n")},
	}

	test.addQueries([]*Query{
		&Query{
			name:    "holt_winters query",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `SELECT HOLT_WINTERS(FIRST(value),6,4) FROM cpu WHERE  time >= '2020-08-10T07:00:00Z' AND time <= '2020-08-10T08:50:00Z' GROUP BY time(20m)`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["time","holt_winters"],"values":[["2020-08-10T09:00:00Z",47.997143423738216],["2020-08-10T09:20:00Z",80.01574957657594],["2020-08-10T09:40:00Z",39.0206910442188],["2020-08-10T10:00:00Z",30.99629387435043],["2020-08-10T10:20:00Z",47.99669178533439],["2020-08-10T10:40:00Z",80.01631956468897]]}]}]}`,
		},
	}...)

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
			} else if !holtWinterResultCompare([]byte(query.act), []byte(query.exp)) {
				t.Error(query.failureMessage())
			}
		})
	}
}

func TestServer_FieldIndex_Query(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewConfig())
	defer s.Close()

	if err := s.CreateDatabaseAndRetentionPolicy("db0", NewRetentionPolicySpec("rp0", 1, 0), true); err != nil {
		t.Fatal(err)
	}

	createQuery := &Query{
		name:    `create measurement cpu`,
		command: "CREATE MEASUREMENT cpu with indextype \"field\" indexlist field_index",
		params:  url.Values{"db": []string{"db0"}},
		exp:     `{"results":[{"statement_id":0}]}`,
	}

	t.Run(createQuery.name, func(t *testing.T) {
		if err := createQuery.Execute(s); err != nil {
			t.Error(createQuery.Error(err))
		} else if !createQuery.success() {
			t.Error(createQuery.failureMessage())
		}
	})

	writes := []string{
		fmt.Sprintf(`cpu,host=server01,region=uswest value=100,field_index="127.0.0.1" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server02,region=uswest value=100,field_index="127.0.0.2" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server03,region=uswest value=100,field_index="127.0.0.3" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server04,region=uswest value=100,field_index="127.0.0.4" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server05,region=uswest value=100,field_index="127.0.0.5" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
		fmt.Sprintf(`cpu,host=server06,region=uswest value=100,field_index="127.0.0.6" %d`, mustParseTime(time.RFC3339Nano, "2009-11-10T23:00:00Z").UnixNano()),
	}

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: strings.Join(writes, "\n")},
	}

	if err := test.init(s); err != nil {
		t.Fatalf("test init failed: %s", err)
	}

	test.addQueries([]*Query{
		&Query{
			name:    `show series exact cardinality`,
			command: "SHOW SERIES EXACT CARDINALITY",
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","columns":["count"],"values":[[6]]}]}]}`,
		},
		&Query{
			name:    `show series cardinality`,
			command: "SHOW SERIES CARDINALITY",
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"columns":["startTime","endTime","count"],"values":[["2009-11-09T00:00:00Z","2009-11-16T00:00:00Z",6]]}]}]}`,
		},
		&Query{
			name:    `select * from cpu group by field_index`,
			command: "SELECT * FROM cpu GROUP BY field_index",
			params:  url.Values{"db": []string{"db0"}},
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"field_index":"127.0.0.1"},"columns":["time","field_index","host","region","value"],"values":[["2009-11-10T23:00:00Z","127.0.0.1","server01","uswest",100]]},{"name":"cpu","tags":{"field_index":"127.0.0.2"},"columns":["time","field_index","host","region","value"],"values":[["2009-11-10T23:00:00Z","127.0.0.2","server02","uswest",100]]},{"name":"cpu","tags":{"field_index":"127.0.0.3"},"columns":["time","field_index","host","region","value"],"values":[["2009-11-10T23:00:00Z","127.0.0.3","server03","uswest",100]]},{"name":"cpu","tags":{"field_index":"127.0.0.4"},"columns":["time","field_index","host","region","value"],"values":[["2009-11-10T23:00:00Z","127.0.0.4","server04","uswest",100]]},{"name":"cpu","tags":{"field_index":"127.0.0.5"},"columns":["time","field_index","host","region","value"],"values":[["2009-11-10T23:00:00Z","127.0.0.5","server05","uswest",100]]},{"name":"cpu","tags":{"field_index":"127.0.0.6"},"columns":["time","field_index","host","region","value"],"values":[["2009-11-10T23:00:00Z","127.0.0.6","server06","uswest",100]]}]}]}`,
		},
	}...)

	InitHaTestEnv(t)

	for _, query := range test.queries {
		t.Run(query.name, func(t *testing.T) {
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

	ReleaseHaTestEnv(t)
}

func TestServer_TagArray(t *testing.T) {
	t.Parallel()
	s := OpenServer(NewParseConfig(testCfgPath))
	defer s.Close()

	test := NewTest("db0", "rp0")
	test.writes = Writes{
		&Write{data: fmt.Sprintf("mst,tk1=tv9 f1=9i 1610380800000000000\n" +
			"mst,tk1=[tv2,tv3,tv4,tv5,tv6,tv7,tv8,tv9] f1=2i 1610380800000000000\n" +
			"mst,tk1=[tv10,tv11] f1=10i 1610380800000000000\n")},
	}

	if err := s.CreateDatabaseTagArrayEnabledAndRp(test.database(), NewRetentionPolicySpec(test.retentionPolicy(), 1, 0), true); err != nil {
		t.Fatal(err)
	}

	test.addQueries([]*Query{
		&Query{
			name:    "field condition exists 1",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select sum(*) from mst where f1=2`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum_f1"],"values":[["1970-01-01T00:00:00Z",16]]}]}]}`,
		},
		&Query{
			name:    "field condition exist 2",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select sum(*) from mst where f1=10`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum_f1"],"values":[["1970-01-01T00:00:00Z",20]]}]}]}`,
		},
		&Query{
			name:    "field condition not exist",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select sum(*) from mst where f2=3`,
			exp:     `{"results":[{"statement_id":0}]}`,
		},
		&Query{
			name:    "no filed condition of sum",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select sum(*) from mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum_f1"],"values":[["1970-01-01T00:00:00Z",45]]}]}]}`,
		},
		&Query{
			name:    "no filed condition of count",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select count(*) from mst`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","count_f1"],"values":[["1970-01-01T00:00:00Z",11]]}]}]}`,
		},
		&Query{
			name:    "tag condition",
			params:  url.Values{"db": []string{"db0"}, "chunk_size": []string{"1"}, "inner_chunk_size": []string{"1"}},
			command: `select sum(*) from mst where tk1='tv4'`,
			exp:     `{"results":[{"statement_id":0,"series":[{"name":"mst","columns":["time","sum_f1"],"values":[["1970-01-01T00:00:00Z",2]]}]}]}`,
		},
	}...)

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
