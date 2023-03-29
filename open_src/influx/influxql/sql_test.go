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

package influxql_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

var cases []string
var benchCases []string

func init() {
	cases = []string{
		"select f1 From table1",                                             // base function test.
		"select f1::tag,f2::float From table1",                              // find specified column.
		"select f1 From db1.table1",                                         // add database.mst .
		"select f1....f2..f3 from table",                                    // check "."
		"select f1,f2 From table1",                                          // add multiple columns.
		"select f1 From table1 ORDER BY c",                                  // add order by token.
		"select f1 From table1 ORDER BY c ASC ",                             // add order by token ASC.
		"select f1 as f From table1",                                        // check as token.
		"select * from cpu where usage_user * 2 -1 >usage_user+2*5",         // add where.
		"select f1 From table1 GROUP BY tg1 fill(3) ORDER BY c ASC",         // add group by token.
		"select f1,f2 From table1 GROUP BY tg1,tg2 ORDER BY c ASC",          // add group by token.
		"select f1 as f From table1 group by f1 ,time(1s)",                  // group by time.
		"select f1 as f From table1 group by f1 ,time(1s) limit 1 offset 1", // add limit offset.
		"select f1 as f From table1 group by f1 ,time(1s) offset 1",         // add limit offset.
		"select f1 as f From table1 limit 1 offset 1 slimit 2 soffset 2",    // add slimit soffset.
		"select * from cpu where time +1 < 10 and time > 0",                 // add comparison symbol.
		"select * from cpu where time +1 < 10 and time > -1",                // add negative number.
		"select min(f1) + max(f1)*2 From table1 GROUP BY f2",                // call + call.
		"select (min(f1) + max(f1))*2 From table1 GROUP BY f2",              // (call + call) * call
		"select min(max(mean(f1))) from table1",                             //multiple calls
		"select min(f1) from table1 group by *",                             //group by *
		"select min(f1) from table1 group by time(0s)",                      //group by time(a)
		"select min(f1) from table1 group by time(1m,1m)",                   //group by time(a,b)
		"select min(f1) from table1 group by time(1m) fill(linear)",         // add fill(linear)
		"select min(f1) from table1 group by time(1m) fill(null)",           // add fill(null)
		"select min(f1) from table1 group by time(1m) fill(null)",           // add fill(none)
		"select min(f1) from table1 group by time(1m) fill(100)",            // add fill(number)
		"select min(f1) from table1 group by time(1m) fill(previous)",       // add fill(previous)
		"select f1 + f2 as a from table1",                                   // add binary expression in field.
		"select sum(f1+f2) as s, min(f1) as m, percentile(f1, f2) From table1 group by f1 ,time(1s) limit 1 offset 1 slimit 2 soffset 2", // add agg function.
		"select f1 + 1 as a from table1", // add var expr.
		"select f2, (case when F1 > F2 then A when f1 > f3 then C else B end),case when F1 > F2 then A when f1 > f3 then C else B end from mst", // add case when.
		//"select a from table1 full outer join table2 on table1.f1 = table2.f2",                                                                  // add join.
		//"select a from table1 full outer join table2 on table1.f1 = table2.f2 full outer join table3 on table1.t1 != table3.t3",                 // add join.
		"select a from (select f1 as a from table1)",                                                     // add subquery.
		"select a,b,c from (select f1 as a from table1), (select sum(f2) as b from table2), table3",      // add multiple subqueries.
		"select a from table1 where a IN (SELECT * FROM TABLE1) AND B NOT IN (C)",                        // IN AND NOT
		"select a from table1 where EXISTS (SELECT * FROM TABLE1) AND NOT EXISTS (SELECT * FROM TABLE1)", // exists.
		//"select a, b+c, sum(c/d), sum(case when F1 > F2 then A when f1 > f3 then C else B end) from table1 full outer join table2 on table1.f1 = table2.f2 full outer join table3 on table1.t1 != table3.t3,(select * from table4)  where a != 1 and b != 2 and a IN (SELECT * FROM TABLE1) AND B NOT IN (C) and EXISTS (SELECT * FROM TABLE1) AND NOT EXISTS (SELECT * FROM TABLE1) group by f1, time(1s) fill(linear) ORDER BY c ASC limit 1 offset 1 slimit 2 soffset 2",
		"CREATE RETENTION POLICY rp3 ON db0 DURATION 1h REPLICATION 1",                                                             //add create retention policy.
		"show series from table where a>b limit 1 offset 1",                                                                        //add show series statement.
		"drop series from a where b > c and time < now() -1d",                                                                      //add drop series.
		"CREATE DATABASE a WITH DURATION 3d REPLICATION 1 SHARD DURATION 1h name AStt",                                             //add create db.
		"CREATE DATABASE a WITH DURATION 3d SHARD DURATION 1h REPLICANUM 1 REPLICATION 1 warm DURATION 7d name AStt",               //add Out-of-order
		"create database test with shardkey tag1,tag2 duration 1d shard duration 1d index duration 1d hot duration 1d name testrp", // add create database with shardkey
		"ALTER RETENTION POLICY rp3 ON db0 DURATION 1h REPLICATION 1",                                                              //add alter retention policy
		"DROP RETENTION POLICY rp3 ON db7",                                                                                         //add drop retention policy
		"delete from add_test where time=1564483",                                                                                  //add delete series.
		"select * from A,db0.C,d,db1.e",                                                                                            //add db.table
		"drop database db7",                                                                                                        //add drop database.
		"select /f1.*/ from /^cpu.*/",                                                                                              //add regular expression
		"select /*+ Filter_Null_Column */ f1,*::tag from mst",                                                                      //add hint
		"SHOW USERS", // add show users
		"CREATE USER jdoe WITH PASSWORD 'Jdoe@1337'",                               //add create user with.
		"grant all privileges to jdoe",                                             //grant privileges to admin.
		"GRANT READ ON db0 TO jdoe",                                                //grant privileges to normal user.
		"DROP USER jdoe",                                                           //drop user
		"REVOKE all privileges FROM admin",                                         //revoke from admin
		"REVOKE READ ON db0 FROM admin",                                            // revoke from normal user.
		"SHOW TAG KEYS on db0  from db0 where a>0 ",                                // show tag keys
		"SHOW TAG values on db0 from t1 with key = k1 where ta>0 limit 2 offset 1", // show tag keys
		"show field keys on db0 from t1 ",                                          // show tag keys
		"SHOW TAG VALUES FROM cpu WITH KEY =~ /(host|region)/ WHERE region = 'uswest' AND time > 0", // add =~
		"SHOW TAG VALUES WITH KEY = host WHERE region =~ /us/ AND time > 0",                         // add !~
		"SHOW TAG VALUES WITH KEY = region WHERE host !~ /server0[12]/",                             // add int
		"explain analyze select * from a where b>0",                                                 // add explain analyze
		"explain select * from a where b>0",                                                         // add explain
		"SHOW FIELD KEY CARDINALITY",                                                                // add show field key cardinality
		"SHOW TAG VALUES EXACT CARDINALITY WITH KEY = host WHERE region =~ /ca.*/",                  // add show tag values cardinality
		"SHOW TAG KEY EXACT CARDINALITY",                                                            // add show tag key cardinality
		"SELECT /l/ FROM \"h2o_feet\" LIMIT 1",                                                      //add select regular
		"SELECT DISTINCT(/l/) FROM \"h2o_feet\" LIMIT 1",                                            //add select regular
		"SELECT * FROM h2o_feet where a>/l/ group by /l/ LIMIT 1",                                   //add group by regular
		"SELECT B % 2 FROM h2o_feet where a>/l/ group by /l/ LIMIT 1",                               //add group by regular
		"SELECT B % 2 FROM h2o_feet",                                                                //add %
		"SELECT A & 255 FROM bitfields",                                                             //add &
		"SELECT A | B FROM bitfields",                                                               //add |
		"SELECT A ^ B FROM bitfields",                                                               //add ^
		"SHOW SERIES on db0",
		"alter retention policy re on db default",                    // alter retention expected DURATION, REPLICATION, SHARD, DEFAULT
		"create retention policy re on db duration 1h replication 1", //create retention essense duration->replication
		"SHOW SERIES CARDINALITY",                                    //add show series cardinality
		"ALTER MEASUREMENT db0",                                      //add alter measurement
		"SHOW SHARD GROUPS",                                          //add show shard groups
		"SHOW SHARDS",                                                //add show shards
		"SHOW SERIES EXACT CARDINALITY on db0",                       //add show series cardinality
		"SHOW MEASUREMENT EXACT CARDINALITY on db0",                  //add SHOW MEASUREMENT EXACT CARDINALITY
		"SHOW GRANTS FOR db",                                         //add SHOW GRANTS
		"DROP SHARD 3",                                               //add DROP SHARD
		"set password for user3 = 'guass_345'",                       //add SET PASSWORD
		"CREATE MEASUREMENT db0",                                     //add CREATE MEASUREMENT
		"select * from db where a>0 tz('UTC')",                       //add time zone
		"drop measurement m1",                                        //drop measurement
		"select * from (select * from t1;select * from t2)",
		"alter measurement tb1", //alter measurement
		"create measurement cpu with indextype text indexlist msg shardkey hostname type range",
		"create measurement cpu with indextype text indexlist msg",
		"create measurement cpu with indextype text indexlist msg text1 indexlist msg1,msg2",
		"create measurement TSDB_SIT_AlterMeasurement_BaseFunction_002 with shardkey tag1,tag2",
		"create user xxxxx with password 'xxxx' with partition privileges", // add partition privileges.
		// select into
		"select a into bd.rp.mst from mst",
		//continuous query
		"create continuous query cq on db0 begin select a into db.rp.mst from mst end",
		"create continuous query cq on db0 resample every 10s begin select a into db.rp.mst from mst end",
		"create continuous query cq on db0 resample for 10s begin select a into db.rp.mst from mst end",
		"create continuous query cq on db0 resample every 10s for 5s begin select a into db.rp.mst from mst end",
		"show continuous queries",
		"drop continuous query cq on db",
		//downsample
		"create downsample on test.rp (float(sum),int(max)) with duration 1d sampleinterval(1d,2d) timeinterval(1m,3m)",
		"create downsample (float(sum),int(max)) with duration 1d sampleinterval(1d,2d) timeinterval(1m,3m)",
		"create downsample on rp (float(sum),int(max)) with duration 1d sampleinterval(1d,2d) timeinterval(1m,3m)",
		"drop downsample on rp",
		"drop downsample on db.rp",
		"drop downsamples",
		"show downsamples on db",
		"show downsamples",
	}

	benchCases = []string{
		"select f1 From table1",                                             // base function test.
		"select f1::tag,f2::float From table1",                              // find specified column.
		"select f1 From db1.table1",                                         // add database.mst .
		"select f1,f2 From table1",                                          // add multiple columns.
		"select f1 From table1 ORDER BY time",                               // add order by token.
		"select f1 From table1 ORDER BY time ASC ",                          // add order by token ASC.
		"select f1 as f From table1",                                        // check as token.
		"select * from cpu where usage_user * 2 -1 >usage_user+2*5",         // add where.
		"select f1,f2 From table1 GROUP BY tg1,tg2 ORDER BY time ASC",       // add group by token.
		"select f1 as f From table1 group by f1 ,time(1s)",                  // group by time.
		"select f1 as f From table1 group by f1 ,time(1s) limit 1 offset 1", // add limit offset.
		"select f1 as f From table1 group by f1 ,time(1s) offset 1",         // add limit offset.
		"select f1 as f From table1 limit 1 offset 1 slimit 2 soffset 2",    // add slimit soffset.
		"select * from cpu where time +1 < 10 and time > 0",                 // add comparison symbol.
		"select * from cpu where time +1 < 10 and time > -1",                // add negative number.
		"select min(f1) + max(f1)*2 From table1 GROUP BY f2",                // call + call.
		"select min(max(mean(f1))) from table1",                             //multiple calls
		"select min(f1) from table1 group by *",                             //group by *
		"select min(f1) from table1 group by time(0s)",                      //group by time(a)
		"select min(f1) from table1 group by time(1m,1m)",                   //group by time(a,b)
		"select f1 + f2 as a from table1",                                   // add binary expression in field.
		"select sum(f1+f2) as s, min(f1) as m, percentile(f1, f2) From table1 group by f1 ,time(1s) limit 1 offset 1 slimit 2 soffset 2", // add agg function.
		"select f1 + 1 as a from table1",                                                            // add var expr. 		// add case when.
		"select a from (select f1 as a from table1)",                                                // add subquery.
		"select a,b,c from (select f1 as a from table1), (select sum(f2) as b from table2), table3", // add multiple subqueries.
		"CREATE RETENTION POLICY rp3 ON db0 DURATION 1h REPLICATION 1",                              //add create retention policy.
		"show series from table where a>b limit 1 offset 1",                                         //add show series statement.
		"drop series from a where b > c and time < now() -1d",                                       //add drop series.
		"CREATE DATABASE a WITH DURATION 3d REPLICATION 1 SHARD DURATION 1h name AStt",              //add create db.
		"ALTER RETENTION POLICY rp3 ON db0 DURATION 1h REPLICATION 1",                               //add alter retention policy
		"DROP RETENTION POLICY rp3 ON db7",                                                          //add drop retention policy
		"delete from add_test where time=1564483",                                                   //add delete series.
		"select * from A,db0.C,d,db1.e",                                                             //add db.table
		"drop database db7",                                                                         //add drop database.
		"select /f1.*/ from /^tag.*/",                                                               //add regular expression
		"select /*+ Filter_Null_Column */ f1,*::tag from mst",                                       //add hint
		"SHOW USERS", // add show users
		"CREATE USER jdoe WITH PASSWORD 'Jdoe@1337'",                     //add create user with.
		"grant all privileges to jdoe",                                   //grant privileges to admin.
		"DROP USER jdoe",                                                 //drop user
		"REVOKE all privileges FROM admin",                               //revoke from admin
		"Drop Shard 123",                                                 //Drop Shard
		"SET PASSWORD FOR \"todd\" = 'password4todd'",                    //add SET PASSWORD
		"SHOW GRANTS FOR \"jdoe\"",                                       //add SHOW GRANTS
		"SHOW MEASUREMENT EXACT CARDINALITY ON mydb",                     //add SHOW MEASUREMENT EXACT CARDINALITY
		"DROP SERIES WHERE a>10",                                         //add DROP SERIES
		"SELECT * FROM a where time >= '2019-10-18T00:00:00Z' tz('UTC')", //add TIME ZONE
		"drop measurement m1",                                            //drop measurement
		"alter measurement tb1",                                          //alter measurement
		"alter measurement tb1 with shardkey tag2,tag1",                  //alter measurement with unsorted key
	}
}

func TestYyParser(t *testing.T) {
	//s := NewScanner(strings.NewReader("SELECT value as a from myseries WHERE a = 'b"))
	YyParser := &influxql.YyParser{
		Query: influxql.Query{},
		//scanner:NewScanner(strings.NewReader("select *  From b where a = 1 order by time")),

	}
	for i, c := range cases {
		YyParser.Scanner = influxql.NewScanner(strings.NewReader(c))
		YyParser.ParseTokens()
		q, err := YyParser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "with sql: %s", q.Statements[i].String())
		}
	}
}

func TestPreviousParser(t *testing.T) {
	for _, c := range []string{
		"select * from (select * from t1)",
	} {
		reader := strings.NewReader(c)
		p := influxql.NewParser(reader)
		q, err := p.ParseQuery()
		if err != nil {
			t.Errorf(err.Error(), "with sql: %s", q.String())
		}
	}
}

func TestParserResult(t *testing.T) {
	for _, c := range benchCases {
		//[]string{
		//"alter retention policy rp on tsdb duration 1h replication 1 shard duration 6h default",
		//"  alter measurement msm",
		//".*",
		//"create measurement tsdb type range",
		//"SHOW MEASUREMENTS ON db_name_052",
		//"CREATE RETENTION POLICY rp1 ON db0 duration 1h replication 1 hot duration 6h default",
		//"ALTER RETENTION POLICY rp ON tsdb hot duration 6h duration 1h ",
		//"CREATE DATABASE IDENT WITH hot duration 6h duration 1h",
		//"ALTER RETENTION POLICY rp ON tsdb  warm duration 6h duration 1h",
		//"SELECT * FROM a where time >= 2019-10-18T00:00:00Z and time <= 2019-10-19T00:00:00Z group by time(12m)",
		//"select * from a where time<123321 order by time DESC group by time(12m)",
		//"select * from mst where tage1 =~/t.*/",
		//"set password for user3 ='Guass_321'",
		//"ALTER RETENTION POLICY rp ON tsdb  hot duration 6h duration 1h ",
		//"select * from (select * from t1 ;select * from t2)",
		//"set password for user3 = 'guass_345'",
		//"SELECT * FROM a where time >= 2019-10-18T00:00:00Z tz('UTC)",
		//"select f1,f2 From table1  where time <100000000000 ORDER BY time ASC GROUP BY tg1,tg2 ",
		//"SHOW SERIES EXACT CARDINALITY ON TSDB_SIT_ShowSeries_BaseFunctions_024 WHERE host = 'server01'",
		//"select min(f1) from table1 group by time(1m) fill(null)",
		//"select min(f1) from table1 group by time(1m) fill(null)",
		//"select aaa from db",
		//}
		{
			YyParser := &influxql.YyParser{
				Query: influxql.Query{},
			}
			YyParser.Scanner = influxql.NewScanner(strings.NewReader(c))
			YyParser.ParseTokens()
			q1, err1 := YyParser.GetQuery()
			if err1 != nil {
				t.Errorf(err1.Error(), "with sql: %s", q1.String())
			}
			reader := strings.NewReader(c)
			p := influxql.NewParser(reader)
			q2, err := p.ParseQuery()
			if err != nil {
				t.Fatal(err.Error())
			}
			if !reflect.DeepEqual(q1.Statements, q2.Statements) {
				t.Errorf("not equal %s", q2.Statements)
			}
		}
	}
}

func TestSingleParser(t *testing.T) {
	YyParser := &influxql.YyParser{
		Query: influxql.Query{},
	}
	c := []string{
		//"SHOW TAG VALUES FROM cpu WITH KEY =~ /(host|region)/ WHERE region = 'uswest' AND time > 0",
		//"select * from (select * from t1)",
		//"create database db? ",
		//"select sum(f1) from (select * from d where a='1')",
		//"select min(f1) from table1 group by time(1m) fill(null)",
		//"select aaa from db;",
		//"select * from (select * from t1;select * from t2)",
		//"drop measurement m1",
		/*
			"select mst1.a,mst2.b from ((select * from mst1) as mst1) full join (select * from mst2) as mst2 on m1.tag1=m2.tag1",
			"create stream test1 into db1.rp1.mst1 on select sum(f1),count(f2) from .rp0.mst0 group by tag1,tag2,time(10s) delay 5s",
			"create stream test1 into db1..mst1 on select sum(f1),count(f2) from mst0 group by tag1,tag2,time(10s) delay 5s",
			"show streams",
			"show streams on db1",
			"drop stream stream1",
		  "select * from mst where a like b and match(a,b) and match_phrase(c,d)",*/
		"create measurement mst0 (tag1name string tag, fieldname int field, index aaa bbb type ccc token cxe tokenizers kkk, index aaa bbb type ccc token cxe, index aaa bbb type ccc)",
	}
	for _, c := range c {
		YyParser.Scanner = influxql.NewScanner(strings.NewReader(c))
		YyParser.ParseTokens()
		q, err := YyParser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "with sql: %s", q.String())
		}
	}
}

func BenchmarkNewParser(b *testing.B) {
	YyParser := &influxql.YyParser{
		Query: influxql.Query{},
		//scanner:NewScanner(strings.NewReader("select *  From b where a = 1 order by time")),
	}
	for i := 0; i < b.N; i++ {
		for _, c := range benchCases {
			YyParser.Query = influxql.Query{}
			YyParser.Scanner = influxql.NewScanner(strings.NewReader(c))
			YyParser.ParseTokens()
		}
	}
}

func BenchmarkPreviousParser(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, c := range benchCases {
			reader := strings.NewReader(c)
			p := influxql.NewParser(reader)
			p.ParseQuery()
		}
	}
}
