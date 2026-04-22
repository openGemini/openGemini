// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package influxql_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
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
		"select a from (select f1 as a from table1)",                                                // add subquery.
		"select a,b,c from (select f1 as a from table1), (select sum(f2) as b from table2), table3", // add multiple subqueries.
		"select a from table1 where a IN (SELECT * FROM TABLE1) AND B NOT IN (C)",                   // IN AND NOT
		"select f1 from mst where f1 in (1)",                                                        // add in for a single int constant
		"select f1 from mst where f1 in (1, 2)",                                                     // add in for multi int constants
		"select f1 from mst where f1 in (a)",                                                        // add in for single string constant
		"select f1 from mst where f1 in (a, b)",                                                     // add in for multi string constant
		"select f1 from mst where f1 in (select f2 from mst2)",                                      // add in for sub-query
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
		"CREATE MEASUREMENT db0 with enginetype = tsstore",           //add CREATE MEASUREMENT
		"select * from db where a>0 tz('UTC')",                       //add time zone
		"drop measurement m1",                                        //drop measurement
		"select * from (select * from t1),(select * from t2)",
		"alter measurement tb1", //alter measurement
		"create measurement cpu with indextype text indexlist msg shardkey hostname type range",
		"create measurement cpu with indextype text indexlist msg",
		"create measurement cpu with indextype text indexlist msg text indexlist msg1,msg2",
		"create measurement TSDB_SIT_AlterMeasurement_BaseFunction_002 with shardkey tag1,tag2",
		"create measurement cpu with enginetype = tsstore indextype text indexlist msg shardkey hostname type range",
		"create measurement cpu with enginetype = tsstore indextype text indexlist msg",
		"create measurement cpu with enginetype = tsstore indextype text indexlist msg text indexlist msg1,msg2",
		"create measurement TSDB_SIT_AlterMeasurement_BaseFunction_002 with enginetype = tsstore shardkey tag1,tag2",
		"create measurement XXX with enginetype = columnstore property",         // empty property = OK
		"create measurement XXX with enginetype = columnstore property x=y",     // one property = OK
		"create measurement XXX with enginetype = columnstore property x=y,y=z", // list of properties = OK
		"create user xxxxx with password 'xxxx' with partition privileges",      // add partition privileges.
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
		"create subscription subs0 on db0.autogen destinations all \"127.0.0.1:1000\", \"127.0.0.1:1001\"",
		"create subscription subs0 on db0 destinations any \"127.0.0.1:1000\", \"127.0.0.1:1001\"",
		"SHOW SUBSCRIPTIONS",
		"DROP ALL SUBSCRIPTIONS",
		"DROP ALL SUBSCRIPTIONS on db0",
		"DROP SUBSCRIPTION subs0 on db0.autogen",
		"DROP SUBSCRIPTION subs0 on db0",
		"create stream test1 into db1..mst1 on select sum(f1),count(f2) from mst0 group by tag1,tag2,time(10s) delay 5s",
		"create stream test1 into db1..mst1 on select sum(f1),count(f2) from mst0 group by tag1,tag2,time(10s)",

		// set config
		`SET CONFIG store "data.write-cold-duration" = aa`,
		`SET CONFIG store 'data.write-cold-duration' = "1s"`,
		`SET CONFIG store "data.max-concurrent-compactions" = 4`,
		`SET CONFIG store "data.max-concurrent-compactions" = 4.1`,
		`SET CONFIG store "retention.check-interval" = "5m"`,
		`SET CONFIG meta logging.level = info`,
		`SET CONFIG store "data.compact-recovery" = false`,
		`SET CONFIG store "data.compact-recovery" = true`,

		// show cluster
		"SHOW CLUSTER",
		"SHOW CLUSTER WHERE nodeID = 2",
		"SHOW CLUSTER WHERE nodeID = 2 AND nodeType = data",
		"SHOW CLUSTER WHERE nodeType = data",
		"SHOW CLUSTER WHERE nodeType = data AND nodeID = 2",
		"SHOW CLUSTER WHERE nodeID = 2 AND nodeType = \"data\"",

		// cte syntax
		"with t1 as (select * from mst) select * from t1",

		// inner join syntax support table options
		"select mean(t1.Active) from t1 INNER JOIN t2 on t1.app=t2.app",
		"with t1 as (select * from \"compact\" limit 10), t2 as (select * from httpd limit 10) select mean(t1.Active) from t1 INNER JOIN t2 on t1.app=t2.app",

		// inner join
		"SELECT t1.cu_as, t1.cu_ts, t2.ce_as, t2.ce_ts FROM  (SELECT count(cpu) AS cu_as, sum(cpu) AS cu_ts FROM CPU WHERE  rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t1 INNER JOIN ( SELECT count(mem) AS ce_as, sum(mem) AS ce_ts FROM Memory WHERE rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t2 ON (t1.rgn = t2.rgn and t1.svc = t2.svc and t1.pAgentSN = t2.pAgentSN) GROUP BY pAgentSN, agentSN, rgn, svc",

		// join
		"SELECT t1.cu_as, t1.cu_ts, t2.ce_as, t2.ce_ts FROM  (SELECT count(cpu) AS cu_as, sum(cpu) AS cu_ts FROM CPU WHERE  rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t1 JOIN ( SELECT count(mem) AS ce_as, sum(mem) AS ce_ts FROM Memory WHERE rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t2 ON (t1.rgn = t2.rgn and t1.svc = t2.svc and t1.pAgentSN = t2.pAgentSN) GROUP BY pAgentSN, agentSN, rgn, svc",

		// left outer join
		"SELECT t1.cu_as, t1.cu_ts, t2.ce_as, t2.ce_ts FROM  (SELECT count(cpu) AS cu_as, sum(cpu) AS cu_ts FROM CPU WHERE  rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t1 LEFT OUTER JOIN ( SELECT count(mem) AS ce_as, sum(mem) AS ce_ts FROM Memory WHERE rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t2 ON (t1.rgn = t2.rgn and t1.svc = t2.svc and t1.pAgentSN = t2.pAgentSN) GROUP BY pAgentSN, agentSN, rgn, svc",

		// left join
		"SELECT t1.cu_as, t1.cu_ts, t2.ce_as, t2.ce_ts FROM  (SELECT count(cpu) AS cu_as, sum(cpu) AS cu_ts FROM CPU WHERE  rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t1 LEFT JOIN ( SELECT count(mem) AS ce_as, sum(mem) AS ce_ts FROM Memory WHERE rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t2 ON (t1.rgn = t2.rgn and t1.svc = t2.svc and t1.pAgentSN = t2.pAgentSN) GROUP BY pAgentSN, agentSN, rgn, svc",

		// right outer join
		"SELECT t1.cu_as, t1.cu_ts, t2.ce_as, t2.ce_ts FROM  (SELECT count(cpu) AS cu_as, sum(cpu) AS cu_ts FROM CPU WHERE  rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t1 RIGHT OUTER JOIN ( SELECT count(mem) AS ce_as, sum(mem) AS ce_ts FROM Memory WHERE rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t2 ON (t1.rgn = t2.rgn and t1.svc = t2.svc and t1.pAgentSN = t2.pAgentSN) GROUP BY pAgentSN, agentSN, rgn, svc",

		// right join
		"SELECT t1.cu_as, t1.cu_ts, t2.ce_as, t2.ce_ts FROM  (SELECT count(cpu) AS cu_as, sum(cpu) AS cu_ts FROM CPU WHERE  rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t1 RIGHT JOIN ( SELECT count(mem) AS ce_as, sum(mem) AS ce_ts FROM Memory WHERE rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t2 ON (t1.rgn = t2.rgn and t1.svc = t2.svc and t1.pAgentSN = t2.pAgentSN) GROUP BY pAgentSN, agentSN, rgn, svc",

		// outer join
		"SELECT t1.cu_as, t1.cu_ts, t2.ce_as, t2.ce_ts FROM  (SELECT count(cpu) AS cu_as, sum(cpu) AS cu_ts FROM CPU WHERE  rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t1 OUTER JOIN ( SELECT count(mem) AS ce_as, sum(mem) AS ce_ts FROM Memory WHERE rgn = '675' AND svc = 'CDN' GROUP BY pAgentSN, agentSN, rgn, svc) AS t2 ON (t1.rgn = t2.rgn and t1.svc = t2.svc and t1.pAgentSN = t2.pAgentSN) GROUP BY pAgentSN, agentSN, rgn, svc",

		// table full join table
		"select * from t1 FULL JOIN t2 on t1.app=t2.app",

		// table as alias full join table as alias
		"select a1.f,a2.f from t1 as a1 FULL JOIN t2 as a2 on a1.app=a2.app",

		// table as alias inner join table as alias
		"select a1.f,a2.f from t1 as a1 INNER JOIN t2 as a2 on a1.app=a2.app",

		// table left outer join table
		"select * from t1 LEFT OUTER JOIN t2 on t1.app=t2.app",

		// table as alias left outer join table as alias
		"select a1.f,a2.f from t1 as a1 LEFT OUTER JOIN t2 as a2 on a1.app=a2.app",

		// table right outer join table
		"select * from t1 RIGHT OUTER JOIN t2 on t1.app=t2.app",

		// table as alias right outer join table as alias
		"select a1.f,a2.f from t1 as a1 RIGHT OUTER JOIN t2 as a2 on a1.app=a2.app",

		// table outer join table
		"select * from t1 OUTER JOIN t2 on t1.app=t2.app",

		// table as alias outer join table as alias
		"select a1.f,a2.f from t1 as a1 OUTER JOIN t2 as a2 on a1.app=a2.app",

		"match path=(startNode{uid:'ELB'})-[es*..3]-() where all(e in edges(path) where edgeprop!='edge-com2') and all(n in nodes(path) where kind='Pod') return path",

		// select with parens
		"(select * from m1)",

		// result union/union all/union by name/union all by name result
		"select * from m1 union select * from m2",
		"(select * from m1) union (select * from m2)",
		"((select * from m1)) union ((select * from m2))",
		"select * from m1 union all select * from m2",
		"select * from m1 union by name select * from m2",
		"select * from m1 union all by name select * from m2",
		// cascade union
		"select * from m1 union select * from m2 union select * from m3",
		"select * from m1 union all by name select * from m2 union select * from m3",
		"select * from m1 union all (select * from m2 union by name (select * from m3))",

		// select from cte union select
		"with t1 as (select * from m1) select * from t1 union all select * from m2",

		// udtf
		"with t1 as (select * from m1) select * from test(t1)",
		"select * from test(t1,'{}')",
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
	for _, c := range cases {
		YyParser.Scanner = influxql.NewScanner(strings.NewReader(c))
		YyParser.ParseTokens()
		_, err := YyParser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "with sql: %s", c)
			break
		}
	}
}

func TestYyDepth(t *testing.T) {
	YyParser := &influxql.YyParser{
		Query: influxql.Query{},
	}
	for _, c := range cases {
		YyParser.Scanner = influxql.NewScanner(strings.NewReader(c))
		YyParser.ParseTokens()
		q1, err := YyParser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "with sql: %s", c)
		}

		YyParser.Scanner = influxql.NewScanner(strings.NewReader(c))
		YyParser.ParseTokens()
		q2, err := YyParser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "with sql: %s", c)
		}
		q2.UpdateDepthForTests()
		if !reflect.DeepEqual(q1, q2) {
			t.Errorf("Updated Depth is not equal for sql: %s", c)

			fmt.Printf("====[%d]%v\n", q1.Depth(), q1)
			influxql.WalkFunc(q1, func(n influxql.Node) { fmt.Printf("  %#v\n", n) })
			fmt.Printf("====[%d]%v\n", q2.Depth(), q2)
			influxql.WalkFunc(q2, func(n influxql.Node) { fmt.Printf("  %#v\n", n) })
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
				t.Errorf(err1.Error(), "with sql: %s", c)
			}
			reader := strings.NewReader(c)
			p := influxql.NewParser(reader)
			q2, err := p.ParseQuery()
			if err != nil {
				t.Fatal(err.Error())
			}
			if !reflect.DeepEqual(q1.Statements, q2.Statements) {
				t.Errorf("[%d]%v not equal [%d]%v", q1.Depth(), q1.Statements, q2.Depth(), q2.Statements)
				fmt.Printf("====[%d]%v\n", q1.Depth(), q1)
				influxql.WalkFunc(q1, func(n influxql.Node) { fmt.Printf("  %#v\n", n) })
				fmt.Printf("====[%d]%v\n", q2.Depth(), q2)
				influxql.WalkFunc(q2, func(n influxql.Node) { fmt.Printf("  %#v\n", n) })
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
		/*		"select mst1.a,mst2.b from ((select * from mst1) as mst1) full join (select * from mst2) as mst2 on m1.tag1=m2.tag1",
				"create stream test1 into db1.rp1.mst1 on select sum(f1),count(f2) from .rp0.mst0 group by tag1,tag2,time(10s) delay 5s",
				"create stream test1 into db1..mst1 on select sum(f1),count(f2) from mst0 group by tag1,tag2,time(10s) delay 5s",
				"show streams",
				"show streams on db1",
				"drop stream stream1",*/
		"select * from mst where thingid = $thingIdTag ",
		"select * from mst where a like b and match(a,b) and matchphrase(c,d)",
		"create measurement mst0 (tag1 tag, field1 int64 field, field2 bool, field3 string, field4 float64)",
		"create measurement mst0 (tag1 tag, field1 int64 field, field2 bool, field3 sTring, field4 flOAt64)",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore SHARDKEY tag1 type hash primarykey tag1 sortkey tag1,field1 property p1=k1,p2=k2",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore type hash primarykey tag1 sortkey tag1,field1 property p1=k1,p2=k2",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore primarykey tag1",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore primarykey tag1,field1 sortkey tag1,field1",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore sortkey field1",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = ColumnStore",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = ColumnStore",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype timecluster(1d)",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype timecluster(1w)",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype timecluster(7d)",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype timecluster(1s)",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype timecluster(1m)",
		"show sortkey from mst",
		"show enginetype from mst",
		"show primarykey from mst",
		"show schema from mst",
		"show indexes from mst",
		"show INDExeS from mst",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype bloomfilter indexlist tag1",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = tsstore indextype text indexlist tag1",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = tsstore indextype field indexlist tag1",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = tsstore indextype field indexlist tag11",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = tsstore indextype text indexlist tag11",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype bloomfilter indexlist tag1 compact block",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype bloomfilter indexlist tag1 compact row",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype timecluster(1m) minmax indexlist field1",
		"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore indextype timecluster(1m) bloomfilter indexlist field1 minmax INDEXLIST field1",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore SHARDKEY tag1 SHARDS AUTO type hash",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = columnstore SHARDKEY tag1 SHARDS 10 type hash",
		"create measurement mst0 (tag1 tag, field1 int64 field) with ENGINETYPE = tsstore SHARDKEY tag1 SHARDS 10 type hash",
		"show shards from db.autogen.mst",
	}
	for _, c := range c {
		YyParser.Scanner = influxql.NewScanner(strings.NewReader(c))
		YyParser.Params = map[string]interface{}{"thingIdTag": "aa"}
		YyParser.ParseTokens()
		q, err := YyParser.GetQuery()
		if err != nil {
			t.Errorf(err.Error(), "with sql: %s", q.String())
		}
	}
}

type ErrCase struct {
	query, err string
}

func TestSingleParserError(t *testing.T) {
	c := []ErrCase{
		{"create measurement mst0 (tag1 tag, field1 int64 field, field2 bool1, field3 string, field4 float64)",
			"expect FLOAT64, INT64, BOOL, STRING for column data type"},
		{"create measurement mst0 (tag1 tag, field1 int64 field, field2 bool, field3 string1, field4 float64)",
			"expect FLOAT64, INT64, BOOL, STRING for column data type"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore shardkey tag11 type hash primarykey tag1 sortkey tag1,field1 property p1=k1,p2=k2",
			"Invalid ShardKey"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore type hash1 primarykey tag1 sortkey tag1,field1 property p1=k1,p2=k2",
			"expect HASH or RANGE for TYPE"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore primarykey tag11 sortkey tag11,field1",
			"Invalid PrimaryKey"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore primarykey tag11",
			"Invalid PrimaryKey/SortKey"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore primarykey tag1 sortkey tag1,field11",
			"Invalid SortKey"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore sortkey field11",
			"Invalid PrimaryKey/SortKey"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore1",
			"syntax error: unexpected IDENT, expecting COLUMNSTORE or TSSTORE"},
		{"create measurement db0.rp0.mst0 (tag1 string tag, field1 int64 field) with enginetype = columnstore",
			"syntax error: unexpected TAG, expecting ',' or ')'"},
		{"create measurement mst0 (column4 float,column1 string,column0 string,column3 float,column2 int) with enginetype = columnstore  shardkey column2,column3 type hash  primarykey column3,column4,column0,column1 sortkey column2,column3,column4,column0,column1",
			"expect FLOAT64, INT64, BOOL, STRING for column data type"},
		{"create measurement mst0 (column4 float64,column1 string,column0 string,column3 float64,column2 int64) with enginetype = columnstore  shardkey column2,column3 type hash  primarykey column3,column4,column0,column1 sortkey column2,column3,column4,column0,column1",
			"PrimaryKey should be left prefix of SortKey"},
		{"show sortkey1 from mst",
			"SHOW command error, only support PRIMARYKEY, SORTKEY, SHARDKEY, ENGINETYPE, INDEXES, SCHEMA, COMPACT"},
		{"show index from mst",
			"syntax error: unexpected INDEX"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = tsstore indextype bloomfilter indexlist tag1",
			"Invalid index type for TSSTORE"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = tsstore indextype field1 indexlist tag1",
			"Invalid index type for TSSTORE"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore indextype field indexlist tag1",
			"Invalid index type for COLUMNSTORE"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore indextype bloomfilter indexlist tag11",
			"Invalid indexlist"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore indextype text indexlist tag11",
			"Invalid indexlist"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore indextype bloomfilter indexlist tag1 compact row0",
			"expect ROW or BLOCK for COMPACT type"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore indextype timecluster(1y)",
			"invalid duration"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore indextype timecluster(1m) field1",
			"syntax error: unexpected $end, expecting INDEXLIST"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore indextype timecluster(1m) minmax1 indexlist field1",
			"Invalid index type for COLUMNSTORE"},
		{"create measurement db0.rp0.mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore indextype timecluster(1m) minmax indexlist field111",
			"Invalid indexlist"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore shardkey tag1 shards -1 type hash",
			"syntax error: unexpected SUB, expecting AUTO or INTEGER"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore shardkey tag1 shards auto1 type hash",
			"syntax error: unexpected IDENT, expecting AUTO or INTEGER"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore shardkey tag1 shards 0 type hash",
			"syntax error: NUM OF SHARDS SHOULD LARGER THAN 0"},
		{"show shards from",
			"syntax error: unexpected $end, expecting REGEX or DOT or IDENT or STRING"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore shardkey tag1 shards 10 type range",
			"Not support to set num-of-shards for range sharding"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = tsstore shardkey tag1 shards 10 type range",
			"Not support to set num-of-shards for range sharding"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = columnstore shardkey tag1 shards auto type range",
			"Not support to set num-of-shards for range sharding"},
		{"create measurement mst0 (tag1 tag, field1 int64 field) with enginetype = tsstore shardkey tag1 shards auto type range",
			"Not support to set num-of-shards for range sharding"},
		// empty list via two empty props are wrong
		{"create measurement xxx with enginetype = columnstore property ,",
			"syntax error: unexpected ','"},
		// empty property in non-empty list of props are wrong
		{"create measurement xxx with enginetype = columnstore property ,y=z",
			"syntax error: unexpected ','"},
		//downsample
		{"create downsample on test.rp (f1,f2) with duration 1d sampleinterval(1d,2d) timeinterval(1m,3m)",
			"syntax error: unexpected ',', expecting '('"},
		{"create downsample (f1,f2) with duration 1d sampleinterval(1d,2d) timeinterval(1m,3m)",
			"syntax error: unexpected ',', expecting '('"},
		{"create downsample on rp (f1,f2) with duration 1d sampleinterval(1d,2d) timeinterval(1m,3m)",
			"syntax error: unexpected ',', expecting '('"},
		{"(((SELECT * FROM t1))",
			"syntax error: unexpected $end, expecting UNION"},
		{"SELECT *:::field",
			"syntax error: unexpected ':', expecting TAG or FIELD"},
		{"SELECT tag",
			"syntax error: unexpected TAG"},
		{"SELECT * INTO t1,t2 FROM t3",
			"syntax error: unexpected ',', expecting FROM"},
		{"WITH cte AS (SELECT * FROM temp)",
			"syntax error: unexpected $end"},
		{"WITH cte1, cte2 AS (SELECT * FROM temp) SELECT * FROM cte1",
			"syntax error: unexpected ',', expecting AS"},
		{"SELECT * FROM table WITH FILL(abc)",
			"syntax error: unexpected WITH"},
		{"select * from (select * from m1,select * from m2)",
			"syntax error: unexpected SELECT"},
		{"select * from m1 UNION DISTINCT select * from m2",
			"syntax error: unexpected IDENT, expecting SELECT or MATCH or GRAPH or '('"},
		{"create stream x on select value from x",
			"syntax error: unexpected ON, expecting INTO"},
		{"select a from table1 where EXISTS (SELECT * FROM TABLE1) AND NOT EXISTS (SELECT * FROM TABLE1)",
			"syntax error: unexpected EXISTS"},
	}

	for _, c := range c {
		YyParser := influxql.NewYyParser(
			influxql.NewScanner(strings.NewReader(c.query)),
			map[string]interface{}{"thingIdTag": "aa"})
		YyParser.ParseTokens()
		_, err := YyParser.GetQuery()
		if err == nil {
			t.Errorf("<no error> instead of <%s> with sql: %s", c.err, c.query)
		} else if err.Error() != c.err {
			t.Errorf(err.Error(), "instead of", c.err, "with sql:", c.query)
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

func TestYyParserParameterizedQuery(t *testing.T) {
	sql := "SELECT * FROM CPU where (tag1 = $tagvalue or field3 = $fvalue) and time < $times_"
	qr := strings.NewReader(sql)
	p := influxql.NewParser(qr)
	defer p.Release()

	// Parse the parameters
	params := map[string]interface{}{
		"times_": "2023-04-04T17:00:00Z", "fvalue": "value10",
	}
	p.SetParams(params)
	YyParser := influxql.NewYyParser(p.GetScanner(), p.GetPara())
	YyParser.ParseTokens()

	_, err := YyParser.GetQuery()
	if err == nil || !strings.Contains(err.Error(), "missing parameter: tagvalue") {
		t.Fatal("expect err: missing parameter")
	}
}
