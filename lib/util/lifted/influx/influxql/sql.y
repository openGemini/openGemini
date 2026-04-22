%{
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

package influxql

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

const DefaultQueryTimeout = time.Duration(0)
// Some large number, that should be enough for real-life queries,
// but prevents exploiting any lurking odd behaviour in codebase.
const MaxAstDepth = 50000

func setParseTree(yylex interface{}, query Query) {
    yylex.(*YyParser).Query = query
}

func deal_Fill (fill interface{})  (FillOption , interface{},bool) {
	switch fill.(type){
	case string:
        switch fill {
        case "null":
            return 0,nil,true
        case "none":
            return 1,nil,true
        case "previous":
            return 3,nil,true
        case "linear":
            return 4,nil,true
        default:
            return -1,nil,false
        }
	case int64:
		return 2,fill.(int64),true
	case float64:
		return 2,fill.(float64),true
	default:
		return -1,nil,false
	}
}

// Function that returns maximum depth of list of nodes, that may be nil.
func maxDepth(nodes ...nodeGuard) int {
    depth := 0
    for _, n := range nodes {
        if n != nil {
            depth = max(depth, n.Depth())
        }
    }
    return depth
}

// Depth-check validator: inspect and either return or raise error.
//
// To be used during construction as either of:
//    Obj{... depth: depthCheck(yylex, 1+1+...)}
//    $$.depth = depthCheck(yylex, ...)
// or as validator for lists construction as either of
//    depthCheck(yylex, len($$))
//    depthCheck(yylex, $$.Depth())
//
// NOTE: While top-level only check (at ALL_QUERIES) is sufficient
// to catch deep trees, check should be done everywhere to protect
// from intermediate large subtree construction!
func depthCheck(yylex interface{}, depth int) int {
    if depth > MaxAstDepth {
        yylex.(*YyParser).Error("AST complexity overflow")
    }
    return depth
}

%}

%union{
    stmt                Statement
    stmts               Statements
    str                 string
    query               Query
    field               *Field
    fields              fieldsList
    sources             sourcesList
    source              Source
    joinType            JoinType
    unionType           UnionType
    sortfs              SortFields
    sortf               *SortField
    ment                *Measurement
    subQuery            *SubQuery
    dimens              dimensionsList
    dimen               *Dimension
    int                 int
    int64               int64
    float64             float64
    dataType            DataType
    expr                Expr
    tdur                time.Duration
    tdurs               []time.Duration
    bool                bool
    groupByCondition    *GroupByCondition
    intSlice            []int
    inter               interface{}
    durations           *Durations
    hints               Hints
    strSlice            []string
    strSlices           [][]string
    location            *time.Location
    indexType           *IndexType
    cqsp                *cqSamplePolicyInfo
    fieldOption         *fieldList
    fieldOptions        []*fieldList
    indexOptions        []*IndexOption
    indexOption         *IndexOption
    databasePolicy      DatabasePolicy
    cmOption            *CreateMeasurementStatementOption
    cte                 *CTE
    ctes                ctesList
    cypherCondition     *CypherCondition
    tableFunctionField  *TableFunctionField
    tableFunctionFields *TableFunctionFields
    tableFunction       *TableFunction

}

%token <str>    FROM MEASUREMENT INTO ON SELECT WHERE AS GROUP BY ORDER LIMIT OFFSET SLIMIT SOFFSET SHOW CREATE FULL PRIVILEGES OUTER JOIN UNION
                TO IN NOT NOTIN EXISTS REVOKE FILL DELETE WITH ENGINETYPE COLUMNSTORE TSSTORE ALL ANY PASSWORD NAME REPLICANUM ALTER USER USERS
                DATABASES DATABASE MEASUREMENTS RETENTION POLICIES POLICY DURATION DEFAULT SHARD INDEX GRANT HOT WARM INDEXCOLD TYPE SET FOR GRANTS
                REPLICATION SERIES DROP CASE WHEN THEN ELSE BEGIN END TRUE FALSE TAG ATTRIBUTE FIELD KEYS VALUES KEY EXPLAIN ANALYZE EXACT CARDINALITY SHARDKEY
                PRIMARYKEY SORTKEY PROPERTY COMPACT SHARDMERGE
                CONTINUOUS DIAGNOSTICS QUERIES QUERIE SHARDS STATS SUBSCRIPTIONS SUBSCRIPTION GROUPS INDEXTYPE INDEXLIST SEGMENT KILL
                EVERY RESAMPLE
                DOWNSAMPLE DOWNSAMPLES SAMPLEINTERVAL TIMEINTERVAL STREAM DELAY STREAMS
                QUERY PARTITION
                TOKEN TOKENIZERS MATCH LIKE MATCHPHRASE CONFIG CONFIGS CLUSTER IPINRANGE
                REPLICAS DETAIL DESTINATIONS
                SCHEMA INDEXES AUTO EXCEPT INNER LEFT RIGHT GRAPH NODE EDGE TTL RETURN
%token <bool>   DESC ASC
%token <str>    COMMA SEMICOLON LPAREN RPAREN REGEX LBRACKET RBRACKET LSQUARE RSQUARE COLON
%token <int>    EQ NEQ LT LTE GT GTE DOT DOUBLECOLON NEQREGEX EQREGEX MULTIHOP
%token <str>    IDENT
%token <int64>  INTEGER
%token <tdur>   DURATIONVAL
%token <str>    STRING
%token <float64> NUMBER
%token <hints>  HINT
%token <expr>   BOUNDPARAM

%left  <int>  AND OR
%left  <int>  ADD SUB BITWISE_OR BITWISE_XOR
%left  <int>  MUL DIV MOD BITWISE_AND
%left  UNION
%right UMINUS

%type <stmt>                        STATEMENT SHOW_DATABASES_STATEMENT CREATE_DATABASE_STATEMENT WITH_CLAUSES CREATE_USER_STATEMENT
                                    SELECT_STATEMENT  SELECT_NO_PARENS SELECT_WITH_PARENS BASE_SELECT_STATEMENT SIMPLE_SELECT_STATEMENT
                                    SHOW_MEASUREMENTS_STATEMENT SHOW_MEASUREMENTS_DETAIL_STATEMENT SHOW_RETENTION_POLICIES_STATEMENT
                                    CREATE_RENTRENTION_POLICY_STATEMENT RP_DURATION_OPTIONS SHOW_SERIES_STATEMENT
                                    SHOW_USERS_STATEMENT DROP_SERIES_STATEMENT DROP_DATABASE_STATEMENT DELETE_SERIES_STATEMENT
                                    ALTER_RENTRENTION_POLICY_STATEMENT
                                    DROP_RETENTION_POLICY_STATEMENT DROP_USER_STATEMENT GRANT_STATEMENT REVOKE_STATEMENT
                                    GRANT_ADMIN_STATEMENT REVOKE_ADMIN_STATEMENT SHOW_TAG_KEYS_STATEMENT SHOW_FIELD_KEYS_STATEMENT SHOW_TAG_VALUES_STATEMENT
                                    TAG_VALUES_WITH  EXPLAIN_STATEMENT SHOW_TAG_KEY_CARDINALITY_STATEMENT SHOW_TAG_VALUES_CARDINALITY_STATEMENT
                                    SHOW_FIELD_KEY_CARDINALITY_STATEMENT CREATE_MEASUREMENT_STATEMENT DROP_SHARD_STATEMENT SET_PASSWORD_USER_STATEMENT
                                    SHOW_GRANTS_FOR_USER_STATEMENT SHOW_MEASUREMENT_CARDINALITY_STATEMENT SHOW_SERIES_CARDINALITY_STATEMENT SHOW_SHARDS_STATEMENT
                                    ALTER_SHARD_KEY_STATEMENT SHOW_SHARD_GROUPS_STATEMENT DROP_MEASUREMENT_STATEMENT
                                    CREATE_CONTINUOUS_QUERY_STATEMENT SHOW_CONTINUOUS_QUERIES_STATEMENT DROP_CONTINUOUS_QUERY_STATEMENT
                                    CREATE_DOWNSAMPLE_STATEMENT DOWNSAMPLE_INTERVALS DROP_DOWNSAMPLE_STATEMENT SHOW_DOWNSAMPLE_STATEMENT
                                    CREATE_STREAM_STATEMENT SHOW_STREAM_STATEMENT DROP_STREAM_STATEMENT COLUMN_LISTS SHOW_MEASUREMENT_KEYS_STATEMENT
                                    SHOW_QUERIES_STATEMENT KILL_QUERY_STATEMENT SHOW_CONFIGS_STATEMENT SET_CONFIG_STATEMENT SHOW_CLUSTER_STATEMENT
                                    CREATE_SUBSCRIPTION_STATEMENT SHOW_SUBSCRIPTION_STATEMENT DROP_SUBSCRIPTION_STATEMENT GRAPH_STATEMENT
%type <fields>                      COLUMN_CLAUSES DOWNSAMPLE_CALLS
%type <field>                       COLUMN_CLAUSE DOWNSAMPLE_CALL
%type <query>                       ALL_QUERIES ALL_QUERY
%type <sources>                     FROM_CLAUSE FROM_LIST FROM_SOURCE SUBQUERY_CLAUSE INTO_CLAUSE JOIN_CLAUSE
%type <joinType>                    JOIN_TYPE
%type <unionType>                   UNION_TYPE
%type <ment>                        TABLE_OPTION  TABLE_NAME_WITH_OPTION TABLE_CASE MEASUREMENT_WITH
%type <expr>                        WHERE_CLAUSE OR_CONDITION AND_CONDITION CONDITION OPERATION_EQUAL COLUMN_VAREF COLUMN COLUMN_CALL CONDITION_COLUMN TAG_KEYS
                                    CASE_WHEN_CASE CASE_WHEN_CASES ADDITIONAL_CONDITION
%type <int>                         CONDITION_OPERATOR
%type <dataType>                    COLUMN_VAREF_TYPE
%type <sortfs>                      SORTFIELDS ORDER_CLAUSES
%type <sortf>                       SORTFIELD
%type <dimens>                      GROUP_BY_CLAUSE EXCEPT_CLAUSE DIMENSION_NAMES
%type <dimen>                       DIMENSION_NAME
%type <intSlice>                    OPTION_CLAUSES LIMIT_OFFSET_OPTION SLIMIT_SOFFSET_OPTION
%type <inter>                       FILL_CLAUSE FILLCONTENT
%type <durations>                   SHARD_HOT_WARM_INDEX_DURATIONS SHARD_HOT_WARM_INDEX_DURATION CREAT_DATABASE_POLICY  CREAT_DATABASE_POLICYS
%type <str>                         REGULAR_EXPRESSION TAG_KEY ON_DATABASE TYPE_CLAUSE SHARD_KEY STRING_TYPE MEASUREMENT_INFO SUBSCRIPTION_TYPE COMPACTION_TYPE_CLAUSE
%type <strSlice>                    SHARDKEYLIST CMOPTION_SHARDKEY INDEX_LIST PRIMARYKEY_LIST SORTKEY_LIST ALL_DESTINATION CMOPTION_PRIMARYKEY CMOPTION_SORTKEY
%type <strSlices>                   MEASUREMENT_PROPERTYS MEASUREMENT_PROPERTY MEASUREMENT_PROPERTYS_LIST CMOPTION_PROPERTIES
%type <location>                    TIME_ZONE
%type <indexType>                   INDEX_TYPE INDEX_TYPES INDEX_TYPE_LIST CMOPTION_INDEXTYPE_TS CMOPTION_INDEXTYPE_CS
%type <cqsp>                        SAMPLE_POLICY
%type <tdur>                        TTL_CLAUSE_OPT DELAY_CLAUSE_OPT
%type <tdurs>                       DURATIONVALS
%type <cqsp>                        SAMPLE_POLICY
%type <int64>                       INTEGERPARA CMOPTION_SHARDNUM
%type <bool>                        ALLOW_TAG_ARRAY
%type <fieldOption>                 FIELD_OPTION FIELD_COLUMN
%type <fieldOptions>                FIELD_OPTIONS

%type <databasePolicy>              DATABASE_POLICY
%type <cmOption>                    CMOPTIONS_TS CMOPTIONS_CS
%type <str>                         CMOPTION_ENGINETYPE_TS CMOPTION_ENGINETYPE_CS
%type <ctes>                        CTE_CLAUSES
%type <cte>                         CTE_CLAUSE
%type <cypherCondition>             CYPHER_CONDITION
%type <tableFunctionField>          TABLE_FUNCTION_PARAM
%type <tableFunction>               TABLE_FUNCTION_PARAMS TABLE_FUNCTION

%%

ALL_QUERIES:
    ALL_QUERY
    {
        $$ = $1
        setParseTree(yylex, $$)
    }

ALL_QUERY:
    STATEMENT
    {
        $$ = Query{
            Statements: []Statement{$1},
            depth: depthCheck(yylex, 2 + $1.Depth()),
        }
    }
    |ALL_QUERY SEMICOLON
    {
        if len($1.Statements) >= 1{
            $$ = $1
        }else{
            yylex.Error("excrescent semicolo")
        }
    }
    |ALL_QUERY SEMICOLON STATEMENT
    {
        q := $1
        q.Statements = append(q.Statements, $3)
        q.depth = depthCheck(yylex, max(q.depth, len(q.Statements) + $3.Depth()))
        $$ = q
    }



STATEMENT:
    SELECT_STATEMENT
    {
        $$ = $1
    }
    |SHOW_DATABASES_STATEMENT
    {
        $$ = $1
    }
    |CREATE_DATABASE_STATEMENT
    {
        $$ = $1
    }
    |CREATE_RENTRENTION_POLICY_STATEMENT
    {
    	$$ = $1
    }
    |CREATE_USER_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_MEASUREMENTS_STATEMENT
    {
        $$ = $1
    }
    |SHOW_MEASUREMENTS_DETAIL_STATEMENT
    {
        $$ = $1
    }
    |SHOW_RETENTION_POLICIES_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_SERIES_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_USERS_STATEMENT
    {
    	$$ = $1
    }
    |DROP_DATABASE_STATEMENT
    {
    	$$ = $1
    }
    |DROP_SERIES_STATEMENT
    {
    	$$ = $1
    }
    |DELETE_SERIES_STATEMENT
    {
    	$$ = $1
    }
    |ALTER_RENTRENTION_POLICY_STATEMENT
    {
        $$ = $1
    }
    |DROP_RETENTION_POLICY_STATEMENT
    {
        $$ = $1
    }
    |GRANT_STATEMENT
    {
    	$$ = $1
    }
    |GRANT_ADMIN_STATEMENT
    {
    	$$ = $1
    }
    |REVOKE_ADMIN_STATEMENT
    {
    	$$ = $1
    }
    |REVOKE_STATEMENT
    {
    	$$ = $1
    }
    |DROP_USER_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_TAG_KEYS_STATEMENT
    {
        $$ = $1
    }
    |SHOW_FIELD_KEYS_STATEMENT
    {
        $$ = $1
    }
    |SHOW_TAG_VALUES_STATEMENT
    {
        $$ = $1
    }
    |EXPLAIN_STATEMENT
    {
        $$ = $1
    }
    |SHOW_TAG_KEY_CARDINALITY_STATEMENT
    {
        $$ = $1
    }
    |SHOW_TAG_VALUES_CARDINALITY_STATEMENT
    {
        $$ = $1
    }
    |SHOW_FIELD_KEY_CARDINALITY_STATEMENT
    {
        $$ = $1
    }
    |CREATE_MEASUREMENT_STATEMENT
    {
        $$ = $1
    }
    |DROP_SHARD_STATEMENT
    {
        $$ = $1
    }
    |SET_PASSWORD_USER_STATEMENT
    {
        $$ = $1
    }
    |SHOW_GRANTS_FOR_USER_STATEMENT
    {
        $$ = $1
    }
    |SHOW_MEASUREMENT_CARDINALITY_STATEMENT
    {
        $$ = $1
    }
    |SHOW_SERIES_CARDINALITY_STATEMENT
    {
        $$ = $1
    }
    |SHOW_SHARDS_STATEMENT
    {
        $$ = $1
    }
    |ALTER_SHARD_KEY_STATEMENT
    {
        $$ = $1
    }
    |SHOW_SHARD_GROUPS_STATEMENT
    {
        $$ = $1
    }
    |DROP_MEASUREMENT_STATEMENT
    {
        $$ = $1
    }
    |CREATE_CONTINUOUS_QUERY_STATEMENT
    {
        $$ = $1
    }
    |SHOW_CONTINUOUS_QUERIES_STATEMENT
    {
    	$$ = $1
    }
    |DROP_CONTINUOUS_QUERY_STATEMENT
    {
    	$$ = $1
    }
    |CREATE_DOWNSAMPLE_STATEMENT
    {
    	$$ = $1
    }
    |DROP_DOWNSAMPLE_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_DOWNSAMPLE_STATEMENT
    {
    	$$ = $1
    }
    |CREATE_STREAM_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_STREAM_STATEMENT
    {
    	$$ = $1
    }
    |DROP_STREAM_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_MEASUREMENT_KEYS_STATEMENT
    {
        $$ = $1
    }
    |SHOW_QUERIES_STATEMENT
    {
    	$$ = $1
    }
    |KILL_QUERY_STATEMENT
    {
    	$$ = $1
    }
    |CREATE_SUBSCRIPTION_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_SUBSCRIPTION_STATEMENT
    {
    	$$ = $1
    }
    |DROP_SUBSCRIPTION_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_CONFIGS_STATEMENT
    {
    	$$ = $1
    }
    |SET_CONFIG_STATEMENT
    {
    	$$ = $1
    }
    |SHOW_CLUSTER_STATEMENT
    {
    	$$ = $1
    }

SELECT_STATEMENT:
    SELECT_NO_PARENS		%prec UMINUS
    {
        $$ = $1
    }
	|SELECT_WITH_PARENS		%prec UMINUS
    {
        $$ = $1
    }

SELECT_WITH_PARENS:
    LPAREN SELECT_NO_PARENS RPAREN
    {
        $$ = $2
    }
    |LPAREN SELECT_WITH_PARENS RPAREN
    {
        $$ = $2
    }

SELECT_NO_PARENS:
    SIMPLE_SELECT_STATEMENT
    {
        $$ = $1
    }
    |WITH CTE_CLAUSES BASE_SELECT_STATEMENT
    {
        $$ = &WithSelectStatement{
            CTEs: $2.ctes,
            Query: $3.(*SelectStatement),
            depth: depthCheck(yylex, 1 + maxDepth($2, $3)),
        }
    }

BASE_SELECT_STATEMENT:
    SIMPLE_SELECT_STATEMENT
    {
        $$ = $1
    }
    |SELECT_WITH_PARENS
    {
        $$ = $1
    }

SIMPLE_SELECT_STATEMENT:
    SELECT COLUMN_CLAUSES INTO_CLAUSE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE EXCEPT_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &SelectStatement{
            Fields: $2.fields,
            Sources: $4.sources,
            Dimensions: $6.dims,
            ExceptDimensions: $7.dims,
            Condition: $5,
            SortFields: $9,
            Limit: $10[0],
            Offset: $10[1],
            SLimit: $10[2],
            SOffset: $10[3],
            depth: depthCheck(yylex, 1 + maxDepth($2, $4, $5, $6, $7, $9)),
        }
        tempfill,tempfillvalue,fillflag := deal_Fill($8)
        if fillflag==false{
            yylex.Error("Invalid characters in fill")
        }else{
            stmt.Fill,stmt.FillValue = tempfill,tempfillvalue
        }
        stmt.IsRawQuery = true
	WalkFunc(stmt.Fields, func(n Node) {
		if _, ok := n.(*Call); ok {
			stmt.IsRawQuery = false
		}
	})
        stmt.Location = $11
        if len($3.sources) > 1{
            yylex.Error("into clause only support one measurement")
        }else if len($3.sources) == 1{
            mst,ok := $3.sources[0].(*Measurement)
            if !ok{
                 yylex.Error("into clause only support measurement clause")
            }
            mst.IsTarget = true
            stmt.Target = &Target{
            	Measurement: mst,
                depth: depthCheck(yylex, 2),
            }
            stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + stmt.Target.depth))
        }
        $$ = stmt
    }
    |SELECT HINT COLUMN_CLAUSES INTO_CLAUSE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE EXCEPT_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &SelectStatement{
            Hints: $2,
            Fields: $3.fields,
            Sources: $5.sources,
            Dimensions: $7.dims,
            ExceptDimensions: $8.dims,
            Condition: $6,
            SortFields: $10,
            Limit: $11[0],
            Offset: $11[1],
            SLimit: $11[2],
            SOffset: $11[3],
            depth: depthCheck(yylex, 1 + maxDepth($3, $5, $6, $7, $8, $10)),
        }

        tempfill,tempfillvalue,fillflag := deal_Fill($9)
        if fillflag==false{
            yylex.Error("Invalid characters in fill")
        }else{
            stmt.Fill,stmt.FillValue = tempfill,tempfillvalue
        }
        stmt.IsRawQuery = true
	WalkFunc(stmt.Fields, func(n Node) {
		if _, ok := n.(*Call); ok {
			stmt.IsRawQuery = false
		}
	})
        stmt.Location = $12
        if len($4.sources) > 1{
            yylex.Error("into clause only support one measurement")
        }else if len($4.sources) == 1{
            mst,ok := $4.sources[0].(*Measurement)
            if !ok{
                 yylex.Error("into clause only support measurement clause")
            }
            mst.IsTarget = true
            stmt.Target = &Target{
            	Measurement: mst,
                depth: depthCheck(yylex, 2),
            }
            stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + stmt.Target.depth))
        }
        $$ = stmt
    }
    |SELECT COLUMN_CLAUSES WHERE_CLAUSE GROUP_BY_CLAUSE EXCEPT_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &SelectStatement{
            Fields: $2.fields,
            Dimensions: $4.dims,
            ExceptDimensions: $5.dims,
            Condition: $3,
            SortFields: $7,
            Limit: $8[0],
            Offset: $8[1],
            SLimit: $8[2],
            SOffset: $8[3],
            depth: depthCheck(yylex, 1 + maxDepth($2, $3, $4, $5, $7)),
        }

        tempfill,tempfillvalue,fillflag := deal_Fill($6)
        if fillflag==false{
            yylex.Error("Invalid characters in fill")
        }else{
            stmt.Fill,stmt.FillValue = tempfill,tempfillvalue
        }
        stmt.IsRawQuery = true
	WalkFunc(stmt.Fields, func(n Node) {
		if _, ok := n.(*Call); ok {
			stmt.IsRawQuery = false
		}
	})
        stmt.Location = $9
        $$ = stmt
    }
    |GRAPH_STATEMENT
    {
        $$ = $1
    }
    |BASE_SELECT_STATEMENT UNION UNION_TYPE BASE_SELECT_STATEMENT
    {
        stmt1,ok := $1.(*SelectStatement)
        if !ok{
            yylex.Error("Expected SelectStatement on left of UNION")
        }
        subquery1 := &SubQuery{Statement: stmt1, Alias: "", depth: depthCheck(yylex, 1+stmt1.Depth())}
        stmt2,ok := $4.(*SelectStatement)
        if !ok{
            yylex.Error("Expected SelectStatement on right of UNION")
        }
        subquery2 := &SubQuery{Statement: stmt2, Alias: "", depth: depthCheck(yylex, 1+stmt2.Depth())}

        union := &Union{
          LSrc: subquery1,
          UnionType: $3,
          RSrc: subquery2,
          depth: depthCheck(yylex, max(subquery1.Depth(), subquery2.Depth())),
        }
        $$ = &SelectStatement{
            Fields: []*Field{{Expr: &Wildcard{Type:Token(MUL)}, depth: depthCheck(yylex, 2)}},
            Sources: []Source{union},
            IsRawQuery: true,
            depth: depthCheck(yylex, 1 + union.Depth()),
        }
    }


UNION_TYPE:
    {
        $$ = UnionDistinct
    }
    |ALL
    {
        $$ = UnionAll
    }
    |BY NAME
    {
        $$ = UnionDistinctByName
    }
    |ALL BY NAME
    {
        $$ = UnionAllByName
    }

GRAPH_STATEMENT:
    GRAPH INTEGER STRING NODE CONDITION EDGE CONDITION
    {
        $$ = &GraphStatement{HopNum:int($2), StartNodeId:$3, NodeCondition:$5, EdgeCondition:$7, depth: depthCheck(yylex, 1+maxDepth($5, $7))}
    }
    |GRAPH INTEGER STRING NODE CONDITION
    {
        $$ = &GraphStatement{HopNum:int($2), StartNodeId:$3, NodeCondition:$5, depth: depthCheck(yylex, 1+maxDepth($5))}
    }
    |GRAPH INTEGER STRING EDGE CONDITION
    {
        $$ = &GraphStatement{HopNum:int($2), StartNodeId:$3, EdgeCondition:$5, depth: depthCheck(yylex, 1+maxDepth($5))}
    }
    |GRAPH INTEGER STRING
    {
        $$ = &GraphStatement{HopNum:int($2), StartNodeId:$3, depth: depthCheck(yylex, 1)}
    }
    |GRAPH
    {
        $$ = &GraphStatement{depth: depthCheck(yylex, 1)}
    }
    |MATCH IDENT EQ LPAREN IDENT LBRACKET IDENT COLON STRING RBRACKET RPAREN SUB LSQUARE IDENT MULTIHOP INTEGER RSQUARE SUB LPAREN RPAREN CYPHER_CONDITION RETURN IDENT
    {
        stmt := &GraphStatement{depth: depthCheck(yylex, 1)}
        stmt.HopNum = int($16)
        if !strings.EqualFold($7, "uid")  {
            yylex.Error("cypher multi-hop-filter stmt node.uid ident err")
        }
        stmt.StartNodeId = $9
        if $21 != nil {
            c := $21
            if $2 != c.PathName && c.PathName != ""  {
                yylex.Error("cypher multi-hop-filter stmt PathName err1")
            }
            stmt.NodeCondition = c.NodeCondition
            stmt.EdgeCondition = c.EdgeCondition
            stmt.AdditionalCondition = c.AdditionalCondition
            stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + maxDepth(stmt.NodeCondition, stmt.EdgeCondition, stmt.AdditionalCondition)))
        }
        if $2 != $23 {
            yylex.Error("cypher multi-hop-filter stmt PathName err2")
        }
        $$ = stmt
    }

CYPHER_CONDITION:
    WHERE CONDITION
    {
        cc := &CypherCondition{}
        cc.AdditionalCondition = $2
        $$ = cc
    }
    |WHERE ALL LPAREN IDENT IN IDENT LPAREN IDENT RPAREN WHERE CONDITION RPAREN ADDITIONAL_CONDITION
    {
        cc := &CypherCondition{}
        cc.PathName = $8
        if strings.EqualFold($6, "nodes") {
            cc.NodeCondition = $11
        } else if strings.EqualFold($6, "edges") {
            cc.EdgeCondition = $11
        } else  {
            yylex.Error("CYPHER_CONDITION nodes/edges func err")
        }
        if $13 != nil {
           cc.AdditionalCondition = $13
        }
        $$ = cc
    }
    |WHERE ALL LPAREN IDENT IN IDENT LPAREN IDENT RPAREN WHERE CONDITION RPAREN AND ALL LPAREN IDENT IN IDENT LPAREN IDENT RPAREN WHERE CONDITION RPAREN ADDITIONAL_CONDITION
    {
        if $8 != $20 {
            yylex.Error("CYPHER_CONDITION PathName err")
        }
        if $6 == $18 {
            yylex.Error("CYPHER_CONDITION funcName same err")
        }
        cc := &CypherCondition{}
        cc.PathName = $8
        if strings.EqualFold($6, "nodes") {
            cc.NodeCondition = $11
        } else if strings.EqualFold($6, "edges") {
            cc.EdgeCondition = $11
        } else  {
            yylex.Error("CYPHER_CONDITION nodes/edges func err1")
        }

        if strings.EqualFold($18, "nodes") {
            cc.NodeCondition = $23
        } else if strings.EqualFold($18, "edges") {
            cc.EdgeCondition = $23
        } else  {
            yylex.Error("CYPHER_CONDITION nodes/edges func err2")
        }
        if $25 != nil {
           cc.AdditionalCondition = $25
        }
        $$ = cc
    }
    |
    {
        $$ = nil
    }

ADDITIONAL_CONDITION:
    {
        $$ = nil
    }
    |AND CONDITION
    {
        $$ = $2
    }

COLUMN_CLAUSES:
    COLUMN_CLAUSE
    {
        $$ = fieldsList{fields:[]*Field{$1}, depth: depthCheck(yylex, 1 + $1.Depth())}
    }
    |COLUMN_CLAUSES COMMA COLUMN_CLAUSE
    {
        fl := $1
        fl.fields = append(fl.fields, $3)
        fl.depth = depthCheck(yylex, max(fl.depth, len(fl.fields) + $3.Depth()))
        $$ = fl
    }

COLUMN_CLAUSE:
    MUL
    {
        $$ = &Field{Expr:&Wildcard{Type:Token($1)}, depth: depthCheck(yylex, 2)}
    }
    |MUL DOUBLECOLON TAG
    {
    	$$ = &Field{Expr:&Wildcard{Type:TAG}, depth: depthCheck(yylex, 2)}
    }
    |MUL DOUBLECOLON FIELD
    {
    	$$ = &Field{Expr:&Wildcard{Type:FIELD}, depth: depthCheck(yylex, 2)}
    }
    |COLUMN
    {
        $$ = &Field{Expr: $1, depth: depthCheck(yylex, 1 + $1.Depth())}
    }
    |COLUMN AS IDENT
    {
        $$ = &Field{Expr: $1, Alias:$3, depth: depthCheck(yylex, 1 + $1.Depth())}
    }
    |COLUMN AS STRING
    {
        $$ = &Field{Expr: $1, Alias:$3, depth: depthCheck(yylex, 1 + $1.Depth())}
    }

CASE_WHEN_CASES:
    CASE_WHEN_CASE
    {
    	$$ = $1
    }
    |CASE_WHEN_CASES CASE_WHEN_CASE
    {
        newcase := $2.(*CaseWhenExpr)
        c := $1.(*CaseWhenExpr)
        c.depth = depthCheck(yylex, max(c.depth, len(c.Conditions) + newcase.depth))
        c.Conditions = append(c.Conditions, newcase.Conditions...)
        c.Assigners = append(c.Assigners, newcase.Assigners...)
        $$ = c
    }

CASE_WHEN_CASE:
    WHEN CONDITION THEN COLUMN
    {
    	c := &CaseWhenExpr{depth: depthCheck(yylex, 2 + maxDepth($2, $4))}
    	c.Conditions = []Expr{$2}
    	c.Assigners = []Expr{$4}
    	$$ = c
    }

COLUMN:
    COLUMN MUL COLUMN
    {
        $$ = &BinaryExpr{Op:Token(MUL), LHS:$1, RHS:$3, depth: depthCheck(yylex, 1 + max($1.Depth(), $3.Depth()))}
    }
    |COLUMN DIV COLUMN
    {
        $$ = &BinaryExpr{Op:Token(DIV), LHS:$1, RHS:$3, depth: depthCheck(yylex, 1 + max($1.Depth(), $3.Depth()))}
    }
    |COLUMN ADD COLUMN
    {
        $$ = &BinaryExpr{Op:Token(ADD), LHS:$1, RHS:$3, depth: depthCheck(yylex, 1 + max($1.Depth(), $3.Depth()))}
    }
    |COLUMN SUB COLUMN
    {
        $$ = &BinaryExpr{Op:Token(SUB), LHS:$1, RHS:$3, depth: depthCheck(yylex, 1 + max($1.Depth(), $3.Depth()))}
    }
    |COLUMN BITWISE_XOR COLUMN
    {
        $$ = &BinaryExpr{Op:Token(BITWISE_XOR), LHS:$1, RHS:$3, depth: depthCheck(yylex, 1 + max($1.Depth(), $3.Depth()))}
    }
    |COLUMN MOD COLUMN
    {
        $$ = &BinaryExpr{Op:Token(MOD), LHS:$1, RHS:$3, depth: depthCheck(yylex, 1 + max($1.Depth(), $3.Depth()))}
    }
    |COLUMN BITWISE_AND COLUMN
    {
        $$ = &BinaryExpr{Op:Token(BITWISE_AND), LHS:$1, RHS:$3, depth: depthCheck(yylex, 1 + max($1.Depth(), $3.Depth()))}
    }
    |COLUMN BITWISE_OR COLUMN
    {
        $$ = &BinaryExpr{Op:Token(BITWISE_OR), LHS:$1, RHS:$3, depth: depthCheck(yylex, 1 + max($1.Depth(), $3.Depth()))}
    }
    |LPAREN COLUMN RPAREN
    {
        $$ = &ParenExpr{Expr: $2, depth: depthCheck(yylex, 1 + $2.Depth())}
    }
    |COLUMN_CALL
    {
        $$ = $1
    }
    |SUB COLUMN %prec UMINUS
    {
        switch s := $2.(type) {
        case *NumberLiteral:
            s.Val = -1*s.Val
        	$$ = $2
        case *IntegerLiteral:
            s.Val = -1*s.Val
            $$ = $2
        default:
        	$$ = &BinaryExpr{Op:Token(MUL), LHS:&IntegerLiteral{Val:-1}, RHS:$2, depth: depthCheck(yylex, 1 + $2.Depth())}
        }

    }
    |COLUMN_VAREF
    {
        $$ = $1
    }
    |DURATIONVAL
    {
    	$$ = &DurationLiteral{Val: $1}
    }
    |CASE CASE_WHEN_CASES ELSE COLUMN END
    {
    	c := $2.(*CaseWhenExpr)
    	c.Assigners = append(c.Assigners, $4)
    	c.depth = depthCheck(yylex, max(c.depth, 1+len(c.Assigners)+$4.Depth()))
    	$$ = c
    }
// Not implemented case, remove for now.
//    |CASE IDENT CASE_WHEN_CASES ELSE IDENT END
//    {
//    	$$ = &VarRef{}
//    }

COLUMN_CALL:
    IDENT LPAREN COLUMN_CLAUSES RPAREN
    {
        if strings.ToLower($1) == "cast" {
            if len($3.fields)!=1 {
                 yylex.Error("The cast format is incorrect.")
            } else {
                name := "Unknown"
                if strings.ToLower($3.fields[0].Alias) == "bool" {
                    name = "cast_bool"
                }
                if strings.ToLower($3.fields[0].Alias) == "float" {
                    name = "cast_float64"
                }
                if strings.ToLower($3.fields[0].Alias) == "int" {
                    name = "cast_int64"
                }
                if strings.ToLower($3.fields[0].Alias) == "string" {
                    name = "cast_string"
                }
                cols := &Call{Name: strings.ToLower(name), Args: []Expr{$3.fields[0].Expr}, depth: depthCheck(yylex, 2 + $3.fields[0].Depth())}
                $$ = cols
            }
        } else {
            cols := &Call{Name: strings.ToLower($1), Args: []Expr{}, depth: depthCheck(yylex, 1)}
            for i,fld := range $3.fields {
                cols.Args = append(cols.Args, fld.Expr)
                cols.depth = depthCheck(yylex, max(cols.depth, 2 + i + fld.Expr.Depth()))
            }
            $$ = cols
        }
    }
    |IDENT LPAREN RPAREN
    {
        cols := &Call{Name: strings.ToLower($1), depth: depthCheck(yylex, 1)}
        $$ = cols
    }

INTO_CLAUSE:
    INTO TABLE_NAME_WITH_OPTION
    {
        $$ = sourcesList{
            sources: []Source{$2},
            depth: depthCheck(yylex, 1 + $2.Depth()),
        }
    }
    |
    {
    	$$ = sourcesList{sources: nil, depth: depthCheck(yylex, 0)}
    }

FROM_CLAUSE:
    FROM FROM_LIST
    {
        $$ = $2
    }

FROM_LIST:
    FROM_SOURCE
    {
        $$ = $1
    }
    |
    FROM_LIST COMMA FROM_SOURCE
    {
        sl := $1;
        sl.depth = depthCheck(yylex, max(sl.depth, len(sl.sources) + $3.depth))
        sl.sources = append(sl.sources, $3.sources...)
        $$ = sl
    }

FROM_SOURCE:

    TABLE_NAME_WITH_OPTION
    {
        $$ = sourcesList{sources:[]Source{$1}, depth:depthCheck(yylex, 1+$1.Depth())}
    }
    | TABLE_NAME_WITH_OPTION AS IDENT
    {
        $1.Alias = $3
        $$ = sourcesList{sources:[]Source{$1}, depth:depthCheck(yylex, 1+$1.Depth())}
    }
    |SUBQUERY_CLAUSE
    {
        $$ = $1
    }
    |JOIN_CLAUSE
    {
        $$ = $1
    }
    |TABLE_FUNCTION
         {
             $$ = sourcesList{sources:[]Source{$1}, depth:depthCheck(yylex, 1+$1.Depth())}
         }

TABLE_FUNCTION:
     IDENT LPAREN TABLE_FUNCTION_PARAMS RPAREN
     {
         tableFunction := $3
         tableFunction.FunctionName = $1
         $$ = tableFunction
     }

TABLE_FUNCTION_PARAMS:
     TABLE_FUNCTION_PARAM
     {
         if($1.GetType() == 0){
             $$ = &TableFunction{TableFunctionSource: []Source{$1.GetSource()}, depth: depthCheck(yylex, 1 + $1.Depth())}
         }else{
             $$ = &TableFunction{Params: $1.GetContent(), depth: depthCheck(yylex, 1 + $1.Depth())}
         }
     }
     |
     TABLE_FUNCTION_PARAMS COMMA TABLE_FUNCTION_PARAM
     {
         tableFunction := $1
         tableFunctionField := $3
         if(tableFunctionField.GetType() == 0){
             tableFunction.TableFunctionSource = append(tableFunction.TableFunctionSource, tableFunctionField.GetSource())
         }else{
             tableFunction.Params = tableFunctionField.GetContent()
         }
         tableFunction.depth = depthCheck(yylex, max(tableFunction.depth, len(tableFunction.TableFunctionFields) + $3.Depth()))
         $$ = tableFunction
     }

TABLE_FUNCTION_PARAM:
     STRING
     {
         tableFunctionField := &TableFunctionField{
             typ : 1,
             content : $1,
             depth : depthCheck(yylex, 1),
         }
         $$ = tableFunctionField
     }
     |
     IDENT
     {
         tableFunctionField := &TableFunctionField{
             typ : 0,
             source : &Measurement{Name:$1},
             depth : depthCheck(yylex, 1),
         }
         $$ = tableFunctionField
     }

JOIN_TYPE:
    FULL JOIN
    {
        $$ = FullJoin
    }
    | INNER JOIN
    {
        $$ = InnerJoin
    }
    | JOIN
    {
        $$ = InnerJoin
    }
    | LEFT OUTER JOIN
    {
        $$ = LeftOuterJoin
    }
    | LEFT JOIN
    {
        $$ = LeftOuterJoin
    }
    | RIGHT OUTER JOIN
    {
        $$ = RightOuterJoin
    }
    | RIGHT JOIN
    {
        $$ = RightOuterJoin
    }
    | OUTER JOIN
    {
        $$ = OuterJoin
    }


JOIN_CLAUSE:
    FROM_SOURCE JOIN_TYPE FROM_SOURCE ON CONDITION
    {
        if len($1.sources) != 1 || len($3.sources) != 1 {
            yylex.Error("expected 1 source")
        }
        join := &Join{
            JoinType: $2,
            Condition: $5,
            depth: depthCheck(yylex, 1 + $5.Depth()),
        }
        if joinLeft,ok := $1.sources[0].(*Join); ok{
             joinStatement := &SelectStatement{
                 Fields: []*Field{{Expr: &Wildcard{Type:Token(MUL)}, depth: depthCheck(yylex, 2)}},
                 Sources: []Source{joinLeft},
                 IsRawQuery: true,
                 depth: depthCheck (yylex, 2 + joinLeft.Depth ()),
             }
             alias := joinLeft.LSrc.GetName() + "," + joinLeft.RSrc.GetName()
             join.LSrc = &SubQuery{Statement: joinStatement, Alias: alias, depth: depthCheck(yylex, 1+joinStatement.Depth())}
        } else {
             join.LSrc =$1.sources[0]
        }
        if joinRight, ok := $3.sources[0].(*Join); ok {
            joinStatement := &SelectStatement {
                Fields: []*Field{{Expr: &Wildcard{Type:Token(MUL)}, depth: depthCheck(yylex, 2)}},
                Sources: []Source{joinRight},
                IsRawQuery: true,
                depth: depthCheck(yylex, 2 + joinRight.Depth()),
            }
            join.RSrc = &SubQuery{Statement: joinStatement, Alias: "", depth: depthCheck(yylex, 1+joinStatement.Depth())}
        } else {
            join.RSrc =$3.sources[0]
        }
        join.depth = depthCheck(yylex, max(join.depth, 1 + maxDepth(join.LSrc, join.RSrc)))
        $$ = sourcesList{sources:[]Source{join}, depth: depthCheck(yylex, 1 + join.depth)}
    }

SUBQUERY_CLAUSE:
    SELECT_WITH_PARENS AS IDENT
    {
        stmt,ok := $1.(*SelectStatement)
        if !ok{
            yylex.Error("expexted SelectStatement")
        }
        subquery := &SubQuery{Statement: stmt, Alias: $3, depth: depthCheck(yylex, 1+stmt.Depth())}
        $$ = sourcesList{sources:[]Source{subquery}, depth: depthCheck(yylex, 1 + subquery.depth)}
    }
    |SELECT_WITH_PARENS
    {
        stmt,ok := $1.(*SelectStatement)
        if !ok{
            yylex.Error("expexted SelectStatement")
        }
        subquery := &SubQuery{Statement: stmt, depth: depthCheck(yylex, 1+stmt.Depth())}
        $$ = sourcesList{sources:[]Source{subquery}, depth: depthCheck(yylex, 1 + subquery.depth)}
    }

TABLE_NAME_WITH_OPTION:
    TABLE_CASE
    {
        $$ = $1
    }

TABLE_CASE:
    IDENT DOT IDENT DOT TABLE_OPTION
    {
    	mst := $5
    	mst.Database = $1
    	mst.RetentionPolicy = $3
    	$$ = mst
    }
    |DOT IDENT DOT TABLE_OPTION
    {
    	mst := $4
    	mst.RetentionPolicy = $2
    	$$ = mst
    }
    |IDENT DOT  DOT TABLE_OPTION
    {
    	mst := $4
    	mst.Database = $1
    	$$ = mst
    }
    |IDENT DOT TABLE_OPTION
    {
    	mst := $3
    	mst.RetentionPolicy = $1
    	$$ = mst
    }
    |TABLE_OPTION
    {
    	$$ = $1
    }

TABLE_OPTION:
    IDENT
    {
    	$$ = &Measurement{Name:$1}
    }
    |STRING
    {
    	$$ = &Measurement{Name:$1}
    }
    |REGULAR_EXPRESSION
    {
	re, err := regexp.Compile($1)
	if err != nil {
	    yylex.Error("Invalid regexprs")
	}

    	$$ = &Measurement{Regex: &RegexLiteral{Val: re}}
    }

GROUP_BY_CLAUSE:
    GROUP BY DIMENSION_NAMES
    {
        $$ = $3
    }
    |
    {
        $$ = dimensionsList{dims: nil, depth: depthCheck(yylex, 0)}
    }

EXCEPT_CLAUSE:
    EXCEPT DIMENSION_NAMES
    {
        $$ = $2
    }
    |
    {
        $$ = dimensionsList{dims: nil, depth: depthCheck(yylex, 0)}
    }

DIMENSION_NAMES:
    DIMENSION_NAME
    {
        $$ = dimensionsList{dims: []*Dimension{$1}, depth: depthCheck(yylex, 1 + $1.Depth())}
    }
    |DIMENSION_NAMES COMMA DIMENSION_NAME
    {
        dl := $1
        dl.dims = append(dl.dims, $3)
        dl.depth = depthCheck(yylex, max(dl.depth, len(dl.dims) + $3.Depth()))
        $$ = dl
    }

STRING_TYPE:
    IDENT
    {
    	$$ = $1
    }
    |STRING
    {
    	$$ = $1
    }

DIMENSION_NAME:
    STRING_TYPE
    {
        $$ = &Dimension{Expr:&VarRef{Val:$1}, depth: depthCheck(yylex, 2)}
    }
    |STRING_TYPE DOUBLECOLON TAG
    {
        $$ = &Dimension{Expr:&VarRef{Val:$1}, depth: depthCheck(yylex, 2)}
    }
    |IDENT LPAREN DURATIONVAL RPAREN
    {
    	if strings.ToLower($1) != "time"{
    	    yylex.Error("Invalid group by combination for no-time tag and time duration")
    	}

    	$$ = &Dimension{Expr:&Call{Name:"time", Args:[]Expr{&DurationLiteral{Val: $3}}, depth: depthCheck(yylex, 3)}, depth: depthCheck(yylex, 4)}
    }
    |IDENT LPAREN DURATIONVAL COMMA DURATIONVAL RPAREN
    {
        if strings.ToLower($1) != "time"{
                    yylex.Error("Invalid group by combination for no-time tag and time duration")
                }

        $$ = &Dimension{Expr:&Call{Name:"time", Args:[]Expr{&DurationLiteral{Val: $3},&DurationLiteral{Val: $5}}, depth: depthCheck(yylex, 4)}, depth: depthCheck(yylex, 5)}
    }
    |IDENT LPAREN DURATIONVAL COMMA SUB DURATIONVAL RPAREN
    {
        if strings.ToLower($1) != "time"{
                    yylex.Error("Invalid group by combination for no-time tag and time duration")
                }

        $$ = &Dimension{Expr:&Call{Name:"time", Args:[]Expr{&DurationLiteral{Val: $3},&DurationLiteral{Val: time.Duration(-$6)}}, depth: depthCheck(yylex, 4)}, depth: depthCheck(yylex, 5)}
    }
    |MUL
    {
        $$ = &Dimension{Expr:&Wildcard{Type:Token($1)}, depth: depthCheck(yylex, 2)}
    }
    |MUL DOUBLECOLON TAG
    {
        $$ = &Dimension{Expr:&Wildcard{Type:Token($1)}, depth: depthCheck(yylex, 2)}
    }
    |REGULAR_EXPRESSION
     {
        re, err := regexp.Compile($1)
        if err != nil {
            yylex.Error("Invalid regexprs")
        }
        $$ = &Dimension{Expr: &RegexLiteral{Val: re}, depth: depthCheck(yylex, 2)}
     }


TIME_ZONE:
    IDENT LPAREN STRING RPAREN
    {
        if strings.ToLower($1) != "tz"{
            yylex.Error("Expect tz")
        }
        loc, err := time.LoadLocation($3)
        if err != nil {
            yylex.Error("nable to find time zone")
        }
        $$ = loc
    }
    |
    {
      $$ = nil
    }

FILL_CLAUSE:
    FILL LPAREN FILLCONTENT RPAREN
    {
        $$ = $3
    }
    |
    {
        $$ = "null"
    }

FILLCONTENT:
    IDENT
    {
        $$ = $1
    }
    |INTEGER
    {
        $$ = $1
    }
    |NUMBER
    {
        $$ = $1
    }
    |SUB FILLCONTENT
    {
        switch s := $2.(type) {
        case int64:
            $$ = -1 * s
        case float64:
            $$ = -1 * s
        default:
            $$ = $2
        }
    }

WHERE_CLAUSE:
    WHERE CONDITION
    {
        $$ = $2
    }
    |
    {
        $$ = nil
    }

AND_CONDITION:
    OPERATION_EQUAL
    {
        $$ = $1
    }
    |CONDITION AND CONDITION
    {
         $$ = &BinaryExpr{Op:Token($2),LHS:$1,RHS:$3, depth: depthCheck(yylex, 1+max($1.Depth(), $3.Depth()))}
    }

OR_CONDITION:
    AND_CONDITION
    {
        $$ = $1
    }
    |CONDITION OR CONDITION
    {
        $$ = &BinaryExpr{Op:Token($2),LHS:$1,RHS:$3, depth: depthCheck(yylex, 1+max($1.Depth(), $3.Depth()))}
    }

CONDITION:
    OR_CONDITION
    {
        $$ = $1
    }
    |LPAREN CONDITION RPAREN
    {
    	$$ = &ParenExpr{Expr:$2, depth: depthCheck(yylex, 1+$2.Depth())}
    }
    |IDENT IN LPAREN COLUMN_CLAUSES RPAREN
    {
        ident := &VarRef{Val:$1}
        vals := make(map[interface{}]bool)
        for _, fld := range $4.fields {
            switch fld.Expr.(type) {
            case *StringLiteral:
                vals[fld.Expr.(*StringLiteral).Val] = true
            case *NumberLiteral:
                vals[float64(fld.Expr.(*NumberLiteral).Val)] = true
            case *IntegerLiteral:
                vals[float64(fld.Expr.(*IntegerLiteral).Val)] = true
            }
        }
        $$ = &BinaryExpr{LHS:ident, Op:Token(IN), RHS:&SetLiteral{Vals: vals}, depth: depthCheck(yylex, 2)}
    }
    |IDENT NOT IN LPAREN COLUMN_CLAUSES RPAREN
    {
        ident := &VarRef{Val:$1}
        vals := make(map[interface{}]bool)
        for _, fld := range $5.fields {
            switch fld.Expr.(type) {
            case *StringLiteral:
                vals[fld.Expr.(*StringLiteral).Val] = true
            case *NumberLiteral:
                vals[float64(fld.Expr.(*NumberLiteral).Val)] = true
            case *IntegerLiteral:
                vals[float64(fld.Expr.(*IntegerLiteral).Val)] = true
            }
        }
        $$ = &BinaryExpr{LHS:ident, Op:Token(NOTIN), RHS:&SetLiteral{Vals: vals}, depth: depthCheck(yylex, 2)}
    }
    |IDENT IN LPAREN SELECT_STATEMENT RPAREN
    {
        if _, ok := $4.(*SelectStatement); !ok {
            yylex.Error("IN requires SELECT statement")
        } else {
            $$ = &InCondition{
                Stmt: $4.(*SelectStatement),
                Column: &VarRef{Val: $1},
                depth: depthCheck(yylex, 1+$4.Depth()),
            }
        }
    }
    |IDENT NOT IN LPAREN SELECT_STATEMENT RPAREN
    {
        if _, ok := $5.(*SelectStatement); !ok {
            yylex.Error("IN requires SELECT statement")
        } else {
            $$ = &InCondition{
                Stmt: $5.(*SelectStatement),
                Column: &VarRef{Val: $1},
                depth: depthCheck(yylex, 1+$5.Depth()),
                NotEqual: true,
            }
        }
    }
    |MATCH LPAREN STRING_TYPE COMMA STRING_TYPE RPAREN
    {
    	$$ = &BinaryExpr{
    		LHS:  &VarRef{Val: $3},
    		RHS:  &StringLiteral{Val: $5},
    		Op: MATCH,
    		depth: depthCheck(yylex, 2),
    	}
    }
    |MATCHPHRASE LPAREN STRING_TYPE COMMA STRING_TYPE RPAREN
    {
    	$$ = &BinaryExpr{
    		LHS:  &VarRef{Val: $3},
    		RHS:  &StringLiteral{Val: $5},
    		Op: MATCHPHRASE,
    		depth: depthCheck(yylex, 2),
    	}
    }
    |IPINRANGE LPAREN STRING_TYPE COMMA STRING_TYPE RPAREN
    {
        $$ = &BinaryExpr{
            LHS:  &VarRef{Val: $3},
            RHS:  &StringLiteral{Val: $5},
            Op: IPINRANGE,
            depth: depthCheck(yylex, 2),
        }
    }

OPERATION_EQUAL:
    CONDITION_COLUMN CONDITION_OPERATOR CONDITION_COLUMN
    {
    	if $2 == NEQREGEX{
    		switch $3.(type){
    		case *RegexLiteral:
    		default:
    			yylex.Error("expected regular expression")
    		}
    	}
        $$ = &BinaryExpr{Op:Token($2),LHS:$1,RHS:$3,depth:depthCheck(yylex, 1+maxDepth($1,$3))}
    }

CONDITION_COLUMN:
    COLUMN
    {
    	$$ = $1
    }
    |LPAREN CONDITION RPAREN
    {
    	$$ = &ParenExpr{Expr:$2,depth:depthCheck(yylex, 1+$2.Depth())}
    }

CONDITION_OPERATOR:
    EQ
    {
        $$ = EQ
    }
    |NEQ
    {
        $$ = NEQ
    }
    |LT
    {
        $$ = LT
    }
    |LTE
    {
        $$ = LTE
    }
    |GT
    {
        $$ = GT
    }
    |GTE
    {
        $$ = GTE
    }
    |EQREGEX
    {
        $$ = EQREGEX
    }
    |NEQREGEX
    {
        $$ = NEQREGEX
    }
    |LIKE
    {
        $$ = LIKE
    }

REGULAR_EXPRESSION:
    REGEX
    {
    	$$ = $1
    }

COLUMN_VAREF:
    IDENT
    {
        $$ = &VarRef{Val:$1}
    }
    |IDENT DOUBLECOLON COLUMN_VAREF_TYPE
    {
    	$$ = &VarRef{Val:$1, Type:$3}
    }
    |NUMBER
    {
        $$ = &NumberLiteral{Val:$1}
    }
    |INTEGER
    {
        $$ = &IntegerLiteral{Val:$1}
    }
    |STRING
    {
        $$ = &StringLiteral{Val:$1}
    }
    |TRUE
    {
    	$$ = &BooleanLiteral{Val:true}
    }
    |FALSE
    {
    	$$ = &BooleanLiteral{Val:false}
    }
    |REGULAR_EXPRESSION
    {
	re, err := regexp.Compile($1)
	if err != nil {
	    yylex.Error("Invalid regexprs")
	}
    	$$ = &RegexLiteral{Val: re}
    }
    |IDENT DOT IDENT
    {
        $$ = &VarRef{Val:$1+"."+$3,Type: Tag}
    }
    |BOUNDPARAM
    {
        $$ = $1
    }

COLUMN_VAREF_TYPE:
    IDENT
    {
    	switch strings.ToLower($1){
    	case "float":
    	    $$ = Float
    	case "integer":
    	    $$ = Integer
    	case "string":
    	    $$ = String
    	case "boolean":
    	    $$ = Boolean
    	case "time":
    	    $$ = Time
    	case "duration":
    	    $$ = Duration
    	case "unsigned":
    	    $$ = Unsigned
    	default:
    	    yylex.Error("wrong field dataType")
    	}
    }
    |TAG
    {
        $$ = Tag
    }
    |FIELD
    {
    	$$ = AnyField
    }

ORDER_CLAUSES:
    ORDER BY SORTFIELDS
    {
        $$ = $3
    }
    |
    {
        $$ = nil
    }

SORTFIELDS:
    SORTFIELD
    {
        $$ = []*SortField{$1}
    }
    |SORTFIELDS COMMA SORTFIELD
    {
        $$ = append($1, $3)
        depthCheck(yylex, len($$))
    }

SORTFIELD:
    IDENT
    {
        $$ = &SortField{Name:$1,Ascending:true}
    }
    |IDENT DESC
    {
        $$ = &SortField{Name:$1,Ascending:false}
    }
    |IDENT ASC
    {
        $$ = &SortField{Name:$1,Ascending:true}
    }

OPTION_CLAUSES:
    LIMIT_OFFSET_OPTION SLIMIT_SOFFSET_OPTION
    {
    	$$ = append($1, $2...)
        depthCheck(yylex, len($$))
    }

INTEGERPARA:
    INTEGER
    {
        $$ = $1
    }
    |
    BOUNDPARAM
    {
        if n,ok := $1.(*IntegerLiteral);ok{
            $$ = n.Val
        }else{
            yylex.Error("unsupported type, expect integer type")
        }
    }

LIMIT_OFFSET_OPTION:
    LIMIT INTEGERPARA OFFSET INTEGERPARA
    {
    	$$ = []int{int($2), int($4)}
    }
    |LIMIT INTEGERPARA
    {
    	$$ = []int{int($2), 0}
    }
    |OFFSET INTEGERPARA
    {
    	$$ = []int{0, int($2)}
    }
    |
    {
        $$ = []int{0, 0}
    }

SLIMIT_SOFFSET_OPTION:
    SLIMIT INTEGER SOFFSET INTEGER
    {
    	$$ = []int{int($2), int($4)}
    }
    |SLIMIT INTEGER
    {
    	$$ = []int{int($2), 0}
    }
    |SOFFSET INTEGER
    {
    	$$ = []int{0, int($2)}
    }
    |
    {
        $$ = []int{0, 0}
    }

SHOW_DATABASES_STATEMENT:
    SHOW DATABASES
    {
        $$ = &ShowDatabasesStatement{ShowDetail:false}
    }
    |SHOW DATABASES DETAIL
    {
        $$ = &ShowDatabasesStatement{ShowDetail:true}
    }

CREATE_DATABASE_STATEMENT:
    CREATE DATABASE IDENT WITH_CLAUSES DATABASE_POLICY
    {
        sms := $4

        sms.(*CreateDatabaseStatement).Name = $3
        sms.(*CreateDatabaseStatement).DatabaseAttr = $5
        $$ = sms
    }
    |CREATE DATABASE IDENT DATABASE_POLICY
    {
        stmt := &CreateDatabaseStatement{}
        stmt.RetentionPolicyCreate = false
        stmt.Name = $3
        stmt.DatabaseAttr = $4
        $$ = stmt
    }

DATABASE_POLICY:
    REPLICAS INTEGER
    {
        $$ = DatabasePolicy{Replicas:uint32($2), EnableTagArray:false}
    }
    |
    ALLOW_TAG_ARRAY
    {
        $$ = DatabasePolicy{EnableTagArray:$1}
    }
    |
    REPLICAS INTEGER ALLOW_TAG_ARRAY
    {
        $$ = DatabasePolicy{Replicas:uint32($2), EnableTagArray:$3}
    }
    |
    ALLOW_TAG_ARRAY REPLICAS INTEGER
    {
        $$ = DatabasePolicy{Replicas:uint32($3), EnableTagArray:$1}
    }
    |
    {
        $$ = DatabasePolicy{EnableTagArray:false}
    }

ALLOW_TAG_ARRAY:
    TAG ATTRIBUTE IDENT
    {
        if strings.ToLower($3) != "array"{
              yylex.Error("unsupport type")
        }
        $$ = true
    }
    |TAG ATTRIBUTE DEFAULT
    {
        $$ = false
    }


WITH_CLAUSES:
    WITH CREAT_DATABASE_POLICYS
    {
        stmt := &CreateDatabaseStatement{}
        stmt.RetentionPolicyCreate = true
        stmt.RetentionPolicyDuration = $2.PolicyDuration
        stmt.RetentionPolicyReplication = $2.Replication
        stmt.RetentionPolicyName = $2.PolicyName
        stmt.ShardKey = $2.ShardKey
        sort.Strings(stmt.ShardKey)

        if $2.rpdefault == true {
            yylex.Error("no default")
        }

        if $2.ShardGroupDuration==-1||$2.ShardGroupDuration==0{
           stmt.RetentionPolicyShardGroupDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyShardGroupDuration = $2.ShardGroupDuration
        }

        if $2.ShardMergeDuration==-1||$2.ShardMergeDuration==0{
           stmt.RetentionPolicyShardMergeDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyShardMergeDuration = $2.ShardMergeDuration
        }

        if $2.HotDuration==-1||$2.HotDuration==0{
           stmt.RetentionPolicyHotDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyHotDuration = $2.HotDuration
        }

        if $2.WarmDuration==-1||$2.WarmDuration==0{
           stmt.RetentionPolicyWarmDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyWarmDuration = $2.WarmDuration
        }

        if $2.IndexColdDuration==-1||$2.IndexColdDuration==0{
           stmt.RetentionPolicyIndexColdDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyIndexColdDuration = $2.IndexColdDuration
        }

        if $2.IndexGroupDuration==-1||$2.IndexGroupDuration==0{
           stmt.RetentionPolicyIndexGroupDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyIndexGroupDuration = $2.IndexGroupDuration
        }
        $$ = stmt
    }



CREAT_DATABASE_POLICYS:
    CREAT_DATABASE_POLICY
    {
        $$ = $1
    }
    |CREAT_DATABASE_POLICY CREAT_DATABASE_POLICYS
    {
        if $1.ShardGroupDuration<0  || $2.ShardGroupDuration<0{
            if $2.ShardGroupDuration>=0 {
                $1.ShardGroupDuration = $2.ShardGroupDuration
            }
        }else{
            yylex.Error("Repeat Shard Group Duration")
        }

        if $1.ShardMergeDuration<0  || $2.ShardMergeDuration<0{
            if $2.ShardMergeDuration>=0 {
                $1.ShardMergeDuration = $2.ShardMergeDuration
            }
        }else{
            yylex.Error("Repeat Shard Merge Duration")
        }

        if len($1.ShardKey) != 0 && len($2.ShardKey) != 0 {
            yylex.Error("Repeat ShardKey")
        } else if len($2.ShardKey) != 0 {
            $1.ShardKey = $2.ShardKey
        }

        if $1.HotDuration<0  || $2.HotDuration<0{
            if $2.HotDuration>=0 {
                $1.HotDuration = $2.HotDuration
            }
        }else{
            yylex.Error("Repeat Hot Duration")
        }

        if $1.WarmDuration<0 || $2.WarmDuration<0{
            if $2.WarmDuration>=0 {
                $1.WarmDuration = $2.WarmDuration
            }
        }else{
            yylex.Error("Repeat Warm Duration")
        }

        if $1.IndexColdDuration<0 || $2.IndexColdDuration<0{
            if $2.IndexColdDuration>=0 {
                $1.IndexColdDuration = $2.IndexColdDuration
            }
        }else{
            yylex.Error("Repeat Index Cold Duration")
        }

        if $1.IndexGroupDuration<0 || $2.IndexGroupDuration<0{
            if $2.IndexGroupDuration>=0 {
                $1.IndexGroupDuration = $2.IndexGroupDuration
            }
        }else{
            yylex.Error("Repeat Index Group Duration")
        }

        if $1.PolicyDuration == nil || $2.PolicyDuration == nil{
            if $2.PolicyDuration != nil{
                $1.PolicyDuration = $2.PolicyDuration
            }
        }else{
            yylex.Error("Repeat Policy Duration")
        }

        if $1.Replication == nil || $2.Replication == nil{
            if $2.Replication != nil{
                 $1.Replication = $2.Replication
            }
        }else{
            yylex.Error("Repeat Policy Replication")
        }

        if len($1.PolicyName) == 0 || len($2.PolicyName) == 0{
            if len($2.PolicyName) != 0{
                $1.PolicyName = $2.PolicyName
            }
        }else{
            yylex.Error("Repeat Policy Name")
        }

        if $1.rpdefault == false || $2.rpdefault == false{
            if $2.rpdefault == true{
                $1.rpdefault = $2.rpdefault
            }
        }else{
            yylex.Error("Repeat rpdefault")
        }
        $$ = $1
    }

CREAT_DATABASE_POLICY:
    SHARD_HOT_WARM_INDEX_DURATION
    {
        $$ = $1
    }
    |DURATION DURATIONVAL
    {
        duration := $2
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,IndexColdDuration: -1,PolicyDuration: &duration, ShardMergeDuration: -1}
    }
    |REPLICATION INTEGER
    {
        replicaN := int($2)
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,IndexColdDuration: -1,Replication: &replicaN, ShardMergeDuration: -1}
    }
    |NAME IDENT
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,IndexColdDuration: -1,PolicyName: $2, ShardMergeDuration: -1}
    }
    |REPLICANUM INTEGER
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,IndexColdDuration: -1,ReplicaNum: uint32($2), ShardMergeDuration: -1}
    }
    |DEFAULT
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,IndexColdDuration: -1,rpdefault: true, ShardMergeDuration: -1}
    }
    |SHARDKEY SHARDKEYLIST
    {
        if len($2) == 0 {
            yylex.Error("ShardKey should not be nil")
        }
        $$ = &Durations{ShardKey:$2,ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,IndexColdDuration: -1,rpdefault: false, ShardMergeDuration: -1}
    }



SHOW_MEASUREMENTS_STATEMENT:
    SHOW MEASUREMENTS ON_DATABASE WITH MEASUREMENT MEASUREMENT_WITH WHERE_CLAUSE ORDER_CLAUSES OPTION_CLAUSES
    {
        $$ = &ShowMeasurementsStatement{
            Database: $3,
            Source: $6,
            Condition: $7,
            SortFields: $8,
            Limit: $9[0],
            Offset: $9[1],
            depth: depthCheck(yylex, 1 + maxDepth($6, $7, $8)),
        }
    }
    |SHOW MEASUREMENTS ON_DATABASE WHERE_CLAUSE ORDER_CLAUSES OPTION_CLAUSES
    {
        $$ = &ShowMeasurementsStatement{
            Database: $3,
            Condition: $4,
            SortFields: $5,
            Limit: $6[0],
            Offset: $6[1],
            depth: depthCheck(yylex, 1 + maxDepth($4, $5)),
        }
    }

SHOW_MEASUREMENTS_DETAIL_STATEMENT:
    SHOW MEASUREMENTS DETAIL ON_DATABASE WITH MEASUREMENT MEASUREMENT_WITH
    {
        $$ = &ShowMeasurementsDetailStatement{
            Database: $4,
            Source: $7,
            depth: depthCheck(yylex, 1 + maxDepth($7)),
        }
    }
    |SHOW MEASUREMENTS DETAIL ON_DATABASE
    {
        $$ = &ShowMeasurementsDetailStatement{
            Database: $4,
            depth: depthCheck(yylex, 1),
        }
    }

MEASUREMENT_WITH:

    EQ IDENT
    {
        $$ = &Measurement{Name:$2}
    }
    |NEQ IDENT
    {
        $$ = &Measurement{Name:$2}
    }
    |EQREGEX REGULAR_EXPRESSION
    {
        re, err := regexp.Compile($2)
        if err != nil {
            yylex.Error("Invalid regexprs")
        }
        $$ = &Measurement{Regex:&RegexLiteral{Val: re}}
    }
    |NEQREGEX REGULAR_EXPRESSION
    {
        re, err := regexp.Compile($2)
        if err != nil {
            yylex.Error("Invalid regexprs")
        }
        $$ = &Measurement{Regex:&RegexLiteral{Val: re}}
    }



SHOW_RETENTION_POLICIES_STATEMENT:
    SHOW RETENTION POLICIES ON IDENT
    {
        $$ = &ShowRetentionPoliciesStatement{
            Database: $5,
        }
    }
    |SHOW RETENTION POLICIES
    {
         $$ = &ShowRetentionPoliciesStatement{ }
    }


CREATE_RENTRENTION_POLICY_STATEMENT:
    CREATE RETENTION POLICY IDENT ON IDENT RP_DURATION_OPTIONS
    {
        stmt := $7.(*CreateRetentionPolicyStatement)
        stmt.Name = $4
        stmt.Database = $6
        $$ = stmt
    }
    |CREATE RETENTION POLICY IDENT ON IDENT RP_DURATION_OPTIONS DEFAULT
    {
        stmt := $7.(*CreateRetentionPolicyStatement)
        stmt.Name = $4
        stmt.Database = $6
        stmt.Default = true
        $$ = stmt
    }

CREATE_USER_STATEMENT:
    CREATE USER IDENT WITH PASSWORD STRING
    {
        stmt := &CreateUserStatement{}
        stmt.Name = $3
        stmt.Password = $6
        $$ = stmt
    }
    |CREATE USER IDENT WITH PASSWORD STRING WITH ALL PRIVILEGES
    {
        stmt := &CreateUserStatement{}
        stmt.Name = $3
        stmt.Password = $6
        stmt.Admin = true
        $$ = stmt
    }
    |CREATE USER IDENT WITH PASSWORD STRING WITH PARTITION PRIVILEGES
    {
        stmt := &CreateUserStatement{}
        stmt.Name = $3
        stmt.Password = $6
        stmt.Rwuser = true
        $$ = stmt
    }


RP_DURATION_OPTIONS:
    DURATION DURATIONVAL REPLICATION INTEGER SHARD_HOT_WARM_INDEX_DURATIONS
    {
    	stmt := &CreateRetentionPolicyStatement{}
    	stmt.Duration = $2
    	stmt.Replication = int($4)

    	if $5.ShardGroupDuration==-1||$5.ShardGroupDuration==0{
           stmt.ShardGroupDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.ShardGroupDuration = $5.ShardGroupDuration
        }

        if $5.HotDuration==-1||$5.HotDuration==0{
           stmt.HotDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.HotDuration = $5.HotDuration
        }

        if $5.WarmDuration==-1||$5.WarmDuration==0{
           stmt.WarmDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.WarmDuration = $5.WarmDuration
        }

        if $5.IndexColdDuration==-1||$5.IndexColdDuration==0{
           stmt.IndexColdDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.IndexColdDuration = $5.IndexColdDuration
        }

        if $5.IndexGroupDuration==-1||$5.IndexGroupDuration==0{
           stmt.IndexGroupDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.IndexGroupDuration = $5.IndexGroupDuration
        }

        if $5.ShardMergeDuration==-1||$5.ShardMergeDuration==0{
           stmt.ShardMergeDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.ShardMergeDuration = $5.ShardMergeDuration
        }

    	$$ = stmt
    }
    |DURATION DURATIONVAL REPLICATION INTEGER
    {
    	stmt := &CreateRetentionPolicyStatement{}
    	stmt.Duration = $2
    	stmt.Replication = int($4)
    	$$ = stmt
    }


SHARD_HOT_WARM_INDEX_DURATIONS:
    SHARD_HOT_WARM_INDEX_DURATION
    {
        $$ = $1
    }
    |SHARD_HOT_WARM_INDEX_DURATION SHARD_HOT_WARM_INDEX_DURATIONS
    {
        if $1.ShardGroupDuration<0  || $2.ShardGroupDuration<0{
            if $2.ShardGroupDuration>=0 {
                $1.ShardGroupDuration = $2.ShardGroupDuration
            }
        }else{
            yylex.Error("Repeat Shard Group Duration")
        }

        if $1.HotDuration<0  || $2.HotDuration<0{
            if $2.HotDuration>=0 {
                $1.HotDuration = $2.HotDuration
            }
        }else{
            yylex.Error("Repeat Hot Duration")
        }

        if $1.WarmDuration<0  || $2.WarmDuration<0{
            if $2.WarmDuration>=0 {
                $1.WarmDuration = $2.WarmDuration
            }
        }else{
            yylex.Error("Repeat Warm Duration")
        }

        if $1.IndexColdDuration<0  || $2.IndexColdDuration<0{
            if $2.IndexColdDuration>=0 {
                $1.IndexColdDuration = $2.IndexColdDuration
            }
        }else{
             yylex.Error("Repeat Index Cold Duration")
        }

        if $1.IndexGroupDuration<0  || $2.IndexGroupDuration<0{
            if $2.IndexGroupDuration>=0 {
                $1.IndexGroupDuration = $2.IndexGroupDuration
            }
        }else{
            yylex.Error("Repeat Index Group Duration")
        }

        if $1.ShardMergeDuration<0  || $2.ShardMergeDuration<0{
            if $2.ShardMergeDuration>=0 {
                $1.ShardMergeDuration = $2.ShardMergeDuration
            }
        }else{
            yylex.Error("Repeat ShardMerge Duration")
        }
        $$ = $1
    }


SHARD_HOT_WARM_INDEX_DURATION:
    SHARD DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: $3,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1, IndexColdDuration: -1, ShardMergeDuration: -1}
    }
    |HOT DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: $3,WarmDuration: -1,IndexGroupDuration: -1, IndexColdDuration: -1, ShardMergeDuration: -1}
    }
    |WARM DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: $3,IndexGroupDuration: -1, IndexColdDuration: -1, ShardMergeDuration: -1}
    }
    |INDEX DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: $3, IndexColdDuration: -1, ShardMergeDuration: -1}
    }
    |INDEXCOLD DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1, IndexColdDuration: $3, ShardMergeDuration: -1}
    }
    |SHARDMERGE DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1, IndexColdDuration: -1, ShardMergeDuration: $3}
    }



SHOW_SERIES_STATEMENT:
    SHOW SERIES ON_DATABASE FROM_CLAUSE WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
    {
    	$$ = &ShowSeriesStatement{
    	    Database: $3,
    	    Sources: $4.sources,
    	    Condition: $5,
    	    SortFields: $6,
    	    Limit: $7[0],
    	    Offset: $7[1],
            depth: depthCheck(yylex, 1 + maxDepth($4, $5, $6)),
    	}
    }
    |SHOW SERIES ON_DATABASE WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
    {
    	$$ = &ShowSeriesStatement{
    	    Database: $3,
    	    Condition: $4,
    	    SortFields: $5,
    	    Limit: $6[0],
    	    Offset: $6[1],
            depth: depthCheck(yylex, 1 + maxDepth($4, $5)),
    	}
    }
    |SHOW HINT SERIES ON_DATABASE FROM_CLAUSE WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
    {
        $$ = &ShowSeriesStatement{
            Hints: $2,
            Database: $4,
            Sources: $5.sources,
            Condition: $6,
            SortFields: $7,
            Limit: $8[0],
            Offset: $8[1],
            depth: depthCheck(yylex, 1 + maxDepth($5, $6, $7)),
        }
    }
    |SHOW HINT SERIES ON_DATABASE WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
    {
        $$ = &ShowSeriesStatement{
            Hints: $2,
            Database: $4,
            Condition: $5,
            SortFields: $6,
            Limit: $7[0],
            Offset: $7[1],
            depth: depthCheck(yylex, 1 + maxDepth($5, $6)),
        }
    }

SHOW_USERS_STATEMENT:
    SHOW USERS
    {
    	$$ = &ShowUsersStatement{}
    }

DROP_DATABASE_STATEMENT:
    DROP DATABASE IDENT
    {
    	$$ = &DropDatabaseStatement{Name: $3}
    }

DROP_SERIES_STATEMENT:
    DROP SERIES FROM_CLAUSE WHERE_CLAUSE
    {
    	$$ = &DropSeriesStatement{
    	    Sources: $3.sources,
    	    Condition: $4,
            depth: depthCheck(yylex, 1 + maxDepth($3, $4)),
        }
    }
    |DROP SERIES WHERE_CLAUSE
    {
        $$ = &DropSeriesStatement{
            Condition: $3,
            depth: depthCheck(yylex, 1 + maxDepth($3)),
        }
    }

DELETE_SERIES_STATEMENT:
    DELETE FROM_CLAUSE WHERE_CLAUSE
    {
    	$$ = &DeleteSeriesStatement{
    	    Sources: $2.sources,
    	    Condition: $3,
            depth: depthCheck(yylex, 1 + maxDepth($2, $3)),
        }
    }
    |DELETE WHERE_CLAUSE
    {
        $$ = &DeleteSeriesStatement{
            Condition: $2,
            depth: depthCheck(yylex, 1 + maxDepth($2)),
        }
    }


ALTER_RENTRENTION_POLICY_STATEMENT:
    ALTER RETENTION POLICY IDENT ON IDENT CREAT_DATABASE_POLICYS
    {
        stmt := &AlterRetentionPolicyStatement{}
        stmt.Name = $4
        stmt.Database = $6
        stmt.Duration = $7.PolicyDuration
        stmt.Replication = $7.Replication
        stmt.Default = $7.rpdefault
        if $7.ShardGroupDuration==-1{
           stmt.ShardGroupDuration = nil
        }else{
           stmt.ShardGroupDuration = &$7.ShardGroupDuration
        }
        if $7.HotDuration==-1{
           stmt.HotDuration = nil
        }else{
           stmt.HotDuration = &$7.HotDuration
        }
        if $7.WarmDuration==-1{
           stmt.WarmDuration = nil
        }else{
           stmt.WarmDuration = &$7.WarmDuration
        }
        if $7.IndexColdDuration==-1{
           stmt.IndexColdDuration = nil
        }else{
           stmt.IndexColdDuration = &$7.IndexColdDuration
        }
        if $7.IndexGroupDuration==-1{
           stmt.IndexGroupDuration = nil
        }else{
           stmt.IndexGroupDuration = &$7.IndexGroupDuration
        }

        if len($7.PolicyName)>0|| $7.ReplicaNum != 0{
           yylex.Error("PolicyName and ReplicaNum")
        }
        $$ = stmt
    }



DROP_RETENTION_POLICY_STATEMENT:
    DROP RETENTION POLICY IDENT ON IDENT
    {
        stmt := &DropRetentionPolicyStatement{}
        stmt.Name = $4
        stmt.Database = $6
        $$ = stmt
    }

GRANT_STATEMENT:
    GRANT ALL ON IDENT TO IDENT
    {
    	stmt := &GrantStatement{}
    	stmt.Privilege = AllPrivileges
    	stmt.On = $4
    	stmt.User = $6
    	$$ = stmt
    }
    |GRANT ALL PRIVILEGES ON IDENT TO IDENT
    {
    	stmt := &GrantStatement{}
    	stmt.Privilege = AllPrivileges
    	stmt.On = $5
    	stmt.User = $7
    	$$ = stmt
    }
    |GRANT IDENT ON IDENT TO IDENT
    {
    	stmt := &GrantStatement{}
    	switch strings.ToLower($2){
    	case "read":
    	    stmt.Privilege = ReadPrivilege
    	case "write":
    	    stmt.Privilege = WritePrivilege
    	default:
    	    yylex.Error("wrong Privilege")
    	}
    	stmt.On = $4
    	stmt.User = $6
    	$$ = stmt
    }

GRANT_ADMIN_STATEMENT:
    GRANT ALL PRIVILEGES TO IDENT
    {
    	$$ = &GrantAdminStatement{User: $5}
    }
    |GRANT ALL TO IDENT
    {
    	$$ = &GrantAdminStatement{User: $4}
    }

REVOKE_STATEMENT:
    REVOKE ALL ON IDENT FROM IDENT
    {
    	stmt := &RevokeStatement{}
    	stmt.Privilege = AllPrivileges
    	stmt.On = $4
    	stmt.User = $6
    	$$ = stmt
    }
    |REVOKE ALL PRIVILEGES ON IDENT FROM IDENT
    {
    	stmt := &RevokeStatement{}
    	stmt.Privilege = AllPrivileges
    	stmt.On = $5
    	stmt.User = $7
    	$$ = stmt
    }
    |REVOKE IDENT ON IDENT FROM IDENT
    {
    	stmt := &RevokeStatement{}
    	switch strings.ToLower($2){
    	case "read":
    	    stmt.Privilege = ReadPrivilege
    	case "write":
    	    stmt.Privilege = WritePrivilege
    	default:
    	    yylex.Error("wrong Privilege")
    	}
    	stmt.On = $4
    	stmt.User = $6
    	$$ = stmt
    }

REVOKE_ADMIN_STATEMENT:
    REVOKE ALL PRIVILEGES FROM IDENT
    {
    	$$ = &RevokeAdminStatement{User: $5}
    }
    |REVOKE ALL FROM IDENT
    {
    	$$ = &RevokeAdminStatement{User: $4}
    }

DROP_USER_STATEMENT:
    DROP USER IDENT
    {
    	$$ = &DropUserStatement{Name:$3}
    }

SHOW_TAG_KEYS_STATEMENT:
  SHOW TAG KEYS ON_DATABASE FROM_CLAUSE WHERE_CLAUSE ORDER_CLAUSES OPTION_CLAUSES
  {
      $$ = &ShowTagKeysStatement{
          Database: $4,
          Sources: $5.sources,
          Condition: $6,
          SortFields: $7,
          Limit: $8[0],
          Offset: $8[1],
          SLimit: $8[2],
          SOffset: $8[3],
          depth: depthCheck(yylex, 1 + maxDepth($5, $6, $7)),
      }

  }
  |SHOW TAG KEYS ON_DATABASE WHERE_CLAUSE ORDER_CLAUSES OPTION_CLAUSES
  {
      $$ = &ShowTagKeysStatement{
          Database: $4,
          Condition: $5,
          SortFields: $6,
          Limit: $7[0],
          Offset: $7[1],
          SLimit: $7[2],
          SOffset: $7[3],
          depth: depthCheck(yylex, 1 + maxDepth($5, $6)),
      }
  }

MEASUREMENT_INFO:
    PRIMARYKEY
    {
        $$ = "PRIMARYKEY"
    }
    |SORTKEY
    {
        $$ = "SORTKEY"
    }
    |PROPERTY
    {
        $$ = "PROPERTY"
    }
    |SHARDKEY
    {
        $$ = "SHARDKEY"
    }
    |ENGINETYPE
    {
        $$ = "ENGINETYPE"
    }
    |SCHEMA
    {
        $$ = "SCHEMA"
    }
    |INDEXES
    {
        $$ = "INDEXES"
    }
    |COMPACT
    {
        $$ = "COMPACT"
    }
    |IDENT
    {
        yylex.Error("SHOW command error, only support PRIMARYKEY, SORTKEY, SHARDKEY, ENGINETYPE, INDEXES, SCHEMA, COMPACT")
    }

SHOW_MEASUREMENT_KEYS_STATEMENT:
    SHOW MEASUREMENT_INFO FROM IDENT
    {
        stmt := &ShowMeasurementKeysStatement{}
        stmt.Name = $2
        stmt.Measurement = $4
        $$ = stmt
    }
    |SHOW MEASUREMENT_INFO FROM IDENT DOT IDENT DOT IDENT
    {
        stmt := &ShowMeasurementKeysStatement{}
        stmt.Name = $2
        stmt.Database = $4
        stmt.Rp = $6
        stmt.Measurement = $8
        $$ = stmt
    }
    |SHOW MEASUREMENT_INFO FROM IDENT DOT DOT IDENT
    {
        stmt := &ShowMeasurementKeysStatement{}
        stmt.Name = $2
        stmt.Database = $4
        stmt.Measurement = $7
        $$ = stmt
    }
    |SHOW MEASUREMENT_INFO FROM DOT IDENT DOT IDENT
    {
        stmt := &ShowMeasurementKeysStatement{}
        stmt.Name = $2
        stmt.Rp = $5
        stmt.Measurement = $7
        $$ = stmt
    }
    |SHOW MEASUREMENT_INFO FROM DOT DOT IDENT
    {
        stmt := &ShowMeasurementKeysStatement{}
        stmt.Name = $2
        stmt.Measurement = $6
        $$ = stmt
    }

ON_DATABASE:
  ON IDENT
  {
      $$ = $2
  }
  |
  {
      $$ = ""
  }

SHOW_FIELD_KEYS_STATEMENT:
  SHOW FIELD KEYS ON_DATABASE FROM_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
  {
      $$ = &ShowFieldKeysStatement{
          Database: $4,
          Sources: $5.sources,
          SortFields: $6,
          Limit: $7[0],
          Offset: $7[1],
          depth: depthCheck(yylex, 1 + maxDepth($5, $6)),
      }
  }
  |SHOW FIELD KEYS ON_DATABASE ORDER_CLAUSES LIMIT_OFFSET_OPTION
   {
       $$ = &ShowFieldKeysStatement{
           Database: $4,
           SortFields: $5,
           Limit: $6[0],
           Offset: $6[1],
           depth: depthCheck(yylex, 1 + maxDepth($5)),
       }
   }


SHOW_TAG_VALUES_STATEMENT:
   SHOW TAG VALUES ON_DATABASE FROM_CLAUSE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
   {
       stmt := $8.(*ShowTagValuesStatement)
       stmt.TagKeyCondition = nil
       stmt.Database = $4
       stmt.Sources = $5.sources
       stmt.Condition = $9
       stmt.SortFields = $10
       stmt.Limit = $11[0]
       stmt.Offset = $11[1]
       stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + maxDepth($5, $9, $10)))
       $$ = stmt
   }
   |SHOW TAG VALUES ON_DATABASE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
   {
       stmt := $7.(*ShowTagValuesStatement)
       stmt.TagKeyCondition = nil
       stmt.Database = $4
       stmt.Condition = $8
       stmt.SortFields = $9
       stmt.Limit = $10[0]
       stmt.Offset = $10[1]
       stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + maxDepth($8, $9)))
       $$ = stmt
   }
  |SHOW HINT TAG VALUES ON_DATABASE FROM_CLAUSE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
  {
       stmt := $9.(*ShowTagValuesStatement)
       stmt.Hints = $2
       stmt.TagKeyCondition = nil
       stmt.Database = $5
       stmt.Sources = $6.sources
       stmt.Condition = $10
       stmt.SortFields = $11
       stmt.Limit = $12[0]
       stmt.Offset = $12[1]
       stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + maxDepth($6, $10, $11)))
       $$ = stmt
  }
  |SHOW HINT TAG VALUES ON_DATABASE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
  {
       stmt := $8.(*ShowTagValuesStatement)
       stmt.Hints = $2
       stmt.TagKeyCondition = nil
       stmt.Database = $5
       stmt.Condition = $9
       stmt.SortFields = $10
       stmt.Limit = $11[0]
       stmt.Offset = $11[1]
       stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + maxDepth($9, $10)))
       $$ = stmt
  }

TAG_VALUES_WITH:
  EQ TAG_KEYS
  {
      $$ = &ShowTagValuesStatement{
          Op: EQ,
          TagKeyExpr: $2.(*ListLiteral),
          depth: depthCheck(yylex, 1 + $2.Depth()),
      }
  }
  |NEQ TAG_KEYS
  {
      $$ = &ShowTagValuesStatement{
          Op: NEQ,
          TagKeyExpr: $2.(*ListLiteral),
          depth: depthCheck(yylex, 1 + $2.Depth()),
      }
  }
  |IN LPAREN TAG_KEYS RPAREN
  {
      $$ = &ShowTagValuesStatement{
          Op: IN,
          TagKeyExpr: $3.(*ListLiteral),
          depth: depthCheck(yylex, 1 + $3.Depth()),
      }
  }
  |EQREGEX REGULAR_EXPRESSION
  {
       stmt := &ShowTagValuesStatement{depth: depthCheck(yylex, 2)}
       stmt.Op = EQREGEX
       re, err := regexp.Compile($2)
       	if err != nil {
       	    yylex.Error("Invalid regexprs")
       	}
       stmt.TagKeyExpr = &RegexLiteral{Val: re}
       $$ = stmt
  }
  |NEQREGEX REGULAR_EXPRESSION
  {
       stmt := &ShowTagValuesStatement{depth: depthCheck(yylex, 2)}
       stmt.Op = NEQREGEX
       re, err := regexp.Compile($2)
       	if err != nil {
       	    yylex.Error("Invalid regexprs")
       	}
       stmt.TagKeyExpr = &RegexLiteral{Val: re}
       $$ = stmt
  }


TAG_KEYS:
  TAG_KEY
  {
     temp := []string{$1}
     $$ = &ListLiteral{Vals: temp}
  }
  |TAG_KEY COMMA TAG_KEYS
  {
      $3.(*ListLiteral).Vals = append($3.(*ListLiteral).Vals, $1)
      $$ = $3
      depthCheck(yylex, $$.Depth())
  }

TAG_KEY:
  IDENT
  {
      $$ = $1
  }



EXPLAIN_STATEMENT:
    EXPLAIN ANALYZE SELECT_STATEMENT
    {
        $$ = &ExplainStatement{
            Statement: $3.(*SelectStatement),
            Analyze: true,
            depth: depthCheck(yylex, 1 + $3.Depth()),
        }
    }
    |EXPLAIN SELECT_STATEMENT
    {
        $$ = &ExplainStatement{
            Statement: $2.(*SelectStatement),
            Analyze: false,
            depth: depthCheck(yylex, 1 + $2.Depth()),
        }
    }


SHOW_TAG_KEY_CARDINALITY_STATEMENT:
    SHOW TAG KEY EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowTagKeyCardinalityStatement{
            Database: $6,
            Exact: true,
            Sources: $7.sources,
            Condition: $8,
            Dimensions: $9.dims,
            Limit: $10[0],
            Offset: $10[1],
            depth: depthCheck(yylex, 1 + maxDepth($7, $8, $9)),
        }
    }
    |SHOW TAG KEY EXACT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowTagKeyCardinalityStatement{
            Database: $6,
            Exact: true,
            Condition: $7,
            Dimensions: $8.dims,
            Limit: $9[0],
            Offset: $9[1],
            depth: depthCheck(yylex, 1 + maxDepth($7, $8)),
        }
    }
    |SHOW TAG KEY CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         $$ = &ShowTagKeyCardinalityStatement{
             Database: $5,
             Exact: false,
             Sources: $6.sources,
             Condition: $7,
             Dimensions: $8.dims,
             Limit: $9[0],
             Offset: $9[1],
             depth: depthCheck(yylex, 1 + maxDepth($6, $7, $8)),
         }
    }
    |SHOW TAG KEY CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         $$ = &ShowTagKeyCardinalityStatement{
             Database: $5,
             Exact: true,
             Condition: $6,
             Dimensions: $7.dims,
             Limit: $8[0],
             Offset: $8[1],
             depth: depthCheck(yylex, 1 + maxDepth($6, $7)),
         }
    }




SHOW_TAG_VALUES_CARDINALITY_STATEMENT:
    SHOW TAG VALUES EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt_temp := $10.(*ShowTagValuesStatement)
        $$ = &ShowTagValuesCardinalityStatement{
            Database: $6,
            Exact: true,
            Sources: $7.sources,
            Op: stmt_temp.Op,
            TagKeyExpr: stmt_temp.TagKeyExpr,
            Condition: $11,
            Dimensions: $12.dims,
            Limit: $13[0],
            Offset: $13[1],
            TagKeyCondition: nil,
            depth: depthCheck(yylex, 1 + max(1 + stmt_temp.TagKeyExpr.Depth(), maxDepth($7, $11, $12))),
        }

    }
    |SHOW TAG VALUES EXACT CARDINALITY ON_DATABASE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt_temp := $9.(*ShowTagValuesStatement)
        $$ = &ShowTagValuesCardinalityStatement{
            Database: $6,
            Exact: true,
            Op: stmt_temp.Op,
            TagKeyExpr: stmt_temp.TagKeyExpr,
            Condition: $10,
            Dimensions: $11.dims,
            Limit: $12[0],
            Offset: $12[1],
            TagKeyCondition: nil,
            depth: depthCheck(yylex, 1 + max(1 + stmt_temp.TagKeyExpr.Depth(), maxDepth($10, $11))),
        }
    }
    |SHOW TAG VALUES CARDINALITY ON_DATABASE FROM_CLAUSE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt_temp := $9.(*ShowTagValuesStatement)
        $$ = &ShowTagValuesCardinalityStatement{
            Database: $5,
            Exact: false,
            Sources: $6.sources,
            Op: stmt_temp.Op,
            TagKeyExpr: stmt_temp.TagKeyExpr,
            Condition: $10,
            Dimensions: $11.dims,
            Limit: $12[0],
            Offset: $12[1],
            TagKeyCondition: nil,
            depth: depthCheck(yylex, 1 + max(1 + stmt_temp.TagKeyExpr.Depth(), maxDepth($6, $10, $11))),
        }
    }
    |SHOW TAG VALUES CARDINALITY ON_DATABASE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt_temp := $8.(*ShowTagValuesStatement)
        $$ = &ShowTagValuesCardinalityStatement{
            Database: $5,
            Exact: true,
            Op: stmt_temp.Op,
            TagKeyExpr: stmt_temp.TagKeyExpr,
            Condition: $9,
            Dimensions: $10.dims,
            Limit: $11[0],
            Offset: $11[1],
            TagKeyCondition: nil,
            depth: depthCheck(yylex, 1 + max(1 + stmt_temp.TagKeyExpr.Depth(), maxDepth($9, $10))),
        }
    }


SHOW_FIELD_KEY_CARDINALITY_STATEMENT:
    SHOW FIELD KEY EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowFieldKeyCardinalityStatement{
            Database: $6,
            Exact: true,
            Sources: $7.sources,
            Condition: $8,
            Dimensions: $9.dims,
            Limit: $10[0],
            Offset: $10[1],
            depth: depthCheck(yylex, 1 + maxDepth($7, $8, $9)),
        }
    }
    |SHOW FIELD KEY EXACT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowFieldKeyCardinalityStatement{
            Database: $6,
            Exact: true,
            Condition: $7,
            Dimensions: $8.dims,
            Limit: $9[0],
            Offset: $9[1],
            depth: depthCheck(yylex, 1 + maxDepth($7, $8)),
        }
    }
    |SHOW FIELD KEY CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowFieldKeyCardinalityStatement{
            Database: $5,
            Exact: false,
            Sources: $6.sources,
            Condition: $7,
            Dimensions: $8.dims,
            Limit: $9[0],
            Offset: $9[1],
            depth: depthCheck(yylex, 1 + maxDepth($6, $7, $8)),
        }
    }
    |SHOW FIELD KEY CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowFieldKeyCardinalityStatement{
            Database: $5,
            Exact: true,
            Condition: $6,
            Dimensions: $7.dims,
            Limit: $8[0],
            Offset: $8[1],
            depth: depthCheck(yylex, 1 + maxDepth($6, $7)),
        }
    }


CREATE_MEASUREMENT_STATEMENT:
    CREATE MEASUREMENT TABLE_CASE COLUMN_LISTS CMOPTIONS_TS
    {
        stmt := &CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        if $4 != nil {
            stmt.Fields = $4.(*CreateMeasurementStatement).Fields
            stmt.Tags = $4.(*CreateMeasurementStatement).Tags
            stmt.IndexOption = $4.(*CreateMeasurementStatement).IndexOption
        }
        if $5.NumOfShards != 0 && $5.Type == "range" {
            yylex.Error("Not support to set num-of-shards for range sharding")
        }
        stmt.IndexType = $5.IndexType
        stmt.IndexList = $5.IndexList
        stmt.ShardKey = $5.ShardKey
        stmt.NumOfShards = $5.NumOfShards
        stmt.Type = $5.Type
        stmt.EngineType = $5.EngineType
        stmt.TTL = $5.TTL
        $$ = stmt
        depthCheck(yylex, $$.Depth())
    }
    |CREATE MEASUREMENT TABLE_CASE COLUMN_LISTS CMOPTIONS_CS
    {
        stmt := &CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        if $4 != nil {
            stmt.Fields = $4.(*CreateMeasurementStatement).Fields
            stmt.Tags = $4.(*CreateMeasurementStatement).Tags
            stmt.IndexOption = $4.(*CreateMeasurementStatement).IndexOption
        }

        // check if PrimaryKey & SortKey is IN Tags/Fields/time
        for _, key := range $5.PrimaryKey {
            _, inTag := stmt.Tags[key]
            _, inField := stmt.Fields[key]
            if !inTag && !inField && key != "time" {
                if len($5.PrimaryKey) != len($5.SortKey) {
                    yylex.Error("Invalid PrimaryKey")
                } else {
                    yylex.Error("Invalid PrimaryKey/SortKey")
                }
                return 1
            }
        }
        for _, key := range $5.SortKey {
            _, inTag := stmt.Tags[key]
            _, inField := stmt.Fields[key]
            if !inTag && !inField && key != "time" {
                if len($5.PrimaryKey) != len($5.SortKey) {
                    yylex.Error("Invalid SortKey")
                } else {
                    yylex.Error("Invalid PrimaryKey/SortKey")
                }
                return 1
            }
        }
        // check if ShardKey is IN Tags/Fields
        for _, key := range $5.ShardKey {
            _, inTag := stmt.Tags[key]
            _, inField := stmt.Fields[key]
            if !inTag && !inField {
                yylex.Error("Invalid ShardKey")
                return 1
            }
        }
        // check if primary key is left prefix of sort key
        if len($5.PrimaryKey) > len($5.SortKey) {
            yylex.Error("PrimaryKey should be left prefix of SortKey")
            return 1
        }
        for i, v := range $5.PrimaryKey {
            if v != $5.SortKey[i] {
                yylex.Error("PrimaryKey should be left prefix of SortKey")
                return 1
            }
        }
        // check if indexlist of secondary is IN Tags/Fields
        for i := range $5.IndexType {
            indextype := $5.IndexType[i]
            if indextype == "timecluster" {
                continue
            }
            indexlist := $5.IndexList[i]
            for _, col := range indexlist {
                _, inTag := stmt.Tags[col]
                _, inField := stmt.Fields[col]
                if !inTag && !inField {
                    yylex.Error("Invalid indexlist")
                }
            }
        }
        if $5.NumOfShards != 0 && $5.Type == "range" {
            yylex.Error("Not support to set num-of-shards for range sharding")
        }

        stmt.EngineType = $5.EngineType
        stmt.IndexType = $5.IndexType
        stmt.IndexList = $5.IndexList
        stmt.TimeClusterDuration = $5.TimeClusterDuration
        stmt.ShardKey = $5.ShardKey
        stmt.NumOfShards = $5.NumOfShards
        stmt.Type = $5.Type
        stmt.PrimaryKey = $5.PrimaryKey
        stmt.SortKey = $5.SortKey
        stmt.Property = $5.Property
        stmt.CompactType = $5.CompactType
        $$ = stmt
        depthCheck(yylex, $$.Depth())
    }

CMOPTIONS_TS:
    {
        option := &CreateMeasurementStatementOption{}
        option.Type = "hash"
        option.EngineType = "tsstore"
        $$ = option
    }
    | WITH CMOPTION_ENGINETYPE_TS CMOPTION_INDEXTYPE_TS CMOPTION_SHARDKEY CMOPTION_SHARDNUM TYPE_CLAUSE TTL_CLAUSE_OPT
    {
        option := &CreateMeasurementStatementOption{}
        if $3 != nil {
            option.IndexType = $3.types
            option.IndexList = $3.lists
        }
        if $4 != nil {
            option.ShardKey = $4
        }
        option.NumOfShards = $5
        option.Type = $6
        option.EngineType = $2
        option.TTL = $7
        $$ = option
    }

CMOPTIONS_CS:
    WITH CMOPTION_ENGINETYPE_CS CMOPTION_INDEXTYPE_CS CMOPTION_SHARDKEY CMOPTION_SHARDNUM TYPE_CLAUSE CMOPTION_PRIMARYKEY CMOPTION_SORTKEY CMOPTION_PROPERTIES COMPACTION_TYPE_CLAUSE
    {
        option := &CreateMeasurementStatementOption{}
        if $3 != nil {
            option.IndexType = $3.types
            option.IndexList = $3.lists
            option.TimeClusterDuration = $3.timeClusterDuration
        }
        if $4 != nil {
            option.ShardKey = $4
        }
        option.NumOfShards = $5
        option.Type = $6
        option.EngineType = $2
        if $7 != nil {
            option.PrimaryKey = $7
        } else if $8 != nil {
            option.PrimaryKey = $8
        }

        if $8 != nil {
            option.SortKey = $8
        } else if $7 != nil {
            option.SortKey = $7
        }
        if $9 != nil {
            option.Property = $9
        }
        option.CompactType = $10
        $$ = option
    }

CMOPTION_INDEXTYPE_TS:
    {
        $$ = nil
    }
    | INDEXTYPE INDEX_TYPES
    {
        validIndexType := map[string]struct{}{}
        validIndexType["text"] = struct{}{}
        validIndexType["field"] = struct{}{}
        if $2 == nil {
            $$ = nil
        } else {
            for _, indexType := range $2.types {
                if _, ok := validIndexType[strings.ToLower(indexType)]; !ok {
                    yylex.Error("Invalid index type for TSSTORE")
                }
            }
            $$ = $2
        }
    }

CMOPTION_INDEXTYPE_CS:
    {
        $$ = nil
    }
    | INDEXTYPE INDEX_TYPES
    {
        validIndexType := map[string]struct{}{}
        validIndexType["bloomfilter"] = struct{}{}
        validIndexType["bloomfilter_ip"] = struct{}{}
        validIndexType["minmax"] = struct{}{}
        validIndexType["text"] = struct{}{}
        if $2 == nil {
            $$ = nil
        } else {
            for _, indexType := range $2.types {
                if _, ok := validIndexType[strings.ToLower(indexType)]; !ok {
                    yylex.Error("Invalid index type for COLUMNSTORE")
                }
            }
            $$ = $2
        }
    }
    | INDEXTYPE IDENT LPAREN DURATIONVAL RPAREN INDEX_TYPES
    {
        indexType := strings.ToLower($2)
        if indexType != "timecluster" {
            yylex.Error("expect TIMECLUSTER for INDEXTYPE")
            return 1
        }
        indextype := &IndexType{
            types: []string{indexType},
            lists: [][]string{{"time"}},
            timeClusterDuration: $4,
        }
        validIndexType := map[string]struct{}{}
        validIndexType["bloomfilter"] = struct{}{}
        validIndexType["bloomfilter_ip"] = struct{}{}
        validIndexType["minmax"] = struct{}{}
        if $6 == nil {
            $$ = indextype
        } else {
            for _, indexType := range $6.types {
                if _, ok := validIndexType[strings.ToLower(indexType)]; !ok {
                    yylex.Error("Invalid index type for COLUMNSTORE")
                }
            }
            indextype.types = append(indextype.types, $6.types...)
            indextype.lists = append(indextype.lists, $6.lists...)
            $$ = indextype
            depthCheck(yylex, max(len($$.types), len($$.lists)))
        }
    }

CMOPTION_SHARDKEY:
    {
        $$ = nil
    }
    | SHARDKEY SHARDKEYLIST
    {
        shardKey := $2
        sort.Strings(shardKey)
        $$ = shardKey
    }

TTL_CLAUSE_OPT:
    {
        $$ = time.Duration(0)
    }
    | TTL DURATIONVAL
    {
        $$ = $2
    }

CMOPTION_SHARDNUM:
    {
        $$ = 0
    }
    | SHARDS AUTO
    {
        $$ = -1
    }
    | SHARDS INTEGER
    {
        if $2 == 0 {
            yylex.Error("syntax error: NUM OF SHARDS SHOULD LARGER THAN 0")
        }
        $$ = $2
    }

CMOPTION_ENGINETYPE_TS:
    {
        $$ = "tsstore" // default engine type
    }
    | ENGINETYPE EQ TSSTORE
    {
        $$ = "tsstore"
    }

CMOPTION_ENGINETYPE_CS:
    ENGINETYPE EQ COLUMNSTORE
    {
        $$ = "columnstore"
    }

CMOPTION_PRIMARYKEY:
    {
        $$ = nil
    }
    | PRIMARYKEY_LIST {
        $$ = $1
    }

CMOPTION_SORTKEY:
    {
        $$ = nil
    }
    | SORTKEY_LIST {
        $$ = $1
    }

CMOPTION_PROPERTIES:
    {
        $$ = nil
    }
    | MEASUREMENT_PROPERTYS_LIST {
        $$ = $1
    }

COMPACTION_TYPE_CLAUSE:
    {
        $$ = "row"
    }
    | COMPACT IDENT
    {
        compactionType := strings.ToLower($2)
        if compactionType != "row" && compactionType != "block" {
            yylex.Error("expect ROW or BLOCK for COMPACT type")
            return 1
        }
        $$ = compactionType
    }

COLUMN_LISTS:
    LPAREN FIELD_OPTIONS
    {
        stmt := &CreateMeasurementStatement {
            Tags: make(map[string]int32),
            Fields : make(map[string]int32),
        }
        for i := range $2 {
            fType := $2[i].tagOrField
            if fType == "tag" {
                stmt.Tags[$2[i].fieldName] = influx.Field_Type_Tag
            } else if fType == "field" {
                fieldType := strings.ToLower($2[i].fieldType)
                fieldName := $2[i].fieldName
                if fieldType == "int64" {
                    stmt.Fields[fieldName] = influx.Field_Type_Int
                } else if fieldType == "float64"{
                    stmt.Fields[fieldName] = influx.Field_Type_Float
                } else if fieldType == "string"{
                    stmt.Fields[fieldName] = influx.Field_Type_String
                } else if fieldType == "bool"{
                    stmt.Fields[fieldName] = influx.Field_Type_Boolean
                } else {
                    yylex.Error("expect FLOAT64, INT64, BOOL, STRING for column data type")
                    return 1 // syntax error
                }
            }
        }
        $$ = stmt
    }
    |
    {
        $$ = nil
    }

FIELD_OPTIONS:
    FIELD_OPTION FIELD_OPTIONS
    {
        fields := []*fieldList{$1}
        $$ = append(fields, $2...)
        depthCheck(yylex, len($$))
    }
    |
    FIELD_OPTION
    {
        $$ = []*fieldList{$1}
    }

FIELD_OPTION:
   FIELD_COLUMN COMMA
   {
        $$ = $1
   }
   |
   FIELD_COLUMN RPAREN
   {
        $$ = $1
   }

FIELD_COLUMN:
    STRING_TYPE TAG
    {
        $$ = &fieldList{
            fieldName: $1,
            fieldType: "string",
            tagOrField: "tag",
        }
    }
    |
    STRING_TYPE STRING_TYPE FIELD
    {
        $$ = &fieldList{
            fieldName: $1,
            fieldType: $2,
            tagOrField: "field",
        }
    }
    |
    STRING_TYPE STRING_TYPE
    {
        $$ = &fieldList{
            fieldName: $1,
            fieldType: $2,
            tagOrField: "field",
        }
    }

INDEX_TYPES:
    {
        $$ = nil
    }
    |
    INDEX_TYPE_LIST
    {
        $$ = $1
    }

INDEX_TYPE_LIST:
    INDEX_TYPE
    {
        $$ = $1
    }
    |
    INDEX_TYPE_LIST INDEX_TYPE
    {
        indextype := $1
        indextype.types = append(indextype.types,$2.types...)
        indextype.lists = append(indextype.lists,$2.lists...)
        $$ = indextype
        depthCheck(yylex, max(len($$.types), len($$.lists)))
    }

INDEX_TYPE:
    IDENT INDEXLIST INDEX_LIST
    {
        $$ = &IndexType{
            types: []string{$1},
            lists: [][]string{$3},
        }
    }
    |
    FIELD INDEXLIST INDEX_LIST
    {
        $$ = &IndexType{
            types: []string{"field"},
            lists: [][]string{$3},
        }
    }

INDEX_LIST:
    IDENT
    {
        $$ = []string{$1}
    }
    |INDEX_LIST COMMA IDENT
    {

        $$ = append($1, $3)
        depthCheck(yylex, len($$))
    }

TYPE_CLAUSE:
    TYPE IDENT
    {
        shardType := strings.ToLower($2)
        if shardType != "hash" && shardType != "range" {
            yylex.Error("expect HASH or RANGE for TYPE")
            return 1
        }
        $$ = shardType
    }
    |
    {
        $$ = "hash"
    }

PRIMARYKEY_LIST:
    PRIMARYKEY INDEX_LIST
    {
        $$ = $2
    }

SORTKEY_LIST:
    SORTKEY INDEX_LIST
    {
        $$ = $2
    }

MEASUREMENT_PROPERTYS_LIST:
    PROPERTY MEASUREMENT_PROPERTYS
    {
        $$ = $2
    }
    |
    PROPERTY
    {
        $$ = nil
    }

MEASUREMENT_PROPERTYS:
    MEASUREMENT_PROPERTYS COMMA MEASUREMENT_PROPERTY
    {
        m := $1
        m[0] = append(m[0], $3[0]...)
        m[1] = append(m[1], $3[1]...)
        $$ = m
        depthCheck(yylex, max(len(m[0]), len(m[1])))
    }
    |
    MEASUREMENT_PROPERTY
    {
        $$ = $1
    }

MEASUREMENT_PROPERTY:
    STRING_TYPE EQ STRING_TYPE
    {
        $$ = [][]string{{$1},{$3}}
    }
    |
    STRING_TYPE EQ INTEGER
    {
        $$ = [][]string{{$1},{fmt.Sprintf("%d",$3)}}
    }

SHARDKEYLIST:
    SHARD_KEY
    {
        $$ = []string{$1}
    }
    |SHARDKEYLIST COMMA SHARD_KEY
    {
        $$ = append($1,$3)
        depthCheck(yylex, len($$))
    }
SHARD_KEY:
    IDENT
    {
        $$ = $1
    }

DROP_SHARD_STATEMENT:
    DROP SHARD INTEGER
    {
        stmt := &DropShardStatement{}
        stmt.ID = uint64($3)
        $$ = stmt
    }

SET_PASSWORD_USER_STATEMENT:
    SET PASSWORD FOR IDENT EQ STRING
    {
        stmt := &SetPasswordUserStatement{}
        stmt.Name = $4
        stmt.Password = $6
        $$ = stmt
    }



SHOW_GRANTS_FOR_USER_STATEMENT:
    SHOW GRANTS FOR IDENT
    {
        stmt := &ShowGrantsForUserStatement{}
        stmt.Name = $4
        $$ = stmt
    }

SHOW_MEASUREMENT_CARDINALITY_STATEMENT:
    SHOW MEASUREMENT EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowMeasurementCardinalityStatement{
            Database: $5,
            Exact: true,
            Sources: $6.sources,
            Condition: $7,
            Dimensions: $8.dims,
            Limit: $9[0],
            Offset: $9[1],
            depth: depthCheck(yylex, 1 + maxDepth($6, $7, $8)),
        }
    }
    |SHOW MEASUREMENT EXACT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowMeasurementCardinalityStatement{
            Database: $5,
            Exact: true,
            Condition: $6,
            Dimensions: $7.dims,
            Limit: $8[0],
            Offset: $8[1],
            depth: depthCheck(yylex, 1 + maxDepth($6, $7)),
        }
    }
    |SHOW MEASUREMENT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowMeasurementCardinalityStatement{
            Database: $4,
            Exact: false,
            Sources: $5.sources,
            Condition: $6,
            Dimensions: $7.dims,
            Limit: $8[0],
            Offset: $8[1],
            depth: depthCheck(yylex, 1 + maxDepth($5, $6, $7)),
        }
    }
    |SHOW MEASUREMENT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowMeasurementCardinalityStatement{
            Database: $4,
            Exact: false,
            Condition: $5,
            Dimensions: $6.dims,
            Limit: $7[0],
            Offset: $7[1],
            depth: depthCheck(yylex, 1 + maxDepth($5, $6)),
        }
    }


SHOW_SERIES_CARDINALITY_STATEMENT:
    SHOW SERIES EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowSeriesCardinalityStatement{
            Database: $5,
            Exact: true,
            Sources: $6.sources,
            Condition: $7,
            Dimensions: $8.dims,
            Limit: $9[0],
            Offset: $9[1],
            depth: depthCheck(yylex, 1 + maxDepth($6, $7, $8)),
        }
    }
    |SHOW SERIES EXACT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowSeriesCardinalityStatement{
            Database: $5,
            Exact: true,
            Condition: $6,
            Dimensions: $7.dims,
            Limit: $8[0],
            Offset: $8[1],
            depth: depthCheck(yylex, 1 + maxDepth($6, $7)),
        }
    }
    |SHOW SERIES CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowSeriesCardinalityStatement{
            Database: $4,
            Exact: false,
            Sources: $5.sources,
            Condition: $6,
            Dimensions: $7.dims,
            Limit: $8[0],
            Offset: $8[1],
            depth: depthCheck(yylex, 1 + maxDepth($5, $6, $7)),
        }
    }
    |SHOW SERIES CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        $$ = &ShowSeriesCardinalityStatement{
            Database: $4,
            Exact: false,
            Condition: $5,
            Dimensions: $6.dims,
            Limit: $7[0],
            Offset: $7[1],
            depth: depthCheck(yylex, 1 + maxDepth($5, $6)),
        }
    }


SHOW_SHARDS_STATEMENT:
    SHOW SHARDS
    {
        stmt := &ShowShardsStatement{}
        $$ = stmt
    }
    | SHOW SHARDS FROM TABLE_CASE
    {
        stmt := &ShowShardsStatement{mstInfo: $4}
        $$ = stmt
    }


ALTER_SHARD_KEY_STATEMENT:
    ALTER MEASUREMENT TABLE_CASE WITH SHARDKEY SHARDKEYLIST TYPE_CLAUSE
    {
        stmt := &AlterShardKeyStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        stmt.ShardKey = $6
        sort.Strings(stmt.ShardKey)
        stmt.Type = $7
        $$ = stmt
    }
    |ALTER MEASUREMENT TABLE_CASE
    {
        stmt := &AlterShardKeyStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        stmt.Type = "hash"
        $$ = stmt
    }




SHOW_SHARD_GROUPS_STATEMENT:
    SHOW SHARD GROUPS
    {
        stmt := &ShowShardGroupsStatement{}
        $$ = stmt
    }

DROP_MEASUREMENT_STATEMENT:
    DROP MEASUREMENT IDENT
    {
        stmt := &DropMeasurementStatement{}
        stmt.Name = $3
        stmt.RpName = ""
        $$ = stmt
    }
    | DROP MEASUREMENT IDENT DOT IDENT
    {
        stmt := &DropMeasurementStatement{}
        stmt.Name = $5
        stmt.RpName = $3
        $$ = stmt
    }


CREATE_CONTINUOUS_QUERY_STATEMENT:
    CREATE CONTINUOUS QUERY IDENT ON IDENT SAMPLE_POLICY BEGIN SELECT_STATEMENT END
    {
    	stmt := &CreateContinuousQueryStatement{
    	    Name: $4,
    	    Database: $6,
    	    Source: $9.(*SelectStatement),
            depth: depthCheck(yylex, 1 + $9.Depth()),
    	}
    	if $7 != nil{
    	    stmt.ResampleEvery = $7.ResampleEvery
    	    stmt.ResampleFor = $7.ResampleFor
    	}
    	$$ = stmt
    }

SAMPLE_POLICY:
    RESAMPLE EVERY DURATIONVAL
    {
    	$$ = &cqSamplePolicyInfo{
    	    ResampleEvery:$3,
    	}
    }
    |RESAMPLE FOR DURATIONVAL
    {
    	$$ = &cqSamplePolicyInfo{
    	    ResampleFor:$3,
    	}
    }
    |RESAMPLE EVERY DURATIONVAL FOR DURATIONVAL
    {
    	$$ = &cqSamplePolicyInfo{
    	    ResampleEvery:$3,
    	    ResampleFor:$5,
    	}
    }
    |
    {
    	$$ = nil
    }

SHOW_CONTINUOUS_QUERIES_STATEMENT:
    SHOW CONTINUOUS QUERIES
    {
        $$ = &ShowContinuousQueriesStatement{}
    }

DROP_CONTINUOUS_QUERY_STATEMENT:
    DROP CONTINUOUS QUERY IDENT ON IDENT
    {
    	$$ = &DropContinuousQueryStatement{
    	    Name: $4,
    	    Database: $6,
    	}
    }
CREATE_DOWNSAMPLE_STATEMENT:
    CREATE DOWNSAMPLE ON IDENT LPAREN DOWNSAMPLE_CALLS RPAREN WITH DOWNSAMPLE_INTERVALS
    {
    	stmt := $9.(*CreateDownSampleStatement)
    	stmt.RpName = $4
    	stmt.Ops = $6.fields
        stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + $6.Depth()))
    	$$ = stmt
    }
    |CREATE DOWNSAMPLE ON IDENT DOT IDENT LPAREN DOWNSAMPLE_CALLS RPAREN WITH DOWNSAMPLE_INTERVALS
    {
    	stmt := $11.(*CreateDownSampleStatement)
    	stmt.RpName = $6
    	stmt.DbName = $4
    	stmt.Ops = $8.fields
        stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + $8.Depth()))
    	$$ = stmt
    }
    |CREATE DOWNSAMPLE LPAREN DOWNSAMPLE_CALLS RPAREN WITH DOWNSAMPLE_INTERVALS
    {
    	stmt := $7.(*CreateDownSampleStatement)
    	stmt.Ops = $4.fields
        stmt.depth = depthCheck(yylex, max(stmt.depth, 1 + $4.Depth()))
    	$$ = stmt
    }

DOWNSAMPLE_CALLS:
    DOWNSAMPLE_CALL
    {
        $$ = fieldsList{fields:[]*Field{$1}, depth: depthCheck(yylex, 1 + $1.Depth())}
    }
    | DOWNSAMPLE_CALLS COMMA DOWNSAMPLE_CALL
    {
        fl := $1
        fl.fields = append(fl.fields, $3)
        fl.depth = depthCheck(yylex, max(fl.depth, len(fl.fields) + $3.Depth()))
        $$ = fl
    }

DOWNSAMPLE_CALL:
    COLUMN_CALL
    {
        $$ = &Field{Expr: $1, depth: depthCheck(yylex, 1+$1.Depth())}
    }


DROP_DOWNSAMPLE_STATEMENT:
    DROP DOWNSAMPLE ON IDENT
    {
    	$$ = &DropDownSampleStatement{
    	    RpName: $4,
    	}
    }
    |DROP DOWNSAMPLE ON IDENT DOT IDENT
    {
    	$$ = &DropDownSampleStatement{
    	    DbName: $4,
    	    RpName: $6,
    	}
    }
    |DROP DOWNSAMPLES
    {
    	$$ = &DropDownSampleStatement{
    	    DropAll: true,
    	}
    }
    |DROP DOWNSAMPLES ON IDENT
    {
    	$$ = &DropDownSampleStatement{
    	    DbName: $4,
    	    DropAll: true,
    	}
    }

SHOW_DOWNSAMPLE_STATEMENT:
    SHOW DOWNSAMPLES
    {
    	$$ = &ShowDownSampleStatement{}
    }
    |SHOW DOWNSAMPLES ON IDENT
    {
    	$$ = &ShowDownSampleStatement{
    	    DbName: $4,
    	}
    }

DOWNSAMPLE_INTERVALS:
    DURATION DURATIONVAL SAMPLEINTERVAL LPAREN DURATIONVALS RPAREN TIMEINTERVAL LPAREN DURATIONVALS RPAREN
    {
	$$ = &CreateDownSampleStatement{
	    Duration: $2,
	    SampleInterval: $5,
	    TimeInterval: $9,
            depth: depthCheck(yylex, 1 + max(len($5), len($9))),
	}
    }

DURATIONVALS:
    DURATIONVAL
    {
    	$$ = []time.Duration{$1}
    }
    |DURATIONVALS COMMA DURATIONVAL
    {
    	$$ = append($1, $3)
        depthCheck(yylex, len($$))
    }

CREATE_STREAM_STATEMENT:
    CREATE STREAM STRING_TYPE INTO TABLE_NAME_WITH_OPTION ON SELECT_STATEMENT DELAY_CLAUSE_OPT
    {
        $5.IsTarget = true
        stmt := &CreateStreamStatement{
            Name: $3,
            Target: &Target{
                Measurement: $5,
                depth: depthCheck(yylex, 2),
            },
            Query: $7,
            Delay: $8,
            depth: depthCheck(yylex, 1 + max(3, $7.Depth())),
        }
        $$ = stmt
    }

DELAY_CLAUSE_OPT:
    {
        $$ = time.Duration(0)
    }
    | DELAY DURATIONVAL
    {
        $$ = $2
    }

SHOW_STREAM_STATEMENT:
    SHOW STREAMS
    {
        $$ = &ShowStreamsStatement{}
    }
    |SHOW STREAMS ON STRING_TYPE
    {
        $$ = &ShowStreamsStatement{Database:$4}
    }

DROP_STREAM_STATEMENT:
    DROP STREAM STRING_TYPE
    {
    	$$ = &DropStreamsStatement{Name: $3}
    }
SHOW_QUERIES_STATEMENT:
    SHOW QUERIES
    {
        $$ = &ShowQueriesStatement{}
    }
KILL_QUERY_STATEMENT:
    KILL QUERY INTEGER
    {
        $$ = &KillQueryStatement{QueryID: uint64($3)}
    }

ALL_DESTINATION:
    STRING_TYPE
    {
        $$ = []string{$1}
    }
    |ALL_DESTINATION COMMA STRING_TYPE
    {
        $$ = append($1, $3)
        depthCheck(yylex, len($$))
    }

SUBSCRIPTION_TYPE:
    ALL
    {
        $$ = "ALL"
    }
    |ANY
    {
        $$ = "ANY"
    }

CREATE_SUBSCRIPTION_STATEMENT:
    CREATE SUBSCRIPTION STRING_TYPE ON STRING_TYPE DOT STRING_TYPE DESTINATIONS SUBSCRIPTION_TYPE ALL_DESTINATION
    {
        $$ = &CreateSubscriptionStatement{Name : $3, Database : $5, RetentionPolicy : $7, Destinations : $10, Mode : $9}
    }
    |CREATE SUBSCRIPTION STRING_TYPE ON STRING_TYPE DESTINATIONS SUBSCRIPTION_TYPE ALL_DESTINATION
    {
        $$ = &CreateSubscriptionStatement{Name : $3, Database : $5, RetentionPolicy : "", Destinations : $8, Mode : $7}
    }

SHOW_SUBSCRIPTION_STATEMENT:
    SHOW SUBSCRIPTIONS
    {
        $$ = &ShowSubscriptionsStatement{}
    }

DROP_SUBSCRIPTION_STATEMENT:
    DROP ALL SUBSCRIPTIONS
    {
        $$ = &DropSubscriptionStatement{Name : "", Database : "", RetentionPolicy : ""}
    }
    |DROP ALL SUBSCRIPTIONS ON STRING_TYPE
    {
        $$ = &DropSubscriptionStatement{Name : "", Database : $5, RetentionPolicy : ""}
    }
    |DROP SUBSCRIPTION STRING_TYPE ON STRING_TYPE DOT STRING_TYPE
    {
        $$ = &DropSubscriptionStatement{Name : $3, Database : $5, RetentionPolicy : $7}
    }
    |DROP SUBSCRIPTION STRING_TYPE ON STRING_TYPE
    {
        $$ = &DropSubscriptionStatement{Name : $3, Database : $5, RetentionPolicy : ""}
    }

SHOW_CONFIGS_STATEMENT:
    SHOW CONFIGS
    {
        stmt := &ShowConfigsStatement{}
        $$ = stmt
    }

SET_CONFIG_STATEMENT:
    SET CONFIG IDENT STRING_TYPE EQ STRING_TYPE
    {
        stmt := &SetConfigStatement{}
        stmt.Component = $3
        stmt.Key = $4
        stmt.Value = $6
        $$ = stmt
    }
    |SET CONFIG IDENT STRING_TYPE EQ INTEGER
    {
        stmt := &SetConfigStatement{}
        stmt.Component = $3
        stmt.Key = $4
        stmt.Value = $6
        $$ = stmt
    }
    |SET CONFIG IDENT STRING_TYPE EQ NUMBER
    {
        stmt := &SetConfigStatement{}
        stmt.Component = $3
        stmt.Key = $4
        stmt.Value = $6
        $$ = stmt
    }
    |SET CONFIG IDENT STRING_TYPE EQ TRUE
    {
        stmt := &SetConfigStatement{}
        stmt.Component = $3
        stmt.Key = $4
        stmt.Value = $6
        $$ = stmt
    }
    |SET CONFIG IDENT STRING_TYPE EQ FALSE
    {
        stmt := &SetConfigStatement{}
        stmt.Component = $3
        stmt.Key = $4
        stmt.Value = $6
        $$ = stmt
    }

SHOW_CLUSTER_STATEMENT:
    SHOW CLUSTER
     {
          stmt := &ShowClusterStatement{}
          stmt.NodeID = 0
          $$ = stmt
     }
     |SHOW CLUSTER WHERE IDENT EQ IDENT
     {
          stmt := &ShowClusterStatement{}
          stmt.NodeID = 0
	  if strings.ToLower($4) == "nodetype"{
	      stmt.NodeType = $6
	  } else {
	      yylex.Error("Invalid where clause")
	  }
	  $$ = stmt
     }
     |SHOW CLUSTER WHERE IDENT EQ INTEGER
     {
         stmt := &ShowClusterStatement{}
         if strings.ToLower($4) == "nodeid"{
	     stmt.NodeID = $6
	 } else {
	     yylex.Error("Invalid where clause")
	 }
	 $$ = stmt
     }
     |SHOW CLUSTER WHERE IDENT EQ INTEGER AND IDENT EQ IDENT
     {
         stmt := &ShowClusterStatement{}
         if strings.ToLower($4) == "nodeid"{
             stmt.NodeID = $6
         } else {
             yylex.Error("Invalid where clause")
         }
         if strings.ToLower($8) == "nodetype"{
             stmt.NodeType = $10
	 } else {
	     yylex.Error("Invalid where clause")
	 }
	 $$ = stmt
     }
     |SHOW CLUSTER WHERE IDENT EQ IDENT AND IDENT EQ INTEGER
     {
         stmt := &ShowClusterStatement{}
         if strings.ToLower($4) == "nodetype"{
             stmt.NodeType = $6
         } else {
             yylex.Error("Invalid where clause")
         }
         if strings.ToLower($8) == "nodeid"{
             stmt.NodeID = $10
	 } else {
	     yylex.Error("Invalid where clause")
	 }
	 $$ = stmt
      }

CTE_CLAUSES:
    CTE_CLAUSE
    {
        $$ = ctesList{ctes: []*CTE{$1}, depth: depthCheck(yylex, 2 + $1.Depth())}
    }
    |CTE_CLAUSES COMMA CTE_CLAUSE
    {
        cl := $1
        cl.ctes = append(cl.ctes, $3)
        cl.depth = depthCheck(yylex, max(cl.depth, len(cl.ctes) + 1 + $3.Depth()))
        $$ = cl
    }

CTE_CLAUSE:
    STRING_TYPE AS SELECT_WITH_PARENS
    {
        cte := &CTE{
            Alias: $1,
            depth: depthCheck(yylex, 1),
        }
        if selectStatement, ok := $3.(*SelectStatement); ok {
            cte.Query = selectStatement
            cte.depth = depthCheck(yylex, max(cte.depth, 1 + cte.Query.Depth()))
        } else if graphStatement, ok := $3.(*GraphStatement); ok {
            cte.GraphQuery = graphStatement
            cte.depth = depthCheck(yylex, max(cte.depth, 1 + cte.GraphQuery.Depth()))
        } else {
            yylex.Error("Invalid stmt clause in cte.query")
        }
        $$ = cte
    }

%%
