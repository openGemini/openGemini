%{
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

func setParseTree(yylex interface{}, stmts Statements) {
    for _,stmt :=range stmts{
        yylex.(*YyParser).Query.Statements = append(yylex.(*YyParser).Query.Statements, stmt)
    }
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


%}

%union{
    stmt                Statement
    stmts               Statements
    str                 string
    query               Query
    field               *Field
    fields              Fields
    sources             Sources
    source              Source
    sortfs              SortFields
    sortf               *SortField
    ment                *Measurement
    subQuery            *SubQuery
    dimens              Dimensions
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
}

%token <str>    FROM MEASUREMENT INTO ON SELECT WHERE AS GROUP BY ORDER LIMIT OFFSET SLIMIT SOFFSET SHOW CREATE FULL PRIVILEGES OUTER JOIN
                TO IN NOT EXISTS REVOKE FILL DELETE WITH ENGINETYPE COLUMNSTORE TSSTORE ALL ANY PASSWORD NAME REPLICANUM ALTER USER USERS
                DATABASES DATABASE MEASUREMENTS RETENTION POLICIES POLICY DURATION DEFAULT SHARD INDEX GRANT HOT WARM TYPE SET FOR GRANTS
                REPLICATION SERIES DROP CASE WHEN THEN ELSE BEGIN END TRUE FALSE TAG ATTRIBUTE FIELD KEYS VALUES KEY EXPLAIN ANALYZE EXACT CARDINALITY SHARDKEY
                PRIMARYKEY SORTKEY PROPERTY COMPACT
                CONTINUOUS DIAGNOSTICS QUERIES QUERIE SHARDS STATS SUBSCRIPTIONS SUBSCRIPTION GROUPS INDEXTYPE INDEXLIST SEGMENT KILL
                EVERY RESAMPLE
                DOWNSAMPLE DOWNSAMPLES SAMPLEINTERVAL TIMEINTERVAL STREAM DELAY STREAMS
                QUERY PARTITION
                TOKEN TOKENIZERS MATCH LIKE MATCHPHRASE CONFIG CONFIGS CLUSTER
                REPLICAS DETAIL DESTINATIONS
                SCHEMA INDEXES AUTO EXCEPT
%token <bool>   DESC ASC
%token <str>    COMMA SEMICOLON LPAREN RPAREN REGEX
%token <int>    EQ NEQ LT LTE GT GTE DOT DOUBLECOLON NEQREGEX EQREGEX
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
%right UMINUS

%type <stmt>                        STATEMENT SHOW_DATABASES_STATEMENT CREATE_DATABASE_STATEMENT WITH_CLAUSES CREATE_USER_STATEMENT
                                    SELECT_STATEMENT SHOW_MEASUREMENTS_STATEMENT SHOW_RETENTION_POLICIES_STATEMENT
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
                                    CREATE_SUBSCRIPTION_STATEMENT SHOW_SUBSCRIPTION_STATEMENT DROP_SUBSCRIPTION_STATEMENT
%type <fields>                      COLUMN_CLAUSES IDENTS
%type <field>                       COLUMN_CLAUSE
%type <stmts>                       ALL_QUERIES ALL_QUERY
%type <sources>                     FROM_CLAUSE TABLE_NAMES SUBQUERY_CLAUSE INTO_CLAUSE
%type <source>                      JOIN_CLAUSE
%type <ment>                        TABLE_OPTION  TABLE_NAME_WITH_OPTION TABLE_CASE MEASUREMENT_WITH
%type <expr>                        WHERE_CLAUSE OR_CONDITION AND_CONDITION CONDITION OPERATION_EQUAL COLUMN_VAREF COLUMN CONDITION_COLUMN TAG_KEYS
                                    CASE_WHEN_CASE CASE_WHEN_CASES
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
%type <indexType>                   INDEX_TYPE INDEX_TYPES CMOPTION_INDEXTYPE_TS CMOPTION_INDEXTYPE_CS
%type <cqsp>                        SAMPLE_POLICY
%type <tdurs>                       DURATIONVALS
%type <cqsp>                        SAMPLE_POLICY
%type <int64>                       INTEGERPARA CMOPTION_SHARDNUM
%type <bool>                        ALLOW_TAG_ARRAY
%type <fieldOption>                 FIELD_OPTION FIELD_COLUMN
%type <fieldOptions>                FIELD_OPTIONS

%type <databasePolicy>              DATABASE_POLICY
%type <cmOption>                    CMOPTIONS_TS CMOPTIONS_CS
%type <str>                         CMOPTION_ENGINETYPE_TS CMOPTION_ENGINETYPE_CS

%%

ALL_QUERIES:
        ALL_QUERY
        {
            setParseTree(yylex, $1)
        }

ALL_QUERY:
    STATEMENT
    {
        $$ = []Statement{$1}
    }
    |ALL_QUERY SEMICOLON
    {
        if len($1) >= 1{
            $$ = $1
        }else{
            yylex.Error("excrescent semicolo")
        }
    }
    |ALL_QUERY SEMICOLON  STATEMENT
    {
        $$ = append($1,$3)
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
    SELECT COLUMN_CLAUSES INTO_CLAUSE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE EXCEPT_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &SelectStatement{}
        stmt.Fields = $2
        stmt.Sources = $4
        stmt.Dimensions = $6
        stmt.ExceptDimensions = $7
        stmt.Condition = $5
        stmt.SortFields = $9
        stmt.Limit = $10[0]
        stmt.Offset = $10[1]
        stmt.SLimit = $10[2]
        stmt.SOffset = $10[3]

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
        if len($3) > 1{
            yylex.Error("into clause only support one measurement")
        }else if len($3) == 1{
            mst,ok := $3[0].(*Measurement)
            if !ok{
                 yylex.Error("into clause only support measurement clause")
            }
            mst.IsTarget = true
            stmt.Target = &Target{
            	Measurement: mst,
            }
        }
        $$ = stmt
    }
    |SELECT HINT COLUMN_CLAUSES INTO_CLAUSE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE EXCEPT_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &SelectStatement{}
        stmt.Hints = $2
        stmt.Fields = $3
        stmt.Sources = $5
        stmt.Dimensions = $7
        stmt.ExceptDimensions = $8
        stmt.Condition = $6
        stmt.SortFields = $10
        stmt.Limit = $11[0]
        stmt.Offset = $11[1]
        stmt.SLimit = $11[2]
        stmt.SOffset = $11[3]

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
        if len($4) > 1{
            yylex.Error("into clause only support one measurement")
        }else if len($4) == 1{
            mst,ok := $4[0].(*Measurement)
            if !ok{
                 yylex.Error("into clause only support measurement clause")
            }
            mst.IsTarget = true
            stmt.Target = &Target{
            	Measurement: mst,
            }
        }
        $$ = stmt
    }
    |SELECT COLUMN_CLAUSES WHERE_CLAUSE GROUP_BY_CLAUSE EXCEPT_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &SelectStatement{}
        stmt.Fields = $2
        stmt.Dimensions = $4
        stmt.ExceptDimensions = $5
        stmt.Condition = $3
        stmt.SortFields = $7
        stmt.Limit = $8[0]
        stmt.Offset = $8[1]
        stmt.SLimit = $8[2]
        stmt.SOffset = $8[3]

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


COLUMN_CLAUSES:
    COLUMN_CLAUSE
    {
        $$ = []*Field{$1}
    }
    |COLUMN_CLAUSE COMMA COLUMN_CLAUSES
    {
        $$ = append([]*Field{$1},$3...)
    }

COLUMN_CLAUSE:
    MUL
    {
        $$ = &Field{Expr:&Wildcard{Type:Token($1)}}
    }
    |MUL DOUBLECOLON TAG
    {
    	$$ = &Field{Expr:&Wildcard{Type:TAG}}
    }
    |MUL DOUBLECOLON FIELD
    {
    	$$ = &Field{Expr:&Wildcard{Type:FIELD}}
    }
    |COLUMN
    {
        $$ = &Field{Expr: $1}
    }
    |COLUMN AS IDENT
    {
        $$ = &Field{Expr: $1, Alias:$3}
    }
    |COLUMN AS STRING
    {
        $$ = &Field{Expr: $1, Alias:$3}
    }

CASE_WHEN_CASES:
    CASE_WHEN_CASE
    {
    	$$ = $1
    }
    |CASE_WHEN_CASE CASE_WHEN_CASES
    {
    	c := $1.(*CaseWhenExpr)
    	c.Conditions = append(c.Conditions, $2.(*CaseWhenExpr).Conditions...)
    	c.Assigners = append(c.Assigners, $2.(*CaseWhenExpr).Assigners...)
        $$ = c
    }

CASE_WHEN_CASE:
    WHEN CONDITION THEN COLUMN
    {
    	c := &CaseWhenExpr{}
    	c.Conditions = []Expr{$2}
    	c.Assigners = []Expr{$4}
    	$$ = c
    }

IDENTS:
   IDENT
   {
       $$ = []*Field{&Field{Expr:&VarRef{Val:$1}}}
   }
   |IDENT COMMA IDENTS
   {
       $$ = append([]*Field{&Field{Expr:&VarRef{Val:$1}}},$3...)
   }

COLUMN:
    COLUMN MUL COLUMN
    {
        $$ = &BinaryExpr{Op:Token(MUL), LHS:$1, RHS:$3}
    }
    |COLUMN DIV COLUMN
    {
        $$ = &BinaryExpr{Op:Token(DIV), LHS:$1, RHS:$3}
    }
    |COLUMN ADD COLUMN
    {
        $$ = &BinaryExpr{Op:Token(ADD), LHS:$1, RHS:$3}
    }
    |COLUMN SUB COLUMN
    {
        $$ = &BinaryExpr{Op:Token(SUB), LHS:$1, RHS:$3}
    }
    |COLUMN BITWISE_XOR COLUMN
    {
        $$ = &BinaryExpr{Op:Token(BITWISE_XOR), LHS:$1, RHS:$3}
    }
    |COLUMN MOD COLUMN
    {
        $$ = &BinaryExpr{Op:Token(MOD), LHS:$1, RHS:$3}
    }
    |COLUMN BITWISE_AND COLUMN
    {
        $$ = &BinaryExpr{Op:Token(BITWISE_AND), LHS:$1, RHS:$3}
    }
    |COLUMN BITWISE_OR COLUMN
    {
        $$ = &BinaryExpr{Op:Token(BITWISE_OR), LHS:$1, RHS:$3}
    }
    |LPAREN COLUMN RPAREN
    {
        $$ = &ParenExpr{Expr: $2}
    }
    |IDENT LPAREN COLUMN_CLAUSES RPAREN
    {
        if strings.ToLower($1) == "cast" {
            if len($3)!=1 {
                 yylex.Error("The cast format is incorrect.")
            } else {
                name := "Unknown"
                if strings.ToLower($3[0].Alias) == "bool" {
                    name = "cast_bool"
                }
                if strings.ToLower($3[0].Alias) == "float" {
                    name = "cast_float64"
                }
                if strings.ToLower($3[0].Alias) == "int" {
                    name = "cast_int64"
                }
                if strings.ToLower($3[0].Alias) == "string" {
                    name = "cast_string"
                }
                cols := &Call{Name: strings.ToLower(name), Args: []Expr{}}
                cols.Args = append(cols.Args, $3[0].Expr)
                $$ = cols
            }
        } else {
            cols := &Call{Name: strings.ToLower($1), Args: []Expr{}}
            for i := range $3 {
                cols.Args = append(cols.Args, $3[i].Expr)
            }
            $$ = cols
        }
    }
    |IDENT LPAREN RPAREN
    {
        cols := &Call{Name: strings.ToLower($1)}
        $$ = cols
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
        	$$ = &BinaryExpr{Op:Token(MUL), LHS:&IntegerLiteral{Val:-1}, RHS:$2}
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
    	$$ = c
    }
    |CASE IDENT CASE_WHEN_CASES ELSE IDENT END
    {
    	$$ = &VarRef{}
    }

INTO_CLAUSE:
    INTO TABLE_NAMES
    {
    	$$ = $2
    }
    |
    {
    	$$ = nil
    }

FROM_CLAUSE:
    FROM TABLE_NAMES
    {
        $$ = $2
    }

TABLE_NAMES:
    TABLE_NAME_WITH_OPTION
    {
        $$ = []Source{$1}
    }
    |TABLE_NAME_WITH_OPTION COMMA TABLE_NAMES
    {
        $$ = append([]Source{$1},$3...)
    }
    |SUBQUERY_CLAUSE
    {
    	$$ = $1

    }
    |SUBQUERY_CLAUSE COMMA TABLE_NAMES
    {
        $$ =  append($1,$3...)
    }
    |TABLE_NAME_WITH_OPTION AS IDENT
    {
    	$1.Alias = $3
        $$ = []Source{$1}
    }
    |TABLE_NAME_WITH_OPTION AS IDENT COMMA TABLE_NAMES
    {
    	$1.Alias = $3
        $$ = append([]Source{$1},$5...)
    }
    |JOIN_CLAUSE
    {
        $$ = []Source{$1}
    }

JOIN_CLAUSE:
    SUBQUERY_CLAUSE FULL JOIN TABLE_NAMES ON CONDITION
    {
        join := &Join{}
        if len($1) != 1 || len($4) != 1{
            yylex.Error("only support one query for join")
        }
        join.LSrc = $1[0]
        join.RSrc = $4[0]
        join.Condition = $6
        $$ = join
    }

SUBQUERY_CLAUSE:
    LPAREN ALL_QUERY RPAREN
    {
        all_subquerys:=  []Source{}
        for _,temp_stmt := range $2 {
            stmt,ok:= temp_stmt.(*SelectStatement)
            if !ok{
                yylex.Error("expexted SelectStatement")
            }
            build_SubQuery := &SubQuery{Statement: stmt}
            all_subquerys =append(all_subquerys,build_SubQuery)
        }
        $$ = all_subquerys
    }
    |LPAREN ALL_QUERY RPAREN AS IDENT
    {
        if len($2) != 1{
            yylex.Error("expexted SelectStatement length")
        }
        all_subquerys:=  []Source{}
        stmt,ok:= $2[0].(*SelectStatement)
        if !ok{
            yylex.Error("expexted SelectStatement")
        }
        build_SubQuery := &SubQuery{
            Statement: stmt,
            Alias: $5,
        }
        all_subquerys =append(all_subquerys,build_SubQuery)
        $$ = all_subquerys
    }
    |LPAREN SUBQUERY_CLAUSE RPAREN
    {
        $$ = $2
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
        $$ = nil
    }

EXCEPT_CLAUSE:
    EXCEPT DIMENSION_NAMES
    {
        $$ = $2
    }
    |
    {
        $$ = nil
    }

DIMENSION_NAMES:
    DIMENSION_NAME
    {
        $$ = []*Dimension{$1}
    }
    |DIMENSION_NAME COMMA DIMENSION_NAMES
    {
        $$ = append([]*Dimension{$1}, $3...)
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
        $$ = &Dimension{Expr:&VarRef{Val:$1}}
    }
    |STRING_TYPE DOUBLECOLON TAG
    {
        $$ = &Dimension{Expr:&VarRef{Val:$1}}
    }
    |IDENT LPAREN DURATIONVAL RPAREN
    {
    	if strings.ToLower($1) != "time"{
    	    yylex.Error("Invalid group by combination for no-time tag and time duration")
    	}

    	$$ = &Dimension{Expr:&Call{Name:"time", Args:[]Expr{&DurationLiteral{Val: $3}}}}
    }
    |IDENT LPAREN DURATIONVAL COMMA DURATIONVAL RPAREN
    {
        if strings.ToLower($1) != "time"{
                    yylex.Error("Invalid group by combination for no-time tag and time duration")
                }

        $$ = &Dimension{Expr:&Call{Name:"time", Args:[]Expr{&DurationLiteral{Val: $3},&DurationLiteral{Val: $5}}}}
    }
    |IDENT LPAREN DURATIONVAL COMMA SUB DURATIONVAL RPAREN
    {
        if strings.ToLower($1) != "time"{
                    yylex.Error("Invalid group by combination for no-time tag and time duration")
                }

        $$ = &Dimension{Expr:&Call{Name:"time", Args:[]Expr{&DurationLiteral{Val: $3},&DurationLiteral{Val: time.Duration(-$6)}}}}
    }
    |MUL
    {
        $$ = &Dimension{Expr:&Wildcard{Type:Token($1)}}
    }
    |MUL DOUBLECOLON TAG
    {
        $$ = &Dimension{Expr:&Wildcard{Type:Token($1)}}
    }
    |REGULAR_EXPRESSION
     {
        re, err := regexp.Compile($1)
        if err != nil {
            yylex.Error("Invalid regexprs")
        }
        $$ = &Dimension{Expr: &RegexLiteral{Val: re}}
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
         $$ = &BinaryExpr{Op:Token($2),LHS:$1,RHS:$3}
    }

OR_CONDITION:
    AND_CONDITION
    {
        $$ = $1
    }
    |CONDITION OR CONDITION
    {
        $$ = &BinaryExpr{Op:Token($2),LHS:$1,RHS:$3}
    }

CONDITION:
    OR_CONDITION
    {
        $$ = $1
    }
    |LPAREN CONDITION RPAREN
    {
    	$$ = &ParenExpr{Expr:$2}
    }
    |IDENT IN LPAREN COLUMN_CLAUSES RPAREN
    {
        ident := &VarRef{Val:$1}
    	var expr,e Expr
    	for i := range $4{
    	    expr = &BinaryExpr{LHS:ident, Op:Token(EQ), RHS:$4[i].Expr}
    	    if e == nil{
    	        e = expr
    	    }else{
    	        e = &BinaryExpr{LHS:e, Op:Token(OR), RHS:expr}
    	    }
    	}
    	$$ = e
    }
    |IDENT IN LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &InCondition{Stmt:$4.(*SelectStatement), Column: &VarRef{Val: $1}}
    }
    |EXISTS LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &BinaryExpr{}
    }
    |IDENT NOT IN LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &BinaryExpr{}
    }
    |IDENT NOT IN LPAREN IDENTS RPAREN
    {
    	$$ = &BinaryExpr{}
    }
    |NOT EXISTS LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &BinaryExpr{}
    }
    |MATCH LPAREN STRING_TYPE COMMA STRING_TYPE RPAREN
    {
    	$$ = &BinaryExpr{
    		LHS:  &VarRef{Val: $3},
    		RHS:  &StringLiteral{Val: $5},
    		Op: MATCH,
    	}
    }
    |MATCHPHRASE LPAREN STRING_TYPE COMMA STRING_TYPE RPAREN
    {
    	$$ = &BinaryExpr{
    		LHS:  &VarRef{Val: $3},
    		RHS:  &StringLiteral{Val: $5},
    		Op: MATCHPHRASE,
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
        $$ = &BinaryExpr{Op:Token($2),LHS:$1,RHS:$3}
    }

CONDITION_COLUMN:
    COLUMN
    {
    	$$ = $1
    }
    |LPAREN CONDITION RPAREN
    {
    	$$ = &ParenExpr{Expr:$2}
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
    |SORTFIELD COMMA SORTFIELDS
    {
        $$ = append([]*SortField{$1}, $3...)
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
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,PolicyDuration: &duration}
    }
    |REPLICATION INTEGER
    {
        if $2 < 1 || $2 % 2 == 0 {
            yylex.Error("REPLICATION must be an odd number")
        }
        replicaN := int($2)
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,Replication: &replicaN}
    }
    |NAME IDENT
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,PolicyName: $2}
    }
    |REPLICANUM INTEGER
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,ReplicaNum: uint32($2)}
    }
    |DEFAULT
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,rpdefault: true}
    }
    |SHARDKEY SHARDKEYLIST
    {
        if len($2) == 0 {
            yylex.Error("ShardKey should not be nil")
        }
        $$ = &Durations{ShardKey:$2,ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,rpdefault: false}
    }



SHOW_MEASUREMENTS_STATEMENT:
    SHOW MEASUREMENTS ON_DATABASE WITH MEASUREMENT MEASUREMENT_WITH WHERE_CLAUSE ORDER_CLAUSES OPTION_CLAUSES
    {
        sms := &ShowMeasurementsStatement{}
        sms.Database = $3
        sms.Source = $6
        sms.Condition = $7
        sms.SortFields = $8
        sms.Limit = $9[0]
        sms.Offset = $9[1]
        $$ = sms
    }
    |SHOW MEASUREMENTS ON_DATABASE WHERE_CLAUSE ORDER_CLAUSES OPTION_CLAUSES
     {
         sms := &ShowMeasurementsStatement{}
         sms.Database = $3
         sms.Condition = $4
         sms.SortFields = $5
         sms.Limit = $6[0]
         sms.Offset = $6[1]
         $$ = sms
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
        if $4 < 1 || $4 % 2 == 0 {
            yylex.Error("REPLICATION must be an odd number")
        }
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

        if $5.IndexGroupDuration==-1||$5.IndexGroupDuration==0{
           stmt.IndexGroupDuration = time.Duration(DefaultQueryTimeout)
        }else{
           stmt.IndexGroupDuration = $5.IndexGroupDuration
        }

    	$$ = stmt
    }
    |DURATION DURATIONVAL REPLICATION INTEGER
    {
    	stmt := &CreateRetentionPolicyStatement{}
    	stmt.Duration = $2
        if $4 < 1 || $4 % 2 == 0 {
            yylex.Error("REPLICATION must be an odd number")
        }
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

        if $1.IndexGroupDuration<0  || $2.IndexGroupDuration<0{
            if $2.IndexGroupDuration>=0 {
                $1.IndexGroupDuration = $2.IndexGroupDuration
            }
        }else{
            yylex.Error("Repeat Index Group Duration")
        }
        $$ = $1
    }


SHARD_HOT_WARM_INDEX_DURATION:
    SHARD DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: $3,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1}
    }
    |HOT DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: $3,WarmDuration: -1,IndexGroupDuration: -1}
    }
    |WARM DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: $3,IndexGroupDuration: -1}
    }
    |INDEX DURATION DURATIONVAL
    {
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: $3}
    }



SHOW_SERIES_STATEMENT:
    SHOW SERIES ON_DATABASE FROM_CLAUSE WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
    {
    	stmt := &ShowSeriesStatement{}
    	stmt.Database = $3
    	stmt.Sources = $4
    	stmt.Condition = $5
    	stmt.SortFields = $6
    	stmt.Limit = $7[0]
    	stmt.Offset = $7[1]
    	$$ = stmt
    }
    |SHOW SERIES ON_DATABASE WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
    {
    	stmt := &ShowSeriesStatement{}
    	stmt.Database = $3
    	stmt.Condition = $4
    	stmt.SortFields = $5
    	stmt.Limit = $6[0]
    	stmt.Offset = $6[1]
    	$$ = stmt
    }

SHOW_USERS_STATEMENT:
    SHOW USERS
    {
    	$$ = &ShowUsersStatement{}
    }

DROP_DATABASE_STATEMENT:
    DROP DATABASE IDENT
    {
    	stmt := &DropDatabaseStatement{}
    	stmt.Name = $3
    	$$ = stmt
    }

DROP_SERIES_STATEMENT:
    DROP SERIES FROM_CLAUSE WHERE_CLAUSE
    {
    	stmt := &DropSeriesStatement{}
    	stmt.Sources = $3
    	stmt.Condition = $4
    	$$ = stmt
    }
    |DROP SERIES WHERE_CLAUSE
    {
        stmt := &DropSeriesStatement{}
        stmt.Condition = $3
        $$ = stmt
    }

DELETE_SERIES_STATEMENT:
    DELETE FROM_CLAUSE WHERE_CLAUSE
    {
    	stmt := &DeleteSeriesStatement{}
    	stmt.Sources = $2
    	stmt.Condition = $3
    	$$ = stmt
    }
    |DELETE WHERE_CLAUSE
    {
        stmt := &DeleteSeriesStatement{}
        stmt.Condition = $2
        $$ = stmt
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
      stmt := &ShowTagKeysStatement{}
      stmt.Database = $4
      stmt.Sources = $5
      stmt.Condition = $6
      stmt.SortFields = $7
      stmt.Limit = $8[0]
      stmt.Offset = $8[1]
      stmt.SLimit = $8[2]
      stmt.SOffset = $8[3]
      $$ = stmt

  }
  |SHOW TAG KEYS ON_DATABASE WHERE_CLAUSE ORDER_CLAUSES OPTION_CLAUSES
  {
      stmt := &ShowTagKeysStatement{}
      stmt.Database = $4
      stmt.Condition = $5
      stmt.SortFields = $6
      stmt.Limit = $7[0]
      stmt.Offset = $7[1]
      stmt.SLimit = $7[2]
      stmt.SOffset = $7[3]
      $$ = stmt
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
      stmt := &ShowFieldKeysStatement{}
      stmt.Database = $4
      stmt.Sources = $5
      stmt.SortFields = $6
      stmt.Limit = $7[0]
      stmt.Offset = $7[1]
      $$ = stmt
  }
  |SHOW FIELD KEYS ON_DATABASE ORDER_CLAUSES LIMIT_OFFSET_OPTION
   {
       stmt := &ShowFieldKeysStatement{}
       stmt.Database = $4
       stmt.SortFields = $5
       stmt.Limit = $6[0]
       stmt.Offset = $6[1]
       $$ = stmt
   }


SHOW_TAG_VALUES_STATEMENT:
   SHOW TAG VALUES ON_DATABASE FROM_CLAUSE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
   {
       stmt := $8.(*ShowTagValuesStatement)
       stmt.TagKeyCondition = nil
       stmt.Database = $4
       stmt.Sources = $5
       stmt.Condition = $9
       stmt.SortFields = $10
       stmt.Limit = $11[0]
       stmt.Offset = $11[1]
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
       $$ = stmt
   }

TAG_VALUES_WITH:
  EQ TAG_KEYS
  {
      stmt := &ShowTagValuesStatement{}
      stmt.Op = EQ
      stmt.TagKeyExpr = $2.(*ListLiteral)
      $$ = stmt
  }
  |NEQ TAG_KEYS
  {
      stmt := &ShowTagValuesStatement{}
      stmt.Op = NEQ
      stmt.TagKeyExpr = $2.(*ListLiteral)
      $$ = stmt
  }
  |IN LPAREN TAG_KEYS RPAREN
  {
      stmt := &ShowTagValuesStatement{}
      stmt.Op = IN
      stmt.TagKeyExpr = $3.(*ListLiteral)
      $$ = stmt
  }
  |EQREGEX REGULAR_EXPRESSION
  {
       stmt := &ShowTagValuesStatement{}
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
       stmt := &ShowTagValuesStatement{}
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
  }

TAG_KEY:
  IDENT
  {
      $$ = $1
  }



EXPLAIN_STATEMENT:
    EXPLAIN ANALYZE SELECT_STATEMENT
    {
        stmt := &ExplainStatement{}
        stmt.Statement = $3.(*SelectStatement)
        stmt.Analyze = true
        $$ = stmt
    }
    |EXPLAIN SELECT_STATEMENT
    {
        stmt := &ExplainStatement{}
        stmt.Statement = $2.(*SelectStatement)
        stmt.Analyze = false
        $$ = stmt
    }


SHOW_TAG_KEY_CARDINALITY_STATEMENT:
    SHOW TAG KEY EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowTagKeyCardinalityStatement{}
        stmt.Database = $6
        stmt.Exact = true
        stmt.Sources = $7
        stmt.Condition = $8
        stmt.Dimensions = $9
        stmt.Limit = $10[0]
        stmt.Offset = $10[1]
        $$ = stmt
    }
    |SHOW TAG KEY EXACT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowTagKeyCardinalityStatement{}
        stmt.Database = $6
        stmt.Exact = true
        stmt.Condition = $7
        stmt.Dimensions = $8
        stmt.Limit = $9[0]
        stmt.Offset = $9[1]
        $$ = stmt
    }
    |SHOW TAG KEY CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         stmt := &ShowTagKeyCardinalityStatement{}
         stmt.Database = $5
         stmt.Exact = false
         stmt.Sources = $6
         stmt.Condition = $7
         stmt.Dimensions = $8
         stmt.Limit = $9[0]
         stmt.Offset = $9[1]
         $$ = stmt
    }
    |SHOW TAG KEY CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         stmt := &ShowTagKeyCardinalityStatement{}
         stmt.Database = $5
         stmt.Exact = true
         stmt.Condition = $6
         stmt.Dimensions = $7
         stmt.Limit = $8[0]
         stmt.Offset = $8[1]
         $$ = stmt
    }




SHOW_TAG_VALUES_CARDINALITY_STATEMENT:
    SHOW TAG VALUES EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowTagValuesCardinalityStatement{}
        stmt.Database = $6
        stmt.Exact = true
        stmt.Sources = $7
        stmt_temp := $10.(*ShowTagValuesStatement)
        stmt.Op = stmt_temp.Op
        stmt.TagKeyExpr = stmt_temp.TagKeyExpr
        stmt.Condition = $11
        stmt.Dimensions = $12
        stmt.Limit = $13[0]
        stmt.Offset = $13[1]
        stmt.TagKeyCondition = nil
        $$ = stmt

    }
    |SHOW TAG VALUES EXACT CARDINALITY ON_DATABASE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         stmt := &ShowTagValuesCardinalityStatement{}
         stmt.Database = $6
         stmt.Exact = true
         stmt_temp := $9.(*ShowTagValuesStatement)
         stmt.Op = stmt_temp.Op
         stmt.TagKeyExpr = stmt_temp.TagKeyExpr
         stmt.Condition = $10
         stmt.Dimensions = $11
         stmt.Limit = $12[0]
         stmt.Offset = $12[1]
         stmt.TagKeyCondition = nil
         $$ = stmt
    }
    |SHOW TAG VALUES CARDINALITY ON_DATABASE FROM_CLAUSE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowTagValuesCardinalityStatement{}
        stmt.Database = $5
        stmt.Exact = false
        stmt.Sources = $6
        stmt_temp := $9.(*ShowTagValuesStatement)
        stmt.Op = stmt_temp.Op
        stmt.TagKeyExpr = stmt_temp.TagKeyExpr
        stmt.Condition = $10
        stmt.Dimensions = $11
        stmt.Limit = $12[0]
        stmt.Offset = $12[1]
        stmt.TagKeyCondition = nil
        $$ = stmt

    }
    |SHOW TAG VALUES CARDINALITY ON_DATABASE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         stmt := &ShowTagValuesCardinalityStatement{}
         stmt.Database = $5
         stmt.Exact = true
         stmt_temp := $8.(*ShowTagValuesStatement)
         stmt.Op = stmt_temp.Op
         stmt.TagKeyExpr = stmt_temp.TagKeyExpr
         stmt.Condition = $9
         stmt.Dimensions = $10
         stmt.Limit = $11[0]
         stmt.Offset = $11[1]
         stmt.TagKeyCondition = nil
         $$ = stmt
    }


SHOW_FIELD_KEY_CARDINALITY_STATEMENT:
    SHOW FIELD KEY EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowFieldKeyCardinalityStatement{}
        stmt.Database = $6
        stmt.Exact = true
        stmt.Sources = $7
        stmt.Condition = $8
        stmt.Dimensions = $9
        stmt.Limit = $10[0]
        stmt.Offset = $10[1]
        $$ = stmt
    }
    |SHOW FIELD KEY EXACT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowFieldKeyCardinalityStatement{}
        stmt.Database = $6
        stmt.Exact = true
        stmt.Condition = $7
        stmt.Dimensions = $8
        stmt.Limit = $9[0]
        stmt.Offset = $9[1]
        $$ = stmt
    }
    |SHOW FIELD KEY CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         stmt := &ShowFieldKeyCardinalityStatement{}
         stmt.Database = $5
         stmt.Exact = false
         stmt.Sources = $6
         stmt.Condition = $7
         stmt.Dimensions = $8
         stmt.Limit = $9[0]
         stmt.Offset = $9[1]
         $$ = stmt
    }
    |SHOW FIELD KEY CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         stmt := &ShowFieldKeyCardinalityStatement{}
         stmt.Database = $5
         stmt.Exact = true
         stmt.Condition = $6
         stmt.Dimensions = $7
         stmt.Limit = $8[0]
         stmt.Offset = $8[1]
         $$ = stmt
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

        $$ = stmt
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
    }

CMOPTIONS_TS:
    {
        option := &CreateMeasurementStatementOption{}
        option.Type = "hash"
        option.EngineType = "tsstore"
        $$ = option
    }
    | WITH CMOPTION_ENGINETYPE_TS CMOPTION_INDEXTYPE_TS CMOPTION_SHARDKEY CMOPTION_SHARDNUM TYPE_CLAUSE
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

INDEX_TYPES:
    INDEX_TYPE INDEX_TYPES
    {
        indextype := $1
        if $2 != nil{
            indextype.types = append(indextype.types,$2.types...)
            indextype.lists = append(indextype.lists,$2.lists...)
        }
        $$ = indextype
    }
    |
    {
        $$ = nil
    }

INDEX_LIST:
    IDENT
    {
        $$ = []string{$1}
    }
    |IDENT COMMA INDEX_LIST
    {

        $$ = append([]string{$1}, $3...)
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

MEASUREMENT_PROPERTYS:
    MEASUREMENT_PROPERTY COMMA MEASUREMENT_PROPERTYS
    {
        m := $1
        if $3 != nil{
            m[0] = append(m[0], $3[0]...)
            m[1] = append(m[1], $3[1]...)
        }
        $$ = m
    }
    |
    MEASUREMENT_PROPERTY
    {
        $$ = $1
    }

MEASUREMENT_PROPERTYS_LIST:
    PROPERTY MEASUREMENT_PROPERTYS
    {
        $$ = $2
    }

MEASUREMENT_PROPERTY:
    STRING_TYPE EQ STRING_TYPE
    {
        $$ = [][]string{{$1},{$3}}
    }
    |STRING_TYPE EQ INTEGER
    {
        $$ = [][]string{{$1},{fmt.Sprintf("%d",$3)}}
    }
    |
    {
        $$ = nil
    }

SHARDKEYLIST:
    SHARD_KEY
    {
        $$ = []string{$1}
    }
    |SHARDKEYLIST COMMA SHARD_KEY
    {
        $$ = append($1,$3)
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
        stmt := &ShowMeasurementCardinalityStatement{}
        stmt.Database = $5
        stmt.Exact = true
        stmt.Sources = $6
        stmt.Condition = $7
        stmt.Dimensions = $8
        stmt.Limit = $9[0]
        stmt.Offset = $9[1]
        $$ = stmt
    }
    |SHOW MEASUREMENT EXACT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowMeasurementCardinalityStatement{}
        stmt.Database = $5
        stmt.Exact = true
        stmt.Condition = $6
        stmt.Dimensions = $7
        stmt.Limit = $8[0]
        stmt.Offset = $8[1]
        $$ = stmt
    }
    |SHOW MEASUREMENT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowMeasurementCardinalityStatement{}
        stmt.Database = $4
        stmt.Exact = false
        stmt.Sources = $5
        stmt.Condition = $6
        stmt.Dimensions = $7
        stmt.Limit = $8[0]
        stmt.Offset = $8[1]
        $$ = stmt
    }
    |SHOW MEASUREMENT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowMeasurementCardinalityStatement{}
        stmt.Database = $4
        stmt.Exact = false
        stmt.Condition = $5
        stmt.Dimensions = $6
        stmt.Limit = $7[0]
        stmt.Offset = $7[1]
        $$ = stmt
    }


SHOW_SERIES_CARDINALITY_STATEMENT:
    SHOW SERIES EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
         stmt := &ShowSeriesCardinalityStatement{}
         stmt.Database = $5
         stmt.Exact = true
         stmt.Sources = $6
         stmt.Condition = $7
         stmt.Dimensions = $8
         stmt.Limit = $9[0]
         stmt.Offset = $9[1]
         $$ = stmt
    }
    |SHOW SERIES EXACT CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowSeriesCardinalityStatement{}
        stmt.Database = $5
        stmt.Exact = true
        stmt.Condition = $6
        stmt.Dimensions = $7
        stmt.Limit = $8[0]
        stmt.Offset = $8[1]
        $$ = stmt
    }
    |SHOW SERIES CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowSeriesCardinalityStatement{}
        stmt.Database = $4
        stmt.Exact = false
        stmt.Sources = $5
        stmt.Condition = $6
        stmt.Dimensions = $7
        stmt.Limit = $8[0]
        stmt.Offset = $8[1]
        $$ = stmt
    }
    |SHOW SERIES CARDINALITY ON_DATABASE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &ShowSeriesCardinalityStatement{}
        stmt.Database = $4
        stmt.Exact = false
        stmt.Condition = $5
        stmt.Dimensions = $6
        stmt.Limit = $7[0]
        stmt.Offset = $7[1]
        $$ = stmt
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
        $$ = stmt
    }


CREATE_CONTINUOUS_QUERY_STATEMENT:
    CREATE CONTINUOUS QUERY IDENT ON IDENT SAMPLE_POLICY BEGIN SELECT_STATEMENT END
    {
    	stmt := &CreateContinuousQueryStatement{
    	    Name: $4,
    	    Database: $6,
    	    Source: $9.(*SelectStatement),
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
    CREATE DOWNSAMPLE ON IDENT LPAREN COLUMN_CLAUSES RPAREN WITH DOWNSAMPLE_INTERVALS
    {
    	stmt := $9.(*CreateDownSampleStatement)
    	stmt.RpName = $4
    	stmt.Ops = $6
    	$$ = stmt
    }
    |CREATE DOWNSAMPLE ON IDENT DOT IDENT LPAREN COLUMN_CLAUSES RPAREN WITH DOWNSAMPLE_INTERVALS
    {
    	stmt := $11.(*CreateDownSampleStatement)
    	stmt.RpName = $6
    	stmt.DbName = $4
    	stmt.Ops = $8
    	$$ = stmt
    }
    |CREATE DOWNSAMPLE LPAREN COLUMN_CLAUSES RPAREN WITH DOWNSAMPLE_INTERVALS
    {
    	stmt := $7.(*CreateDownSampleStatement)
    	stmt.Ops = $4
    	$$ = stmt
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
	    Duration : $2,
	    SampleInterval: $5,
	    TimeInterval: $9,
	}
    }

DURATIONVALS:
    DURATIONVAL
    {
    	$$ = []time.Duration{$1}
    }
    |DURATIONVAL COMMA DURATIONVALS
    {
    	$$ = append([]time.Duration{$1},$3...)
    }


CREATE_STREAM_STATEMENT:
    CREATE STREAM STRING_TYPE INTO_CLAUSE ON SELECT_STATEMENT DELAY DURATIONVAL
    {
    	stmt := &CreateStreamStatement{
    	    Name: $3,
    	    Query: $6,
    	    Delay: $8,
    	}
        if len($4) > 1{
            yylex.Error("into clause only support one target")
        }
        if len($4) == 1{
            mst,ok := $4[0].(*Measurement)
            if !ok{
                 yylex.Error("into clause only support measurement clause")
            }
            mst.IsTarget = true
            stmt.Target = &Target{
            	Measurement: mst,
            }
        }
        $$ = stmt
    }
    |CREATE STREAM STRING_TYPE INTO_CLAUSE ON SELECT_STATEMENT
    {
    	stmt := &CreateStreamStatement{
    	    Name: $3,
    	    Query: $6,
    	}
        if len($4) > 1{
            yylex.Error("into clause only support one target")
        }
        if len($4) == 1{
            mst,ok := $4[0].(*Measurement)
            if !ok{
                 yylex.Error("into clause only support measurement clause")
            }
            mst.IsTarget = true
            stmt.Target = &Target{
            	Measurement: mst,
            }
        }
        $$ = stmt
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
    |STRING_TYPE COMMA ALL_DESTINATION
    {
        $$ = append([]string{$1}, $3...)
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

%%
