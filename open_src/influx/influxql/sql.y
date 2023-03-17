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
	"regexp"
	"sort"
	"strings"
	"time"
	"unsafe"

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
    location            *time.Location
    indexType           *IndexType
    cqsp                *cqSamplePolicyInfo
    fieldOption         *fieldList
    fieldOptions        []*fieldList
    indexInfos          []*IndexInfo
    indexInfo           *IndexInfo
}

%token <str>    FROM MEASUREMENT INTO ON SELECT WHERE AS GROUP BY ORDER LIMIT OFFSET SLIMIT SOFFSET SHOW CREATE FULL PRIVILEGES OUTER JOIN
                TO IN NOT EXISTS REVOKE FILL DELETE WITH ALL PASSWORD NAME REPLICANUM ALTER USER USERS
                DATABASES DATABASE MEASUREMENTS RETENTION POLICIES POLICY DURATION DEFAULT SHARD INDEX GRANT HOT WARM TYPE SET FOR GRANTS
                REPLICATION SERIES DROP CASE WHEN THEN ELSE BEGIN END TRUE FALSE TAG FIELD KEYS VALUES KEY EXPLAIN ANALYZE EXACT CARDINALITY SHARDKEY
                CONTINUOUS DIAGNOSTICS QUERIES QUERIE SHARDS STATS SUBSCRIPTIONS SUBSCRIPTION GROUPS INDEXTYPE INDEXLIST
                EVERY RESAMPLE
                DOWNSAMPLE DOWNSAMPLES SAMPLEINTERVAL TIMEINTERVAL STREAM DELAY STREAMS
                QUERY PARTITION
		        TOKEN TOKENIZERS MATCH LIKE MATCHPHRASE
%token <bool>   DESC ASC
%token <str>    COMMA SEMICOLON LPAREN RPAREN REGEX
%token <int>    EQ NEQ LT LTE GT GTE DOT DOUBLECOLON NEQREGEX EQREGEX
%token <str>    IDENT
%token <int64>  INTEGER
%token <tdur>   DURATIONVAL
%token <str>    STRING
%token <float64> NUMBER
%token <hints>  HINT

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
                                    CREATE_STREAM_STATEMENT SHOW_STREAM_STATEMENT DROP_STREAM_STATEMENT COLUMN_LISTS
%type <fields>                      COLUMN_CLAUSES IDENTS
%type <field>                       COLUMN_CLAUSE
%type <stmts>                       ALL_QUERIES ALL_QUERY
%type <sources>                     FROM_CLAUSE TABLE_NAMES SUBQUERY_CLAUSE INTO_CLAUSE
%type <source>                      JOIN_CLAUSE
%type <ment>                        TABLE_OPTION  TABLE_NAME_WITH_OPTION TABLE_CASE MEASUREMENT_WITH
%type <expr>                        WHERE_CLAUSE CONDITION OPERATION_EQUAL COLUMN_VAREF COLUMN CONDITION_COLUMN TAG_KEYS
				    CASE_WHEN_CASE CASE_WHEN_CASES
%type <int>                         CONDITION_OPERATOR
%type <dataType>                    COLUMN_VAREF_TYPE
%type <sortfs>                      SORTFIELDS ORDER_CLAUSES
%type <sortf>                       SORTFIELD
%type <dimens>                      GROUP_BY_CLAUSE DIMENSION_NAMES
%type <dimen>                       DIMENSION_NAME
%type <intSlice>                    OPTION_CLAUSES LIMIT_OFFSET_OPTION SLIMIT_SOFFSET_OPTION
%type <inter>                       FILL_CLAUSE FILLCONTENT
%type <durations>                   SHARD_HOT_WARM_INDEX_DURATIONS SHARD_HOT_WARM_INDEX_DURATION CREAT_DATABASE_POLICY  CREAT_DATABASE_POLICYS
%type <str>                         REGULAR_EXPRESSION TAG_KEY ON_DATABASE TYPE_CALUSE SHARD_KEY STRING_TYPE TOKEN_CLAUSE TOKENIZERS_CLAUSE
%type <strSlice>                    SHARDKEYLIST INDEX_LIST
%type <location>                    TIME_ZONE
%type <indexType>                   INDEX_TYPE INDEX_TYPES
%type <cqsp>                        SAMPLE_POLICY
%type <tdurs>                       DURATIONVALS
%type <cqsp>                        SAMPLE_POLICY
%type <fieldOption>                 FIELD_OPTION
%type <fieldOptions>                FIELD_OPTIONS
%type <indexInfos>                  INDEX_INFOS
%type <indexInfo>                   INDEX_INFO
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

        if len($1) ==1{
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

SELECT_STATEMENT:
    SELECT COLUMN_CLAUSES INTO_CLAUSE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &SelectStatement{}
        stmt.Fields = $2
        stmt.Sources = $4
        stmt.Dimensions = $6
        stmt.Condition = $5
        stmt.SortFields = $8
        stmt.Limit = $9[0]
        stmt.Offset = $9[1]
        stmt.SLimit = $9[2]
        stmt.SOffset = $9[3]

        tempfill,tempfillvalue,fillflag := deal_Fill($7)
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
        stmt.Location = $10
        if len($3) > 1{
            yylex.Error("into caluse only support one measurement")
        }else if len($3) == 1{
            mst,ok := $3[0].(*Measurement)
            if !ok{
                 yylex.Error("into caluse only support measurement caluse")
            }
            mst.IsTarget = true
            stmt.Target = &Target{
            	Measurement: mst,
            }
        }
        $$ = stmt
    }
    |SELECT HINT COLUMN_CLAUSES INTO_CLAUSE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &SelectStatement{}
        stmt.Hints = $2
        stmt.Fields = $3
        stmt.Sources = $5
        stmt.Dimensions = $7
        stmt.Condition = $6
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
        if len($4) > 1{
            yylex.Error("into caluse only support one measurement")
        }else if len($4) == 1{
            mst,ok := $4[0].(*Measurement)
            if !ok{
                 yylex.Error("into caluse only support measurement caluse")
            }
            mst.IsTarget = true
            stmt.Target = &Target{
            	Measurement: mst,
            }
        }
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
        $$ = $2
    }
    |IDENT LPAREN COLUMN_CLAUSES RPAREN
    {
        cols := &Call{Name: strings.ToLower($1), Args: []Expr{}}
        for i := range $3{
            cols.Args = append(cols.Args, $3[i].Expr)
        }
        $$ = cols
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

CONDITION:
    OPERATION_EQUAL
    {
        $$ = $1
    }
    |LPAREN CONDITION RPAREN
    {
    	$$ = &ParenExpr{Expr:$2}
    }
    |CONDITION AND CONDITION
    {
        $$ = &BinaryExpr{Op:Token($2),LHS:$1,RHS:$3}
    }
    |CONDITION OR CONDITION
    {
        $$ = &BinaryExpr{Op:Token($2),LHS:$1,RHS:$3}
    }
    |IDENT IN LPAREN IDENTS RPAREN
    {
    	$$ = &BinaryExpr{}
    }
    |IDENT IN LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &BinaryExpr{}
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
	$$ = &MatchExpr{
	    Field:  &VarRef{Val: $3},
	    Value:  &VarRef{Val: $5},
	    Op: "match",
        }
    }
    |MATCHPHRASE LPAREN STRING_TYPE COMMA STRING_TYPE RPAREN
    {
	$$ = &MatchExpr{
	    Field:  &VarRef{Val: $3},
	    Value:  &VarRef{Val: $5},
	    Op: "match_phrase",
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
	$$ = NEQREGEX
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

LIMIT_OFFSET_OPTION:
    LIMIT INTEGER OFFSET INTEGER
    {
    	$$ = []int{int($2), int($4)}
    }
    |LIMIT INTEGER
    {
    	$$ = []int{int($2), 0}
    }
    |OFFSET INTEGER
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
        $$ = &ShowDatabasesStatement{}
    }

CREATE_DATABASE_STATEMENT:
    CREATE DATABASE IDENT WITH_CLAUSES
    {
        sms := $4

        sms.(*CreateDatabaseStatement).Name = $3
        $$ = sms
    }
    |CREATE DATABASE IDENT
    {
        stmt := &CreateDatabaseStatement{}
        stmt.RetentionPolicyCreate = false
        stmt.Name = $3
        $$ = stmt
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
        stmt.ReplicaNum = $2.ReplicaNum
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

        if $2.ReplicaNum != 0{
            $1.ReplicaNum = $2.ReplicaNum
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
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,PolicyDuration: &$2}
    }
    |REPLICATION INTEGER
    {
        if $2<1 ||$2 > 2147483647{
            yylex.Error("REPLICATION must be 1 <= n <= 2147483647")
        }
        int_integer := *(*int)(unsafe.Pointer(&$2))
        $$ = &Durations{ShardGroupDuration: -1,HotDuration: -1,WarmDuration: -1,IndexGroupDuration: -1,Replication: &int_integer}
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
    	if $4<1 ||$4 > 2147483647{
            yylex.Error("REPLICATION must be 1 <= n <= 2147483647")
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
    	if $4<1 ||$4 > 2147483647{
            yylex.Error("REPLICATION must be 1 <= n <= 2147483647")
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
    CREATE MEASUREMENT TABLE_CASE COLUMN_LISTS WITH INDEXTYPE INDEX_TYPES SHARDKEY SHARDKEYLIST TYPE_CALUSE
    {
        stmt := &CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        if $4 != nil {
            stmt.Fields = $4.(*CreateMeasurementStatement).Fields
	        stmt.Tags = $4.(*CreateMeasurementStatement).Tags
	        stmt.IndexInfo = $4.(*CreateMeasurementStatement).IndexInfo
        }
	if $7 != nil {
	    stmt.IndexType = $7.types
	    stmt.IndexList = $7.lists
	}
        stmt.ShardKey = $9
        sort.Strings(stmt.ShardKey)
        stmt.Type = $10
        $$ = stmt
    }
    |CREATE MEASUREMENT TABLE_CASE COLUMN_LISTS WITH SHARDKEY SHARDKEYLIST TYPE_CALUSE
    {
        stmt := &CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
	if $4 != nil {
	    stmt.Fields = $4.(*CreateMeasurementStatement).Fields
	    stmt.Tags = $4.(*CreateMeasurementStatement).Tags
	    stmt.IndexInfo = $4.(*CreateMeasurementStatement).IndexInfo
	}
        stmt.ShardKey = $7
        sort.Strings(stmt.ShardKey)
        stmt.Type = $8
        $$ = stmt
    }
    |CREATE MEASUREMENT TABLE_CASE COLUMN_LISTS WITH INDEXTYPE INDEX_TYPES TYPE_CALUSE
    {
        stmt := &CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        if $4 != nil {
	    stmt.Fields = $4.(*CreateMeasurementStatement).Fields
	    stmt.Tags = $4.(*CreateMeasurementStatement).Tags
    	    stmt.IndexInfo = $4.(*CreateMeasurementStatement).IndexInfo
        }
	if $7 != nil {
	    stmt.IndexType = $7.types
	    stmt.IndexList = $7.lists
	}
        stmt.Type = $8
        $$ = stmt
    }
    |CREATE MEASUREMENT TABLE_CASE COLUMN_LISTS
    {
        stmt := &CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        stmt.Type = "hash"
	if $4 != nil {
	    stmt.Fields = $4.(*CreateMeasurementStatement).Fields
	    stmt.Tags = $4.(*CreateMeasurementStatement).Tags
            stmt.IndexInfo = $4.(*CreateMeasurementStatement).IndexInfo
	}
        $$ = stmt
    }

COLUMN_LISTS:
    LPAREN FIELD_OPTIONS INDEX_INFOS RPAREN
    {
	stmt := &CreateMeasurementStatement{
	    Tags: make([]string,0,0),
	    Fields : make(map[string]int32),
	}
	for i := range $2{
	    fType := $2[i].tagOrField
	    if  fType == "tag" {
            if  strings.ToLower($2[i].fieldType) != "string" {
                yylex.Error("tag only support string")
            }
		    stmt.Tags = append(stmt.Tags, $2[i].fieldName)
	    } else if fType == "field" {
		    filedType := strings.ToLower($2[i].fieldType)
		    if filedType == "int" || filedType == "integer" {
		        stmt.Fields[$2[i].fieldName] = 1
		    } else if filedType == "uint" {
    		    stmt.Fields[$2[i].fieldName] = 2
		    } else if filedType == "float" {
		        stmt.Fields[$2[i].fieldName] = 3
		    } else if filedType == "string"{
		        stmt.Fields[$2[i].fieldName] = 4
		    } else if filedType == "boolean" || filedType == "bool" {
		        stmt.Fields[$2[i].fieldName] = 5
		    } else {
                yylex.Error("field only support string,bool,int,float")
            }
	    }
	}
	stmt.IndexInfo = $3
	$$ = stmt
    }
    |
    {
	$$ = nil
    }

INDEX_INFOS:
    INDEX_INFO COMMA INDEX_INFOS
    {
        info := []*IndexInfo{$1}
        $$ = append(info,$3...)
    }
    |
    INDEX_INFO
    {
        $$ = []*IndexInfo{$1}
    }
    |
    {
        $$ = nil
    }
INDEX_INFO:
    INDEX STRING_TYPE STRING_TYPE TYPE STRING_TYPE TOKEN_CLAUSE TOKENIZERS_CLAUSE
    {
        $$ = &IndexInfo{
            IndexName: $2,
            FieldName: $3,
            IndexType: strings.ToLower($5),
            Tokens: $6,
     	    Tokenizers: $7,
        }
    }
TOKEN_CLAUSE:
    TOKEN STRING_TYPE
    {
        $$ = $2
    }
    |
    {
        $$ = ""
    }
 
TOKENIZERS_CLAUSE:
    TOKENIZERS STRING_TYPE
    {
        $$ = $2
    }
    |
    {
        $$ = ""
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
    STRING_TYPE STRING_TYPE TAG COMMA
    {
        $$ = &fieldList{
            fieldName: $1,
            fieldType: $2,
            tagOrField: "tag",
        }
    }
    |
    STRING_TYPE STRING_TYPE FIELD COMMA
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

TYPE_CALUSE:
    TYPE IDENT
    {
        $$ = $2
    }
    |
    {
        $$ = "hash"
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


ALTER_SHARD_KEY_STATEMENT:
    ALTER MEASUREMENT TABLE_CASE WITH SHARDKEY SHARDKEYLIST TYPE_CALUSE
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
            yylex.Error("into caluse only support one target")
        }
        if len($4) == 1{
            mst,ok := $4[0].(*Measurement)
            if !ok{
                 yylex.Error("into caluse only support measurement caluse")
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
            yylex.Error("into caluse only support one target")
        }
        if len($4) == 1{
            mst,ok := $4[0].(*Measurement)
            if !ok{
                 yylex.Error("into caluse only support measurement caluse")
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

%%
