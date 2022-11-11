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

package yacc

import (
	"regexp"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/openGemini/openGemini/open_src/influx/influxql"
	"github.com/openGemini/openGemini/open_src/influx/query"
)

func setParseTree(yylex interface{}, stmts influxql.Statements) {
    for _,stmt :=range stmts{
        yylex.(*YyParser).Query.Statements = append(yylex.(*YyParser).Query.Statements, stmt)
    }
}

func deal_Fill (fill interface{})  (influxql.FillOption , interface{},bool) {
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
    stmt                influxql.Statement
    stmts               influxql.Statements
    str                 string
    query               influxql.Query
    field               *influxql.Field
    fields              influxql.Fields
    sources             influxql.Sources
    sortfs              influxql.SortFields
    sortf               *influxql.SortField
    ment                *influxql.Measurement
    subQuery            *influxql.SubQuery
    dimens              influxql.Dimensions
    dimen               *influxql.Dimension
    int                 int
    int64               int64
    float64             float64
    dataType            influxql.DataType
    expr                influxql.Expr
    tdur                time.Duration
    bool                bool
    groupByCondition    *GroupByCondition
    intSlice            []int
    inter               interface{}
    durations           *Durations
    hints               influxql.Hints
    strSlice            []string
    location            *time.Location
    indexType           *IndexType
}

%token <str>    FROM MEASUREMENT ON SELECT WHERE AS GROUP BY ORDER LIMIT OFFSET SLIMIT SOFFSET SHOW CREATE FULL PRIVILEGES OUTER JOIN
                TO IN NOT EXISTS REVOKE FILL DELETE WITH ALL PASSWORD NAME REPLICANUM ALTER USER USERS
                DATABASES DATABASE MEASUREMENTS RETENTION POLICIES POLICY DURATION DEFAULT SHARD INDEX GRANT HOT WARM TYPE SET FOR GRANTS
                REPLICATION SERIES DROP CASE WHEN THEN ELSE END TRUE FALSE TAG FIELD KEYS VALUES KEY EXPLAIN ANALYZE EXACT CARDINALITY SHARDKEY
                CONTINUOUS DIAGNOSTICS QUERIES QUERIE SHARDS STATS SUBSCRIPTIONS SUBSCRIPTION GROUPS INDEXTYPE INDEXLIST
                QUERY PARTITION
                FUZZY MATCH REGEX
%token <bool>   DESC ASC
%token <str>    COMMA SEMICOLON LPAREN RPAREN
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
%type <fields>                      COLUMN_CLAUSES IDENTS
%type <field>                       COLUMN_CLAUSE
%type <stmts>                       ALL_QUERIES ALL_QUERY
%type <sources>                     FROM_CLAUSE TABLE_NAMES SUBQUERY_CLAUSE
%type <ment>                        TABLE_OPTION JOIN_CLAUSES JOIN_CLAUSE TABLE_NAME_WITH_OPTION TABLE_CASE MEASUREMENT_WITH
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
%type <str>                         REGULAR_EXPRESSION TAG_KEY ON_DATABASE TYPE_CALUSE SHARD_KEY STRING_TYPE
%type <strSlice>                    SHARDKEYLIST INDEX_LIST
%type <location>                    TIME_ZONE
%type <indexType>                   INDEX_TYPE INDEX_TYPES
%%

ALL_QUERIES:
        ALL_QUERY
        {
            setParseTree(yylex, $1)
        }

ALL_QUERY:
    STATEMENT
    {
        $$ = []influxql.Statement{$1}
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



SELECT_STATEMENT:
    SELECT COLUMN_CLAUSES FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &influxql.SelectStatement{}
        stmt.Fields = $2
        stmt.Sources = $3
        stmt.Dimensions = $5
        stmt.Condition = $4
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
	influxql.WalkFunc(stmt.Fields, func(n influxql.Node) {
		if _, ok := n.(*influxql.Call); ok {
			stmt.IsRawQuery = false
		}
	})
        stmt.Location = $9
        $$ = stmt
    }
    |SELECT HINT COLUMN_CLAUSES FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE FILL_CLAUSE ORDER_CLAUSES OPTION_CLAUSES TIME_ZONE
    {
        stmt := &influxql.SelectStatement{}
        stmt.Hints = $2
        stmt.Fields = $3
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
	influxql.WalkFunc(stmt.Fields, func(n influxql.Node) {
		if _, ok := n.(*influxql.Call); ok {
			stmt.IsRawQuery = false
		}
	})
        stmt.Location = $10
        $$ = stmt
    }



COLUMN_CLAUSES:
    COLUMN_CLAUSE
    {
        $$ = []*influxql.Field{$1}
    }
    |COLUMN_CLAUSE COMMA COLUMN_CLAUSES
    {
        $$ = append([]*influxql.Field{$1},$3...)
    }

COLUMN_CLAUSE:
    MUL
    {
        $$ = &influxql.Field{Expr:&influxql.Wildcard{Type:influxql.Token($1)}}
    }
    |MUL DOUBLECOLON TAG
    {
    	$$ = &influxql.Field{Expr:&influxql.Wildcard{Type:influxql.TAG}}
    }
    |MUL DOUBLECOLON FIELD
    {
    	$$ = &influxql.Field{Expr:&influxql.Wildcard{Type:influxql.FIELD}}
    }
    |COLUMN
    {
        $$ = &influxql.Field{Expr: $1}
    }
    |COLUMN AS IDENT
    {
        $$ = &influxql.Field{Expr: $1, Alias:$3}
    }
    |COLUMN AS STRING
    {
        $$ = &influxql.Field{Expr: $1, Alias:$3}
    }

CASE_WHEN_CASES:
    CASE_WHEN_CASE
    {
    	$$ = $1
    }
    |CASE_WHEN_CASE CASE_WHEN_CASES
    {
    	c := $1.(*influxql.CaseWhenExpr)
    	c.Conditions = append(c.Conditions, $2.(*influxql.CaseWhenExpr).Conditions...)
    	c.Assigners = append(c.Assigners, $2.(*influxql.CaseWhenExpr).Assigners...)
        $$ = c
    }

CASE_WHEN_CASE:
    WHEN CONDITION THEN COLUMN
    {
    	c := &influxql.CaseWhenExpr{}
    	c.Conditions = []influxql.Expr{$2}
    	c.Assigners = []influxql.Expr{$4}
    	$$ = c
    }

IDENTS:
   IDENT
   {
       $$ = []*influxql.Field{&influxql.Field{Expr:&influxql.VarRef{Val:$1}}}
   }
   |IDENT COMMA IDENTS
   {
       $$ = append([]*influxql.Field{&influxql.Field{Expr:&influxql.VarRef{Val:$1}}},$3...)
   }

COLUMN:
    COLUMN MUL COLUMN
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.MUL), LHS:$1, RHS:$3}
    }
    |COLUMN DIV COLUMN
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.DIV), LHS:$1, RHS:$3}
    }
    |COLUMN ADD COLUMN
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.ADD), LHS:$1, RHS:$3}
    }
    |COLUMN SUB COLUMN
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.SUB), LHS:$1, RHS:$3}
    }
    |COLUMN BITWISE_XOR COLUMN
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.BITWISE_XOR), LHS:$1, RHS:$3}
    }
    |COLUMN MOD COLUMN
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.MOD), LHS:$1, RHS:$3}
    }
    |COLUMN BITWISE_AND COLUMN
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.BITWISE_AND), LHS:$1, RHS:$3}
    }
    |COLUMN BITWISE_OR COLUMN
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.BITWISE_OR), LHS:$1, RHS:$3}
    }
    |LPAREN COLUMN RPAREN
    {
        $$ = $2
    }
    |IDENT LPAREN COLUMN_CLAUSES RPAREN
    {
        cols := &influxql.Call{Name: strings.ToLower($1), Args: []influxql.Expr{}}
        for i := range $3{
            cols.Args = append(cols.Args, $3[i].Expr)
        }
        $$ = cols
    }
    |IDENT LPAREN RPAREN
    {
        cols := &influxql.Call{Name: strings.ToLower($1)}
        $$ = cols
    }
    |SUB COLUMN %prec UMINUS
    {
        switch s := $2.(type) {
        case *influxql.NumberLiteral:
            s.Val = -1*s.Val
        	$$ = $2
        case *influxql.IntegerLiteral:
            s.Val = -1*s.Val
            $$ = $2
        default:
        	$$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.MUL), LHS:&influxql.IntegerLiteral{Val:-1}, RHS:$2}
        }

    }
    |COLUMN_VAREF
    {
        $$ = $1
    }
    |DURATIONVAL
    {
    	$$ = &influxql.DurationLiteral{Val: $1}
    }
    |CASE CASE_WHEN_CASES ELSE COLUMN END
    {
    	c := $2.(*influxql.CaseWhenExpr)
    	c.Assigners = append(c.Assigners, $4)
    	$$ = c
    }
    |CASE IDENT CASE_WHEN_CASES ELSE IDENT END
    {
    	$$ = &influxql.VarRef{}
    }

FROM_CLAUSE:
    FROM TABLE_NAMES
    {
        $$ = $2
    }

TABLE_NAMES:
    TABLE_NAME_WITH_OPTION
    {
        $$ = []influxql.Source{$1}
    }
    |TABLE_NAME_WITH_OPTION COMMA TABLE_NAMES
    {
        $$ = append([]influxql.Source{$1},$3...)
    }
    |SUBQUERY_CLAUSE
    {
    	$$ = $1

    }
    |SUBQUERY_CLAUSE COMMA TABLE_NAMES
    {
        $$ =  append($1,$3...)
    }

SUBQUERY_CLAUSE:
    LPAREN ALL_QUERY RPAREN
    {
        all_subquerys:=  []influxql.Source{}
        for _,temp_stmt := range $2 {

            stmt,ok:= temp_stmt.(*influxql.SelectStatement)
            if !ok{
                yylex.Error("expexted SelectStatement")
            }
            build_SubQuery := &influxql.SubQuery{Statement: stmt}
            all_subquerys =append(all_subquerys,build_SubQuery)
        }
        $$ = all_subquerys
    }

TABLE_NAME_WITH_OPTION:
    TABLE_CASE JOIN_CLAUSES
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
    	$$ = &influxql.Measurement{Name:$1}
    }
    |STRING
    {
    	$$ = &influxql.Measurement{Name:$1}
    }
    |REGULAR_EXPRESSION
    {
	re, err := regexp.Compile($1)
	if err != nil {
	    yylex.Error("Invalid regexprs")
	}

    	$$ = &influxql.Measurement{Regex: &influxql.RegexLiteral{Val: re}}
    }

JOIN_CLAUSES:
    JOIN_CLAUSE JOIN_CLAUSES
    {
    	$$ = &influxql.Measurement{}
    }
    |
    {
    	$$ = &influxql.Measurement{}
    }

JOIN_CLAUSE:
    FULL OUTER JOIN IDENT ON IDENT CONDITION_OPERATOR IDENT
    {
    	$$ = &influxql.Measurement{}
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
        $$ = []*influxql.Dimension{$1}
    }
    |DIMENSION_NAME COMMA DIMENSION_NAMES
    {
        $$ = append([]*influxql.Dimension{$1}, $3...)
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
        $$ = &influxql.Dimension{Expr:&influxql.VarRef{Val:$1}}
    }
    |STRING_TYPE DOUBLECOLON TAG
    {
        $$ = &influxql.Dimension{Expr:&influxql.VarRef{Val:$1}}
    }
    |IDENT LPAREN DURATIONVAL RPAREN
    {
    	if strings.ToLower($1) != "time"{
    	    yylex.Error("Invalid group by combination for no-time tag and time duration")
    	}

    	$$ = &influxql.Dimension{Expr:&influxql.Call{Name:"time", Args:[]influxql.Expr{&influxql.DurationLiteral{Val: $3}}}}
    }
    |IDENT LPAREN DURATIONVAL COMMA DURATIONVAL RPAREN
    {
        if strings.ToLower($1) != "time"{
                    yylex.Error("Invalid group by combination for no-time tag and time duration")
                }

        $$ = &influxql.Dimension{Expr:&influxql.Call{Name:"time", Args:[]influxql.Expr{&influxql.DurationLiteral{Val: $3},&influxql.DurationLiteral{Val: $5}}}}
    }
    |IDENT LPAREN DURATIONVAL COMMA SUB DURATIONVAL RPAREN
    {
        if strings.ToLower($1) != "time"{
                    yylex.Error("Invalid group by combination for no-time tag and time duration")
                }

        $$ = &influxql.Dimension{Expr:&influxql.Call{Name:"time", Args:[]influxql.Expr{&influxql.DurationLiteral{Val: $3},&influxql.DurationLiteral{Val: time.Duration(-$6)}}}}
    }
    |MUL
    {
        $$ = &influxql.Dimension{Expr:&influxql.Wildcard{Type:influxql.Token($1)}}
    }
    |MUL DOUBLECOLON TAG
    {
        $$ = &influxql.Dimension{Expr:&influxql.Wildcard{Type:influxql.Token($1)}}
    }
    |REGULAR_EXPRESSION
     {
        re, err := regexp.Compile($1)
        if err != nil {
            yylex.Error("Invalid regexprs")
        }
        $$ = &influxql.Dimension{Expr: &influxql.RegexLiteral{Val: re}}
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
    	$$ = &influxql.ParenExpr{Expr:$2}
    }
    |CONDITION AND CONDITION
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token($2),LHS:$1,RHS:$3}
    }
    |CONDITION OR CONDITION
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token($2),LHS:$1,RHS:$3}
    }
    |IDENT IN LPAREN IDENTS RPAREN
    {
    	$$ = &influxql.BinaryExpr{}
    }
    |IDENT IN LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &influxql.BinaryExpr{}
    }
    |EXISTS LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &influxql.BinaryExpr{}
    }
    |IDENT NOT IN LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &influxql.BinaryExpr{}
    }
    |IDENT NOT IN LPAREN IDENTS RPAREN
    {
    	$$ = &influxql.BinaryExpr{}
    }
    |NOT EXISTS LPAREN SELECT_STATEMENT RPAREN
    {
    	$$ = &influxql.BinaryExpr{}
    }

OPERATION_EQUAL:
    CONDITION_COLUMN CONDITION_OPERATOR CONDITION_COLUMN
    {
    	if $2 == influxql.NEQREGEX{
    		switch $3.(type){
    		case *influxql.RegexLiteral:
    		default:
    			yylex.Error("expected regular expression")
    		}
    	}
        $$ = &influxql.BinaryExpr{Op:influxql.Token($2),LHS:$1,RHS:$3}
    }

CONDITION_COLUMN:
    COLUMN
    {
    	$$ = $1
    }
    |LPAREN CONDITION RPAREN
    {
    	$$ = &influxql.ParenExpr{Expr:$2}
    }

CONDITION_OPERATOR:
    EQ
    {
        $$ = influxql.EQ
    }
    |NEQ
    {
        $$ = influxql.NEQ
    }
    |LT
    {
        $$ = influxql.LT
    }
    |LTE
    {
        $$ = influxql.LTE
    }
    |GT
    {
        $$ = influxql.GT
    }
    |GTE
    {
        $$ = influxql.GTE
    }
    |EQREGEX
    {
        $$ = influxql.EQREGEX
    }
    |NEQREGEX
    {
        $$ = influxql.NEQREGEX
    }
	|REGEX
    {
        $$ = influxql.REGEX
    }
    |FUZZY
    {
        $$ = influxql.FUZZY
    }
    |MATCH
    {
        $$ = influxql.MATCH
    }

REGULAR_EXPRESSION:
    REGEX
    {
    	$$ = $1
    }

COLUMN_VAREF:
    IDENT
    {
        $$ = &influxql.VarRef{Val:$1}
    }
    |IDENT DOUBLECOLON COLUMN_VAREF_TYPE
    {
    	$$ = &influxql.VarRef{Val:$1, Type:$3}
    }
    |NUMBER
    {
        $$ = &influxql.NumberLiteral{Val:$1}
    }
    |INTEGER
    {
        $$ = &influxql.IntegerLiteral{Val:$1}
    }
    |STRING
    {
        $$ = &influxql.StringLiteral{Val:$1}
    }
    |TRUE
    {
    	$$ = &influxql.BooleanLiteral{Val:true}
    }
    |FALSE
    {
    	$$ = &influxql.BooleanLiteral{Val:false}
    }
    |REGULAR_EXPRESSION
    {
	re, err := regexp.Compile($1)
	if err != nil {
	    yylex.Error("Invalid regexprs")
	}
    	$$ = &influxql.RegexLiteral{Val: re}
    }

COLUMN_VAREF_TYPE:
    IDENT
    {
    	switch strings.ToLower($1){
    	case "float":
    	    $$ = influxql.Float
    	case "integer":
    	    $$ = influxql.Integer
    	case "string":
    	    $$ = influxql.String
    	case "boolean":
    	    $$ = influxql.Boolean
    	case "time":
    	    $$ = influxql.Time
    	case "duration":
    	    $$ = influxql.Duration
    	case "unsigned":
    	    $$ = influxql.Unsigned
    	default:
    	    yylex.Error("wrong field dataType")
    	}
    }
    |TAG
    {
        $$ = influxql.Tag
    }
    |FIELD
    {
    	$$ = influxql.AnyField
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
        $$ = []*influxql.SortField{$1}
    }
    |SORTFIELD COMMA SORTFIELDS
    {
        $$ = append([]*influxql.SortField{$1}, $3...)
    }

SORTFIELD:
    IDENT
    {
        $$ = &influxql.SortField{Name:$1,Ascending:true}
    }
    |IDENT DESC
    {
        $$ = &influxql.SortField{Name:$1,Ascending:false}
    }
    |IDENT ASC
    {
        $$ = &influxql.SortField{Name:$1,Ascending:true}
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
        $$ = &influxql.ShowDatabasesStatement{}
    }

CREATE_DATABASE_STATEMENT:
    CREATE DATABASE IDENT WITH_CLAUSES
    {
        sms := $4

        sms.(*influxql.CreateDatabaseStatement).Name = $3
        $$ = sms
    }
    |CREATE DATABASE IDENT
    {
        stmt := &influxql.CreateDatabaseStatement{}
        stmt.RetentionPolicyCreate = false
        stmt.Name = $3
        $$ = stmt
    }

WITH_CLAUSES:
    WITH CREAT_DATABASE_POLICYS
    {
        stmt := &influxql.CreateDatabaseStatement{}
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
           stmt.RetentionPolicyShardGroupDuration = time.Duration(query.DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyShardGroupDuration = $2.ShardGroupDuration
        }

        if $2.HotDuration==-1||$2.HotDuration==0{
           stmt.RetentionPolicyHotDuration = time.Duration(query.DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyHotDuration = $2.HotDuration
        }

        if $2.WarmDuration==-1||$2.WarmDuration==0{
           stmt.RetentionPolicyWarmDuration = time.Duration(query.DefaultQueryTimeout)
        }else{
           stmt.RetentionPolicyWarmDuration = $2.WarmDuration
        }

        if $2.IndexGroupDuration==-1||$2.IndexGroupDuration==0{
           stmt.RetentionPolicyIndexGroupDuration = time.Duration(query.DefaultQueryTimeout)
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
        sms := &influxql.ShowMeasurementsStatement{}
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
         sms := &influxql.ShowMeasurementsStatement{}
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
        $$ = &influxql.Measurement{Name:$2}
    }
    |NEQ IDENT
    {
        $$ = &influxql.Measurement{Name:$2}
    }
    |EQREGEX REGULAR_EXPRESSION
    {
        re, err := regexp.Compile($2)
        if err != nil {
            yylex.Error("Invalid regexprs")
        }
        $$ = &influxql.Measurement{Regex:&influxql.RegexLiteral{Val: re}}
    }
    |NEQREGEX REGULAR_EXPRESSION
    {
        re, err := regexp.Compile($2)
        if err != nil {
            yylex.Error("Invalid regexprs")
        }
        $$ = &influxql.Measurement{Regex:&influxql.RegexLiteral{Val: re}}
    }



SHOW_RETENTION_POLICIES_STATEMENT:
    SHOW RETENTION POLICIES ON IDENT
    {
        $$ = &influxql.ShowRetentionPoliciesStatement{
            Database: $5,
        }
    }
    |SHOW RETENTION POLICIES
    {
         $$ = &influxql.ShowRetentionPoliciesStatement{ }
    }


CREATE_RENTRENTION_POLICY_STATEMENT:
    CREATE RETENTION POLICY IDENT ON IDENT RP_DURATION_OPTIONS
    {
        stmt := $7.(*influxql.CreateRetentionPolicyStatement)
        stmt.Name = $4
        stmt.Database = $6
        $$ = stmt
    }
    |CREATE RETENTION POLICY IDENT ON IDENT RP_DURATION_OPTIONS DEFAULT
    {
        stmt := $7.(*influxql.CreateRetentionPolicyStatement)
        stmt.Name = $4
        stmt.Database = $6
        stmt.Default = true
        $$ = stmt
    }

CREATE_USER_STATEMENT:
    CREATE USER IDENT WITH PASSWORD STRING
    {
        stmt := &influxql.CreateUserStatement{}
        stmt.Name = $3
        stmt.Password = $6
        $$ = stmt
    }
    |CREATE USER IDENT WITH PASSWORD STRING WITH ALL PRIVILEGES
    {
        stmt := &influxql.CreateUserStatement{}
        stmt.Name = $3
        stmt.Password = $6
        stmt.Admin = true
        $$ = stmt
    }
    |CREATE USER IDENT WITH PASSWORD STRING WITH PARTITION PRIVILEGES
    {
        stmt := &influxql.CreateUserStatement{}
        stmt.Name = $3
        stmt.Password = $6
        stmt.Rwuser = true
        $$ = stmt
    }


RP_DURATION_OPTIONS:
    DURATION DURATIONVAL REPLICATION INTEGER SHARD_HOT_WARM_INDEX_DURATIONS
    {
    	stmt := &influxql.CreateRetentionPolicyStatement{}
    	stmt.Duration = $2
    	if $4<1 ||$4 > 2147483647{
            yylex.Error("REPLICATION must be 1 <= n <= 2147483647")
        }
    	stmt.Replication = int($4)

    	if $5.ShardGroupDuration==-1||$5.ShardGroupDuration==0{
           stmt.ShardGroupDuration = time.Duration(query.DefaultQueryTimeout)
        }else{
           stmt.ShardGroupDuration = $5.ShardGroupDuration
        }

        if $5.HotDuration==-1||$5.HotDuration==0{
           stmt.HotDuration = time.Duration(query.DefaultQueryTimeout)
        }else{
           stmt.HotDuration = $5.HotDuration
        }

        if $5.WarmDuration==-1||$5.WarmDuration==0{
           stmt.WarmDuration = time.Duration(query.DefaultQueryTimeout)
        }else{
           stmt.WarmDuration = $5.WarmDuration
        }

        if $5.IndexGroupDuration==-1||$5.IndexGroupDuration==0{
           stmt.IndexGroupDuration = time.Duration(query.DefaultQueryTimeout)
        }else{
           stmt.IndexGroupDuration = $5.IndexGroupDuration
        }

    	$$ = stmt
    }
    |DURATION DURATIONVAL REPLICATION INTEGER
    {
    	stmt := &influxql.CreateRetentionPolicyStatement{}
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
    	stmt := &influxql.ShowSeriesStatement{}
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
    	stmt := &influxql.ShowSeriesStatement{}
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
    	$$ = &influxql.ShowUsersStatement{}
    }

DROP_DATABASE_STATEMENT:
    DROP DATABASE IDENT
    {
    	stmt := &influxql.DropDatabaseStatement{}
    	stmt.Name = $3
    	$$ = stmt
    }

DROP_SERIES_STATEMENT:
    DROP SERIES FROM_CLAUSE WHERE_CLAUSE
    {
    	stmt := &influxql.DropSeriesStatement{}
    	stmt.Sources = $3
    	stmt.Condition = $4
    	$$ = stmt
    }
    |DROP SERIES WHERE_CLAUSE
    {
        stmt := &influxql.DropSeriesStatement{}
        stmt.Condition = $3
        $$ = stmt
    }

DELETE_SERIES_STATEMENT:
    DELETE FROM_CLAUSE WHERE_CLAUSE
    {
    	stmt := &influxql.DeleteSeriesStatement{}
    	stmt.Sources = $2
    	stmt.Condition = $3
    	$$ = stmt
    }
    |DELETE WHERE_CLAUSE
    {
        stmt := &influxql.DeleteSeriesStatement{}
        stmt.Condition = $2
        $$ = stmt
    }


ALTER_RENTRENTION_POLICY_STATEMENT:
    ALTER RETENTION POLICY IDENT ON IDENT CREAT_DATABASE_POLICYS
    {
        stmt := &influxql.AlterRetentionPolicyStatement{}
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
        stmt := &influxql.DropRetentionPolicyStatement{}
        stmt.Name = $4
        stmt.Database = $6
        $$ = stmt
    }

GRANT_STATEMENT:
    GRANT ALL ON IDENT TO IDENT
    {
    	stmt := &influxql.GrantStatement{}
    	stmt.Privilege = influxql.AllPrivileges
    	stmt.On = $4
    	stmt.User = $6
    	$$ = stmt
    }
    |GRANT ALL PRIVILEGES ON IDENT TO IDENT
    {
    	stmt := &influxql.GrantStatement{}
    	stmt.Privilege = influxql.AllPrivileges
    	stmt.On = $5
    	stmt.User = $7
    	$$ = stmt
    }
    |GRANT IDENT ON IDENT TO IDENT
    {
    	stmt := &influxql.GrantStatement{}
    	switch strings.ToLower($2){
    	case "read":
    	    stmt.Privilege = influxql.ReadPrivilege
    	case "write":
    	    stmt.Privilege = influxql.WritePrivilege
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
    	$$ = &influxql.GrantAdminStatement{User: $5}
    }
    |GRANT ALL TO IDENT
    {
    	$$ = &influxql.GrantAdminStatement{User: $4}
    }

REVOKE_STATEMENT:
    REVOKE ALL ON IDENT FROM IDENT
    {
    	stmt := &influxql.RevokeStatement{}
    	stmt.Privilege = influxql.AllPrivileges
    	stmt.On = $4
    	stmt.User = $6
    	$$ = stmt
    }
    |REVOKE ALL PRIVILEGES ON IDENT FROM IDENT
    {
    	stmt := &influxql.RevokeStatement{}
    	stmt.Privilege = influxql.AllPrivileges
    	stmt.On = $5
    	stmt.User = $7
    	$$ = stmt
    }
    |REVOKE IDENT ON IDENT FROM IDENT
    {
    	stmt := &influxql.RevokeStatement{}
    	switch strings.ToLower($2){
    	case "read":
    	    stmt.Privilege = influxql.ReadPrivilege
    	case "write":
    	    stmt.Privilege = influxql.WritePrivilege
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
    	$$ = &influxql.RevokeAdminStatement{User: $5}
    }
    |REVOKE ALL FROM IDENT
    {
    	$$ = &influxql.RevokeAdminStatement{User: $4}
    }

DROP_USER_STATEMENT:
    DROP USER IDENT
    {
    	$$ = &influxql.DropUserStatement{Name:$3}
    }

SHOW_TAG_KEYS_STATEMENT:
  SHOW TAG KEYS ON_DATABASE FROM_CLAUSE WHERE_CLAUSE ORDER_CLAUSES OPTION_CLAUSES
  {
      stmt := &influxql.ShowTagKeysStatement{}
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
      stmt := &influxql.ShowTagKeysStatement{}
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
      stmt := &influxql.ShowFieldKeysStatement{}
      stmt.Database = $4
      stmt.Sources = $5
      stmt.SortFields = $6
      stmt.Limit = $7[0]
      stmt.Offset = $7[1]
      $$ = stmt
  }
  |SHOW FIELD KEYS ON_DATABASE ORDER_CLAUSES LIMIT_OFFSET_OPTION
   {
       stmt := &influxql.ShowFieldKeysStatement{}
       stmt.Database = $4
       stmt.SortFields = $5
       stmt.Limit = $6[0]
       stmt.Offset = $6[1]
       $$ = stmt
   }


SHOW_TAG_VALUES_STATEMENT:
   SHOW TAG VALUES ON_DATABASE FROM_CLAUSE WITH KEY TAG_VALUES_WITH WHERE_CLAUSE ORDER_CLAUSES LIMIT_OFFSET_OPTION
   {
       stmt := $8.(*influxql.ShowTagValuesStatement)
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
       stmt := $7.(*influxql.ShowTagValuesStatement)
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
      stmt := &influxql.ShowTagValuesStatement{}
      stmt.Op = influxql.EQ
      stmt.TagKeyExpr = $2.(*influxql.ListLiteral)
      $$ = stmt
  }
  |NEQ TAG_KEYS
  {
      stmt := &influxql.ShowTagValuesStatement{}
      stmt.Op = influxql.NEQ
      stmt.TagKeyExpr = $2.(*influxql.ListLiteral)
      $$ = stmt
  }
  |IN LPAREN TAG_KEYS RPAREN
  {
      stmt := &influxql.ShowTagValuesStatement{}
      stmt.Op = influxql.IN
      stmt.TagKeyExpr = $3.(*influxql.ListLiteral)
      $$ = stmt
  }
  |EQREGEX REGULAR_EXPRESSION
  {
       stmt := &influxql.ShowTagValuesStatement{}
       stmt.Op = influxql.EQREGEX
       re, err := regexp.Compile($2)
       	if err != nil {
       	    yylex.Error("Invalid regexprs")
       	}
       stmt.TagKeyExpr = &influxql.RegexLiteral{Val: re}
       $$ = stmt
  }
  |NEQREGEX REGULAR_EXPRESSION
  {
       stmt := &influxql.ShowTagValuesStatement{}
       stmt.Op = influxql.NEQREGEX
       re, err := regexp.Compile($2)
       	if err != nil {
       	    yylex.Error("Invalid regexprs")
       	}
       stmt.TagKeyExpr = &influxql.RegexLiteral{Val: re}
       $$ = stmt
  }


TAG_KEYS:
  TAG_KEY
  {
     temp := []string{$1}
     $$ = &influxql.ListLiteral{Vals: temp}
  }
  |TAG_KEY COMMA TAG_KEYS
  {
      $3.(*influxql.ListLiteral).Vals = append($3.(*influxql.ListLiteral).Vals, $1)
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
        stmt := &influxql.ExplainStatement{}
        stmt.Statement = $3.(*influxql.SelectStatement)
        stmt.Analyze = true
        $$ = stmt
    }
    |EXPLAIN SELECT_STATEMENT
    {
        stmt := &influxql.ExplainStatement{}
        stmt.Statement = $2.(*influxql.SelectStatement)
        stmt.Analyze = false
        $$ = stmt
    }


SHOW_TAG_KEY_CARDINALITY_STATEMENT:
    SHOW TAG KEY EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &influxql.ShowTagKeyCardinalityStatement{}
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
        stmt := &influxql.ShowTagKeyCardinalityStatement{}
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
         stmt := &influxql.ShowTagKeyCardinalityStatement{}
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
         stmt := &influxql.ShowTagKeyCardinalityStatement{}
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
        stmt := &influxql.ShowTagValuesCardinalityStatement{}
        stmt.Database = $6
        stmt.Exact = true
        stmt.Sources = $7
        stmt_temp := $10.(*influxql.ShowTagValuesStatement)
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
         stmt := &influxql.ShowTagValuesCardinalityStatement{}
         stmt.Database = $6
         stmt.Exact = true
         stmt_temp := $9.(*influxql.ShowTagValuesStatement)
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
        stmt := &influxql.ShowTagValuesCardinalityStatement{}
        stmt.Database = $5
        stmt.Exact = false
        stmt.Sources = $6
        stmt_temp := $9.(*influxql.ShowTagValuesStatement)
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
         stmt := &influxql.ShowTagValuesCardinalityStatement{}
         stmt.Database = $5
         stmt.Exact = true
         stmt_temp := $8.(*influxql.ShowTagValuesStatement)
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
        stmt := &influxql.ShowFieldKeyCardinalityStatement{}
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
        stmt := &influxql.ShowFieldKeyCardinalityStatement{}
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
         stmt := &influxql.ShowFieldKeyCardinalityStatement{}
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
         stmt := &influxql.ShowFieldKeyCardinalityStatement{}
         stmt.Database = $5
         stmt.Exact = true
         stmt.Condition = $6
         stmt.Dimensions = $7
         stmt.Limit = $8[0]
         stmt.Offset = $8[1]
         $$ = stmt
    }


CREATE_MEASUREMENT_STATEMENT:
    CREATE MEASUREMENT TABLE_CASE WITH INDEXTYPE INDEX_TYPES SHARDKEY SHARDKEYLIST TYPE_CALUSE
    {
        stmt := &influxql.CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        if $6 != nil {
            stmt.IndexType = $6.types
            stmt.IndexList = $6.lists
        }
        stmt.ShardKey = $8
        sort.Strings(stmt.ShardKey)
        stmt.Type = $9
        $$ = stmt
    }
    |CREATE MEASUREMENT TABLE_CASE WITH SHARDKEY SHARDKEYLIST TYPE_CALUSE
    {
        stmt := &influxql.CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        stmt.ShardKey = $6
        sort.Strings(stmt.ShardKey)
        stmt.Type = $7
        $$ = stmt
    }
    |CREATE MEASUREMENT TABLE_CASE WITH INDEXTYPE INDEX_TYPES
    {
         stmt := &influxql.CreateMeasurementStatement{}
         stmt.Database = $3.Database
         stmt.Name = $3.Name
         stmt.RetentionPolicy = $3.RetentionPolicy
         if $6 != nil {
               stmt.IndexType = $6.types
               stmt.IndexList = $6.lists
          }
          $$ = stmt
    }
    |CREATE MEASUREMENT TABLE_CASE
    {
        stmt := &influxql.CreateMeasurementStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        stmt.Type = "hash"
        $$ = stmt
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
        stmt := &influxql.DropShardStatement{}
        stmt.ID = uint64($3)
        $$ = stmt
    }

SET_PASSWORD_USER_STATEMENT:
    SET PASSWORD FOR IDENT EQ STRING
    {
        stmt := &influxql.SetPasswordUserStatement{}
        stmt.Name = $4
        stmt.Password = $6
        $$ = stmt
    }



SHOW_GRANTS_FOR_USER_STATEMENT:
    SHOW GRANTS FOR IDENT
    {
        stmt := &influxql.ShowGrantsForUserStatement{}
        stmt.Name = $4
        $$ = stmt
    }

SHOW_MEASUREMENT_CARDINALITY_STATEMENT:
    SHOW MEASUREMENT EXACT CARDINALITY ON_DATABASE FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_OFFSET_OPTION
    {
        stmt := &influxql.ShowMeasurementCardinalityStatement{}
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
        stmt := &influxql.ShowMeasurementCardinalityStatement{}
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
        stmt := &influxql.ShowMeasurementCardinalityStatement{}
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
        stmt := &influxql.ShowMeasurementCardinalityStatement{}
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
         stmt := &influxql.ShowSeriesCardinalityStatement{}
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
        stmt := &influxql.ShowSeriesCardinalityStatement{}
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
        stmt := &influxql.ShowSeriesCardinalityStatement{}
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
        stmt := &influxql.ShowSeriesCardinalityStatement{}
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
        stmt := &influxql.ShowShardsStatement{}
        $$ = stmt
    }


ALTER_SHARD_KEY_STATEMENT:
    ALTER MEASUREMENT TABLE_CASE WITH SHARDKEY SHARDKEYLIST TYPE_CALUSE
    {
        stmt := &influxql.AlterShardKeyStatement{}
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
        stmt := &influxql.AlterShardKeyStatement{}
        stmt.Database = $3.Database
        stmt.Name = $3.Name
        stmt.RetentionPolicy = $3.RetentionPolicy
        stmt.Type = "hash"
        $$ = stmt
    }




SHOW_SHARD_GROUPS_STATEMENT:
    SHOW SHARD GROUPS
    {
        stmt := &influxql.ShowShardGroupsStatement{}
        $$ = stmt
    }

DROP_MEASUREMENT_STATEMENT:
    DROP MEASUREMENT IDENT
    {
        stmt := &influxql.DropMeasurementStatement{}
        stmt.Name = $3
        $$ = stmt
    }



%%
