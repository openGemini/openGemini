%{
/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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

package logparser

import (
	"github.com/openGemini/openGemini/open_src/influx/influxql"
)

func setParseTree(yylex interface{}, stmts influxql.Statements) {
    for _,stmt :=range stmts{
        yylex.(*YyParser).Query.Statements = append(yylex.(*YyParser).Query.Statements, stmt)
    }
}

%}

%union{
    stmt                influxql.Statement
    stmts               influxql.Statements
    str                 string
    query               influxql.Query
    int                 int
    int64               int64
    float64             float64
    expr                influxql.Expr
    exprs               []influxql.Expr
    unnest              *influxql.Unnest
    strSlice            []string
}

%token <str>    EXTRACT AS LPAREN RPAREN
%token <str>    IDENT
%token <str>    STRING

%left  <int>    OR
%left  <int>    AND  BITWISE_OR
%left  <int>    COLON COMMA

%type <stmt>            STATEMENT BITWISE_OR_CONDITION
%type <stmts>           ALL_QUERIES ALL_QUERY
%type <expr>            CONDITION  CONDITION_COLUMN COLUMN COLUMN_VAREF COLUMN_SEMI CONDTION_CLAUSE BAND
%type <strSlice>        COLUMN_VAREFS
%type <unnest>          EXTRACT_CLAUSE

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

STATEMENT:
    BITWISE_OR_CONDITION
    {
        $$ = $1
    }

BITWISE_OR_CONDITION:
    BITWISE_OR_CONDITION BITWISE_OR BITWISE_OR_CONDITION
    {
        cond1, ok := $1.(*influxql.LogPipeStatement)
        if !ok {
            yylex.Error("expexted LogPipeStatement")
        }
        cond2, ok := $3.(*influxql.LogPipeStatement)
        if !ok {
            yylex.Error("expexted LogPipeStatement")
        }

        var unnest *influxql.Unnest
        if cond1.Unnest != nil && cond2.Unnest != nil {
            yylex.Error("only one extract statement is supported")
        } else {
            if cond1.Unnest != nil {
                unnest = cond1.Unnest
            } else {
                unnest = cond2.Unnest
            }
        }

        var cond influxql.Expr
        if cond1.Cond != nil && cond2.Cond != nil {
            cond = &influxql.BinaryExpr{Op:influxql.Token(influxql.AND),LHS:cond1.Cond, RHS:cond2.Cond,}
        } else {
            if cond1.Cond != nil {
                cond = cond1.Cond
            } else {
                cond = cond2.Cond
            }
        }

        $$ =  &influxql.LogPipeStatement{
            Cond: cond,
            Unnest: unnest}
    }
    | EXTRACT_CLAUSE
    {
        $$ = &influxql.LogPipeStatement{Unnest: $1}
    }
    |CONDTION_CLAUSE
    {
        $$ = &influxql.LogPipeStatement{Cond: $1}
    }

CONDTION_CLAUSE:
    CONDITION
    {
        $$ = $1
    }

CONDITION:
    CONDITION_COLUMN
    {
        $$ = $1
    }
    |LPAREN CONDITION RPAREN
    {
    	$$ = &influxql.ParenExpr{Expr:$2}
    }
    |CONDITION AND CONDITION
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.AND),LHS:$1,RHS:$3}
    }
    |CONDITION OR CONDITION
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.OR),LHS:$1,RHS:$3}
    }
 
CONDITION_COLUMN:
    COLUMN
    {
    	$$ = $1
    }
    |BAND
    {
        $$ = $1
    }

COLUMN:
    COLUMN_SEMI
    {
        $$ = $1
    }

BAND:
    COLUMN_SEMI COLUMN_SEMI
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.AND),LHS:$1,RHS:$2}
    }
    | BAND COLUMN_SEMI
    {
        $$ = &influxql.BinaryExpr{Op:influxql.Token(influxql.AND),LHS:$1,RHS:$2}
    }

EXTRACT_CLAUSE:
    EXTRACT LPAREN COLUMN_SEMI RPAREN AS LPAREN COLUMN_VAREFS RPAREN
    {
        unnest := &influxql.Unnest{}
        unnest.Mst = &influxql.Measurement{}
        unnest.CastFunc = "cast"

        columnsemi, ok := $3.(*influxql.BinaryExpr)
        if !ok {
            yylex.Error("expexted BinaryExpr")
        }
        unnest.ParseFunc = &influxql.Call{
            Name: "match_all",
            Args: []influxql.Expr{
                &influxql.VarRef{Val:columnsemi.RHS.(*influxql.StringLiteral).Val},
                columnsemi.LHS},
        }

        unnest.DstColumns = []string{}
        dstFunc := &influxql.Call{
            Name: "array",
            Args: []influxql.Expr{},
        }
        for _, f := range $7 {
            unnest.DstColumns = append(unnest.DstColumns, f)
            dstFunc.Args = append(dstFunc.Args, &influxql.VarRef{Val:"varchar"})
        }
        unnest.DstFunc =dstFunc

        $$ = unnest
    }

COLUMN_SEMI:
    COLUMN_VAREF
    {
        var expr influxql.Expr
        switch $1.(type) {
        case *influxql.Wildcard:
            expr = &influxql.BinaryExpr{Op:influxql.Token(influxql.NEQ),LHS:&influxql.VarRef{Val:"content", Type:influxql.String},RHS:&influxql.StringLiteral{Val:""}}
        case *influxql.VarRef:
            expr = &influxql.BinaryExpr{Op:influxql.Token(influxql.MATCHPHRASE),LHS:&influxql.VarRef{Val:"content", Type:influxql.String},RHS:&influxql.StringLiteral{Val:$1.(*influxql.VarRef).Val}}
        default:
           expr = &influxql.BinaryExpr{Op:influxql.Token(influxql.MATCHPHRASE),LHS:&influxql.VarRef{Val:"content", Type:influxql.String},RHS: $1}
        }
        $$ = expr
    }
    |
    COLUMN_VAREF COLON COLUMN_VAREF
    {
        var field influxql.Expr
        if strVal, ok := $1.(*influxql.StringLiteral); ok {
            field = &influxql.VarRef{Val:strVal.Val}
        } else {
            field = $1
        }

        var expr influxql.Expr
        switch $3.(type) {
        case *influxql.Wildcard:
            expr = &influxql.BinaryExpr{Op:influxql.Token(influxql.NEQ), LHS:field, RHS:&influxql.StringLiteral{Val:""}}
        case *influxql.VarRef:
            expr = &influxql.BinaryExpr{Op:influxql.Token(influxql.MATCHPHRASE), LHS:field, RHS:&influxql.StringLiteral{Val:$3.(*influxql.VarRef).Val}}
        default:
           expr = &influxql.BinaryExpr{Op:influxql.Token(influxql.MATCHPHRASE), LHS:field, RHS: $3}
        }
        $$ = expr
    }

COLUMN_VAREFS:
   COLUMN_VAREF
   {
       if _, ok := $1.(*influxql.VarRef); ok {
           $$ = []string{$1.(*influxql.VarRef).Val}
       } else {
           $$ = []string{$1.(*influxql.StringLiteral).Val}
       }
   }
   |COLUMN_VAREF COMMA COLUMN_VAREFS
   {
       if _, ok := $1.(*influxql.VarRef); ok {
           $$ = append([]string{$1.(*influxql.VarRef).Val}, $3...)
       } else {
           $$ = append([]string{$1.(*influxql.StringLiteral).Val}, $3...)
       }
   }

COLUMN_VAREF:
    IDENT
    {
        if $1 == "*" {
	        $$ = &influxql.Wildcard{Type:influxql.MUL}
	    }else{
            $$ = &influxql.VarRef{Val:$1, Type:influxql.String}
        }
    }
    |STRING
    {
        $$ = &influxql.StringLiteral{Val:$1}
    }
