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
	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func setParseTree(yylex interface{}, stmts influxql.Statements) {
    for _,stmt :=range stmts{
        yylex.(*YyParser).Query.Statements = append(yylex.(*YyParser).Query.Statements, stmt)
    }
}

func transCondOpToInflux(op int) int {
    switch op {
        case EQ: 
            return influxql.MATCHPHRASE
        case LT:
            return influxql.LT
        case LTE:
            return influxql.LTE
        case GT:
            return influxql.GT
        case GTE:
            return influxql.GTE
        case NEQ:
            return influxql.NEQ
        default:
            return influxql.NEQ
    }
}

func buildCondExpr(first influxql.Expr, op int, right influxql.Expr) influxql.Expr {
	var field influxql.Expr
	if first != nil {
		if strVal, ok := first.(*influxql.StringLiteral); ok {
			field = &influxql.VarRef{Val: strVal.Val}
		} else {
			field = first
		}
	} else {
		field = &influxql.VarRef{Val: "content"}
	}

	newOp := transCondOpToInflux(op)
	var expr influxql.Expr
	switch right.(type) {
	case *influxql.Wildcard:
		expr = &influxql.BinaryExpr{
			Op:  influxql.NEQ,
			LHS: field,
			RHS: &influxql.StringLiteral{Val: ""},
		}
	default:
		expr = &influxql.BinaryExpr{
			Op:  influxql.Token(newOp),
			LHS: field,
			RHS: right,
		}
	}
	return expr
}

func buildRangeExpr(first influxql.Expr, lop int, left influxql.Expr, rop int, right influxql.Expr) influxql.Expr {
	var field influxql.Expr
	if strVal, ok := first.(*influxql.StringLiteral); ok {
		field = &influxql.VarRef{Val: strVal.Val}
	} else {
		field = first
	}
	res := &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.Token(lop),
			LHS: field,
			RHS: left,
		},
		RHS: &influxql.BinaryExpr{
			Op:  influxql.Token(rop),
			LHS: field,
			RHS: right,
		},
	}
	return res
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

%token <str>    EXTRACT AS LPAREN RPAREN LSQUARE RSQUARE IN
%token <int>    EQ LT LTE GT GTE NEQ

%token <str>    IDENT
%token <str>    STRING

%left  <int>    OR
%left  <int>    AND  BITWISE_OR
%left  <int>    COLON COMMA

%type <int>             CONDITION_OPERATOR
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

        columnsemi, ok := $3.(*influxql.BinaryExpr)
        if !ok {
            yylex.Error("expexted BinaryExpr")
        }
        unnest.Expr = &influxql.Call{
            Name: "match_all",
            Args: []influxql.Expr{
                &influxql.VarRef{Val:columnsemi.RHS.(*influxql.StringLiteral).Val},
                columnsemi.LHS},
        }
        unnest.Aliases = []string{}
        for _, alias := range $7 {
            unnest.Aliases = append(unnest.Aliases, alias)
            unnest.DstType = append(unnest.DstType, influxql.String)
        }

        $$ = unnest
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


COLUMN_SEMI:
    COLUMN_VAREF
    {
       expr := buildCondExpr(nil, EQ, $1)
        $$ = expr
    }
    | COLUMN_VAREF COLON COLUMN_VAREF
    {
        expr := buildCondExpr($1, EQ, $3)
        $$ = expr
    }
    | COLUMN_VAREF CONDITION_OPERATOR COLUMN_VAREF
    {
        expr := buildCondExpr($1, $2, $3)
        $$ = expr
    }
    | COLUMN_VAREF IN LPAREN COLUMN_VAREF COLUMN_VAREF RPAREN
    {
        expr := buildRangeExpr($1, influxql.GT, $4, influxql.LT, $5)
        $$ = expr
    }
    | COLUMN_VAREF IN LPAREN COLUMN_VAREF COLUMN_VAREF RSQUARE
    {
        expr := buildRangeExpr($1, influxql.GT, $4, influxql.LTE, $5)
        $$ = expr
    }
    | COLUMN_VAREF IN LSQUARE COLUMN_VAREF COLUMN_VAREF RPAREN
    {
        expr := buildRangeExpr($1, influxql.GTE, $4, influxql.LT, $5)
        $$ = expr
    }
    | COLUMN_VAREF IN LSQUARE COLUMN_VAREF COLUMN_VAREF RSQUARE
    {
        expr := buildRangeExpr($1, influxql.GTE, $4, influxql.LTE, $5)
        $$ = expr
    }

COLUMN_VAREF:
    IDENT
    {
        if $1 == "*" {
            $$ = &influxql.Wildcard{Type:influxql.MUL}
        }else{
            $$ = &influxql.StringLiteral{Val:$1}
        }
    }
    |STRING
    {
        $$ = &influxql.StringLiteral{Val:$1}
    }

CONDITION_OPERATOR:
    EQ
    {
        $$ = EQ
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