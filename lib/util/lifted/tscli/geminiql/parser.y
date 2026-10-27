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

package geminiql

import (
	"strconv"
)

func updateStmt(QLlex interface{}, stmt Statement) {
    QLlex.(*QLLexerImpl).UpdateStmt(stmt)
}

%}

// fields inside this union end up as the fields in a structure known
// as ${PREFIX}SymType, of which a reference is passed to the lexer.
%union{
    stmts []Statement
    stmt Statement
    str string
    strslice []string
    integer int64
    decimal float64
    pair Pair
    pairs Pairs
}

// any non-terminal which returns a value needs a type, which is
// really a field name in the above union struct
%type <stmts> STATEMENTS
%type <stmt> INSERT_STATEMENT USE_STATEMENT SET_STATEMENT CHUNKED_STATEMENT CHUNK_SIZE_STATEMENT AUTH_STATEMENT HELP_STATEMENT PRECISION_STATEMENT TIMER_STATEMENT DEBUG_STATEMENT PROMPT_STATEMENT VERTICAL_STATEMENT
%type <str> LINE_PROTOCOL TIME_SERIE MEASUREMENT KV_RAW KV_RAWS TIME
%type <integer> NUM_CHUNK_SIZE
%type <strslice> NAMESPACE
%type <pair> KEY_VALUE
%type <pairs> KEY_VALUES

// same for terminals
%token <str> INSERT INTO USE SET CHUNKED CHUNK_SIZE AUTH HELP PRECISION TIMER DEBUG PROMPT VERTICAL
%token <str> DOT COMMA
%token <str> EQ
%token <str> IDENT
%token <integer> INTEGER
%token <decimal> DECIMAL
%token <str> STRING
%token <str> RAW

%%

STATEMENTS:
    INSERT_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |USE_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |SET_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |CHUNKED_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |CHUNK_SIZE_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |AUTH_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |HELP_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |PRECISION_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |TIMER_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |DEBUG_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |PROMPT_STATEMENT
    {
        updateStmt(QLlex, $1)
    }
    |VERTICAL_STATEMENT
    {
        updateStmt(QLlex, $1)
    }

SET_STATEMENT:
    SET KEY_VALUES
    {
        stmt := &SetStatement{}
        stmt.KVS = $2
        $$ = stmt
    }

USE_STATEMENT:
    USE NAMESPACE
    {
        stmt := &UseStatement{}
        if len($2) == 1 {
            stmt.DB = $2[0]
            $$ = stmt
        } else if len($2) == 2 {
            stmt.DB = $2[0]
            stmt.RP = $2[1]
            $$ = stmt
        } else {
            QLlex.Error("namespace must be <db>.<rp>")
        }
    }

INSERT_STATEMENT:
    INSERT INTO NAMESPACE LINE_PROTOCOL
    {
        stmt := &InsertStatement{}
        stmt.LineProtocol = $4

        if len($3) != 2 {
            QLlex.Error("namespace must be <db>.<rp>")
        } else {
            stmt.DB = $3[0]
            stmt.RP = $3[1]
            $$ = stmt
        }
    }
    |INSERT LINE_PROTOCOL
    {
        stmt := &InsertStatement{}
        stmt.LineProtocol = $2
        $$ = stmt
    }

CHUNKED_STATEMENT:
    CHUNKED
    {
        stmt := &ChunkedStatement{}
        $$ = stmt
    }

CHUNK_SIZE_STATEMENT:
    CHUNK_SIZE NUM_CHUNK_SIZE
    {
        stmt := &ChunkSizeStatement{}
        stmt.Size = $2
        $$ = stmt
    }

NUM_CHUNK_SIZE:
    INTEGER
    {
        $$ = $1
    }

AUTH_STATEMENT:
    AUTH
    {
        stmt := &AuthStatement{}
        $$ = stmt
    }

HELP_STATEMENT:
    HELP
    {
        stmt := &HelpStatement{}
        $$ = stmt
    }

PRECISION_STATEMENT:
    PRECISION IDENT
    {
        stmt := &PrecisionStatement{}
        stmt.Precision = $2
        $$ = stmt
    }

TIMER_STATEMENT:
    TIMER
    {
        stmt := &TimerStatement{}
        $$ = stmt
    }

DEBUG_STATEMENT:
    DEBUG
    {
        stmt := &DebugStatement{}
        $$ = stmt
    }

PROMPT_STATEMENT:
    PROMPT
    {
        stmt := &PromptStatement{}
        $$ = stmt
    }

VERTICAL_STATEMENT:
    VERTICAL
    {
        stmt := &VerticalStatement{}
        $$ = stmt
    }

NAMESPACE:
    IDENT
    {
        $$ = []string{$1}
    }
    |IDENT DOT NAMESPACE
    {
        ns := []string{$1}
        $$ = append(ns, $3...)
    }

LINE_PROTOCOL:
    TIME_SERIE
    {
        $$ = $1
    }
    |TIME_SERIE TIME
    {
        $$ = $1 + " " + $2
    }

TIME_SERIE:
    MEASUREMENT COMMA KV_RAWS KV_RAWS
    {
        $$ = $1 + $2 + $3 + " " + $4
    }
    |MEASUREMENT KV_RAWS
    {
        $$ = $1 + " " + $2
    }

KEY_VALUE:
    IDENT EQ IDENT
    {
        p := NewPair($1, $3)
        $$ = *p
    }
    |IDENT EQ STRING
    {
        p := NewPair($1, $3)
        $$ = *p
    }
    |IDENT EQ INTEGER
    {
        p := NewPair($1, $3)
        $$ = *p
    }
    |IDENT EQ DECIMAL
    {
        p := NewPair($1, $3)
        $$ = *p
    }

KEY_VALUES:
    KEY_VALUE
    {
        $$ = Pairs{$1}
    }
    |KEY_VALUE COMMA KEY_VALUES
    {
        $$ = append($3, $1)
    }

KV_RAWS:
    KV_RAW
    {
        $$ = $1
    }
    |KV_RAW COMMA KV_RAWS
    {
        $$ = $1 + $2 + $3
    }

KV_RAW:
    IDENT EQ RAW
    {
        $$ = $1 + $2 + $3
    }

MEASUREMENT:
    IDENT
    {
        $$ = $1
    }

TIME:
    INTEGER
    {
        $$ = strconv.FormatInt($1, 10)
    }

%%
