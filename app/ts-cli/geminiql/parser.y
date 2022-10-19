// Copyright 2011 Bobby Powers. All rights reserved.
// Use of this source code is governed by the MIT
// license that can be found in the LICENSE file.

// based off of Appendix A from http://dinosaur.compilertools.net/yacc/

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
}

// any non-terminal which returns a value needs a type, which is
// really a field name in the above union struct
%type <stmts> STATEMENTS
%type <stmt> INSERT_STATEMENT USE_STATEMENT
%type <str> LINE_PROTOCOL TIME_SERIES KEY_VALUES KEY_VALUE MEASUREMENT
%type <strslice> NAMESPACE

// same for terminals
%token <str> INSERT INTO USE
%token <str> DOT COMMA
%token <str> EQ
%token <str> IDENT
%token <str> DIGIT
%token <str> STRING

%%

STATEMENTS:
    INSERT_STATEMENT
    {

    }
    | USE_STATEMENT
    {
        
    }

USE_STATEMENT:
    USE NAMESPACE
    {
        stmt := &UseStatement{}
        if len($2) == 1 {
            stmt.DB = $2[0]
            $$ = stmt
            updateStmt(QLlex, $$)
        } else if len($2) == 2 {
            stmt.DB = $2[0]
            stmt.RP = $2[1]
            $$ = stmt
            updateStmt(QLlex, $$)
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
            updateStmt(QLlex, $$)
        }
    }
    | INSERT INTO LINE_PROTOCOL
    {
        stmt := &InsertStatement{}
        stmt.LineProtocol = $3
        $$ = stmt
        updateStmt(QLlex, $$)
    }

NAMESPACE:
    IDENT
    {
        $$ = []string{$1}
    }
    | IDENT DOT NAMESPACE
    {
        ns := []string{$1}
        $$ = append(ns, $3...)
    }

LINE_PROTOCOL:
    TIME_SERIES DIGIT
    {
        $$ = $1 + " " + $2
    }
    | TIME_SERIES STRING
    {
        $$ = $1 + " " + $2
    }

TIME_SERIES:
    MEASUREMENT COMMA KEY_VALUES KEY_VALUES
    {
        $$ = $1 + $2 + $3 + " " + $4
    }

KEY_VALUE:
    IDENT EQ IDENT
    {
        $$ = $1 + $2 + $3
    }
    | IDENT EQ DIGIT
    {
        $$ = $1 + $2 + $3
    }
    | IDENT EQ STRING
    {
        $$ = $1 + $2 + $3
    }

KEY_VALUES:
    KEY_VALUE
    {
        $$ = $1
    }
    | KEY_VALUE COMMA KEY_VALUES
    {
        $$ = $1 + $2 + $3
    }

MEASUREMENT:
    IDENT
    {
        $$ = $1
    }
%%