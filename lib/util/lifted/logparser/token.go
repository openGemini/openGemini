/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxql/blob/v1.1.0/token.go
*/

package logparser

import (
	"strings"
)

// Token is a lexical token of the InfluxQL language.
type Token int

// These are a comprehensive list of InfluxQL language tokens.
const (
	// ILLEGAL Token, EOF, WS are Special InfluxQL tokens.
	ILLEGAL Token = iota
	EOF
	WS

	BADSTRING // "abc
	BADESCAPE // \q
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:     "IDENT",
	STRING:    "STRING",
	BADSTRING: "BADSTRING",
	BADESCAPE: "BADESCAPE",

	BITWISE_OR: "|",

	EXTRACT: "EXTRACT",
	AS:      "AS",
	AND:     "AND",
	OR:      "OR",
	IN:      "IN",

	LPAREN:  "(",
	RPAREN:  ")",
	LSQUARE: "[",
	RSQUARE: "]",

	COMMA: ",",
	COLON: ":",
	EQ:    "=",
	LT:    "<",
	LTE:   "<=",
	GT:    ">",
	GTE:   ">=",
}

var keywords map[string]int

func init() {
	keywords = make(map[string]int)
	for _, tok := range []int{EXTRACT, AS, AND, OR, IN} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return Token(tok)
	}
	return IDENT
}

// Pos specifies the line and character position of a token.
// The Char and Line are both zero-based indexes.
type Pos struct {
	Line int
	Char int
}
