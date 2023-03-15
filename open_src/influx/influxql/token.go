package influxql

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxql/blob/v1.1.0/token.go

2022.01.23 Add new tokens: PARTITION, PREPARE, SNAPSHOT, GET, RUNTIMEINFO, HINT, HOT, WARM, INDEX.
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
*/

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
	COMMENT

	literalBeg
	// IDENT and the following are InfluxQL literal tokens.
	//IDENT       // main
	BOUNDPARAM // $param
	//NUMBER      // 12345.67
	//INTEGER     // 12345
	//DURATIONVAL // 13h
	//STRING      // "abc"
	BADSTRING // "abc
	BADESCAPE // \q
	//TRUE      // true
	//FALSE     // false
	//REGEX    // Regular expressions
	BADREGEX // `.*
	literalEnd

	operatorBeg
	// ADD and the following are InfluxQL Operators
	//ADD // +
	//SUB // -
	//MUL         // *
	//DIV         // /
	//MOD // %
	//BITWISE_AND // &
	//BITWISE_OR  // |
	//BITWISE_XOR // ^

	//AND // AND
	//OR  // OR

	//EQ       // =
	//NEQ      // !=
	//EQREGEX  // =~
	//NEQREGEX // !~
	//LT       // <
	//LTE      // <=
	//GT       // >
	//GTE      // >=
	operatorEnd

	//LPAREN // (
	//RPAREN // )
	//COMMA       // ,
	COLON // :
	//DOUBLECOLON // ::
	//SEMICOLON   // ;
	//DOT // .

	keywordBeg
	// ALL and the following are InfluxQL Keywords
	//ALL
	//ALTER
	//ANALYZE
	ANY
	//AS
	//ASC
	//BEGIN //CREATE CONTINUOUS QUERY ON "telegraf" BEGIN
	//BY
	//CARDINALITY
	//CREATE
	//CONTINUOUS // ContinuousQuery
	//DATABASE
	//DATABASES
	//DEFAULT
	//DELETE
	//DESC
	DESTINATIONS

	//DIAGNOSTICS  // SHOW DIAGNOSTICS
	DISTINCT //distinct()
	//DROP
	//DURATION
	//END
	//EVERY
	//EXACT
	//EXPLAIN
	//FIELD
	//FOR  //SHOW GRANTS FOR "jdoe"
	//FROM
	//GRANT
	//GRANTS
	//GROUP
	//GROUPS
	//IN
	INF
	INSERT
	//INTO
	//KEY
	//KEYS
	KILL
	//LIMIT
	//MEASUREMENT
	//MEASUREMENTS
	//NAME
	//OFFSET
	//ON
	//ORDER
	//PASSWORD
	//POLICY
	//POLICIES
	//PRIVILEGES
	//QUERIES
	//QUERY
	READ //privilege        = "ALL" [ "PRIVILEGES" ] | "READ" | "WRITE" .
	//REPLICATION
	//RESAMPLE
	//RETENTION
	//REVOKE
	//SELECT
	//SERIES
	//SET
	//SHOW
	//SHARD
	//SHARDKEY
	//SHARDS
	//SLIMIT
	//SOFFSET
	//STATS
	//SUBSCRIPTION
	//SUBSCRIPTIONS
	//TYPE
	//TAG
	//TO
	//USER
	//USERS
	//VALUES
	//WHERE
	//WITH
	WRITE
	//PARTITION
	PREPARE
	SNAPSHOT
	GET
	RUNTIMEINFO
	//HINT
	//HOT
	//WARM
	//INDEX
	keywordEnd
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:       "IDENT",
	NUMBER:      "NUMBER",
	DURATIONVAL: "DURATIONVAL",
	STRING:      "STRING",
	BADSTRING:   "BADSTRING",
	BADESCAPE:   "BADESCAPE",
	TRUE:        "TRUE",
	FALSE:       "FALSE",
	REGEX:       "REGEX",

	ADD:         "+",
	SUB:         "-",
	MUL:         "*",
	DIV:         "/",
	MOD:         "%",
	BITWISE_AND: "&",
	BITWISE_OR:  "|",
	BITWISE_XOR: "^",

	AND: "AND",
	OR:  "OR",

	EQ:       "=",
	NEQ:      "!=",
	EQREGEX:  "=~",
	NEQREGEX: "!~",
	LT:       "<",
	LTE:      "<=",
	GT:       ">",
	GTE:      ">=",

	LPAREN:      "(",
	RPAREN:      ")",
	COMMA:       ",",
	COLON:       ":",
	DOUBLECOLON: "::",
	SEMICOLON:   ";",
	DOT:         ".",

	ALL:            "ALL",
	ALTER:          "ALTER",
	ANALYZE:        "ANALYZE",
	ANY:            "ANY",
	AS:             "AS",
	ASC:            "ASC",
	TOKEN:          "TOKEN",
	TOKENIZERS:     "TOKENIZERS",
	LIKE:           "LIKE",
	MATCH:          "MATCH",
	MATCH_PHRASE:   "MATCH_PHRASE",
	BEGIN:          "BEGIN",
	BY:             "BY",
	CARDINALITY:    "CARDINALITY",
	CREATE:         "CREATE",
	CONTINUOUS:     "CONTINUOUS",
	DATABASE:       "DATABASE",
	DATABASES:      "DATABASES",
	DEFAULT:        "DEFAULT",
	DELETE:         "DELETE",
	DESC:           "DESC",
	DESTINATIONS:   "DESTINATIONS",
	DIAGNOSTICS:    "DIAGNOSTICS",
	DISTINCT:       "DISTINCT",
	DROP:           "DROP",
	DURATION:       "DURATION",
	CASE:           "CASE",
	WHEN:           "WHEN",
	THEN:           "THEN",
	ELSE:           "ELSE",
	END:            "END",
	EVERY:          "EVERY",
	EXACT:          "EXACT",
	EXPLAIN:        "EXPLAIN",
	FIELD:          "FIELD",
	FOR:            "FOR",
	FROM:           "FROM",
	GRANT:          "GRANT",
	GRANTS:         "GRANTS",
	GROUP:          "GROUP",
	GROUPS:         "GROUPS",
	IN:             "IN",
	NOT:            "NOT",
	EXISTS:         "EXISTS",
	INF:            "INF",
	INSERT:         "INSERT",
	INTO:           "INTO",
	KEY:            "KEY",
	KEYS:           "KEYS",
	KILL:           "KILL",
	LIMIT:          "LIMIT",
	MEASUREMENT:    "MEASUREMENT",
	MEASUREMENTS:   "MEASUREMENTS",
	NAME:           "NAME",
	OFFSET:         "OFFSET",
	ON:             "ON",
	ORDER:          "ORDER",
	PASSWORD:       "PASSWORD",
	POLICY:         "POLICY",
	POLICIES:       "POLICIES",
	PRIVILEGES:     "PRIVILEGES",
	QUERIES:        "QUERIES",
	QUERY:          "QUERY",
	READ:           "READ",
	REPLICATION:    "REPLICATION",
	RESAMPLE:       "RESAMPLE",
	RETENTION:      "RETENTION",
	REVOKE:         "REVOKE",
	SELECT:         "SELECT",
	SERIES:         "SERIES",
	SET:            "SET",
	SHOW:           "SHOW",
	SHARD:          "SHARD",
	SHARDKEY:       "SHARDKEY",
	SHARDS:         "SHARDS",
	SLIMIT:         "SLIMIT",
	SOFFSET:        "SOFFSET",
	STATS:          "STATS",
	SUBSCRIPTION:   "SUBSCRIPTION",
	SUBSCRIPTIONS:  "SUBSCRIPTIONS",
	TYPE:           "TYPE",
	TAG:            "TAG",
	TO:             "TO",
	USER:           "USER",
	USERS:          "USERS",
	VALUES:         "VALUES",
	WHERE:          "WHERE",
	WITH:           "WITH",
	WRITE:          "WRITE",
	PARTITION:      "PARTITION",
	PREPARE:        "PREPARE",
	SNAPSHOT:       "SNAPSHOT",
	GET:            "GET",
	RUNTIMEINFO:    "RUNTIMEINFO",
	HINT:           "HINT",
	HOT:            "HOT",
	WARM:           "WARM",
	INDEX:          "INDEX",
	FULL:           "FULL",
	OUTER:          "OUTER",
	JOIN:           "JOIN",
	FILL:           "FILL",
	REPLICANUM:     "REPLICANUM",
	INDEXTYPE:      "INDEXTYPE",
	INDEXLIST:      "INDEXLIST",
	DOWNSAMPLE:     "DOWNSAMPLE",
	DOWNSAMPLES:    "DOWNSAMPLES",
	SAMPLEINTERVAL: "SAMPLEINTERVAL",
	TIMEINTERVAL:   "TIMEINTERVAL",
	STREAM:         "STREAM",
	STREAMS:        "STREAMS",
	DELAY:          "DELAY",
}

var keywords map[string]int

func init() {
	keywords = make(map[string]int)
	for tok := FROM; tok <= ASC; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []int{AND, OR} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	/*	keywords["true"] = TRUE
		keywords["false"] = FALSE*/
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

var operatorMap = map[Token]int{
	OR:       OR,
	AND:      AND,
	EQ:       EQ,
	NEQ:      NEQ,
	EQREGEX:  EQREGEX,
	NEQREGEX: NEQREGEX,
	LT:       LT,
	LTE:      LTE,
	GT:       GT,
	GTE:      GTE,
	ADD:      ADD,
	SUB:      SUB,
	MUL:      MUL,
	DIV:      DIV,
	MOD:      1, //fixme
}

// Precedence returns the operator precedence of the binary operator token.
func (tok Token) Precedence() int {
	switch tok {
	case OR:
		return 1
	case AND:
		return 2
	case EQ, NEQ, EQREGEX, NEQREGEX, LT, LTE, GT, GTE:
		return 3
	case ADD, SUB, BITWISE_OR, BITWISE_XOR:
		return 4
	case MUL, DIV, MOD, BITWISE_AND:
		return 5
	}
	return 0
}

// isOperator returns true for operator tokens.
func (tok Token) isOperator() bool {
	_, ok := operatorMap[tok]
	return ok
}

// tokstr returns a literal if provided, otherwise returns the token string.
func tokstr(tok Token, lit string) string {
	if lit != "" {
		return lit
	}
	return tok.String()
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
