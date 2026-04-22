/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxql/blob/v1.1.0/parser.go
*/

package logparser

import (
	"io"
	"sync"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

const DefaultFieldForFullText = "__log___"

// Parser represents an InfluxQL parser.
type Parser struct {
	s *bufScanner
}

var parserPool = sync.Pool{}

// NewParser returns a new instance of Parser.
func NewParser(r io.Reader) *Parser {
	v := parserPool.Get()
	if v != nil {
		p := v.(*Parser)
		p.reset(r)
		return p
	}
	return &Parser{s: newBufScanner(r)}
}

func (p *Parser) GetScanner() *Scanner {
	return p.s.s
}

func (p *Parser) reset(r io.Reader) {
	p.s.reset(r)
}

func (p *Parser) Release() {
	parserPool.Put(p)
}

type YyParser struct {
	Query   influxql.Query
	Scanner *Scanner
	error   YyParserError
}

type YyParserError string

func (p YyParserError) Error() string {
	return string(p)
}

func NewYyParser(s *Scanner) YyParser {
	return YyParser{Scanner: s}
}

func (p *YyParser) ParseTokens() {
	yyParse(p)
}

func (p *YyParser) SetScanner(s *Scanner) {
	p.Scanner = s
}

func (p *YyParser) GetQuery() (*influxql.Query, error) {
	if len(p.error) > 0 {
		return &p.Query, p.error
	}
	return &p.Query, nil
}

func (p *YyParser) Lex(lval *yySymType) int {
	var typ Token
	var val string

	for {
		typ, _, val = p.Scanner.Scan()
		switch typ {
		case ILLEGAL:
			p.Error("unexpected " + string(val) + ", it's ILLEGAL")
		case EOF:
			return 0
		case AND:
			{
				lval.int = int(AND)
			}
		case OR:
			{
				lval.int = int(OR)
			}
		}
		if typ != WS {
			break
		}
	}
	lval.str = val
	return int(typ)
}

func (p *YyParser) Error(err string) {
	p.error = YyParserError(err)
}

func (p *YyParser) HasError() bool {
	return len(p.error) > 0
}
