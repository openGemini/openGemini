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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
	EOF rune = 0
)

const (
	EOF_TOKEN int = iota
	WS_TOKEN
	BAD_STRING
	BAD_ESCAPE
	ILLEGAL_TOKEN
)

func isWhitespace(ch rune) bool {
	return ch == ' ' || ch == '\t' || ch == '\n'
}

func isChar(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isDigit(ch rune) bool {
	return (ch >= '0' && ch <= '9')
}

func isIdentChar(ch rune) bool {
	return isChar(ch) || isDigit(ch)
}

type Tokenizer struct {
	r        io.RuneScanner
	keywords map[string]int
}

func NewTokenizer(rd io.Reader) *Tokenizer {
	t := &Tokenizer{
		r:        bufio.NewReader(rd),
		keywords: make(map[string]int),
	}

	for i, name := range QLToknames {
		if i-3 < 0 {
			continue
		}
		t.keywords[strings.ToUpper(name)] = INSERT + i - 3
	}

	return t
}

func (t *Tokenizer) Scan() (int, string) {
	ch := t.peek()

	if isWhitespace(ch) {
		return t.scanWhiteSpace()
	} else if isChar(ch) {
		return t.scanIdentifier()
	} else if isDigit(ch) {
		return t.scanDigit()
	}

	switch ch {
	case EOF:
		return EOF_TOKEN, ""
	case '\'':
		return t.scanString()
	case '.':
		ch = t.read()
		return DOT, string(ch)
	case '=':
		ch = t.read()
		return EQ, string(ch)
	case ',':
		ch = t.read()
		return COMMA, string(ch)
	}
	return ILLEGAL_TOKEN, ""
}

func (t *Tokenizer) scanIdentifier() (int, string) {
	var buf bytes.Buffer

	for {
		ch := t.read()
		if ch == EOF {
			break
		} else if !isIdentChar(ch) {
			t.r.UnreadRune()
			break
		} else {
			buf.WriteRune(ch)
		}
	}

	tok := t.scanKeywords(buf.String())
	return tok, buf.String()
}

func (t *Tokenizer) scanKeywords(s string) int {
	if tok, ok := t.keywords[strings.ToUpper(s)]; ok {
		return tok
	}
	return IDENT
}

func (t *Tokenizer) scanString() (int, string) {
	end := t.read()

	var buf bytes.Buffer
	for {
		ch := t.read()
		if ch == end {
			return STRING, buf.String()
		} else if ch == EOF || ch == '\n' {
			return BAD_STRING, buf.String()
		} else if ch == '\\' {
			nch := t.read()
			if nch == 'n' {
				buf.WriteRune('\n')
			} else if nch == '\\' {
				buf.WriteRune('\\')
			} else if nch == '"' {
				buf.WriteRune('"')
			} else if nch == '\'' {
				buf.WriteRune('\'')
			} else {
				return BAD_ESCAPE, string(ch) + string(nch)
			}
		} else {
			buf.WriteRune(ch)
		}
	}
}

func (t *Tokenizer) scanDigit() (int, string) {
	var buf bytes.Buffer

	for {
		ch := t.read()
		if ch == EOF {
			break
		} else if !isDigit(ch) {
			t.r.UnreadRune()
			break
		} else {
			buf.WriteRune(ch)
		}
	}
	return DIGIT, buf.String()
}

func (t *Tokenizer) scanWhiteSpace() (int, string) {
	var buf bytes.Buffer

	for {
		ch := t.read()
		if ch == EOF {
			break
		} else if !isWhitespace(ch) {
			t.r.UnreadRune()
			break
		} else {
			buf.WriteRune(ch)
		}
	}
	return WS_TOKEN, buf.String()
}

func (t *Tokenizer) peek() rune {
	ch, _, err := t.r.ReadRune()
	defer t.r.UnreadRune()

	if err != nil {
		ch = EOF
	}
	return ch
}

func (t *Tokenizer) read() rune {
	ch, _, err := t.r.ReadRune()

	if err != nil {
		ch = EOF
	}
	return ch
}

type QLLexerImpl struct {
	tokenizer *Tokenizer
	ast       *CmdAst
}

func QLNewLexer(tokenizer *Tokenizer, ast *CmdAst) QLLexer {
	l := &QLLexerImpl{
		tokenizer: tokenizer,
		ast:       ast,
	}

	return l
}

func (l *QLLexerImpl) Lex(lval *QLSymType) int {
	var tok int
	var val string

	for {
		tok, val = l.tokenizer.Scan()
		switch tok {
		case ILLEGAL_TOKEN:
			l.Error(fmt.Sprintf("illegal token %s", val))
		case EOF_TOKEN:
			return 0
		case BAD_STRING:
			l.Error(fmt.Sprintf("bad string %s", val))
		case BAD_ESCAPE:
			l.Error(fmt.Sprintf("bad escape %s", val))
		}

		if tok != WS_TOKEN {
			break
		}
	}

	lval.str = val
	return tok
}

func (l *QLLexerImpl) Error(s string) {
	l.ast.Error = fmt.Errorf("ERR: %s", s)
}

func (l *QLLexerImpl) UpdateStmt(stmt Statement) {
	l.ast.Stmt = stmt
}
