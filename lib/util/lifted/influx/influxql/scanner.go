package influxql

/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxql/blob/v1.1.0/scanner.go
*/

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// Scanner represents a lexical scanner for InfluxQL.
type Scanner struct {
	r        *reader
	preToken Token
	checkDOT bool

	buf [64]bytes.Buffer
	idx int
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: &reader{r: bufio.NewReader(r)}}
}

func (s *Scanner) allocBuf() *bytes.Buffer {
	if s.idx >= len(s.buf) {
		return &bytes.Buffer{}
	}

	buf := &s.buf[s.idx]
	buf.Reset()
	s.idx++
	return buf
}

func (s *Scanner) reset(r io.Reader) {
	s.r.r.(*bufio.Reader).Reset(r)
	s.r.i = 0
	s.r.n = 0
	s.r.pos.Char = 0
	s.r.pos.Line = 0
	for i := range s.r.buf {
		s.r.buf[i].ch = 0
		s.r.buf[i].pos.Char = 0
		s.r.buf[i].pos.Line = 0
	}
	s.r.eof = false
	s.idx = 0
}

// Scan returns the next token and position from the underlying reader.
// Also returns the literal text read for strings, numbers, and duration tokens
// since these token types can have different literal representations.
func (s *Scanner) Scan() (tok Token, pos Pos, lit string) {
	defer func() {
		if tok != WS {
			s.preToken = tok
		}
		if tok >= FROM && tok <= ON {
			s.checkDOT = true
		} else if tok > ON && tok <= ASC {
			s.checkDOT = false
		}
	}()
	// Read next code point.
	ch0, pos := s.r.read()

	// If we see whitespace then consume all contiguous whitespace.
	// If we see a letter, or certain acceptable special characters, then consume
	// as an ident or reserved word.
	if isWhitespace(ch0) {
		return s.scanWhitespace()
	} else if isLetter(ch0) || ch0 == '_' {
		s.r.unread()
		return s.scanIdent(true)
	} else if isDigit(ch0) {
		return s.scanNumber()
	}

	// Otherwise parse individual characters.
	switch ch0 {
	case eof:
		return EOF, pos, ""
	case '"':
		s.r.unread()
		return s.scanIdent(true)
	case '\'':
		return s.scanString()
	case '.':
		ch1, _ := s.r.read()
		s.r.unread()
		if isDigit(ch1) {
			return s.scanNumber()
		}
		return DOT, pos, ""
	case '$':
		tok, _, lit = s.scanIdent(false)
		if tok != IDENT {
			return tok, pos, "$" + lit
		}
		return BOUNDPARAM, pos, "$" + lit
	case '+':
		return ADD, pos, ""
	case '-':
		ch1, _ := s.r.read()
		if ch1 == '-' {
			s.skipUntilNewline()
			return COMMENT, pos, ""
		}
		s.r.unread()
		return SUB, pos, ""
	case '*':
		ch1, _ := s.r.read()
		if ch1 == '.' {
			ch2, pos2 := s.r.read()
			if ch2 == '.' {
				return MULTIHOP, pos2, ""
			}
			s.r.unread()
		}
		s.r.unread()
		return MUL, pos, ""
	case '/':
		var comm string
		var err error
		ch1, _ := s.r.read()
		if ch1 == '*' {
			if comm, err = s.skipUntilEndComment(); err != nil {
				return ILLEGAL, pos, ""
			}
			if len(comm) > 1 && comm[0] == '+' {
				return HINT, pos, comm
			}
			return COMMENT, pos, ""
		} else {
			s.r.unread()
		}
		if s.preToken == 0 || s.preToken == RPAREN || s.preToken == IDENT || s.preToken == DURATION ||
			s.preToken == INTEGER || s.preToken == NUMBER {
			return DIV, pos, ""
		}
		if comm, err = s.skipUntilEndRegex(); err != nil {
			return ILLEGAL, pos, ""
		}
		return REGEX, pos, comm
	case '%':
		return MOD, pos, ""
	case '&':
		return BITWISE_AND, pos, ""
	case '|':
		return BITWISE_OR, pos, ""
	case '^':
		return BITWISE_XOR, pos, ""
	case '=':
		if ch1, _ := s.r.read(); ch1 == '~' {
			return EQREGEX, pos, ""
		}
		s.r.unread()
		return EQ, pos, ""
	case '!':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return NEQ, pos, ""
		} else if ch1 == '~' {
			return NEQREGEX, pos, ""
		}
		s.r.unread()
	case '>':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return GTE, pos, ""
		}
		s.r.unread()
		return GT, pos, ""
	case '<':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return LTE, pos, ""
		} else if ch1 == '>' {
			return NEQ, pos, ""
		}
		s.r.unread()
		return LT, pos, ""
	case '(':
		return LPAREN, pos, ""
	case ')':
		return RPAREN, pos, ""
	case ',':
		return COMMA, pos, ""
	case ';':
		return SEMICOLON, pos, ""
	case ':':
		if ch1, _ := s.r.read(); ch1 == ':' {
			return DOUBLECOLON, pos, ""
		}
		s.r.unread()
		return COLON, pos, ""
	case '{':
		return LBRACKET, pos, ""
	case '}':
		return RBRACKET, pos, ""
	case '[':
		return LSQUARE, pos, ""
	case ']':
		return RSQUARE, pos, ""
	}

	return ILLEGAL, pos, string(ch0)
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() (tok Token, pos Pos, lit string) {
	// Create a buffer and read the current character into it.
	buf := s.allocBuf()

	ch, pos := s.r.curr()
	_, _ = buf.WriteRune(ch)

	// Read every subsequent whitespace character into the buffer.
	// Non-whitespace characters and EOF will cause the loop to exit.
	for {
		ch, _ = s.r.read()
		if ch == eof {
			break
		} else if !isWhitespace(ch) {
			s.r.unread()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}

	return WS, pos, buf.String()
}

// skipUntilNewline skips characters until it reaches a newline.
func (s *Scanner) skipUntilNewline() {
	for {
		if ch, _ := s.r.read(); ch == '\n' || ch == eof {
			return
		}
	}
}

// skipUntilEndComment skips characters until it reaches a '*/' symbol.
func (s *Scanner) skipUntilEndComment() (comment string, err error) {
	var buf bytes.Buffer
	for {
		ch1, _ := s.r.read()
		if ch1 == '*' {
			// We might be at the end.
		star:
			ch2, _ := s.r.read()
			if ch2 == '/' {
				return buf.String(), nil
			} else if ch2 == '*' {
				// We are back in the state machine since we see a star.
				goto star
			} else if ch2 == eof {
				return buf.String(), io.EOF
			}
		} else {
			buf.WriteRune(ch1)
			if ch1 == eof {
				return buf.String(), io.EOF
			}
		}
	}
}

func (s *Scanner) skipUntilEndRegex() (comment string, err error) {
	var buf bytes.Buffer
	skip := true
	for {
		ch1, _ := s.r.read()
		// We might be at the end.
		if ch1 == '/' && skip {
			return buf.String(), nil
		} else if ch1 == eof {
			return buf.String(), io.EOF
		}
		if ch1 != '\\' {
			skip = true
		} else {
			skip = false
		}
		buf.WriteRune(ch1)
		if ch1 == eof {
			return buf.String(), io.EOF
		}
	}
}

func (s *Scanner) scanIdent(lookup bool) (tok Token, pos Pos, lit string) {
	// Save the starting position of the identifier.
	_, pos = s.r.read()
	s.r.unread()

	buf := s.allocBuf()

	for {
		if ch, _ := s.r.read(); ch == eof {
			break
		} else if ch == '"' {
			tok0, pos0, lit0 := s.scanString()
			if tok0 == BADSTRING || tok0 == BADESCAPE {
				return tok0, pos0, lit0
			}
			return IDENT, pos, lit0
		} else if isIdentChar(ch) {
			s.r.unread()
			buf.WriteString(s.ScanBareIdent(s.r, s.checkDOT))
		} else {
			s.r.unread()
			break
		}
	}
	lit = buf.String()

	// If the literal matches a keyword then return that keyword.
	if lookup {
		if tok = Lookup(lit); tok != IDENT {
			return tok, pos, ""
		}
	}
	return IDENT, pos, lit
}

// scanString consumes a contiguous string of non-quote characters.
// Quote characters can be consumed if they're first escaped with a backslash.
func (s *Scanner) scanString() (tok Token, pos Pos, lit string) {
	s.r.unread()
	_, pos = s.r.curr()

	var err error
	lit, err = s.ScanString(s.r)
	if err == errBadString {
		return BADSTRING, pos, lit
	} else if err == errBadEscape {
		_, pos = s.r.curr()
		return BADESCAPE, pos, lit
	}
	return STRING, pos, lit
}

// ScanRegex consumes a token to find escapes
func (s *Scanner) ScanRegex() (tok Token, pos Pos, lit string) {
	_, pos = s.r.curr()

	// Start & end sentinels.
	start, end := '/', '/'
	// Valid escape chars.
	escapes := map[rune]rune{'/': '/'}

	b, err := ScanDelimited(s.r, start, end, escapes, true)

	if err == errBadEscape {
		_, pos = s.r.curr()
		return BADESCAPE, pos, lit
	} else if err != nil {
		return BADREGEX, pos, lit
	}
	return REGEX, pos, string(b)
}

// scanNumber consumes anything that looks like the start of a number.
func (s *Scanner) scanNumber() (tok Token, pos Pos, lit string) {
	buf := s.allocBuf()

	// Check if the initial rune is a ".".
	ch, pos := s.r.curr()
	if ch == '.' {
		// Peek and see if the next rune is a digit.
		ch1, _ := s.r.read()
		s.r.unread()
		if !isDigit(ch1) {
			return ILLEGAL, pos, "."
		}

		// Unread the full stop so we can read it later.
		s.r.unread()
	} else {
		s.r.unread()
	}

	// Read as many digits as possible.
	_, _ = buf.WriteString(s.scanDigits())

	// If next code points are a full stop and digit then consume them.
	isDecimal := false
	if ch0, _ := s.r.read(); ch0 == '.' {
		isDecimal = true
		if ch1, _ := s.r.read(); isDigit(ch1) {
			_, _ = buf.WriteRune(ch0)
			_, _ = buf.WriteRune(ch1)
			_, _ = buf.WriteString(s.scanDigits())
		} else {
			s.r.unread()
		}
	} else {
		s.r.unread()
	}

	// Read as a duration or integer if it doesn't have a fractional part.
	if !isDecimal {
		// If the next rune is a letter then this is a duration token.
		if ch0, _ := s.r.read(); isLetter(ch0) || ch0 == 'µ' {
			_, _ = buf.WriteRune(ch0)
			for {
				ch1, _ := s.r.read()
				if !isLetter(ch1) && ch1 != 'µ' {
					s.r.unread()
					break
				}
				_, _ = buf.WriteRune(ch1)
			}

			// Continue reading digits and letters as part of this token.
			for {
				if ch0, _ := s.r.read(); isLetter(ch0) || ch0 == 'µ' || isDigit(ch0) {
					_, _ = buf.WriteRune(ch0)
				} else {
					s.r.unread()
					break
				}
			}
			return DURATIONVAL, pos, buf.String()
		} else {
			s.r.unread()
			return INTEGER, pos, buf.String()
		}
	}
	return NUMBER, pos, buf.String()
}

// scanDigits consumes a contiguous series of digits.
func (s *Scanner) scanDigits() string {
	buf := s.allocBuf()

	for {
		ch, _ := s.r.read()
		if !isDigit(ch) {
			s.r.unread()
			break
		}
		_, _ = buf.WriteRune(ch)
	}
	return buf.String()
}

// isWhitespace returns true if the rune is a space, tab, or newline.
func isWhitespace(ch rune) bool { return ch == ' ' || ch == '\t' || ch == '\n' }

// isLetter returns true if the rune is a letter.
func isLetter(ch rune) bool { return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') }

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

// isIdentChar returns true if the rune can be used in an unquoted identifier.
func isIdentChar(ch rune) bool { return isLetter(ch) || isDigit(ch) || ch == '_' }

// isIdentFirstChar returns true if the rune can be used as the first char in an unquoted identifer.
func isIdentFirstChar(ch rune) bool { return isLetter(ch) || ch == '_' }

// bufScanner represents a wrapper for scanner to add a buffer.
// It provides a fixed-length circular buffer that can be unread.
type bufScanner struct {
	s   *Scanner
	i   int // buffer index
	n   int // buffer size
	buf [3]struct {
		tok Token
		pos Pos
		lit string
	}
}

// newBufScanner returns a new buffered scanner for a reader.
func newBufScanner(r io.Reader) *bufScanner {
	return &bufScanner{s: NewScanner(r)}
}

// Scan reads the next token from the scanner.
func (s *bufScanner) Scan() (tok Token, pos Pos, lit string) {
	return s.scanFunc(s.s.Scan)
}

// ScanRegex reads a regex token from the scanner.
func (s *bufScanner) ScanRegex() (tok Token, pos Pos, lit string) {
	return s.scanFunc(s.s.ScanRegex)
}

func (s *bufScanner) reset(r io.Reader) {
	s.i = 0
	s.n = 0
	for i := range s.buf {
		s.buf[i].tok = ILLEGAL
		s.buf[i].pos.Char = 0
		s.buf[i].pos.Line = 0
		s.buf[i].lit = ""
	}
	s.s.reset(r)
}

// scanFunc uses the provided function to scan the next token.
func (s *bufScanner) scanFunc(scan func() (Token, Pos, string)) (tok Token, pos Pos, lit string) {
	// If we have unread tokens then read them off the buffer first.
	if s.n > 0 {
		s.n--
		return s.curr()
	}

	// Move buffer position forward and save the token.
	s.i = (s.i + 1) % len(s.buf)
	buf := &s.buf[s.i]
	buf.tok, buf.pos, buf.lit = scan()

	return s.curr()
}

// Unscan pushes the previously token back onto the buffer.
func (s *bufScanner) Unscan() { s.n++ }

// curr returns the last read token.
func (s *bufScanner) curr() (tok Token, pos Pos, lit string) {
	buf := &s.buf[(s.i-s.n+len(s.buf))%len(s.buf)]
	return buf.tok, buf.pos, buf.lit
}

// reader represents a buffered rune reader used by the scanner.
// It provides a fixed-length circular buffer that can be unread.
type reader struct {
	r   io.RuneScanner
	i   int // buffer index
	n   int // buffer char count
	pos Pos // last read rune position
	buf [3]struct {
		ch  rune
		pos Pos
	}
	eof bool // true if reader has ever seen eof.
}

// ReadRune reads the next rune from the reader.
// This is a wrapper function to implement the io.RuneReader interface.
// Note that this function does not return size.
func (r *reader) ReadRune() (ch rune, size int, err error) {
	ch, _ = r.read()
	if ch == eof {
		err = io.EOF
	}
	return
}

// UnreadRune pushes the previously read rune back onto the buffer.
// This is a wrapper function to implement the io.RuneScanner interface.
func (r *reader) UnreadRune() error {
	r.unread()
	return nil
}

// read reads the next rune from the reader.
func (r *reader) read() (ch rune, pos Pos) {
	// If we have unread characters then read them off the buffer first.
	if r.n > 0 {
		r.n--
		return r.curr()
	}

	// Read next rune from underlying reader.
	// Any error (including io.EOF) should return as EOF.
	ch, _, err := r.r.ReadRune()
	if err != nil {
		ch = eof
	} else if ch == '\r' {
		if ch, _, err := r.r.ReadRune(); err != nil {
			// nop
		} else if ch != '\n' {
			_ = r.r.UnreadRune()
		}
		ch = '\n'
	}

	// Save character and position to the buffer.
	r.i = (r.i + 1) % len(r.buf)
	buf := &r.buf[r.i]
	buf.ch, buf.pos = ch, r.pos

	// Update position.
	// Only count EOF once.
	if ch == '\n' {
		r.pos.Line++
		r.pos.Char = 0
	} else if !r.eof {
		r.pos.Char++
	}

	// Mark the reader as EOF.
	// This is used so we don't double count EOF characters.
	if ch == eof {
		r.eof = true
	}

	return r.curr()
}

// unread pushes the previously read rune back onto the buffer.
func (r *reader) unread() {
	r.n++
}

// curr returns the last read character and position.
func (r *reader) curr() (ch rune, pos Pos) {
	i := (r.i - r.n + len(r.buf)) % len(r.buf)
	buf := &r.buf[i]
	return buf.ch, buf.pos
}

// eof is a marker code point to signify that the reader can't read any more.
const eof = rune(0)

// ScanDelimited reads a delimited set of runes
func ScanDelimited(r io.RuneScanner, start, end rune, escapes map[rune]rune, escapesPassThru bool) ([]byte, error) {
	// Scan start delimiter.
	if ch, _, err := r.ReadRune(); err != nil {
		return nil, err
	} else if ch != start {
		return nil, fmt.Errorf("expected %s; found %s", string(start), string(ch))
	}

	var buf bytes.Buffer
	for {
		ch0, _, err := r.ReadRune()
		if ch0 == end {
			return buf.Bytes(), nil
		} else if err != nil {
			return buf.Bytes(), err
		} else if ch0 == '\n' {
			return nil, errors.New("delimited text contains new line")
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, err := r.ReadRune()
			if err != nil {
				return nil, err
			}

			c, ok := escapes[ch1]
			if !ok {
				if escapesPassThru {
					// Unread ch1 (char after the \)
					_ = r.UnreadRune()
					// Write ch0 (\) to the output buffer.
					_, _ = buf.WriteRune(ch0)
					continue
				} else {
					buf.Reset()
					_, _ = buf.WriteRune(ch0)
					_, _ = buf.WriteRune(ch1)
					return buf.Bytes(), errBadEscape
				}
			}

			_, _ = buf.WriteRune(c)
		} else {
			_, _ = buf.WriteRune(ch0)
		}
	}
}

// ScanString reads a quoted string from a rune reader.
func (s *Scanner) ScanString(r io.RuneScanner) (string, error) {
	ending, _, err := r.ReadRune()
	if err != nil {
		return "", errBadString
	}

	buf := s.allocBuf()
	for {
		ch0, _, err := r.ReadRune()
		if ch0 == ending {
			return buf.String(), nil
		} else if err != nil || ch0 == '\n' {
			return buf.String(), errBadString
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, _ := r.ReadRune()
			if ch1 == 'n' {
				_, _ = buf.WriteRune('\n')
			} else if ch1 == '\\' {
				_, _ = buf.WriteRune('\\')
			} else if ch1 == '"' {
				_, _ = buf.WriteRune('"')
			} else if ch1 == '\'' {
				_, _ = buf.WriteRune('\'')
			} else {
				return string(ch0) + string(ch1), errBadEscape
			}
		} else {
			_, _ = buf.WriteRune(ch0)
		}
	}
}

var errBadString = errors.New("bad string")
var errBadEscape = errors.New("bad escape")

// ScanBareIdent reads bare identifier from a rune reader.
func (s *Scanner) ScanBareIdent(r io.RuneScanner, checkDOT bool) string {
	// Read every ident character into the buffer.
	// Non-ident characters and EOF will cause the loop to exit.
	buf := s.allocBuf()
	for {
		ch, _, err := r.ReadRune()
		if err != nil {
			break
		} else if !checkDOT && IsDOT(ch) {
			_, _ = buf.WriteRune(ch)
			continue
		} else if !isIdentChar(ch) {
			r.UnreadRune()
			break
		} else {
			_, _ = buf.WriteRune(ch)
		}
	}
	return buf.String()
}

// IsRegexOp returns true if the operator accepts a regex operand.
func IsRegexOp(t Token) bool {
	return (t == EQREGEX || t == NEQREGEX)
}
func IsDOT(ch rune) bool {
	return ch == '.'
}

func IsInOp(t Token) bool {
	return (t == IN || t == NOTIN)
}
