/*
Copyright (c) 2018 InfluxData
This code is originally from: https://github.com/influxdata/influxql/blob/v1.1.0/scanner.go
*/

package logparser

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

var errBadString = errors.New("bad string")
var errBadEscape = errors.New("bad escape")

// Scanner represents a lexical scanner for InfluxQL.
type Scanner struct {
	r *reader
}

// NewScanner returns a new instance of Scanner.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: &reader{r: bufio.NewReader(r)}}
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
}

// Scan returns the next token and position from the underlying reader.
// Also returns the literal text read for strings, numbers, and duration tokens
// since these token types can have different literal representations.
func (s *Scanner) Scan() (tok Token, pos Pos, lit string) {
	// Read next code point.
	ch0, pos := s.r.read()

	// First, handle the symbols with priority for the log service.
	switch ch0 {
	case eof:
		return EOF, pos, ""
	case ' ', '\t', '\n':
		// If we see whitespace then consume all contiguous whitespace.
		return s.scanWhitespace()
	case '"':
		// Double quotation marks indicate a string
		return s.scanString()
	case '|':
		return BITWISE_OR, pos, ""
	case '(':
		return LPAREN, pos, ""
	case ')':
		return RPAREN, pos, ""
	case '[':
		return LSQUARE, pos, ""
	case ']':
		return RSQUARE, pos, ""
	case '>':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return GTE, pos, ""
		}
		s.r.unread()
		return GT, pos, ""
	case '<':
		if ch1, _ := s.r.read(); ch1 == '=' {
			return LTE, pos, ""
		}
		s.r.unread()
		return LT, pos, ""
	case '=':
		return EQ, pos, ""
	case ':':
		return COLON, pos, ""
	case ',':
		return COMMA, pos, ""
	}

	// deal the time "10:10:10"
	if isDigit(ch0) {
		s.r.unread()
		return s.scanNumber()
	}

	// ident string
	s.r.unread()
	return s.scanIdent(true)
}

// scanWhitespace consumes the current rune and all contiguous whitespace.
func (s *Scanner) scanWhitespace() (tok Token, pos Pos, lit string) {
	// Create a buffer and read the current character into it.
	var buf bytes.Buffer
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

func (s *Scanner) scanIdent(lookup bool) (tok Token, pos Pos, lit string) {
	// Save the starting position of the identifier.
	_, pos = s.r.read()
	s.r.unread()

	var buf bytes.Buffer
	for {
		ch, _ := s.r.read()
		if ch == eof {
			break
		} else if isTerminator(ch) {
			s.r.unread()
			break
		}
		_, _ = buf.WriteRune(ch)
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
	lit, err = ScanString(s.r)
	if err == errBadString {
		return BADSTRING, pos, lit
	} else if err == errBadEscape {
		_, pos = s.r.curr()
		return BADESCAPE, pos, lit
	}
	return STRING, pos, lit
}

// scanNumber consumes anything that looks like the start of a number.
func (s *Scanner) scanNumber() (tok Token, pos Pos, lit string) {
	var buf bytes.Buffer

	// Read as many digits as possible.
	_, _ = buf.WriteString(s.scanDigits())

	// The first non numeric character
	ch0, _ := s.r.read()
	if isNumTerminator(ch0) {
		// INTEGER
		s.r.unread()
		return STRING, pos, buf.String()
	} else if ch0 == '.' {
		_, _ = buf.WriteRune(ch0)
		_, _ = buf.WriteString(s.scanDigits())
		ch0, _ = s.r.read()
		if isNumTerminator(ch0) {
			// NUMBER
			s.r.unread()
			return STRING, pos, buf.String()
		}
	}
	s.r.unread()

	// STRING， not contain any blank
	var isRegexP bool
	for {
		ch, _ := s.r.read()
		if isNumTerminator(ch) {
			s.r.unread()
			break
		}
		if isRegex(ch) {
			isRegexP = true
		}
		_, _ = buf.WriteRune(ch)
	}

	if isRegexP {
		return STRING, pos, buf.String()
	}

	return STRING, pos, buf.String()
}

// scanDigits consumes a contiguous series of digits.
func (s *Scanner) scanDigits() string {
	var buf bytes.Buffer
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

// isDigit returns true if the rune is a digit.
func isDigit(ch rune) bool { return (ch >= '0' && ch <= '9') }

func isRegex(ch rune) bool { return (ch == '*' || ch == '?') }

// isNumTerminator returns ture if the run is a terminator symbols
func isNumTerminator(ch rune) bool {
	return (ch == '|' || ch == '(' || ch == ')' || ch == '[' || ch == ']' ||
		ch == '<' || ch == '>' || ch == '=' || ch == ' ' || ch == eof)
}

func isTerminator(ch rune) bool {
	return (isNumTerminator(ch) || ch == ':' || ch == ',')
}

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
			err0 := r.r.UnreadRune()
			if err0 != nil {
				return eof, Pos{}
			}
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

// ScanString reads a quoted string from a rune reader.
func ScanString(r io.RuneScanner) (string, error) {
	ending, _, err := r.ReadRune()
	if err != nil {
		return "", errBadString
	}

	var buf bytes.Buffer
	for {
		ch0, _, err0 := r.ReadRune()
		if err0 != nil || ch0 == '\n' {
			return buf.String(), errBadString
		} else if ch0 == ending {
			return buf.String(), nil
		} else if ch0 == '\\' {
			// If the next character is an escape then write the escaped char.
			// If it's not a valid escape then return an error.
			ch1, _, err1 := r.ReadRune()
			if err1 != nil {
				return buf.String(), errBadEscape
			}
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
