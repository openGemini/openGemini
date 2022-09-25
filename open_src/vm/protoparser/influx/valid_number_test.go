package influx_test

import (
	"regexp"
	"testing"

	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
)

type Item struct {
	s   string
	exp bool
}

var floatNumRef = regexp.MustCompile(`^[+-]?(\d+([.]\d*)?([eE][+-]?\d+)?|[.]\d+([eE][+-]?\d+)?)$`)

func isValidNumber(s string) bool {
	return floatNumRef.MatchString(s)
}

func TestIsValidNumber(t *testing.T) {
	items := []string{
		"123", "-123", "1.1", "001", "0.1", "+3", "-3", "-.3", "+.3",
		"1e3", "1E3", "-1E3", "-1.1E3", "1.3e-3", "1", ".1", ".0", "0.0",
		"1.", "0.", "1.e1",

		"1.1.1", "..1", "++3", "--3", "-e3", "e3", "1.1e0.3", ".", "+", "-",
		"a123", "e1", "1ee1", "e.1", "1.2e-e", "e", "1-e2", "111s", "111e",
	}

	for _, s := range items {
		got := influx.IsValidNumber(s)
		exp := isValidNumber(s)

		if got != exp {
			t.Fatalf("check failed, string: %s; exp: %v, got: %v", s, exp, got)

		}
	}
}

func BenchmarkIsValidNumber(b *testing.B) {
	for i := 0; i < b.N; i++ {
		influx.IsValidNumber("-1234e567")
		influx.IsValidNumber("aabbcceeff")
		influx.IsValidNumber("1111122222333a")
	}
}

func BenchmarkIsValidNumberRegex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		isValidNumber("-1234e567")
		isValidNumber("aabbcceeff")
		isValidNumber("1111122222333a")
	}
}
