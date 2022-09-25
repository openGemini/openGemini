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

package tsi

import (
	"testing"

	"github.com/influxdata/influxdb/pkg/testing/assert"
)

func TestGetRegexpPrefix(t *testing.T) {
	f := func(t *testing.T, s, expectedPrefix, expectedSuffix string) {
		t.Helper()

		prefix, suffix := getRegexpPrefix([]byte(s))
		if string(prefix) != expectedPrefix {
			t.Fatalf("unexpected prefix for s=%q; got %q; want %q", s, prefix, expectedPrefix)
		}
		if string(suffix) != expectedSuffix {
			t.Fatalf("unexpected suffix for s=%q; got %q; want %q", s, suffix, expectedSuffix)
		}

		// Get the prefix from cache.
		prefix, suffix = getRegexpPrefix([]byte(s))
		if string(prefix) != expectedPrefix {
			t.Fatalf("unexpected prefix for s=%q; got %q; want %q", s, prefix, expectedPrefix)
		}
		if string(suffix) != expectedSuffix {
			t.Fatalf("unexpected suffix for s=%q; got %q; want %q", s, suffix, expectedSuffix)
		}
	}

	_, expr := getRegexpPrefix([]byte("U^un"))
	rcv, _ := getRegexpFromCache(expr)
	assert.Equal(t, rcv.reMatch([]byte("buntu16.04LTS")), false)
	_, expr = getRegexpPrefix([]byte("^Ub^un"))
	rcv, _ = getRegexpFromCache(expr)
	assert.Equal(t, rcv.reMatch([]byte("untu16.04LTS")), true)
	_, expr = getRegexpPrefix([]byte("^U|s$"))
	rcv, _ = getRegexpFromCache(expr)
	assert.Equal(t, rcv.reMatch([]byte("untu16.04LTS")), false)
	assert.Equal(t, rcv.reMatch([]byte("Untu16.04LTS")), true)
	assert.Equal(t, rcv.reMatch([]byte("untu16.04LTs")), true)
	assert.Equal(t, rcv.reMatch([]byte("Untu16.04LTs")), true)

	_, expr = getRegexpPrefix([]byte("^U|s$"))
	rcv, _ = getRegexpFromCache(expr)
	assert.Equal(t, rcv.reMatch([]byte("Ubuntu16.04LTS")), true)
	assert.Equal(t, rcv.reMatch([]byte("LTs")), true)
	assert.Equal(t, rcv.reMatch([]byte("bun")), false)
	assert.Equal(t, rcv.reMatch([]byte("aUbuntu")), true)
	_, expr = getRegexpPrefix([]byte("Un^bun"))
	rcv, _ = getRegexpFromCache(expr)
	assert.Equal(t, rcv.reMatch([]byte("Ubuntu16.04LTS")), false)
	assert.Equal(t, rcv.reMatch([]byte("bunLTS")), true)
	assert.Equal(t, rcv.reMatch([]byte("Unnt")), false)

	f(t, "Ubuntu", "Ubuntu", "")
	f(t, "^U|s$", "", "U(?-s:.)*|(?-s:.)*s(?-s:.)*")
	f(t, "^U", "U", "(?-s:.)*")
	f(t, "s$", "", "(?-s:.)*s(?-s:.)*")
	f(t, "", "", "")
	f(t, "^", "", "")
	f(t, "$", "", "")
	f(t, "^()$", "", "")
	f(t, "^(?:)$", "", "")
	f(t, "foobar", "foobar", "")
	f(t, "^foo$|^foobar", "foo", "(?:(?:)|bar(?-s:.)*)")
	f(t, "^(^foo$|^foobar)$", "foo", "(?:(?:)|bar(?-s:.)*)")
	f(t, "foobar|foobaz", "fooba", "[rz]")
	f(t, "(fo|(zar|bazz)|x)", "", "fo|zar|bazz|x")
	f(t, "(тестЧЧ|тест)", "тест", "(?:ЧЧ|(?:))")
	f(t, "foo(bar|baz|bana)", "fooba", "(?:[rz]|na)")
	f(t, "^foobar|^foobaz", "fooba", "(?:r(?-s:.)*|z(?-s:.)*)")
	f(t, "^foobar|^foobaz$", "fooba", "(?:r(?-s:.)*|z)")
	f(t, "foobar|foobaz", "fooba", "[rz]")
	f(t, "(?:^foobar|^foobaz)aa.*", "fooba", "(?:r(?-s:.)*|z(?-s:.)*)aa(?-s:.)*")
	f(t, "foo[bar]+", "foo", "[a-br]+")
	f(t, "foo[a-z]+", "foo", "[a-z]+")
	f(t, "foo[bar]*", "foo", "[a-br]*")
	f(t, "foo[a-z]*", "foo", "[a-z]*")
	f(t, "foo[x]+", "foo", "x+")
	f(t, "foo[^x]+", "foo", "[^x]+")
	f(t, "foo[x]*", "foo", "x*")
	f(t, "foo[^x]*", "foo", "[^x]*")
	f(t, "foo[x]*bar", "foo", "x*bar(?-s:.)*")
	f(t, "fo\\Bo[x]*bar?", "fo", "\\Box*bar?")
	f(t, "foo.+bar", "foo", "(?-s:.)+bar(?-s:.)*")
	f(t, "a(b|c.*).+", "a", "(?:b|c(?-s:.)*)(?-s:.)+")
	f(t, "ab|ac", "a", "[b-c]")
	f(t, "(?i)xyz", "", "(?i:XYZ)")
	f(t, "(?i)foo|bar", "", "(?i:FOO)|(?i:BAR)")
	f(t, "(?i)up.+x", "", "(?i:UP)(?-s:.)+(?i:X)(?-s:.)*")
	f(t, "(?smi)xy.*z$", "", "(?i:XY)(?s:.)*(?i:Z)(?m:$)")

	// test invalid regexps
	f(t, "a(", "a(", "")
	f(t, "a[", "a[", "")
	f(t, "a[]", "a[]", "")
	f(t, "a{", "a{", "")
	f(t, "a{}", "a{}", "")
	f(t, "invalid(regexp", "invalid(regexp", "")

	// The transformed regexp mustn't match aba
	f(t, "a?(^ba|c)", "", "a?(?:\\Aba(?-s:.)*|c)")

	// The transformed regexp mustn't match barx
	f(t, "(foo|bar$)x*", "", "(?:foo|(?-s:.)*bar(?-m:$))x*")
}
