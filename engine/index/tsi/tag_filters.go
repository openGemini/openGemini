package tsi

/*
Copyright 2019-2022 VictoriaMetrics, Inc.
This code is originally from: This code is originally from: https://github.com/VictoriaMetrics/VictoriaMetrics/blob/v1.67.0/lib/storage/tag_filters.go and has been modified and used for quickly search tag
*/

import (
	"bytes"
	"fmt"
	"regexp"
	"regexp/syntax"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/memory"
	"github.com/openGemini/openGemini/engine/index/mergeindex"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
)

// tagFilter represents a filter used for filtering tags.
type tagFilter struct {
	key   []byte
	value []byte
	name  []byte

	// Additionally it contains:
	//  - value if !isRegexp.
	//  - non-regexp prefix if isRegexp.
	prefix []byte

	// or values obtained from regexp suffix if it equals to "foo|bar|..."
	//
	// This array is also populated with matching Graphite metrics if key="__graphite__"
	orSuffixes []string

	// Matches regexp suffix.
	reSuffixMatch func(b []byte) bool

	// Contains reverse suffix for Graphite wildcard.
	// I.e. for `{__name__=~"foo\\.[^.]*\\.bar\\.baz"}` the value will be `zab.rab.`
	graphiteReverseSuffix []byte

	// matchCost is a cost for matching a filter against a single string.
	matchCost uint64

	isNegative bool
	isRegexp   bool

	// Set to true for filters matching empty value.
	isEmptyMatch bool

	// eg, host =~ /.*/
	isAllMatch bool

	// eg, host ='' or host != ''
	isEmptyValue bool
}

type TagFilters struct {
	tfs []tagFilter

	// Common prefix for all the tag filters.
	// Contains encoded nsPrefixTagToMetricIDs.
	commonPrefix []byte
}

var (
	prefixesCacheMap  = make(map[string]prefixSuffix)
	prefixesCacheLock sync.RWMutex
)

type prefixSuffix struct {
	prefix []byte
	suffix []byte
}

var (
	maxRegexpCacheSize     int
	maxRegexpCacheSizeOnce sync.Once
)

var (
	regexpCacheMap  = make(map[string]regexpCacheValue)
	regexpCacheLock sync.RWMutex

	regexpCacheRequests uint64
	regexpCacheMisses   uint64
)

type regexpCacheValue struct {
	orValues      []string
	reMatch       func(b []byte) bool
	reCost        uint64
	literalSuffix string
}

const (
	fullMatchCost    = 1
	prefixMatchCost  = 2
	literalMatchCost = 3
	suffixMatchCost  = 4
	middleMatchCost  = 6
	reMatchCost      = 100
)
const maxOrValues = 20

var tagCharsRegexpEscaper = strings.NewReplacer(
	"\\x00", "\\x000", // escapeChar
	"\x00", "\\x000", // escapeChar
	"\\x01", "\\x001", // tagSeparatorChar
	"\x01", "\\x001", // tagSeparatorChar
	"\\x02", "\\x002", // kvSeparatorChar
	"\x02", "\\x002", // kvSeparatorChar
)

var tagCharsReverseRegexpEscaper = strings.NewReplacer(
	"\\x000", "\x00", // escapeChar
	"\x000", "\x00", // escapeChar
	"\\x001", "\x01", // tagSeparatorChar
	"\x001", "\x01", // tagSeparatorChar
	"\\x002", "\x02", // kvSeparatorChar
	"\x002", "\x02", // kvSeparatorChar
)

func (tf *tagFilter) String() string {
	op := "="
	if tf.isNegative {
		op = "!="
		if tf.isRegexp {
			op = "!~"
		}
	} else if tf.isRegexp {
		op = "=~"
	}

	return fmt.Sprintf("%s%s%q", tf.key, op, tf.value)
}

// Marshal appends marshaled tf to dst
// and returns the result.
func (tf *tagFilter) Marshal(dst []byte) []byte {
	dst = marshalTagValue(dst, tf.key)
	dst = marshalTagValue(dst, tf.value)

	isNegative := byte(0)
	if tf.isNegative {
		isNegative = 1
	}

	isRegexp := byte(0)
	if tf.isRegexp {
		isRegexp = 1
	}

	dst = append(dst, isNegative, isRegexp)
	return dst
}

// Reset resets the tf
func (tfs *TagFilters) Reset() {
	tfs.tfs = tfs.tfs[:0]
	tfs.commonPrefix = mergeindex.MarshalCommonPrefix(tfs.commonPrefix[:0], nsPrefixTagToTSIDs)
}

func (tf *tagFilter) matchSuffix(b []byte) (bool, error) {
	// Remove the trailing tagSeparatorChar.
	if len(b) == 0 || b[len(b)-1] != tagSeparatorChar {
		return false, fmt.Errorf("unexpected end of b; want %d; b=%q", tagSeparatorChar, b)
	}
	b = b[:len(b)-1]
	if !tf.isRegexp {
		return len(b) == 0, nil
	}
	ok := tf.reSuffixMatch(b)
	return ok, nil
}

func (tf *tagFilter) Less(other *tagFilter) bool {
	if tf.matchCost != other.matchCost {
		return tf.matchCost < other.matchCost
	}
	if tf.isRegexp != other.isRegexp {
		return !tf.isRegexp
	}
	if len(tf.orSuffixes) != len(other.orSuffixes) {
		return len(tf.orSuffixes) < len(other.orSuffixes)
	}
	if tf.isNegative != other.isNegative {
		return !tf.isNegative
	}
	return bytes.Compare(tf.prefix, other.prefix) < 0
}

func (tf *tagFilter) Init(name, key, value []byte, isNegative, isRegexp bool) error {
	tf.key = append(tf.key[:0], key...)
	tf.value = append(tf.value[:0], value...)
	tf.name = append(tf.name[:0], name...)
	tf.isNegative = isNegative
	tf.isRegexp = isRegexp
	tf.matchCost = 0
	tf.reSuffixMatch = nil
	tf.isEmptyValue = false
	if len(value) == 0 {
		tf.isEmptyValue = true
	}
	tf.prefix = tf.prefix[:0]
	tf.orSuffixes = tf.orSuffixes[:0]
	tf.isEmptyMatch = false
	tf.isAllMatch = false
	tf.graphiteReverseSuffix = tf.graphiteReverseSuffix[:0]

	compositeKey := kbPool.Get()
	compositeKey.B = marshalCompositeTagKey(compositeKey.B[:0], name, key)
	tf.prefix = append(tf.prefix, nsPrefixTagToTSIDs)
	tf.prefix = marshalTagValue(tf.prefix, compositeKey.B)
	kbPool.Put(compositeKey)

	var expr []byte
	prefix := tf.value
	if tf.isRegexp {
		prefix, expr = getRegexpPrefix(tf.value)
		if len(expr) == 0 {
			tf.value = append(tf.value[:0], prefix...)
			// select /Ubuntu/ should return match value which contain Ubuntu
			tf.reSuffixMatch = func(b []byte) bool {
				return bytes.Contains(b, tf.value)
			}
			return nil
		}
	}
	tf.prefix = marshalTagValueNoTrailingTagSeparator(tf.prefix, prefix)
	if !tf.isRegexp {
		// tf contains plain value without regexp.
		// Add empty orSuffix in order to trigger fast path for orSuffixes
		// during the search for matching metricIDs.
		tf.isEmptyMatch = len(prefix) == 0
		return nil
	}
	rcv, err := getRegexpFromCache(expr)
	if err != nil {
		return err
	}
	tf.orSuffixes = append(tf.orSuffixes[:0], rcv.orValues...)
	tf.reSuffixMatch = rcv.reMatch
	tf.matchCost = rcv.reCost
	tf.isEmptyMatch = len(prefix) == 0 && tf.reSuffixMatch(nil)
	if !tf.isNegative && len(key) == 0 && strings.IndexByte(rcv.literalSuffix, '.') >= 0 {
		// Reverse suffix is needed only for non-negative regexp filters on __name__ that contains dots.
		tf.graphiteReverseSuffix = reverseBytes(tf.graphiteReverseSuffix[:0], []byte(rcv.literalSuffix))
	}
	return nil
}

func (tf *tagFilter) SetRegexMatchAll(match bool) {
	tf.isAllMatch = match
}

func reverseBytes(dst, src []byte) []byte {
	for i := len(src) - 1; i >= 0; i-- {
		dst = append(dst, src[i])
	}
	return dst
}

func getRegexpPrefix(b []byte) ([]byte, []byte) {
	// Fast path - search the prefix in the cache.
	prefixesCacheLock.RLock()
	ps, ok := prefixesCacheMap[string(b)]
	prefixesCacheLock.RUnlock()

	if ok {
		return ps.prefix, ps.suffix
	}

	// Slow path - extract the regexp prefix from b.
	prefix, suffix := extractRegexpPrefix(b)

	// Put the prefix and the suffix to the cache.
	prefixesCacheLock.Lock()
	if overflow := len(prefixesCacheMap) - getMaxPrefixesCacheSize(); overflow > 0 {
		overflow = int(float64(len(prefixesCacheMap)) * 0.1)
		for k := range prefixesCacheMap {
			delete(prefixesCacheMap, k)
			overflow--
			if overflow <= 0 {
				break
			}
		}
	}
	prefixesCacheMap[string(b)] = prefixSuffix{
		prefix: prefix,
		suffix: suffix,
	}
	prefixesCacheLock.Unlock()

	return prefix, suffix
}

var (
	maxPrefixesCacheSize     int
	maxPrefixesCacheSizeOnce sync.Once
)

func getMaxPrefixesCacheSize() int {
	maxPrefixesCacheSizeOnce.Do(func() {
		n := memory.Allowed() / 1024 / 1024
		if n < 100 {
			n = 100
		}
		maxPrefixesCacheSize = n
	})
	return maxPrefixesCacheSize
}

func getRegexpFromCache(expr []byte) (regexpCacheValue, error) {
	atomic.AddUint64(&regexpCacheRequests, 1)

	regexpCacheLock.RLock()
	rcv, ok := regexpCacheMap[string(expr)]
	regexpCacheLock.RUnlock()
	if ok {
		// Fast path - the regexp found in the cache.
		return rcv, nil
	}

	// Slow path - build the regexp.
	atomic.AddUint64(&regexpCacheMisses, 1)
	exprOrig := string(expr)

	expr = []byte(tagCharsRegexpEscaper.Replace(exprOrig))
	exprStr := string(expr)
	re, err := regexp.Compile(exprStr)
	if err != nil {
		return rcv, fmt.Errorf("invalid regexp %q: %w", exprStr, err)
	}

	sExpr := string(expr)
	orValues := getOrValues(sExpr)
	var reMatch func(b []byte) bool
	var reCost uint64
	var literalSuffix string
	if len(orValues) > 0 {
		reMatch, reCost = newMatchFuncForOrSuffixes(orValues)
	} else {
		reMatch, literalSuffix, reCost = getOptimizedReMatchFunc(re.Match, sExpr)
	}

	// Put the reMatch in the cache.
	rcv.orValues = orValues
	rcv.reMatch = reMatch
	rcv.reCost = reCost
	rcv.literalSuffix = literalSuffix

	regexpCacheLock.Lock()
	if overflow := len(regexpCacheMap) - getMaxRegexpCacheSize(); overflow > 0 {
		overflow = int(float64(len(regexpCacheMap)) * 0.1)
		for k := range regexpCacheMap {
			delete(regexpCacheMap, k)
			overflow--
			if overflow <= 0 {
				break
			}
		}
	}
	regexpCacheMap[exprOrig] = rcv
	regexpCacheLock.Unlock()

	return rcv, nil
}

func getMaxRegexpCacheSize() int {
	maxRegexpCacheSizeOnce.Do(func() {
		n := memory.Allowed() / 1024 / 1024
		if n < 100 {
			n = 100
		}
		maxRegexpCacheSize = n
	})
	return maxRegexpCacheSize
}

func getOptimizedReMatchFunc(reMatch func(b []byte) bool, expr string) (func(b []byte) bool, string, uint64) {
	sre, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		logger.Panicf("BUG: unexpected error when parsing verified expr=%q: %s", expr, err)
	}
	if matchFunc, literalSuffix, reCost := getOptimizedReMatchFuncExt(reMatch, sre); matchFunc != nil {
		// Found optimized function for matching the expr.
		suffixUnescaped := tagCharsReverseRegexpEscaper.Replace(literalSuffix)
		return matchFunc, suffixUnescaped, reCost
	}
	// Fall back to un-optimized reMatch.
	return reMatch, "", reMatchCost
}

func getOptimizedReMatchFuncExt(reMatch func(b []byte) bool, sre *syntax.Regexp) (func(b []byte) bool, string, uint64) {
	if isDotStar(sre) {
		// '.*'
		return func(b []byte) bool {
			return true
		}, "", fullMatchCost
	}
	if isDotPlus(sre) {
		// '.+'
		return func(b []byte) bool {
			return len(b) > 0
		}, "", fullMatchCost
	}
	switch sre.Op {
	case syntax.OpCapture:
		// Remove parenthesis from expr, i.e. '(expr) -> expr'
		return getOptimizedReMatchFuncExt(reMatch, sre.Sub[0])
	case syntax.OpLiteral:
		if !isLiteral(sre) {
			return nil, "", 0
		}
		s := string(sre.Rune)
		// Literal match
		return func(b []byte) bool {
			return string(b) == s
		}, s, literalMatchCost
	case syntax.OpConcat:
		if len(sre.Sub) == 2 {
			if isLiteral(sre.Sub[0]) {
				prefix := []byte(string(sre.Sub[0].Rune))
				if isDotStar(sre.Sub[1]) {
					// 'prefix.*'
					return func(b []byte) bool {
						return bytes.HasPrefix(b, prefix)
					}, "", prefixMatchCost
				}
				if isDotPlus(sre.Sub[1]) {
					// 'prefix.+'
					return func(b []byte) bool {
						return len(b) > len(prefix) && bytes.HasPrefix(b, prefix)
					}, "", prefixMatchCost
				}
			}
			if isLiteral(sre.Sub[1]) {
				suffix := []byte(string(sre.Sub[1].Rune))
				if isDotStar(sre.Sub[0]) {
					// '.*suffix'
					return func(b []byte) bool {
						return bytes.HasSuffix(b, suffix)
					}, string(suffix), suffixMatchCost
				}
				if isDotPlus(sre.Sub[0]) {
					// '.+suffix'
					return func(b []byte) bool {
						return len(b) > len(suffix) && bytes.HasSuffix(b[1:], suffix)
					}, string(suffix), suffixMatchCost
				}
			}
		}
		if len(sre.Sub) == 3 && isLiteral(sre.Sub[1]) {
			middle := []byte(string(sre.Sub[1].Rune))
			if isDotStar(sre.Sub[0]) {
				if isDotStar(sre.Sub[2]) {
					// '.*middle.*'
					return func(b []byte) bool {
						return bytes.Contains(b, middle)
					}, "", middleMatchCost
				}
				if isDotPlus(sre.Sub[2]) {
					// '.*middle.+'
					return func(b []byte) bool {
						return len(b) > len(middle) && bytes.Contains(b[:len(b)-1], middle)
					}, "", middleMatchCost
				}
			}
			if isDotPlus(sre.Sub[0]) {
				if isDotStar(sre.Sub[2]) {
					// '.+middle.*'
					return func(b []byte) bool {
						return len(b) > len(middle) && bytes.Contains(b[1:], middle)
					}, "", middleMatchCost
				}
				if isDotPlus(sre.Sub[2]) {
					// '.+middle.+'
					return func(b []byte) bool {
						return len(b) > len(middle)+1 && bytes.Contains(b[1:len(b)-1], middle)
					}, "", middleMatchCost
				}
			}
		}
		// Verify that the string matches all the literals found in the regexp
		// before applying the regexp.
		// This should optimize the case when the regexp doesn't match the string.
		var literals [][]byte
		for _, sub := range sre.Sub {
			if isLiteral(sub) {
				literals = append(literals, []byte(string(sub.Rune)))
			}
		}
		var suffix []byte
		if isLiteral(sre.Sub[len(sre.Sub)-1]) {
			suffix = literals[len(literals)-1]
			literals = literals[:len(literals)-1]
		}
		return func(b []byte) bool {
			if len(suffix) > 0 && !bytes.HasSuffix(b, suffix) {
				// Fast path - b has no the given suffix
				return false
			}
			bOrig := b
			for _, literal := range literals {
				n := bytes.Index(b, literal)
				if n < 0 {
					// Fast path - b doesn't match the regexp.
					return false
				}
				b = b[n+len(literal):]
			}
			// Fall back to slow path.
			return reMatch(bOrig)
		}, string(suffix), reMatchCost
	default:
		return nil, "", 0
	}
}

func isDotStar(sre *syntax.Regexp) bool {
	switch sre.Op {
	case syntax.OpCapture:
		return isDotStar(sre.Sub[0])
	case syntax.OpAlternate:
		for _, reSub := range sre.Sub {
			if isDotStar(reSub) {
				return true
			}
		}
		return false
	case syntax.OpStar:
		switch sre.Sub[0].Op {
		case syntax.OpAnyCharNotNL, syntax.OpAnyChar:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func isDotPlus(sre *syntax.Regexp) bool {
	switch sre.Op {
	case syntax.OpCapture:
		return isDotPlus(sre.Sub[0])
	case syntax.OpAlternate:
		for _, reSub := range sre.Sub {
			if isDotPlus(reSub) {
				return true
			}
		}
		return false
	case syntax.OpPlus:
		switch sre.Sub[0].Op {
		case syntax.OpAnyCharNotNL, syntax.OpAnyChar:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func isLiteral(sre *syntax.Regexp) bool {
	if sre.Op == syntax.OpCapture {
		return isLiteral(sre.Sub[0])
	}
	return sre.Op == syntax.OpLiteral && sre.Flags&syntax.FoldCase == 0
}

func newMatchFuncForOrSuffixes(orValues []string) (reMatch func(b []byte) bool, reCost uint64) {
	if len(orValues) == 1 {
		v := orValues[0]
		reMatch = func(b []byte) bool {
			return string(b) == v
		}
	} else {
		reMatch = func(b []byte) bool {
			for _, v := range orValues {
				if string(b) == v {
					return true
				}
			}
			return false
		}
	}
	reCost = uint64(len(orValues)) * literalMatchCost
	return reMatch, reCost
}

func getOrValues(expr string) []string {
	sre, err := syntax.Parse(expr, syntax.Perl)
	if err != nil {
		logger.Panicf("BUG: unexpected error when parsing verified expr=%q: %s", expr, err)
	}
	orValues := getOrValuesExt(sre)

	// Sort orValues for faster index seek later
	sort.Strings(orValues)

	return orValues
}

func getOrValuesExt(sre *syntax.Regexp) []string {
	switch sre.Op {
	case syntax.OpCapture:
		return getOrValuesExt(sre.Sub[0])
	case syntax.OpLiteral:
		if !isLiteral(sre) {
			return nil
		}
		return []string{string(sre.Rune)}
	case syntax.OpEmptyMatch:
		return []string{""}
	case syntax.OpAlternate:
		a := make([]string, 0, len(sre.Sub))
		for _, reSub := range sre.Sub {
			ca := getOrValuesExt(reSub)
			if len(ca) == 0 {
				return nil
			}
			a = append(a, ca...)
			if len(a) > maxOrValues {
				// It is cheaper to use regexp here.
				return nil
			}
		}
		return a
	case syntax.OpCharClass:
		a := make([]string, 0, len(sre.Rune)/2)
		for i := 0; i < len(sre.Rune); i += 2 {
			start := sre.Rune[i]
			end := sre.Rune[i+1]
			for start <= end {
				a = append(a, string(start))
				start++
				if len(a) > maxOrValues {
					// It is cheaper to use regexp here.
					return nil
				}
			}
		}
		return a
	case syntax.OpConcat:
		if len(sre.Sub) < 1 {
			return []string{""}
		}
		prefixes := getOrValuesExt(sre.Sub[0])
		if len(prefixes) == 0 {
			return nil
		}
		sre.Sub = sre.Sub[1:]
		suffixes := getOrValuesExt(sre)
		if len(suffixes) == 0 {
			return nil
		}
		if len(prefixes)*len(suffixes) > maxOrValues {
			// It is cheaper to use regexp here.
			return nil
		}
		a := make([]string, 0, len(prefixes)*len(suffixes))
		for _, prefix := range prefixes {
			for _, suffix := range suffixes {
				s := prefix + suffix
				a = append(a, s)
			}
		}
		return a
	default:
		return nil
	}
}

func extractRegexpPrefix(b []byte) ([]byte, []byte) {
	sre, err := syntax.Parse(string(b), syntax.Perl)
	if err != nil {
		// Cannot parse the regexp. Return it all as prefix.
		return b, nil
	}
	sre = simplifyRegexp(sre)
	if sre == emptyRegexp {
		return nil, nil
	}
	if isLiteral(sre) {
		return []byte(string(sre.Rune)), nil
	}
	var prefix []byte
	if sre.Op == syntax.OpConcat {
		sub0 := sre.Sub[0]
		if isLiteral(sub0) {
			prefix = []byte(string(sub0.Rune))
			sre.Sub = sre.Sub[1:]
			if len(sre.Sub) == 0 {
				return nil, nil
			}
		}
	}
	if _, err := syntax.Compile(sre); err != nil {
		// Cannot compile the regexp. Return it all as prefix.
		return b, nil
	}
	return prefix, []byte(sre.String())
}

func simplifyRegexp(sre *syntax.Regexp) *syntax.Regexp {
	s := sre.String()
	for {
		sre = simplifyRegexpExt(sre, false, false)
		sre = sre.Simplify()
		if sre.Op == syntax.OpBeginText || sre.Op == syntax.OpEndText {
			sre = emptyRegexp
		}
		sNew := sre.String()
		if sNew == s {
			return sre
		}
		var err error
		sre, err = syntax.Parse(sNew, syntax.Perl)
		if err != nil {
			logger.Panicf("BUG: cannot parse simplified regexp %q: %s", sNew, err)
		}
		s = sNew
	}
}

func simplifyRegexpExt(sre *syntax.Regexp, hasPrefix, hasSuffix bool) *syntax.Regexp {
	switch sre.Op {
	case syntax.OpCapture:
		// Substitute all the capture regexps with non-capture regexps.
		sre.Op = syntax.OpAlternate
		sre.Sub[0] = simplifyRegexpExt(sre.Sub[0], hasPrefix, hasSuffix)
		if sre.Sub[0] == emptyRegexp {
			return emptyRegexp
		}
		return sre
	case syntax.OpStar, syntax.OpPlus, syntax.OpQuest, syntax.OpRepeat:
		sre.Sub[0] = simplifyRegexpExt(sre.Sub[0], hasPrefix, hasSuffix)
		if sre.Sub[0] == emptyRegexp {
			return emptyRegexp
		}
		return sre
	case syntax.OpAlternate:
		// Do not remove empty captures from OpAlternate, since this may break regexp.
		for i, sub := range sre.Sub {
			sre.Sub[i] = simplifyRegexpExt(sub, hasPrefix, hasSuffix)
		}
		return sre
	case syntax.OpConcat:
		subs := sre.Sub[:0]
		begin := sre.Sub[0]
		tail := sre.Sub[len(sre.Sub)-1]
		for i, sub := range sre.Sub {
			if sub = simplifyRegexpExt(sub, i > 0, i+1 < len(sre.Sub)); sub != emptyRegexp {
				subs = append(subs, sub)
			}
		}
		sre.Sub = subs

		// Remove anchros from the beginning and the end of regexp, since they
		// will be added later.
		if !hasPrefix {
			for len(sre.Sub) > 0 && sre.Sub[0].Op == syntax.OpBeginText {
				sre.Sub = sre.Sub[1:]
			}
		}

		if !hasSuffix {
			for len(sre.Sub) > 0 && sre.Sub[len(sre.Sub)-1].Op == syntax.OpEndText {
				sre.Sub = sre.Sub[:len(sre.Sub)-1]
			}
		}

		if len(sre.Sub) == 0 {
			return emptyRegexp
		}

		// if regex string is ^foo$ transform foo, and use len(mst) mst tagKey seperator foo to find the item
		if begin.Op == syntax.OpBeginText && tail.Op == syntax.OpEndText {
			return sre
		}
		// if regex string is ^foo transform to foo.*, prefix is foo, suffix is *, foo1 can match it
		if tail.Op != syntax.OpEndText && sre.Sub[len(sre.Sub)-1].Op == syntax.OpLiteral {
			sreNew, _ := syntax.Parse(".*", syntax.Perl)
			sre.Sub = append(sre.Sub, sreNew)
		}
		// if regex string is o$ transform to .*o, prefix is nil, suffix is *, foo can match it
		if tail.Op == syntax.OpEndText && sre.Sub[0].Op == syntax.OpLiteral {
			sreNew, _ := syntax.Parse(".*", syntax.Perl)
			sre.Sub = append([]*syntax.Regexp{sreNew}, sre.Sub...)
		}

		return sre
	case syntax.OpEmptyMatch:
		return emptyRegexp
	default:
		return sre
	}
}

var emptyRegexp = &syntax.Regexp{
	Op: syntax.OpEmptyMatch,
}

// eg, tagsBuf
// {Key: "tk1", Value: "value1", V2: 0},
// {Key: "tk2", Value: "value2", V2: 0},
// {Key: "tk3", Value: "value3", V2: 0},
// matchKey: tk1, matchValue: val.*1
func (tf *tagFilter) Contains(tagsBuf *influx.PointTags, matchKey, matchValue string, isNegative, isRegexp bool) bool {
	var match bool
	for _, tag := range *tagsBuf {
		if tag.Key == matchKey {
			if isRegexp {
				match = matchWithRegex(matchValue, tag.Value)
			} else {
				match = matchWithNoRegex(matchValue, tag.Value)
			}
			if isNegative {
				return !match
			}
			return match
		}
	}

	return false
}
