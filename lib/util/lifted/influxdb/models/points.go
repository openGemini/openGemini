// Package models implements basic objects used throughout the TICK stack.
package models // import "github.com/influxdata/influxdb/models"

import (
	"bytes"
	"sort"
)

type escapeSet struct {
	k   [1]byte
	esc [2]byte
}

var (
	tagEscapeCodes = [...]escapeSet{
		{k: [1]byte{','}, esc: [2]byte{'\\', ','}},
		{k: [1]byte{' '}, esc: [2]byte{'\\', ' '}},
		{k: [1]byte{'='}, esc: [2]byte{'\\', '='}},
	}
)

func escapeTag(in []byte) []byte {
	for i := range tagEscapeCodes {
		c := &tagEscapeCodes[i]
		if bytes.IndexByte(in, c.k[0]) != -1 {
			in = bytes.Replace(in, c.k[:], c.esc[:], -1)
		}
	}
	return in
}

// Tag represents a single key/value tag pair.
type Tag struct {
	Key   []byte
	Value []byte
}

// NewTag returns a new Tag.
func NewTag(key, value []byte) Tag {
	return Tag{
		Key:   key,
		Value: value,
	}
}

// Size returns the size of the key and value.
func (t Tag) Size() int { return len(t.Key) + len(t.Value) }

// Clone returns a shallow copy of Tag.
//
// Tags associated with a Point created by ParsePointsWithPrecision will hold references to the byte slice that was parsed.
// Use Clone to create a Tag with new byte slices that do not refer to the argument to ParsePointsWithPrecision.
func (t Tag) Clone() Tag {
	other := Tag{
		Key:   make([]byte, len(t.Key)),
		Value: make([]byte, len(t.Value)),
	}

	copy(other.Key, t.Key)
	copy(other.Value, t.Value)

	return other
}

// String returns the string reprsentation of the tag.
func (t *Tag) String() string {
	var buf bytes.Buffer
	buf.WriteByte('{')
	buf.WriteString(string(t.Key))
	buf.WriteByte(' ')
	buf.WriteString(string(t.Value))
	buf.WriteByte('}')
	return buf.String()
}

// Tags represents a sorted list of tags.
type Tags []Tag

// NewTags returns a new Tags from a map.
func NewTags(m map[string]string) Tags {
	if len(m) == 0 {
		return nil
	}
	a := make(Tags, 0, len(m))
	for k, v := range m {
		a = append(a, NewTag([]byte(k), []byte(v)))
	}
	sort.Sort(a)
	return a
}

// Keys returns the list of keys for a tag set.
func (a Tags) Keys() []string {
	if len(a) == 0 {
		return nil
	}
	keys := make([]string, len(a))
	for i, tag := range a {
		keys[i] = string(tag.Key)
	}
	return keys
}

// Values returns the list of values for a tag set.
func (a Tags) Values() []string {
	if len(a) == 0 {
		return nil
	}
	values := make([]string, len(a))
	for i, tag := range a {
		values[i] = string(tag.Value)
	}
	return values
}

// String returns the string representation of the tags.
func (a Tags) String() string {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i := range a {
		buf.WriteString(a[i].String())
		if i < len(a)-1 {
			buf.WriteByte(' ')
		}
	}
	buf.WriteByte(']')
	return buf.String()
}

// Size returns the number of bytes needed to store all tags. Note, this is
// the number of bytes needed to store all keys and values and does not account
// for data structures or delimiters for example.
func (a Tags) Size() int {
	var total int
	for i := range a {
		total += a[i].Size()
	}
	return total
}

// Clone returns a copy of the slice where the elements are a result of calling `Clone` on the original elements
//
// Tags associated with a Point created by ParsePointsWithPrecision will hold references to the byte slice that was parsed.
// Use Clone to create Tags with new byte slices that do not refer to the argument to ParsePointsWithPrecision.
func (a Tags) Clone() Tags {
	if len(a) == 0 {
		return nil
	}

	others := make(Tags, len(a))
	for i := range a {
		others[i] = a[i].Clone()
	}

	return others
}

// sorted returns true if a is sorted and is an optimization
// to avoid an allocation when calling sort.IsSorted, improving
// performance as much as 50%.
func (a Tags) sorted() bool {
	for i := len(a) - 1; i > 0; i-- {
		if bytes.Compare(a[i].Key, a[i-1].Key) == -1 {
			return false
		}
	}
	return true
}

func (a Tags) Len() int { return len(a) }

func (a Tags) Less(i, j int) bool { return bytes.Compare(a[i].Key, a[j].Key) == -1 }

func (a Tags) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Equal returns true if a equals other.
func (a Tags) Equal(other Tags) bool {
	if len(a) != len(other) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i].Key, other[i].Key) || !bytes.Equal(a[i].Value, other[i].Value) {
			return false
		}
	}
	return true
}

// Get returns the value for a key.
func (a Tags) Get(key []byte) []byte {
	// OPTIMIZE: Use sort.Search if tagset is large.

	for _, t := range a {
		if bytes.Equal(t.Key, key) {
			return t.Value
		}
	}
	return nil
}

// GetString returns the string value for a string key.
func (a Tags) GetString(key string) string {
	return string(a.Get([]byte(key)))
}

// Set sets the value for a key.
func (a *Tags) Set(key, value []byte) {
	for i, t := range *a {
		if bytes.Equal(t.Key, key) {
			(*a)[i].Value = value
			return
		}
	}
	*a = append(*a, Tag{Key: key, Value: value})
	sort.Sort(*a)
}

// SetString sets the string value for a string key.
func (a *Tags) SetString(key, value string) {
	a.Set([]byte(key), []byte(value))
}

// Delete removes a tag by key.
func (a *Tags) Delete(key []byte) {
	for i, t := range *a {
		if bytes.Equal(t.Key, key) {
			copy((*a)[i:], (*a)[i+1:])
			(*a)[len(*a)-1] = Tag{}
			*a = (*a)[:len(*a)-1]
			return
		}
	}
}

// Map returns a map representation of the tags.
func (a Tags) Map() map[string]string {
	m := make(map[string]string, len(a))
	for _, t := range a {
		m[string(t.Key)] = string(t.Value)
	}
	return m
}

// Merge merges the tags combining the two. If both define a tag with the
// same key, the merged value overwrites the old value.
// A new map is returned.
func (a Tags) Merge(other map[string]string) Tags {
	merged := make(map[string]string, len(a)+len(other))
	for _, t := range a {
		merged[string(t.Key)] = string(t.Value)
	}
	for k, v := range other {
		merged[k] = v
	}
	return NewTags(merged)
}

// HashKey hashes all of a tag's keys.
func (a Tags) HashKey() []byte {
	return a.AppendHashKey(nil)
}

func (a Tags) needsEscape() bool {
	for i := range a {
		t := &a[i]
		for j := range tagEscapeCodes {
			c := &tagEscapeCodes[j]
			if bytes.IndexByte(t.Key, c.k[0]) != -1 || bytes.IndexByte(t.Value, c.k[0]) != -1 {
				return true
			}
		}
	}
	return false
}

// AppendHashKey appends the result of hashing all of a tag's keys and values to dst and returns the extended buffer.
func (a Tags) AppendHashKey(dst []byte) []byte {
	// Empty maps marshal to empty bytes.
	if len(a) == 0 {
		return dst
	}

	// Type invariant: Tags are sorted

	sz := 0
	var escaped Tags
	if a.needsEscape() {
		var tmp [20]Tag
		if len(a) < len(tmp) {
			escaped = tmp[:len(a)]
		} else {
			escaped = make(Tags, len(a))
		}

		for i := range a {
			t := &a[i]
			nt := &escaped[i]
			nt.Key = escapeTag(t.Key)
			nt.Value = escapeTag(t.Value)
			sz += len(nt.Key) + len(nt.Value)
		}
	} else {
		sz = a.Size()
		escaped = a
	}

	sz += len(escaped) + (len(escaped) * 2) // separators

	// Generate marshaled bytes.
	if cap(dst)-len(dst) < sz {
		nd := make([]byte, len(dst), len(dst)+sz)
		copy(nd, dst)
		dst = nd
	}
	buf := dst[len(dst) : len(dst)+sz]
	idx := 0
	for i := range escaped {
		k := &escaped[i]
		if len(k.Value) == 0 {
			continue
		}
		buf[idx] = ','
		idx++
		copy(buf[idx:], k.Key)
		idx += len(k.Key)
		buf[idx] = '='
		idx++
		copy(buf[idx:], k.Value)
		idx += len(k.Value)
	}
	return dst[:len(dst)+idx]
}
