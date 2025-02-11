package models

import (
	"math"
	"sort"

	"github.com/bytedance/sonic"
)

// Row represents a single row returned from the execution of a statement.
type Row struct {
	Name    string            `json:"name,omitempty"`
	Tags    map[string]string `json:"tags,omitempty"`
	Columns []string          `json:"columns,omitempty"`
	Values  [][]interface{}   `json:"values,omitempty"`
	Partial bool              `json:"partial,omitempty"`
}

// This function is reserved.In the future, the scenarios of dividing by zero is supported, It can be changed to MarshalJSON.
func (r *Row) marshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Name    string            `json:"name,omitempty"`
		Tags    map[string]string `json:"tags,omitempty"`
		Columns []string          `json:"columns,omitempty"`
		Values  [][]interface{}   `json:"values,omitempty"`
		Partial bool              `json:"partial,omitempty"`
	}

	// Copy fields to output struct.
	o.Name = r.Name
	o.Tags = r.Tags
	o.Columns = r.Columns
	o.Values = r.Values

	for i, value := range o.Values {
		for i2 := range value {
			switch v := value[i2].(type) {
			case float64:
				if math.IsNaN(v) {
					o.Values[i][i2] = 0
				}
				if math.IsInf(v, 1) {
					o.Values[i][i2] = 0
				}
				if math.IsInf(v, -1) {
					o.Values[i][i2] = 0
				}
			case float32:
				if math.IsNaN(float64(v)) {
					o.Values[i][i2] = 0
				}
				if math.IsInf(float64(v), 1) {
					o.Values[i][i2] = 0
				}
				if math.IsInf(float64(v), -1) {
					o.Values[i][i2] = 0
				}
			}
		}
	}
	o.Partial = r.Partial
	return sonic.ConfigStd.Marshal(&o)
}

// SameSeries returns true if r contains values for the same series as o.
func (r *Row) SameSeries(o *Row) bool {
	if r.Name != o.Name {
		return false
	}
	if len(r.Tags) != len(o.Tags) {
		return false
	}
	for k, v1 := range r.Tags {
		if v2, ok := o.Tags[k]; !ok || v1 != v2 {
			return false
		}
	}
	return true
}

// tagsHash returns a hash of tag key/value pairs.
func (r *Row) tagsHash() uint64 {
	h := NewInlineFNV64a()
	keys := r.tagsKeys()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte(r.Tags[k]))
	}
	return h.Sum64()
}

// tagKeys returns a sorted list of tag keys.
func (r *Row) tagsKeys() []string {
	a := make([]string, 0, len(r.Tags))
	for k := range r.Tags {
		a = append(a, k)
	}
	sort.Strings(a)
	return a
}

// Rows represents a collection of rows. Rows implements sort.Interface.
type Rows []*Row

// Len implements sort.Interface.
func (p Rows) Len() int { return len(p) }

// Less implements sort.Interface.
func (p Rows) Less(i, j int) bool {
	// Sort by name first.
	if p[i].Name != p[j].Name {
		return p[i].Name < p[j].Name
	}

	// Sort by tag set hash. Tags don't have a meaningful sort order so we
	// just compute a hash and sort by that instead. This allows the tests
	// to receive rows in a predictable order every time.
	return p[i].tagsHash() < p[j].tagsHash()
}

// Swap implements sort.Interface.
func (p Rows) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
