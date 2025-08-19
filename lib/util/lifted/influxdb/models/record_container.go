package models

import (
	"bytes"
	"strings"

	"github.com/apache/arrow/go/v13/arrow"
)

var (
	NAME   = []byte(`"name":`)
	TAGS   = []byte(`"tags":`)
	PARSER = []byte(`":"`)
)

type RecordContainer struct {
	// Name is the measurement name.
	Name string `json:"name,omitempty"`
	// Tags for the series.
	Tags map[string]string `json:"tags,omitempty"`

	Partial bool `json:"partial,omitempty"`

	Data arrow.Record `json:"-"`
}

func (c *RecordContainer) NewSlice(start int64, end int64) *RecordContainer {
	if c.Data == nil {
		return nil
	}
	recordSlice := c.Data.NewSlice(start, end)
	return &RecordContainer{Name: c.Name, Tags: c.Tags, Data: recordSlice}
}

func (c *RecordContainer) NumRows() int64 {
	return c.Data.NumRows()
}

func (c *RecordContainer) Schema() *arrow.Schema {
	if c.Data == nil {
		return nil
	}
	return c.Data.Schema()
}

func (c *RecordContainer) BuildAppMetadata() []byte {
	var buf bytes.Buffer
	buf.WriteByte('{')
	hasName := c.Name != ""
	if hasName {
		buf.Write(NAME)
		buf.WriteString(escapeJSONString(c.Name))
	}
	if len(c.Tags) == 0 {
		buf.WriteByte('}')
		return buf.Bytes()
	}
	if hasName {
		buf.WriteByte(',')
	}
	buf.Write(TAGS)
	c.MarshalStringMap(&buf)
	buf.WriteByte('}')
	return buf.Bytes()
}

func (c *RecordContainer) MarshalStringMap(buf *bytes.Buffer) {
	buf.WriteByte('{')
	idx := 0
	for k, v := range c.Tags {
		buf.WriteByte('"')
		buf.WriteString(escapeJSONString(k))
		buf.Write(PARSER)
		buf.WriteString(escapeJSONString(v))
		buf.WriteByte('"')
		if idx < len(c.Tags)-1 {
			buf.WriteByte(',')
		}
		idx++
	}
	buf.WriteByte('}')
}

func escapeJSONString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\r", `\r`)
	s = strings.ReplaceAll(s, "\t", `\t`)
	s = strings.ReplaceAll(s, "\b", `\b`)
	s = strings.ReplaceAll(s, "\f", `\f`)
	return s
}
