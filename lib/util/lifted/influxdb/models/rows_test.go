package models

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSameSeries(t *testing.T) {
	var mst = "aaaaaabbbbbbbbbbccccccccc"
	row1 := &Row{Name: mst, Tags: map[string]string{"a": "abb"}}
	row2 := &Row{Name: mst, Tags: map[string]string{"aa": "bb"}}
	assert.False(t, row1.SameSeries(row2))
}
