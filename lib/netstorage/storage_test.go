package netstorage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNetStorage_GetQueriesOnNode(t *testing.T) {
	mc := MockMetaClient{}
	mc.addDataNode(newDataNode(1, "127.0.0.9:12345"))
	ns := NewNetStorage(&mc)

	_, err := ns.GetQueriesOnNode(1)
	assert.EqualError(t, fmt.Errorf("no connections available, node: 1, 127.0.0.9:12345"), err.Error())
}
