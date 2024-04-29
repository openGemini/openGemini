package consume

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeCursor(t *testing.T) {
	cursors := make([]*ConsumeCursor, 1)
	for k, _ := range cursors {
		cursors[k] = &ConsumeCursor{
			Time:           0,
			Reverse:        false,
			CursorID:       k,
			TaskNum:        1,
			CurrTotalPtNum: 1,
			Tasks:          make([]*ConsumeSegmentTask, 0, 1),
		}
	}
	cursors[0].Tasks = append(cursors[0].Tasks, &ConsumeSegmentTask{PtID: 0, CurrTask: &ConsumeTask{MetaIndexId: 1, BlockID: 2, Timestamp: 0, RemotePath: "shard", SgID: 1}})
	encode, err := cursors[0].EncodeToByte()
	if err != nil {
		t.Fatal(err.Error())
	}
	assert.Equal(t, "H4sIAAAAAAAA/zKoMayBYQ2DuDjDGsMaoxqDmuKMxKIUTUAAAAD//x7qGe0eAAAA", base64.StdEncoding.EncodeToString(encode))
}
