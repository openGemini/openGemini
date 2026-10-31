package influx_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/stretchr/testify/require"
)

func TestGetStreamContextMaxLineSize(t *testing.T) {
	data := "12345678901234567890123456789012" // length == 32

	ctx1 := influx.GetStreamContext(bytes.NewBuffer([]byte(data)), 20)
	ctx2 := influx.GetStreamContext(bytes.NewBuffer([]byte(data)), 32)
	ctx3 := influx.GetStreamContext(bytes.NewBuffer([]byte(data)), 33)

	//expect false
	if ctx1.Read(len(data)) {
		t.Errorf("expected Read to return false when data length exceeds maxLineSize")
	}

	//expext true
	if !ctx2.Read(len(data)) {
		t.Errorf("expected Read to return true when data length exceeds maxLineSize")
	}

	//expext true
	if !ctx3.Read(len(data)) {
		t.Errorf("expected Read to return true when data length exceeds maxLineSize")
	}

}

func TestReadLinesBlockExt_NewlineInQuotedStringField(t *testing.T) {
	values := []string{
		"line1\nline2",
		"{\n\t\"a\": 1,\n\t\"b\": \"s\"\n}",
		"no newline",
		"\n\n\n",
		"tail\n",
	}
	var data strings.Builder
	for i, v := range values {
		v = strings.ReplaceAll(v, `"`, `\"`)
		_, _ = fmt.Fprintf(&data, "mst,host=a\"b value=\"%s\",id=%di %d\n", v, i, i+1)
	}
	// the last row has no trailing newline
	data.WriteString(`mst,host=c value="end",id=5i 6`)
	values = append(values, "end")

	// small blocks make block boundaries fall inside the quoted string field values
	for _, blockSize := range []int{8, 16, 32, 64, 1024} {
		var dstBuf, tailBuf []byte
		var err error
		var rows influx.PointRows
		var got []string
		r := strings.NewReader(data.String())
		for {
			dstBuf, tailBuf, err = influx.ReadLinesBlockExt(r, dstBuf, tailBuf, 1024, blockSize)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			require.NoError(t, rows.Unmarshal(string(dstBuf), false), "blockSize: %d, block: %q", blockSize, dstBuf)
			for _, row := range rows.Rows {
				require.Len(t, row.Fields, 2)
				require.Equal(t, float64(len(got)), row.Fields[1].NumValue)
				require.Equal(t, int64(len(got)+1), row.Timestamp)
				got = append(got, row.Fields[0].StrValue)
			}
		}
		require.Equal(t, values, got, "blockSize: %d", blockSize)
	}
}
