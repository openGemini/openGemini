package influx_test

import (
	"bytes"
	"testing"

	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
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
