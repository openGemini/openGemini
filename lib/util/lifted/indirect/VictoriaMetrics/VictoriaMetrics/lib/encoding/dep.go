package encoding

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
)

var MarshalVarUint64s = encoding.MarshalVarUint64s
var GetUint64s = encoding.GetUint64s
var PutUint64s = encoding.PutUint64s
var CompressZSTDLevel = encoding.CompressZSTDLevel

var DecompressZSTD = encoding.DecompressZSTD
var UnmarshalVarUint64s = encoding.UnmarshalVarUint64s
var MarshalUint64 = encoding.MarshalUint64
var UnmarshalUint64 = encoding.UnmarshalUint64

var MarshalBytes = encoding.MarshalBytes
var MarshalUint32 = encoding.MarshalUint32
var UnmarshalBytes = encoding.UnmarshalBytes
var UnmarshalUint32 = encoding.UnmarshalUint32
