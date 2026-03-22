package bytesutil

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
)

type ByteBuffer = bytesutil.ByteBuffer
type ByteBufferPool = bytesutil.ByteBufferPool

var ToUnsafeBytes = bytesutil.ToUnsafeBytes
var ToUnsafeString = bytesutil.ToUnsafeString
var ResizeNoCopyNoOverallocate = bytesutil.ResizeNoCopyNoOverallocate
var ResizeWithCopyNoOverallocate = bytesutil.ResizeWithCopyNoOverallocate
var ResizeNoCopyMayOverallocate = bytesutil.ResizeNoCopyMayOverallocate
