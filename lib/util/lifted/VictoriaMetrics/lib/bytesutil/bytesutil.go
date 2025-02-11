package bytesutil

import (
	"unsafe"
)

// Resize resizes b to n bytes and returns b (which may be newly allocated).
func Resize(b []byte, n int) []byte {
	if nn := n - cap(b); nn > 0 {
		b = append(b[:cap(b)], make([]byte, nn)...)
	}
	return b[:n]
}

// ToUnsafeString converts b to string without memory allocations.
//
// The returned string is valid only until b is reachable and unmodified.
func ToUnsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// ToUnsafeBytes converts s to a byte slice without memory allocations.
//
// The returned byte slice is valid only until s is reachable and unmodified.
func ToUnsafeBytes(s string) (b []byte) {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
