//go:build !amd64 && !arm64

package models

import "encoding/json"

var json2 = struct {
	Marshal func(v any) ([]byte, error)
}{
	Marshal: json.Marshal,
}
