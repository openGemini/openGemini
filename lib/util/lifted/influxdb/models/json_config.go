//go:build amd64 || arm64

package models

import "github.com/bytedance/sonic"

var json2 = sonic.Config{
	SortMapKeys:             true,
	NoValidateJSONMarshaler: true,
	NoValidateJSONSkip:      true,
}.Froze()
