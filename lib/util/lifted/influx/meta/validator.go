// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"strings"
	"unicode"
)

var unsupportedCharsInDBName = `,:;./\`
var unsupportedCharsInMstName = `,;/\`

// ValidName checks to see if the given name can would be valid for DB/RP name
func ValidName(name string) bool {
	return validName(name, unsupportedCharsInDBName)
}

func ValidMeasurementName(name string) bool {
	if name == "." || name == ".." {
		return false
	}
	return validName(name, unsupportedCharsInMstName)
}

func validName(name string, unsupported string) bool {
	for _, r := range name {
		if !unicode.IsPrint(r) {
			return false
		}
	}

	return name != "" && !strings.ContainsAny(name, unsupported)
}
