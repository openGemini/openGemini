// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package influxql

import (
	"bytes"
)

func (f *Field) WriteDigest(buf *bytes.Buffer) {
	switch item := f.Expr.(type) {
	case *VarRef:
		item.WriteDigest(buf)
	default:
		buf.WriteString(item.String())
	}
	buf.WriteString(" AS ")
	buf.WriteString(f.Alias)
}

func (f *Field) Equal(other *Field) bool {
	if f == nil && other == nil {
		return true
	}
	if (f == nil && other != nil) || (f != nil && other == nil) {
		return false
	}

	if f.Alias != other.Alias {
		return false
	}

	switch e1 := f.Expr.(type) {
	case *VarRef:
		e2, ok := other.Expr.(*VarRef)
		return ok && e1.Equal(e2)
	default:
		return f.String() == other.String()
	}
}

func (r *VarRef) WriteDigest(buf *bytes.Buffer) {
	buf.WriteString(r.Val)
	if r.Type == Unknown {
		return
	}
	buf.WriteString("::")
	buf.WriteString(r.Type.String())
}

func (r *VarRef) Equal(other *VarRef) bool {
	if r == nil && other == nil {
		return true
	}
	if r != nil && other != nil {
		return r.Val == other.Val && r.Alias == other.Alias && r.Type == other.Type
	}
	return false
}

func (a Fields) WriteDigest(buf *bytes.Buffer) {
	for _, f := range a {
		f.WriteDigest(buf)
		buf.WriteByte(',')
	}
}

// WriteDigest returns a string representation of the call.
func (c *Call) WriteDigest(b *bytes.Buffer) {
	b.WriteString(c.Name)
	b.WriteString("(")

	for i, arg := range c.Args {
		if i > 0 {
			b.WriteString(",")
		}
		switch item := arg.(type) {
		case *VarRef:
			item.WriteDigest(b)
		default:
			b.WriteString(item.String())
		}
	}
	b.WriteString(")")
}
