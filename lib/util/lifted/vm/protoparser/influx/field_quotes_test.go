package influx

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These cases were also checked against models.ParsePointsWithPrecision in
// InfluxDB v1.0.0 and v2.0.0. Quotes in keys are literal; only a double quote at
// the beginning of a field value opens a string, which may contain newlines.
// See testdata/README.md for upstream decoding differences and reproduction.
func TestFieldQuoteBoundaries(t *testing.T) {
	var cases []struct {
		Name, Input, Measurement string
		Fields                   map[string]interface{}
		Tags                     map[string]string
		Timestamp                int64
		Error                    bool
	}
	data, err := os.ReadFile("testdata/field_quotes.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &cases))
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			input := tc.Input + "\n"
			if !tc.Error {
				input += "sentinel value=42i 42\n"
				// Quotes in a key must not make a string extend into the next point.
				require.Equal(t, len(tc.Input), nextRowEnd(input, !strings.Contains(input, `\`)))
			}
			checkRow := func(t *testing.T, r Row, index int) {
				t.Helper()
				if index == 1 {
					require.Equal(t, "sentinel", r.Name)
					require.Equal(t, int64(42), r.Timestamp)
					require.Len(t, r.Fields, 1)
					require.Equal(t, "value", r.Fields[0].Key)
					require.Equal(t, int32(Field_Type_Int), r.Fields[0].Type)
					require.Equal(t, float64(42), r.Fields[0].NumValue)
					return
				}
				require.Zero(t, index)
				measurement := tc.Measurement
				if measurement == "" {
					measurement = "cpu"
				}
				require.Equal(t, measurement, r.Name)
				timestamp := tc.Timestamp
				if timestamp == 0 {
					timestamp = NoTimestamp
				}
				require.Equal(t, timestamp, r.Timestamp)
				require.Len(t, r.Tags, len(tc.Tags))
				for _, tag := range r.Tags {
					require.Contains(t, tc.Tags, tag.Key)
					require.Equal(t, tc.Tags[tag.Key], tag.Value)
				}
				require.Len(t, r.Fields, len(tc.Fields))
				for _, field := range r.Fields {
					require.Contains(t, tc.Fields, field.Key)
					switch want := tc.Fields[field.Key].(type) {
					case string:
						require.Equal(t, int32(Field_Type_String), field.Type)
						require.Equal(t, want, field.StrValue)
					case float64:
						require.Equal(t, int32(Field_Type_Int), field.Type)
						require.Equal(t, want, field.NumValue)
					default:
						t.Fatalf("unsupported expected value %T", want)
					}
				}
			}
			t.Run("rows", func(t *testing.T) {
				var rows PointRows
				err := rows.Unmarshal(input, false)
				if tc.Error {
					require.Error(t, err)
					require.Empty(t, rows.Rows)
					return
				}
				require.NoError(t, err)
				require.Len(t, rows.Rows, 2)
				for i, row := range rows.Rows {
					checkRow(t, row, i)
				}
			})
			for _, blockSize := range []int{1, 8, 16, 32, 64, 1024} {
				t.Run(fmt.Sprintf("block_%d", blockSize), func(t *testing.T) {
					reader := strings.NewReader(input)
					var buf, tail []byte
					var rows PointRows
					count, parseErrors := 0, 0
					for {
						buf, tail, err = ReadLinesBlockExt(reader, buf, tail, 1024, blockSize)
						if err == io.EOF {
							break
						}
						require.NoError(t, err)
						err = rows.Unmarshal(string(buf), false)
						if err != nil {
							parseErrors++
						}
						if !tc.Error {
							require.NoError(t, err)
							for _, row := range rows.Rows {
								checkRow(t, row, count)
								count++
							}
						} else {
							count += len(rows.Rows)
						}
					}
					if tc.Error {
						require.Positive(t, parseErrors)
						require.Zero(t, count)
					} else {
						require.Equal(t, 2, count)
					}
				})
			}
		})
	}
}

func TestRowBoundaryOutsideFieldValues(t *testing.T) {
	for _, row := range []string{
		`cpu a"b`,
		`cpu "key=value"`,
		`cpu value=1i "123`,
		`cpu value=1"`,
		`cpu a"b=1i\`,
	} {
		t.Run(row, func(t *testing.T) {
			input := row + "\n" + "sentinel value=42i 42\n"
			require.Equal(t, len(row), nextRowEnd(input, !strings.Contains(input, `\`)))
			require.Equal(t, len(row), lastRowEnd([]byte(row+"\n")))
		})
	}
}

func BenchmarkFieldQuoteParsing(b *testing.B) {
	for _, tc := range []struct{ name, row string }{
		{"numeric", "cpu,host=a value=1i,other=2i 1\n"},
		{"strings", "cpu,host=a value=\"hello world\",other=\"another value\" 1\n"},
		{"multiline", "cpu,host=a value=\"line1\nline2\",other=\"line3\nline4\" 1\n"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			input := strings.Repeat(tc.row, 256)
			var rows PointRows
			b.SetBytes(int64(len(input)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := rows.Unmarshal(input, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
