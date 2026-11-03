# Field quote compatibility

`field_quotes.json` is used by `TestFieldQuoteBoundaries` for whole-payload parsing
and streaming at block sizes 1, 8, 16, 32, 64, and 1024 bytes. Every valid case is
followed by a sentinel point to detect merged or dropped rows. Invalid cases must
return an error and no points.

The same 21 inputs were executed against the unmodified `models` and `pkg/escape`
packages from the exact InfluxDB **v1.0.0** and **v2.0.0** release tags, using
`models.ParsePointsWithPrecision`. Both implementations:

- Treat quotes in measurement names, tag keys/values, and field keys as literal
  characters, rather than string delimiters.
- Accept quoted string field values containing raw LF or CRLF, including with
  quoted characters in the field key and with following fields or points.
- Reject a quote spanning the key/value separator, e.g. `cpu "key=value"`.
- Reject single-quoted string values and unterminated double-quoted values.

This is parser-level verification, not a server HTTP/storage integration test.
The current [v1 documentation](https://docs.influxdata.com/influxdb/v1/write_protocols/line_protocol_reference/)
and [v2 documentation](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/)
say raw newlines are unsupported. The multiline tests describe actual behavior
of the pinned parsers, not a documented protocol guarantee.

Sources:

- [v1.0.0 parser](https://github.com/influxdata/influxdb/blob/v1.0.0/models/points.go)
- [v2.0.0 parser](https://github.com/influxdata/influxdb/blob/v2.0.0/models/points.go)

## Observed decoding differences

The fixture's `fields` entries are the expected openGemini results.
`influxdb1Fields` and `influxdb2Fields` record upstream differences explicitly:

- Both upstream versions remove a backslash before a quote in a field key.
  openGemini already preserves that backslash. This change retains its existing
  key unescaping behavior; escaping a literal key quote is unnecessary.
- In v1.0.0, decoding a string ending with an escaped backslash can consume the
  following field into the string value. v2.0.0 and openGemini decode the two
  fields correctly. The v1.0.0 decoding bug is not reproduced.

## Reproduce the upstream checks

Download the source archives for tags `v1.0.0` and `v2.0.0` from
`https://github.com/influxdata/influxdb`. Extract `models/*.go` and
`pkg/escape/*.go` into separate temporary directories. Give these directories
minimal `go.mod` files with the module paths below (the selected production
packages only depend on the standard library and each other).

In a separate temporary harness directory, copy `verify_influxdb.go` and create:

```go
module compatibility

go 1.25.0

require (
    github.com/influxdata/influxdb v1.0.0
    github.com/influxdata/influxdb/v2 v2.0.0
)

replace github.com/influxdata/influxdb => /absolute/path/to/influxdb-1.0.0
replace github.com/influxdata/influxdb/v2 => /absolute/path/to/influxdb-2.0.0
```

Run `go run verify_influxdb.go /absolute/path/to/field_quotes.json`.
All 42 version/case combinations must pass, including the explicitly recorded
decoding differences. No upstream dependency is added to openGemini.
