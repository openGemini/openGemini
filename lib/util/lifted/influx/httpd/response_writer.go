package httpd

import (
	"encoding/csv"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/influxdata/influxdb/models"
	jsoniter "github.com/json-iterator/go"
	"github.com/openGemini/openGemini/lib/util/lifted/promql2influxql"
	"github.com/tinylib/msgp/msgp"
)

// ResponseWriter is an interface for writing a response.
type ResponseWriter interface {
	// WriteResponse writes a response.
	WriteResponse(resp Response) (int, error)
	WritePromResponse(resp promql2influxql.Response) (int, error)

	http.ResponseWriter
}

type formatter interface {
	WriteResponse(w io.Writer, resp Response) error
	WritePromResponse(w io.Writer, resp promql2influxql.Response) error
}

type supportedContentType struct {
	full          string
	acceptType    string
	acceptSubType string
	formatter     func(pretty bool) formatter
}

var (
	csvFormatFactory     = func(pretty bool) formatter { return &csvFormatter{statementID: -1} }
	msgpackFormatFactory = func(pretty bool) formatter { return &msgpackFormatter{} }
	jsonFormatFactory    = func(pretty bool) formatter { return &jsonFormatter{Pretty: pretty} }

	contentTypes = []supportedContentType{
		{full: "application/json", acceptType: "application", acceptSubType: "json", formatter: jsonFormatFactory},
		{full: "application/csv", acceptType: "application", acceptSubType: "csv", formatter: csvFormatFactory},
		{full: "text/csv", acceptType: "text", acceptSubType: "csv", formatter: csvFormatFactory},
		{full: "application/x-msgpack", acceptType: "application", acceptSubType: "x-msgpack", formatter: msgpackFormatFactory},
	}
	defaultContentType = contentTypes[0]
)

// NewResponseWriter creates a new ResponseWriter based on the Accept header
// in the request that wraps the ResponseWriter.
func NewResponseWriter(w http.ResponseWriter, r *http.Request) ResponseWriter {
	pretty := r.URL.Query().Get("pretty") == "true"
	rw := &responseWriter{ResponseWriter: w}

	acceptHeaders := parseAccept(r.Header["Accept"])
	for _, accept := range acceptHeaders {
		for _, ct := range contentTypes {
			if match(accept, ct) {
				w.Header().Add("Content-Type", ct.full)
				rw.formatter = ct.formatter(pretty)
				return rw
			}
		}
	}
	w.Header().Add("Content-Type", defaultContentType.full)
	rw.formatter = defaultContentType.formatter(pretty)
	return rw
}

func match(ah accept, sct supportedContentType) bool {
	return (ah.Type == "*" || ah.Type == sct.acceptType) &&
		(ah.SubType == "*" || ah.SubType == sct.acceptSubType)
}

// responseWriter is an implementation of ResponseWriter.
type responseWriter struct {
	formatter interface {
		WriteResponse(w io.Writer, resp Response) error
		WritePromResponse(w io.Writer, resp promql2influxql.Response) error
	}
	http.ResponseWriter
}

type bytesCountWriter struct {
	w io.Writer
	n int
}

func (w *bytesCountWriter) Write(data []byte) (int, error) {
	n, err := w.w.Write(data)
	w.n += n
	return n, err
}

// WriteResponse writes the response using the formatter.
func (w *responseWriter) WriteResponse(resp Response) (int, error) {
	writer := bytesCountWriter{w: w.ResponseWriter}
	err := w.formatter.WriteResponse(&writer, resp)
	return writer.n, err
}

// WriteResponse writes the response using the formatter.
func (w *responseWriter) WritePromResponse(resp promql2influxql.Response) (int, error) {
	writer := bytesCountWriter{w: w.ResponseWriter}
	err := w.formatter.WritePromResponse(&writer, resp)
	return writer.n, err
}

// Flush flushes the ResponseWriter if it has a Flush() method.
func (w *responseWriter) Flush() {
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

// CloseNotify calls CloseNotify on the underlying http.ResponseWriter if it
// exists. Otherwise, it returns a nil channel that will never notify.
func (w *responseWriter) CloseNotify() <-chan bool {
	if notifier, ok := w.ResponseWriter.(http.CloseNotifier); ok {
		return notifier.CloseNotify()
	}
	return nil
}

type sonicJsonFormatter struct {
	Pretty bool
}

func (f *sonicJsonFormatter) WriteResponse(w io.Writer, resp Response) error {
	var b []byte
	var err error
	if f.Pretty {
		b, err = json2.MarshalIndent(resp, "", "    ")
	} else {
		b, err = sonic.Marshal(resp)
	}

	if err != nil {
		unnestedErr := unnestError(err)
		// ignore any errors in this section, we already have a 'real' error to return
		resp := Response{Err: unnestedErr}
		if f.Pretty {
			b, _ = json2.MarshalIndent(resp, "", "    ")
		} else {
			b, _ = sonic.Marshal(resp)
		}
		w.Write(b)
		w.Write([]byte("\n"))
		return err
	}

	_, err = w.Write(b)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("\n"))
	return err
}

type jsonFormatter struct {
	Pretty bool
}

func (f *jsonFormatter) WriteResponse(w io.Writer, resp Response) error {
	var b []byte
	var err error
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if f.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		unnestedErr := unnestError(err)
		// ignore any errors in this section, we already have a 'real' error to return
		resp := Response{Err: unnestedErr}
		if f.Pretty {
			b, _ = json.MarshalIndent(resp, "", "    ")
		} else {
			b, _ = json.Marshal(resp)
		}
		w.Write(b)
		w.Write([]byte("\n"))
		return err
	}

	_, err = w.Write(b)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("\n"))
	return err
}

func (f *jsonFormatter) WritePromResponse(w io.Writer, resp promql2influxql.Response) error {
	var b []byte
	var err error
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	if f.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		unnestedErr := unnestError(err)
		// ignore any errors in this section, we already have a 'real' error to return
		resp := Response{Err: unnestedErr}
		if f.Pretty {
			b, _ = json.MarshalIndent(resp, "", "    ")
		} else {
			b, _ = json.Marshal(resp)
		}
		w.Write(b)
		w.Write([]byte("\n"))
		return err
	}

	_, err = w.Write(b)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("\n"))
	return err
}

func unnestError(err error) error {
	for errNested := err; errNested != nil; errNested = errors.Unwrap(err) {
		err = errNested
	}
	return err
}

type csvFormatter struct {
	statementID int
	columns     []string
}

func (f *csvFormatter) WriteResponse(w io.Writer, resp Response) (err error) {
	csv := csv.NewWriter(w)
	if resp.Err != nil {
		csv.Write([]string{"error"})
		csv.Write([]string{resp.Err.Error()})
		csv.Flush()
		return csv.Error()
	}

	for _, result := range resp.Results {
		if result.StatementID != f.statementID {
			// If there are no series in the result, skip past this result.
			if len(result.Series) == 0 {
				continue
			}

			// Set the statement id and print out a newline if this is not the first statement.
			if f.statementID >= 0 {
				// Flush the csv writer and write a newline.
				csv.Flush()
				if err := csv.Error(); err != nil {
					return err
				}

				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}
			}
			f.statementID = result.StatementID

			// Print out the column headers from the first series.
			f.columns = make([]string, 2+len(result.Series[0].Columns))
			f.columns[0] = "name"
			f.columns[1] = "tags"
			copy(f.columns[2:], result.Series[0].Columns)
			if err := csv.Write(f.columns); err != nil {
				return err
			}
		}

		for i, row := range result.Series {
			if i > 0 && !stringsEqual(result.Series[i-1].Columns, row.Columns) {
				// The columns have changed. Print a newline and reprint the header.
				csv.Flush()
				if err := csv.Error(); err != nil {
					return err
				}

				if _, err := io.WriteString(w, "\n"); err != nil {
					return err
				}

				f.columns = make([]string, 2+len(row.Columns))
				f.columns[0] = "name"
				f.columns[1] = "tags"
				copy(f.columns[2:], row.Columns)
				if err := csv.Write(f.columns); err != nil {
					return err
				}
			}

			f.columns[0] = row.Name
			f.columns[1] = ""
			if len(row.Tags) > 0 {
				hashKey := models.NewTags(row.Tags).HashKey()
				if len(hashKey) > 0 {
					f.columns[1] = string(hashKey[1:])
				}
			}
			for _, values := range row.Values {
				for i, value := range values {
					if value == nil {
						f.columns[i+2] = ""
						continue
					}

					switch v := value.(type) {
					case float64:
						f.columns[i+2] = strconv.FormatFloat(v, 'f', -1, 64)
					case int64:
						f.columns[i+2] = strconv.FormatInt(v, 10)
					case uint64:
						f.columns[i+2] = strconv.FormatUint(v, 10)
					case string:
						f.columns[i+2] = v
					case bool:
						if v {
							f.columns[i+2] = "true"
						} else {
							f.columns[i+2] = "false"
						}
					case time.Time:
						f.columns[i+2] = strconv.FormatInt(v.UnixNano(), 10)
					case *float64, *int64, *string, *bool:
						f.columns[i+2] = ""
					}
				}
				csv.Write(f.columns)
			}
		}
	}
	csv.Flush()
	return csv.Error()
}

func (f *csvFormatter) WritePromResponse(w io.Writer, resp promql2influxql.Response) (err error) {
	return nil
}

type msgpackFormatter struct{}

func (f *msgpackFormatter) ContentType() string {
	return "application/x-msgpack"
}

func (f *msgpackFormatter) WriteResponse(w io.Writer, resp Response) (err error) {
	enc := msgp.NewWriter(w)
	defer enc.Flush()

	enc.WriteMapHeader(1)
	if resp.Err != nil {
		enc.WriteString("error")
		enc.WriteString(resp.Err.Error())
		return nil
	} else {
		enc.WriteString("results")
		enc.WriteArrayHeader(uint32(len(resp.Results)))
		for _, result := range resp.Results {
			if result.Err != nil {
				enc.WriteMapHeader(1)
				enc.WriteString("error")
				enc.WriteString(result.Err.Error())
				continue
			}

			sz := 2
			if len(result.Messages) > 0 {
				sz++
			}
			if result.Partial {
				sz++
			}
			enc.WriteMapHeader(uint32(sz))
			enc.WriteString("statement_id")
			enc.WriteInt(result.StatementID)
			if len(result.Messages) > 0 {
				enc.WriteString("messages")
				enc.WriteArrayHeader(uint32(len(result.Messages)))
				for _, msg := range result.Messages {
					enc.WriteMapHeader(2)
					enc.WriteString("level")
					enc.WriteString(msg.Level)
					enc.WriteString("text")
					enc.WriteString(msg.Text)
				}
			}
			enc.WriteString("series")
			enc.WriteArrayHeader(uint32(len(result.Series)))
			for _, series := range result.Series {
				sz := 2
				if series.Name != "" {
					sz++
				}
				if len(series.Tags) > 0 {
					sz++
				}
				if series.Partial {
					sz++
				}
				enc.WriteMapHeader(uint32(sz))
				if series.Name != "" {
					enc.WriteString("name")
					enc.WriteString(series.Name)
				}
				if len(series.Tags) > 0 {
					enc.WriteString("tags")
					enc.WriteMapHeader(uint32(len(series.Tags)))
					for k, v := range series.Tags {
						enc.WriteString(k)
						enc.WriteString(v)
					}
				}
				enc.WriteString("columns")
				enc.WriteArrayHeader(uint32(len(series.Columns)))
				for _, col := range series.Columns {
					enc.WriteString(col)
				}
				enc.WriteString("values")
				enc.WriteArrayHeader(uint32(len(series.Values)))
				for _, values := range series.Values {
					enc.WriteArrayHeader(uint32(len(values)))
					for _, v := range values {
						enc.WriteIntf(v)
					}
				}
				if series.Partial {
					enc.WriteString("partial")
					enc.WriteBool(series.Partial)
				}
			}
			if result.Partial {
				enc.WriteString("partial")
				enc.WriteBool(true)
			}
		}
	}
	return nil
}

func (f *msgpackFormatter) WritePromResponse(w io.Writer, resp promql2influxql.Response) (err error) {
	return nil
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
