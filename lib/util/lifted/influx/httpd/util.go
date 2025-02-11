package httpd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/openGemini/openGemini/lib/statisticsPusher"
)

func hasSensitiveWord(url string) bool {
	url = strings.ToLower(url)
	sensitiveInfo := "password"
	return strings.Contains(url, sensitiveInfo)
}

func HideQueryPassword(url string) string {
	if !hasSensitiveWord(url) {
		return url
	}
	var buf strings.Builder

	create := "with password"
	url = strings.ToLower(url)
	if strings.Contains(url, create) {
		fields := strings.Fields(url)
		for i, s := range fields {
			if s == "password" {
				buf.WriteString(strings.Join(fields[:i+1], " "))
				buf.WriteString(" [REDACTED] ")
				if i < len(fields)-2 {
					buf.WriteString(strings.Join(fields[i+2:], " "))
				}
				return buf.String()
			}
		}
	}
	set := "set password"
	if strings.Contains(url, set) {
		fields := strings.SplitAfter(url, "=")
		buf.WriteString(fields[0])
		buf.WriteString(" [REDACTED]")
		return buf.String()
	}
	return url
}

func SetStatsResponse(pusher *statisticsPusher.StatisticsPusher, w http.ResponseWriter, r *http.Request) {
	if pusher == nil {
		return
	}

	stats, err := pusher.CollectOpsStatistics()
	if err != nil {
		HttpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintln(w, "{")

	first := true
	uniqueKeys := make(map[string]int)
	for _, s := range stats {
		val, err := json.Marshal(s)
		if err != nil {
			continue
		}

		// Very hackily create a unique key.
		buf := bytes.NewBufferString(s.Name)
		key := buf.String()
		v := uniqueKeys[key]
		uniqueKeys[key] = v + 1
		if v > 0 {
			fmt.Fprintf(buf, ":%d", v)
			key = buf.String()
		}

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q: ", key)
		_, err = w.Write(bytes.TrimSpace(val))
		if err != nil {
			HttpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	fmt.Fprintln(w, "\n}")
}

// HttpError writes an error to the client in a standard format.
func HttpError(w http.ResponseWriter, errmsg string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", ""))
	} else if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	response := Response{Err: errors.New(errmsg)}
	if rw, ok := w.(ResponseWriter); ok {
		w.WriteHeader(code)
		_, _ = rw.WriteResponse(response)
		return
	}

	// Default implementation if the response writer hasn't been replaced
	// with our special response writer type.
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)
	b, err := json.Marshal(response)
	if err != nil {
		fmt.Println("json marshal error", err)
	}
	_, err = w.Write(b)
	if err != nil {
		fmt.Println("ResponseWriter write error", err)
	}
}
