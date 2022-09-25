package httpd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/syscontrol"
)

func (h *Handler) serveDebug(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	mod := q.Get("mod")
	if mod == "" {
		h.httpError(w, "invalid mod", http.StatusBadRequest)
		return
	}

	var req netstorage.SysCtrlRequest

	mp := make(map[string]string, len(q))
	for k, v := range q {
		if k == mod {
			continue
		}
		if len(v) < 1 {
			continue
		}
		mp[k] = v[0]
	}
	req.SetParam(mp)
	req.SetMod(mod)

	var sb strings.Builder
	sb.WriteString("{\n\t")
	err := syscontrol.ProcessRequest(req, &sb)
	if err != nil {
		h.httpError(w, "sysctrl execute error: "+err.Error(), http.StatusBadRequest)
		return
	}
	sb.WriteString("\n}\n")
	_, _ = fmt.Fprintln(w, sb.String())
}
