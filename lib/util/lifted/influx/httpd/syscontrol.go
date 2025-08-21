package httpd

import (
	"bufio"
	"fmt"
	"net/http"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/msgservice"
	"github.com/openGemini/openGemini/lib/syscontrol"
	"github.com/openGemini/openGemini/lib/util"
)

// curl -i -XGET 'http://127.0.0.1:8086/debug/query?mod=shards&db=mydb&rp=myrp&pt=2&shard=1'
func (h *Handler) serveDebugQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if r.Method != http.MethodGet {
		h.httpError(w, "invalid method", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	mod := q.Get("mod")
	if mod == "" {
		h.httpError(w, "invalid mod", http.StatusBadRequest)
		return
	}

	p := make(map[string]string, len(q))
	for k, v := range q {
		if k != mod && len(v) > 0 {
			p[k] = v[0]
		}
	}

	resp, err := func(mod string, param map[string]string) (string, error) {
		switch mod {
		case "shards":
			return syscontrol.ProcessQueryRequest(syscontrol.QueryShardStatus, param)
		default:
			return "", fmt.Errorf("unknown mod: %s", mod)
		}
	}(mod, p)
	if err != nil {
		h.httpError(w, "process query request error: "+err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := fmt.Fprintln(w, resp); err != nil {
		h.httpError(w, "write query request resp error: "+err.Error(), http.StatusBadRequest)
	}
}

func (h *Handler) serveDebug(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	mod := q.Get("mod")
	if mod == "" {
		h.httpError(w, "invalid mod", http.StatusBadRequest)
		return
	}

	var req msgservice.SysCtrlRequest

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

	nw := bufio.NewWriter(w)
	nw.WriteString("{\n\t")
	var err error
	if req.Mod() == syscontrol.Backup {
		err = syscontrol.ProcessBackup(req, nw, config.CombineDomain(h.SQLConfig.HTTP.Domain, h.Config.BindAddress))
	} else {
		err = syscontrol.ProcessRequest(req, nw)
	}
	if err != nil {
		h.httpError(w, "sysctrl execute error: "+err.Error(), http.StatusBadRequest)
		return
	}
	nw.WriteString("\n}\n")
	util.MustRun(nw.Flush)
}
