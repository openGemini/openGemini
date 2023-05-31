package httpd

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bmizerany/pat"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/openGemini/openGemini/open_src/influx/httpd/config"
)

var addr = "127.0.0.2:8901"

func mockHTTPServer(t *testing.T) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Error(err)
	}

	h := &Handler{
		mux: pat.New(),
		Config: &config.Config{
			AuthEnabled: false,
		},
		requestTracker: httpd.NewRequestTracker(),
	}
	h.AddRoutes([]Route{
		Route{ // sysCtrl
			"sysCtrl",
			"POST", "/debug/ctrl", false, true, h.serveSysCtrl,
		},
	}...)

	err = http.Serve(ln, h)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		t.Errorf("listener failed: addr=%s, err=%s", ln.Addr(), err)
	}
}

func TestDebugCtrl(t *testing.T) {
	go mockHTTPServer(t)
	time.Sleep(time.Second)
	resp, err := http.Post(fmt.Sprintf("http://%s/debug/ctrl?mod=invalid", addr), "", nil)
	if err != nil {
		t.Fatalf("%v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%v", err)
	}
	exp := `{"error":"sysctrl execute error: unknown sysctrl mod: invalid"}`

	if strings.TrimSpace(string(body)) != exp {
		t.Fatalf("invalid response data. exp: %s; got: %s", exp, string(body))
	}
}
