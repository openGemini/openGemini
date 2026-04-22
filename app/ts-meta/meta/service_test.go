// Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.
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
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func Test_StartReportServer(t *testing.T) {
	svc := &Service{
		store: &Store{
			cacheData: &meta.Data{
				ClusterID: 1314520,
			},
		},
		closing: make(chan struct{}),
		Logger:  logger.NewLogger(errno.ModuleUnknown),
	}
	go svc.StartReportServer()
	close(svc.closing)
	svc.closing = make(chan struct{})

	svc.runReportServer()
}

func Test_httpServer_httpsListener(t *testing.T) {
	// create cert and key of server and ca
	dir := t.TempDir()
	err := execCommand([]string{
		"openssl genrsa -out {{dir}}/ca.key 2048",
		`openssl req -new -key {{dir}}/ca.key -out {{dir}}/ca.csr -subj "{{subj}}"`,
		`openssl x509 -req -days 365 -sha256 -extensions v3_ca -signkey {{dir}}/ca.key -in {{dir}}/ca.csr -out {{dir}}/ca.cer`,

		`openssl genrsa -out {{dir}}/server.key 2048`,
		`openssl req -new -key {{dir}}/server.key -out {{dir}}/server.csr -subj "{{subj}}"`,
		`openssl x509 -req -days 365 -sha256 -extensions v3_req -CA {{dir}}/ca.cer -CAkey {{dir}}/ca.key -CAcreateserial -in {{dir}}/server.csr -out {{dir}}/server.cer`,
	}, dir)
	require.NoError(t, err)

	type fields struct {
		conf   *config.Meta
		logger *logger.Logger
		tls    *tls.Config
	}
	tests := []struct {
		name    string
		fields  fields
		want    net.Listener
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "case1: https case failed to parse the server certificate",
			fields: fields{
				conf: &config.Meta{
					HTTPSCertificate:       "invalid_server.cer",
					HTTPSPrivateKey:        dir + "/server.key",
					HTTPSClientCertificate: dir + "/ca.cer",
					HTTPBindAddress:        "127.0.0.8:12345",
				},
				tls:    &tls.Config{},
				logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta_http")),
			},
			wantErr: assert.Error,
		},
		{
			name: "case2: https case the client root certificate is not configured but still success",
			fields: fields{
				conf: &config.Meta{
					HTTPSCertificate:       dir + "/server.cer",
					HTTPSPrivateKey:        dir + "/server.key",
					HTTPSClientCertificate: "",
					HTTPBindAddress:        "127.0.0.8:12345",
				},
				tls:    &tls.Config{},
				logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta_http")),
			},
			wantErr: assert.NoError,
		},
		{
			name: "case3: https case failed to parse the ca certificate",
			fields: fields{
				conf: &config.Meta{
					HTTPSCertificate:       dir + "/server.cer",
					HTTPSPrivateKey:        dir + "/server.key",
					HTTPSClientCertificate: "invalid_ca.cer",
					HTTPBindAddress:        "127.0.0.8:12345",
				},
				tls:    &tls.Config{},
				logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta_http")),
			},
			wantErr: assert.Error,
		},
		{
			name: "case4: https case failed to listen address",
			fields: fields{
				conf: &config.Meta{
					HTTPSCertificate:       dir + "/server.cer",
					HTTPSPrivateKey:        dir + "/server.key",
					HTTPSClientCertificate: dir + "/ca.cer",
					HTTPBindAddress:        "invalid_address",
				},
				tls:    &tls.Config{},
				logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta_http")),
			},
			wantErr: assert.Error,
		},
		{
			name: "case5: https listener was created normally",
			fields: fields{
				conf: &config.Meta{
					HTTPSCertificate:       dir + "/server.cer",
					HTTPSPrivateKey:        dir + "/server.key",
					HTTPSClientCertificate: dir + "/ca.cer",
					HTTPBindAddress:        "127.0.0.8:12345",
				},
				tls:    &tls.Config{},
				logger: logger.NewLogger(errno.ModuleMeta).With(zap.String("service", "meta_http")),
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &httpServer{
				conf:   tt.fields.conf,
				logger: tt.fields.logger,
				tls:    tt.fields.tls,
			}
			ln, errGot := s.httpsListener()
			if ln != nil {
				if err = ln.Close(); err != nil {
					t.Errorf("Failed to close listener: %v", err)
				}
			}
			if !tt.wantErr(t, errGot, fmt.Sprintf("httpsListener()")) {
				return
			}
		})
	}
}

const subject = "/C=CN/ST=BJ/L=openGemini/O=openGemini/OU=openGemini/CN=openGemini"

func execCommand(cmdList []string, dir string) error {
	for _, c := range cmdList {
		c = strings.ReplaceAll(c, "{{dir}}", dir)
		c = strings.ReplaceAll(c, "{{subj}}", subject)
		fmt.Println(c)
		err := exec.Command("/bin/bash", "-c", c).Run()
		if err != nil {
			return err
		}
	}
	return nil
}

func TestHttpServer(t *testing.T) {
	dir := t.TempDir()
	cert := dir + "/cert.data"
	err := os.WriteFile(cert, make([]byte, 10), 0600)
	require.NoError(t, err)

	conf := config.NewMeta()
	conf.HTTPSEnabled = true
	conf.HTTPSCertificate = cert
	svc := newHttpServer(conf, &tls.Config{})
	err = svc.open(&httpHandler{})
	require.Error(t, err)
}
