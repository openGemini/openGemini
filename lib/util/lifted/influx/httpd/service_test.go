package httpd

import (
	"crypto/tls"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"

	"github.com/openGemini/openGemini/lib/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestService_Openlistener(t *testing.T) {

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
		Ln         []net.Listener
		addr       string
		https      bool
		cert       string
		key        string
		clientCert string
		tlsConfig  *tls.Config
		Logger     *zap.Logger
	}
	type args struct {
		addr string
	}
	defaultFields := fields{
		Ln:         []net.Listener{},
		https:      true,
		cert:       dir + "/server.cer",
		key:        dir + "/server.key",
		clientCert: dir + "/ca.cer",
		tlsConfig:  &tls.Config{},
		Logger:     logger.GetLogger().With(zap.String("service", "httpd")),
	}

	tests := []struct {
		name    string
		fields  func(f fields) fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "case1:https case failed to parse the server certificate",
			fields: func(f fields) fields {
				f.cert = "invalid_server.cer"
				return f
			},
			args: args{
				"127.0.0.8:12345",
			},
			wantErr: assert.Error,
		},
		{
			name: "case2:https case failed to parse the ca certificate",
			fields: func(f fields) fields {
				f.clientCert = "invalid_ca.cer"
				return f
			},
			args: args{
				"127.0.0.8:12345",
			},
			wantErr: assert.Error,
		},
		{
			name: "case3:https case failed to listen address",
			fields: func(f fields) fields {
				return f
			},
			args: args{
				"invalid_address",
			},
			wantErr: assert.Error,
		},
		{
			name: "case4:https listener was created normally",
			fields: func(f fields) fields {
				return f
			},
			args: args{
				"127.0.0.8:12345",
			},
			wantErr: assert.NoError,
		},
		{
			name: "case5:http case failed to listen address",
			fields: func(f fields) fields {
				f.https = false
				return f
			},
			args: args{
				"invalid_address",
			},
			wantErr: assert.Error,
		},
		{
			name: "case6:http case normal",
			fields: func(f fields) fields {
				f.https = false
				return f
			},
			args: args{
				"127.0.0.8:12345",
			},
			wantErr: assert.NoError,
		},
		{
			name: "case7:the client root certificate is not configured but still success",
			fields: func(f fields) fields {
				f.clientCert = ""
				return f
			},
			args: args{
				"127.0.0.8:12345",
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.fields(defaultFields)
			s := &Service{
				Ln:         f.Ln,
				addr:       f.addr,
				https:      f.https,
				cert:       f.cert,
				key:        f.key,
				clientCert: f.clientCert,
				tlsConfig:  f.tlsConfig,
				Logger:     f.Logger,
			}
			tt.wantErr(t, s.Openlistener(tt.args.addr), fmt.Sprintf("Openlistener(%v)", tt.args.addr))
			for _, l := range s.Ln {
				if err = l.Close(); err != nil {
					t.Errorf("Failed to close listener: %v", err)
				}
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
