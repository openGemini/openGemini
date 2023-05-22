package httpd

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/services/httpd"
	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/open_src/influx/httpd/config"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"go.uber.org/zap"
)

// Service manages the listener and handler for an HTTP endpoint.
type Service struct {
	Ln        []net.Listener
	addr      string
	https     bool
	cert      string
	key       string
	limit     int
	tlsConfig *tls.Config
	err       chan error

	unixSocket         bool
	unixSocketPerm     uint32
	unixSocketGroup    int
	bindSocket         string
	unixSocketListener net.Listener

	Handler *Handler

	Logger    *zap.Logger
	whiteList string
}

// NewService returns a new instance of Service.
func NewService(c config.Config) *Service {
	s := &Service{
		addr:           c.BindAddress,
		https:          c.HTTPSEnabled,
		cert:           c.HTTPSCertificate,
		key:            c.HTTPSPrivateKey,
		limit:          c.MaxConnectionLimit,
		tlsConfig:      c.TLS,
		err:            make(chan error),
		unixSocket:     c.UnixSocketEnabled,
		unixSocketPerm: uint32(c.UnixSocketPermissions),
		bindSocket:     c.BindSocket,
		Logger:         logger.GetLogger().With(zap.String("service", "httpd")),
		whiteList:      c.BindAddress,
		Handler:        NewHandler(c),
	}
	if s.tlsConfig == nil {
		s.tlsConfig = new(tls.Config)
	}
	if s.key == "" {
		s.key = s.cert
	}
	if c.UnixSocketGroup != nil {
		s.unixSocketGroup = int(*c.UnixSocketGroup)
	}
	return s
}

func (s *Service) Openlistener(addr string) error {
	// Open listener.
	if s.https {
		cert, err := tls.X509KeyPair([]byte(crypto.DecryptFromFile(s.cert)), []byte(crypto.DecryptFromFile(s.key)))
		if err != nil {
			return err
		}

		tlsConfig := s.tlsConfig.Clone()
		tlsConfig.Certificates = []tls.Certificate{cert}

		listener, err := tls.Listen("tcp", addr, tlsConfig)
		if err != nil {
			return err
		}

		s.Ln = append(s.Ln, listener)
	} else {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}

		s.Ln = append(s.Ln, listener)
	}
	s.Logger.Info("Listening on HTTP",
		zap.Stringer("addr", s.Ln[len(s.Ln)-1].Addr()),
		zap.Bool("https", s.https))
	return nil
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting HTTP service", zap.Bool("authentication", s.Handler.Config.AuthEnabled))

	s.Handler.Open()

	addrs := strings.Split(s.addr, ",")
	if len(addrs) <= 0 {
		return fmt.Errorf("http addr format error")
	}
	for _, addr := range addrs {
		if err := s.Openlistener(addr); err != nil {
			return err
		}
	}

	// Open unix socket listener.
	if s.unixSocket {
		if runtime.GOOS == "windows" {
			return fmt.Errorf("unable to use unix socket on windows")
		}
		if err := os.MkdirAll(path.Dir(s.bindSocket), 0750); err != nil {
			return err
		}
		if err := syscall.Unlink(s.bindSocket); err != nil && !os.IsNotExist(err) {
			return err
		}

		listener, err := net.Listen("unix", s.bindSocket)
		if err != nil {
			return err
		}
		if s.unixSocketPerm != 0 {
			if err := os.Chmod(s.bindSocket, os.FileMode(s.unixSocketPerm)); err != nil {
				return err
			}
		}
		if s.unixSocketGroup != 0 {
			if err := os.Chown(s.bindSocket, -1, s.unixSocketGroup); err != nil {
				return err
			}
		}

		s.Logger.Info("Listening on unix socket",
			zap.Stringer("addr", listener.Addr()))
		s.unixSocketListener = listener

		go s.serveUnixSocket()
	}

	// Enforce a connection limit if one has been given.
	if s.limit > 0 {
		for id, _ := range s.Ln {
			s.Ln[id] = httpd.LimitListener(s.Ln[id], s.limit)
		}
	}

	// wait for the listeners to start
	timeout := time.Now().Add(time.Second)
	for {
		initNet := true
		for _, ln := range s.Ln {
			if ln.Addr() == nil {
				initNet = false
				break
			}
		}
		if initNet {
			break
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("unable to open without http listener running")
		}
		time.Sleep(10 * time.Millisecond)
	}

	influx.StartUnmarshalWorkers()

	// Begin listening for requests in a separate goroutine.
	for _, ln := range s.Ln {
		go s.serveTCP(ln)
	}
	return nil
}

// Close closes the underlying listener.
func (s *Service) Close() error {
	s.Handler.Close()

	for _, ln := range s.Ln {
		if ln != nil {
			if err := ln.Close(); err != nil {
				return err
			}
		}
	}
	if s.unixSocketListener != nil {
		if err := s.unixSocketListener.Close(); err != nil {
			return err
		}
	}
	influx.StopUnmarshalWorkers()
	return nil
}

// Err returns a channel for fatal errors that occur on the listener.
func (s *Service) Err() <-chan error { return s.err }

// Addr returns the listener's address. Returns nil if listener is closed.
// test func, so return 0 index addr
func (s *Service) Addr() net.Addr {
	if s.Ln[0] != nil {
		return s.Ln[0].Addr()
	}
	return nil
}

// BoundHTTPAddr returns the string version of the address that the HTTP server is listening on.
// This is useful if you start an ephemeral server in test with bind address localhost:0.
// test func, so return 0 index addr
func (s *Service) BoundHTTPAddr() string {
	return s.Ln[0].Addr().String()
}

// serveTCP serves the handler from the TCP listener.
func (s *Service) serveTCP(listener net.Listener) {
	s.serve(listener)
}

// serveUnixSocket serves the handler from the unix socket listener.
func (s *Service) serveUnixSocket() {
	s.serve(s.unixSocketListener)
}

// serve serves the handler from the listener.
func (s *Service) serve(listener net.Listener) {
	// The listener was closed so exit
	// See https://github.com/golang/go/issues/4373
	err := http.Serve(listener, s.Handler)
	if err != nil && !strings.Contains(err.Error(), "closed") {
		s.err <- fmt.Errorf("listener failed: addr=%s, err=%s", listener.Addr(), err)
	}
}
