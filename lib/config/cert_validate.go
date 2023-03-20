/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/openGemini/openGemini/lib/crypto"
	"github.com/openGemini/openGemini/lib/errno"
)

const (
	certExpireWarn      = time.Hour * 24 * 30
	minRSAPublicKeySize = 256 // 2048 bit
)

type CertValidator struct {
	certFile string
	keyFile  string

	cert *tls.Certificate
	leaf *x509.Certificate
}

func NewCertValidator(certFile, keyFile string) *CertValidator {
	return &CertValidator{
		certFile: certFile,
		keyFile:  keyFile,
	}
}

func (c *CertValidator) init() error {
	cert, err := tls.X509KeyPair([]byte(crypto.DecryptFromFile(c.certFile)), []byte(crypto.DecryptFromFile(c.keyFile)))
	if err != nil {
		return err
	}
	c.cert = &cert

	if cert.Leaf != nil {
		c.leaf = cert.Leaf
		return nil
	}

	c.leaf, err = x509.ParseCertificate(cert.Certificate[0])
	return err
}

func (c *CertValidator) Validate() error {
	if err := c.init(); err != nil {
		return err
	}

	var handles = []func() error{
		c.validatePublicKeySize,
		c.validateExpire,
		c.validateSignAlgo,
	}

	for _, fn := range handles {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

func (c *CertValidator) validatePublicKeySize() error {
	switch c.leaf.PublicKeyAlgorithm {
	case x509.RSA:
		pk, ok := c.leaf.PublicKey.(*rsa.PublicKey)
		if !ok {
			return errno.NewError(errno.InvalidPublicKey, reflect.TypeOf(c.leaf.PublicKey))
		}

		if pk.Size() < minRSAPublicKeySize {
			return errno.NewError(errno.ShortPublicKey, minRSAPublicKeySize*8, pk.Size()*8)
		}
	default:
		return nil
	}

	return nil
}

func (c *CertValidator) validateSignAlgo() error {
	var err error
	switch c.leaf.SignatureAlgorithm {
	case x509.MD2WithRSA, x509.MD5WithRSA, x509.SHA1WithRSA, x509.ECDSAWithSHA1:
		err = errno.NewError(errno.UnsupportedSignAlgo, c.leaf.SignatureAlgorithm.String())
	default:
		err = nil
	}
	return err
}

func (c *CertValidator) validateExpire() error {
	if c.leaf.NotAfter.Before(time.Now()) {
		return errno.NewError(errno.CertificateExpired,
			c.certFile, c.leaf.NotAfter.Format("2006-01-02"))
	}

	if c.leaf.NotAfter.Before(time.Now().Add(certExpireWarn)) {
		msg := fmt.Sprintf("certificate: %s will expire on %s",
			c.certFile, c.leaf.NotAfter.Format("2006-01-02"))
		fmt.Println(msg)
		_, _ = os.Stderr.WriteString(msg)
	}

	return nil
}
