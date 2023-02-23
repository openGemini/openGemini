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
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/openGemini/openGemini/lib/errno"
)

type conf struct {
	C Castor `toml:"castor"`
}

func newConf() *conf {
	c := NewCastor()
	return &conf{c}
}

func Test_CorrectConfig(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		pyworker-addr = ["127.0.0.1:6666"]
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 10  # default: 30 second
  	[castor.detect]
		algorithm = ['BatchDIFFERENTIATEAD']
		config_filename = ['detect_base']
 	[castor.fit_detect]
		algorithm = ['DIFFERENTIATEAD']
		config_filename = ['detect_base']
  	[castor.predict]
		algorithm = ['METROPD']
		config_filename = ['predict_base']
  	[castor.fit]
		algorithm = ['METROPD']
		config_filename = ['fit_base']
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); err != nil {
		t.Fatal(err)
	}
}

func Test_InvalidPoolSize(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 0  # default: 30, connection pool to pyworker
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidPoolSize) {
		t.Fatal(err)
	}
}

func Test_InvalidResultWaitTimeout(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 0  # default: 30 second
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidResultWaitTimeout) {
		t.Fatal(err)
	}
}

func Test_InvalidAddr(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["abc:6666"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidAddr) {
		t.Fatal(err)
	}
}

func Test_InvalidAddr2(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["abc"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidAddr) {
		t.Fatal(err)
	}
}

func Test_InvalidAddr3(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidAddr) {
		t.Fatal(err)
	}
}

func Test_InvalidPort(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["127.0.0.1:abc"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidPort) {
		t.Fatal(err)
	}
}

func Test_InvalidPort2(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["127.0.0.1:-1"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.InvalidPort) {
		t.Fatal(err)
	}
}

func Test_IncompleteAlgoConf(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["127.0.0.1:6666"]
	[castor.detect]
		algorithm = ['BatchDIFFERENTIATEAD']
		config_filename = []
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.AlgoConfNotFound) {
		t.Fatal(err)
	}
}

func Test_IncompleteAlgo(t *testing.T) {
	confStr := `
	[castor]
		enabled = true
		connect-pool-size = 1  # default: 30, connection pool to pyworker
		result-wait-timeout = 1  # default: 30 second
		pyworker-addr = ["127.0.0.1:6666"]
	[castor.detect]
		algorithm = []
		config_filename = ["detect"]
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); !errno.Equal(err, errno.AlgoNotFound) {
		t.Fatal(err)
	}
}

func Test_CheckAlgoAndConfExistence(t *testing.T) {
	confStr := `
	[castor]
	enabled = true
	pyworker-addr = ["127.0.0.1:6666"]
	connect-pool-size = 1  # default: 30, connection pool to each pyworker
	result-wait-timeout = 10  # default: 30 second
	[castor.detect]
		algorithm = ['BatchDIFFERENTIATEAD']
		config_filename = ['detect_base']
	[castor.fit_detect]
		algorithm = ['DIFFERENTIATEAD']
		config_filename = ['detect_base']
	[castor.predict]
		algorithm = ['METROPD']
		config_filename = ['predict_base']
	[castor.fit]
		algorithm = ['METROPD']
		config_filename = ['fit_base']
	`
	c := newConf()
	toml.Decode(confStr, c)
	if err := c.C.Validate(); err != nil {
		t.Fatal(err)
	}

	if err := c.C.CheckAlgoAndConfExistence("BatchDIFFERENTIATEAD", "detect_base", "detect"); err != nil {
		t.Fatal(err)
	}
	if err := c.C.CheckAlgoAndConfExistence("BatchDIFFERENTIATEAD", "detect_base", "fit_detect"); !errno.Equal(err, errno.AlgoNotFound) {
		t.Fatal(err)
	}
	if err := c.C.CheckAlgoAndConfExistence("BatchDIFFERENTIATEAD", "fit_base", "detect"); !errno.Equal(err, errno.AlgoConfNotFound) {
		t.Fatal(err)
	}
	if err := c.C.CheckAlgoAndConfExistence("BatchDIFFERENTIATEAD", "detect_base", "abc"); !errno.Equal(err, errno.AlgoTypeNotFound) {
		t.Fatal(err)
	}
}
