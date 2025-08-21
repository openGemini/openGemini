// Copyright 2025 openGemini Authors.
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

package config

import (
	"errors"
	"fmt"
	"net/url"
)

const (
	DefaultTopoManagerUrl = ""
)

type Topo struct {
	TopoManagerUrl string `toml:"topo-manager-url"`
}

func NewTopo() Topo {
	return Topo{
		TopoManagerUrl: DefaultTopoManagerUrl,
	}
}

func (c Topo) Validate() error {
	if c.TopoManagerUrl != "" {
		parsedUrl, err := url.Parse(c.TopoManagerUrl)
		if err != nil {
			return fmt.Errorf("invalid TopoManagerUrl: %s", err)
		}

		if parsedUrl.Scheme != "https" {
			return errors.New("TopoManagerUrl must use https scheme")
		}

		if parsedUrl.Host == "" {
			return errors.New("TopoManagerUrl must contain a valid hostname")
		}
	}
	return nil
}
