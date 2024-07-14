/*
Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.

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

/*
Copyright (c) 2013-2018 InfluxData Inc.
This code is originally from:
https://github.com/influxdata/influxdb/blob/1.8/services/graphite/config_test.go
*/

package config_test

import (
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/openGemini/openGemini/lib/config"
)

func TestGraphiteConfig_Parse(t *testing.T) {
	// Parse configuration.
	var c config.GraphiteConfig
	if _, err := toml.Decode(`
bind-address = ":8080"
database = "mydb"
retention-policy = "myrp"
enabled = true
protocol = "tcp"
batch-size=100
batch-pending=77
batch-timeout="1s"
consistency-level="one"
templates=["servers.* .host.measurement*"]
tags=["region=us-east"]
`, &c); err != nil {
		t.Fatal(err)
	}

	// Validate configuration.
	if c.BindAddress != ":8080" {
		t.Fatalf("unexpected bind address: %s", c.BindAddress)
	} else if c.Database != "mydb" {
		t.Fatalf("unexpected database selected: %s", c.Database)
	} else if c.RetentionPolicy != "myrp" {
		t.Fatalf("unexpected retention policy selected: %s", c.RetentionPolicy)
	} else if !c.Enabled {
		t.Fatalf("unexpected graphite enabled: %v", c.Enabled)
	} else if c.Protocol != "tcp" {
		t.Fatalf("unexpected graphite protocol: %s", c.Protocol)
	} else if c.BatchSize != 100 {
		t.Fatalf("unexpected graphite batch size: %d", c.BatchSize)
	} else if c.BatchPending != 77 {
		t.Fatalf("unexpected graphite batch pending: %d", c.BatchPending)
	} else if time.Duration(c.BatchTimeout) != time.Second {
		t.Fatalf("unexpected graphite batch timeout: %v", c.BatchTimeout)
	} else if c.ConsistencyLevel != "one" {
		t.Fatalf("unexpected graphite consistency setting: %s", c.ConsistencyLevel)
	}

	if len(c.Templates) != 1 && c.Templates[0] != "servers.* .host.measurement*" {
		t.Fatalf("unexpected graphite templates setting: %v", c.Templates)
	}
	if len(c.Tags) != 1 && c.Tags[0] != "regsion=us-east" {
		t.Fatalf("unexpected graphite templates setting: %v", c.Tags)
	}
}

func TestConfigValidateEmptyTemplate(t *testing.T) {
	c := &config.GraphiteConfig{}
	c.Templates = []string{""}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Templates = []string{"     "}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}
}

func TestConfigValidateTooManyField(t *testing.T) {
	c := &config.GraphiteConfig{}
	c.Templates = []string{"a measurement b c"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}
}

func TestConfigValidateTemplatePatterns(t *testing.T) {
	c := &config.GraphiteConfig{}
	c.Templates = []string{"*measurement"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Templates = []string{".host.region"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}
}

func TestConfigValidateFilter(t *testing.T) {
	c := &config.GraphiteConfig{}
	c.Templates = []string{".server measurement*"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Templates = []string{".    .server measurement*"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Templates = []string{"server* measurement*"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}
}

func TestConfigValidateTemplateTags(t *testing.T) {
	c := &config.GraphiteConfig{}
	c.Templates = []string{"*.server measurement* foo"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Templates = []string{"*.server measurement* foo=bar="}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Templates = []string{"*.server measurement* foo=bar,"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Templates = []string{"*.server measurement* ="}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}
}

func TestConfigValidateDefaultTags(t *testing.T) {
	c := &config.GraphiteConfig{}
	c.Tags = []string{"foo"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Tags = []string{"foo=bar="}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Tags = []string{"foo=bar", ""}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	c.Tags = []string{"="}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}
}

func TestConfigValidateFilterDuplicates(t *testing.T) {
	c := &config.GraphiteConfig{}
	c.Templates = []string{"foo measurement*", "foo .host.measurement"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

	// duplicate default templates
	c.Templates = []string{"measurement*", ".host.measurement"}
	if err := c.Validate(); err == nil {
		t.Errorf("config validate expected error. got nil")
	}

}
