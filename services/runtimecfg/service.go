// Copyright 2024 Huawei Cloud Computing Technologies Co., Ltd.
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

package runtimecfg

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/goccy/go-yaml"
	_ "github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

var (
	errMultipleDocuments = errors.New("the provided runtime configuration contains multiple documents")
)

// runtimeConfig are values that can be reloaded from configuration file while service is running.
// Reloading is done by runtimecfg.Service, which also keeps the currently loaded config.
// These values are then pushed to the components that are interested in them.
type runtimeConfig struct {
	TenantLimits map[string]*config.Limits `yaml:"overrides,anchor"`
}

func loadRuntimeConfig(r io.Reader) (*runtimeConfig, error) {
	cfg := &runtimeConfig{TenantLimits: make(map[string]*config.Limits)}
	decoder := yaml.NewDecoder(r)

	// Decode the first document. An empty document (EOF) is OK.
	if err := decoder.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	// Ensure the provided YAML config is not composed of multiple documents,
	if err := decoder.Decode(&runtimeConfig{}); !errors.Is(err, io.EOF) {
		return nil, errMultipleDocuments
	}

	return cfg, nil
}

// include Shard retention polices and Index retention polices
type Service struct {
	services.Base

	svrCfg config.RuntimeConfig
	logger *logger.Logger

	configMtx sync.RWMutex
	config    *runtimeConfig
}

func NewService(config config.RuntimeConfig, logger *logger.Logger) *Service {
	if !config.Enabled || logger == nil {
		return nil
	}
	s := &Service{svrCfg: config}
	s.logger = logger.With(zap.String("service", "runtimecfg"), zap.String("path", config.LoadPath), zap.Any("period", config.ReloadPeriod))

	s.Init("runtimecfg", time.Duration(config.ReloadPeriod), s.handle)
	return s
}

func (s *Service) handle() {
	buf, err := os.ReadFile(s.svrCfg.LoadPath)
	if err != nil {
		s.logger.Error("read file", zap.Error(err))
		return
	}
	cfg, err := loadRuntimeConfig(bytes.NewReader(buf))
	if err != nil {
		s.logger.Error("load file", zap.Error(err))
		return
	}
	s.setConfig(cfg)
}

func (s *Service) setConfig(config *runtimeConfig) {
	s.configMtx.Lock()
	defer s.configMtx.Unlock()

	s.config = config
}

// GetConfig returns last loaded config value, possibly nil.
func (s *Service) getConfig() *runtimeConfig {
	s.configMtx.RLock()
	defer s.configMtx.RUnlock()

	return s.config
}

// implement interface's methods: TenantLimits
func (s *Service) ByUserID(userID string) *config.Limits {
	return s.AllByUserID()[userID]
}

func (s *Service) AllByUserID() map[string]*config.Limits {
	cfg := s.getConfig()
	if cfg != nil {
		return cfg.TenantLimits
	}

	return nil
}

// path:/runtime_config            Current Runtime Config (incl. Overrides)
// path:/runtime_config?mode=diff  Current Runtime Config (show only values that differ from the defaults)
func RuntimeConfigHandler(s *Service, defaultLimits config.Limits) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if s == nil {
			writeTextResponse(w, "runtimecfg service doesn't init")
			return
		}
		cfg := s.getConfig()
		if cfg == nil {
			writeTextResponse(w, "runtime config file doesn't exist")
			return
		}

		var output interface{}
		switch r.URL.Query().Get("mode") {
		case "diff":
			// Default runtime config is just empty struct, but to make diff work,
			// we set defaultLimits for every tenant that exists in runtime config.
			defaultCfg := runtimeConfig{}
			defaultCfg.TenantLimits = map[string]*config.Limits{}
			for k, v := range cfg.TenantLimits {
				if v != nil {
					defaultCfg.TenantLimits[k] = &defaultLimits
				}
			}

			cfgYaml, err := YAMLMarshalUnmarshal(cfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			defaultCfgYaml, err := YAMLMarshalUnmarshal(defaultCfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			output, err = diffConfig(defaultCfgYaml, cfgYaml)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

		default:
			output = cfg
		}
		writeYAMLResponse(w, output)
	}
}

// YAMLMarshalUnmarshal utility function that converts a YAML interface in a map
// doing marshal and unmarshal of the parameter
// note: yaml.v2 default map model like map[interface{}]interface{}
// upgrade to v3, map model breaking change to map[string]interface{}
func YAMLMarshalUnmarshal(in interface{}) (map[string]interface{}, error) {
	yamlBytes, err := yaml.Marshal(in)
	if err != nil {
		return nil, err
	}

	object := make(map[string]interface{})
	if err := yaml.Unmarshal(yamlBytes, &object); err != nil {
		return nil, err
	}

	return object, nil
}

// diffConfig utility function that returns the diff between two config map objects
func diffConfig(defaultConfig, actualConfig map[string]interface{}) (map[string]interface{}, error) {
	output := make(map[string]interface{})

	for key, value := range actualConfig {

		defaultValue, ok := defaultConfig[key]
		if !ok {
			output[key] = value
			continue
		}

		switch v := value.(type) {
		case map[string]interface{}:
			defaultV, ok := defaultValue.(map[string]interface{})
			if !ok {
				output[key] = value
			}
			diff, err := diffConfig(defaultV, v)
			if err != nil {
				return nil, err
			}
			if len(diff) > 0 {
				output[key] = diff
			}
		default:
			if !reflect.DeepEqual(defaultValue, value) {
				output[key] = value
			}
		}
	}

	return output, nil
}

// writeYAMLResponse writes some YAML as a HTTP response.
func writeYAMLResponse(w http.ResponseWriter, v interface{}) {
	// There is not standardised content-type for YAML, text/plain ensures the
	// YAML is displayed in the browser instead of offered as a download
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	data, err := yaml.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We ignore errors here, because we cannot do anything about them.
	// Write will trigger sending Status code, so we cannot send a different status code afterwards.
	// Also this isn't internal error, but error communicating with client.
	_, err = w.Write(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Sends message as text/plain response with 200 status code.
func writeTextResponse(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "text/plain")

	_, err := w.Write([]byte(message))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
