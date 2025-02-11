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

package validation

import (
	"testing"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/stretchr/testify/require"
)

type mockTenantLimits struct {
	limits map[string]*config.Limits
}

// newMockTenantLimits creates a new mockTenantLimits that returns per-tenant limits based on
// the given map
func newMockTenantLimits(limits map[string]*config.Limits) *mockTenantLimits {
	return &mockTenantLimits{
		limits: limits,
	}
}

func (l *mockTenantLimits) ByUserID(userID string) *config.Limits {
	return l.limits[userID]
}

func (l *mockTenantLimits) AllByUserID() map[string]*config.Limits {
	return l.limits
}

func TestOverridesMaxLabelNameLength(t *testing.T) {
	tests := map[string]struct {
		setup    func(limits *config.Limits)
		expected int
	}{
		"should return the default legacy setting with the default config": {
			setup:    func(limits *config.Limits) {},
			expected: 1024,
		},
		"should return overrides config": {
			setup: func(limits *config.Limits) {
				limits.MaxLabelNameLength = 10
			},
			expected: 10,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := config.NewLimits()
			testData.setup(&limits)

			overrides := InitOverrides(limits, nil)
			require.Equal(t, testData.expected, overrides.MaxLabelNameLength("test"))
		})
	}
}

func TestGetOverridesConfig(t *testing.T) {
	tenantLimits := map[string]*config.Limits{}

	defaults := config.Limits{
		MaxLabelNamesPerSeries: 100,
	}
	overrides := InitOverrides(defaults, newMockTenantLimits(tenantLimits))

	require.Equal(t, 100, overrides.MaxLabelNamesPerSeries("user1"))
	require.Equal(t, 0, overrides.MaxLabelValueLength("user1"))

	// Update limits for tenant user1. We only update single field, the rest is copied from defaults.
	// (That is how limits work when loaded from YAML)
	l := config.Limits{}
	l = defaults
	l.MaxLabelValueLength = 150

	tenantLimits["user1"] = &l

	// Checking whether overrides were enforced
	require.Equal(t, 100, overrides.MaxLabelNamesPerSeries("user1"))
	require.Equal(t, 150, overrides.MaxLabelValueLength("user1"))

	// Verifying user2 limits are not impacted by overrides
	require.Equal(t, 100, overrides.MaxLabelNamesPerSeries("user2"))
	require.Equal(t, 0, overrides.MaxLabelValueLength("user2"))
}
