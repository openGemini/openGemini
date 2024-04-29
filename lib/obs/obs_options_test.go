/*
Copyright 2023 Huawei Cloud Computing Technologies Co., Ltd.

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
package obs

import (
	"testing"
)

func TestObsOptionsClone(t *testing.T) {
	srcOpts := &ObsOptions{
		Enabled:    true,
		BucketName: "test_bucket_name",
		Endpoint:   "test_endpoint",
		Ak:         "test_ak",
		Sk:         "test_sk",
		BasePath:   "test_base_path",
	}
	cloneOpts := srcOpts.Clone()
	if cloneOpts.Enabled != srcOpts.Enabled || cloneOpts.BucketName != srcOpts.BucketName {
		t.Fatal("ObsOptions clone failed")
	}
	if !cloneOpts.Validate() {
		t.Fatal("ObsOptions clone failed")
	}

	srcOpts = nil
	cloneOpts = srcOpts.Clone()
	if cloneOpts != nil {
		t.Fatal("ObsOptions clone failed")
	}
}
