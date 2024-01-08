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

type ObsOptions struct {
	Enabled    bool   `json:"enabled"`
	BucketName string `json:"bucket_name"`
	Endpoint   string `json:"endpoint"`
	Ak         string `json:"ak"`
	Sk         string `json:"sk"`
	BasePath   string `json:"path"`
}

func (cro *ObsOptions) Clone() *ObsOptions {
	if cro == nil {
		return nil
	}
	clone := *cro
	return &clone
}

func (cro *ObsOptions) Validate() bool {
	return cro.BucketName != "" && cro.Ak != "" && cro.Sk != "" && cro.Endpoint != "" && cro.BasePath != ""
}
