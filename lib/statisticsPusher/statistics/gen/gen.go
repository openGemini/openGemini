// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package gen

//go:generate tmpl -data=@merge.data -o=../merge_statistics.gen.go statistics.tmpl

//go:generate tmpl -data=@meta.data -o=../meta_statistics.gen.go statistics.tmpl

//go:generate tmpl -data=@stream.data -o=../stream_statistics.gen.go statistics.tmpl

//go:generate tmpl -data=@downsample.data -o=../downsample_statistics.gen.go statistics.tmpl

//go:generate tmpl -data=@record.data -o=../record_statistics.gen_test.go statistics_test.tmpl

//go:generate tmpl -data=@hit_ratio.data -o=../hit_ratio.gen.go statistics.tmpl
//go:generate tmpl -data=@hit_ratio.data -o=../hit_ratio.gen_test.go statistics_test.tmpl
