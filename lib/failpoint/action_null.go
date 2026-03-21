//go:build !failpoint

// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package failpoint

func Call(name string, fn func(val *Value)) {}

func Sleep(name string, fn func()) {}

func ApplyFunc(name string, target, double any) {}

func ApplyMethod(name string, target any, method string, double any) {}

func ApplyPrivateMethod(name string, target any, method string, double any) {}
