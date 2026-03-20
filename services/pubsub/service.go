// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
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

package pubsub

// Global unique broker instance
var defaultBroker = newBroker()

// Publish sends a message to the specified topic.
// If the topic does not exist, the message is silently dropped.
func Publish(topicName string, msg Message) {
	defaultBroker.publish(topicName, msg)
}

// Subscribe Return a message listener for receiving broadcast messages
func Subscribe(topicName string) *Listener {
	return defaultBroker.listen(topicName)
}
