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
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

const ObsMaxRetryCount = 10

var obsClientMap sync.Map

// for ut test
func SaveObsClientToMap(id ObsConf, obsClient *ObjectClient) {
	obsClientMap.Store(id, obsClient)
}

func GetObsClient(logPath *LogPath) (*ObjectClient, error) {
	id := ObsConf{
		ak:         logPath.Ak,
		sk:         logPath.Sk,
		endpoint:   logPath.Endpoint,
		bucketName: logPath.BucketName,
	}
	client, ok := obsClientMap.Load(id)
	if !ok {
		obsClient, err := NewObsClient(&id)
		if err != nil {
			return nil, err
		}
		obsClientMap.Store(id, obsClient)
		return obsClient, nil
	}
	return client.(*ObjectClient), nil
}

func NewObsClient(conf *ObsConf) (*ObjectClient, error) {
	wrapObsClient, err := NewWrapObsClient(conf)
	if err != nil {
		return nil, err
	}
	wrapHttpClient := NewWrapHttpClient()
	return NewObsClientByObject(conf, wrapObsClient, wrapHttpClient)
}

type WrapObsClient struct {
	obsClient *obs.ObsClient
}

func NewWrapObsClient(conf *ObsConf) (*WrapObsClient, error) {
	if conf == nil {
		return nil, fmt.Errorf("obs conf is nil")
	}
	obsClient, err := obs.New(conf.ak, conf.sk, conf.endpoint, obs.WithMaxRetryCount(ObsMaxRetryCount))
	if err != nil {
		return nil, err
	}
	client := &WrapObsClient{obsClient: obsClient}
	return client, nil
}

func (w *WrapObsClient) ListBuckets(input *obs.ListBucketsInput) (output *obs.ListBucketsOutput, err error) {
	return w.obsClient.ListBuckets(input)
}

func (w *WrapObsClient) GetObject(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
	return w.obsClient.GetObject(input)
}

func (w *WrapObsClient) ListObjects(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error) {
	return w.obsClient.ListObjects(input)
}

func (w *WrapObsClient) DeleteObject(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error) {
	return w.obsClient.DeleteObject(input)
}

func (w *WrapObsClient) ModifyObject(input *obs.ModifyObjectInput) (output *obs.ModifyObjectOutput, err error) {
	return w.obsClient.ModifyObject(input)
}

func (w *WrapObsClient) PutObject(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error) {
	return w.obsClient.PutObject(input)
}

func (w *WrapObsClient) GetObjectMetadata(input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error) {
	return w.obsClient.GetObjectMetadata(input)
}

type WrapHttpClient struct {
	httpClient *http.Client
}

func NewWrapHttpClient() *WrapHttpClient {
	httpClient := &http.Client{
		Timeout: time.Duration(10) * time.Second,
	}
	client := &WrapHttpClient{httpClient: httpClient}
	return client
}

func (w *WrapHttpClient) Do(req *http.Request) (*http.Response, error) {
	return w.httpClient.Do(req)
}
