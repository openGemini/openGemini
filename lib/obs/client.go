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
	"bytes"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/request"
)

const (
	OBS_READ_RETRY_TIMES    = 3
	OBS_CONTENT_RANGE_TOKEN = 6
)

type ObjectClient struct {
	obsClient  ObsClient
	httpClient HttpClient
	obsConf    *ObsConf
}

type ObsClient interface {
	ListBuckets(input *obs.ListBucketsInput) (output *obs.ListBucketsOutput, err error)
	ListObjects(input *obs.ListObjectsInput) (output *obs.ListObjectsOutput, err error)
	GetObject(input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error)
	DeleteObject(input *obs.DeleteObjectInput) (output *obs.DeleteObjectOutput, err error)
	ModifyObject(input *obs.ModifyObjectInput) (output *obs.ModifyObjectOutput, err error)
	PutObject(input *obs.PutObjectInput) (output *obs.PutObjectOutput, err error)
	GetObjectMetadata(input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error)
}

type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

func NewObsClientByObject(conf *ObsConf, obsClient ObsClient, httpClient HttpClient) (*ObjectClient, error) {
	r := &ObjectClient{
		obsConf: conf,
	}
	listBuckets := &obs.ListBucketsInput{}
	listBuckets.BucketType = obs.POSIX
	buckets, err := obsClient.ListBuckets(listBuckets)
	if err != nil {
		return nil, fmt.Errorf("logkeeper client init failed, please check if endpoint, ak, sk is right")
	}

	isPosix := false
	for _, b := range buckets.Buckets {
		if b.Name == r.obsConf.bucketName {
			isPosix = true
		}
	}
	if !isPosix {
		return nil, fmt.Errorf("logkeeper client init failed, please check if bucket [%s] is a PFS bucket", r.obsConf.bucketName)
	}

	r.obsClient = obsClient
	r.httpClient = httpClient
	return r, nil
}

type ObsConf struct {
	ak         string
	sk         string
	endpoint   string
	bucketName string
}

func (r *ObjectClient) InitAll(obsClient ObsClient, httpClient HttpClient, obsConf *ObsConf) {
	r.obsConf = obsConf
	r.httpClient = httpClient
	r.obsClient = obsClient
}

func (r *ObjectClient) StreamReadMultiRange(objectName string, offs []int64, sizes []int64, minBlockSize int64,
	c chan *request.StreamReader, obsRangeSize int) int64 {
	defer close(c)
	rangeRequests, err := NewObsReadRequest(offs, sizes, minBlockSize, obsRangeSize)
	if err != nil {
		c <- &request.StreamReader{Err: err}
		return 0
	}
	readLen := r.StreamReadRangeRequests(0, objectName, c, rangeRequests, obsRangeSize)
	return readLen
}

func (r *ObjectClient) getHttpRequest(objectName string, rangeRequest *RangeRequest) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodGet, r.buildObsEndpoint(objectName), nil)
	req.Header = http.Header{}
	dateStr := time.Now().UTC().Format(time.RFC1123)
	req.Header.Add("Authorization", headerSignature(r.obsConf.ak, r.obsConf.sk, r.obsConf.bucketName, http.MethodGet, dateStr, objectName))
	req.Header.Add("Date", dateStr)
	req.Header.Add("Range", "bytes="+rangeRequest.GetRangeString())
	return req, err
}

func (r *ObjectClient) streamReadByError(retryTimes int32, c chan *request.StreamReader, retryRange map[int64]int64, retryMutex *sync.RWMutex, chErrorCloseOnce *sync.Once, chError chan int, rangeRequest *RangeRequest, err error) {
	if retryTimes >= OBS_READ_RETRY_TIMES {
		logger.GetLogger().Error(fmt.Sprintf("obsClient.StreamReadMultiRange http request failed, error: %s", err))
		select {
		case c <- &request.StreamReader{Err: errno.NewError(errno.OBSClientRead, fmt.Sprintf("obsClient.StreamReadMultiRange http request failed, error: %s", err))}:
			chErrorCloseOnce.Do(func() {
				close(chError)
			})
		case <-chError:
			return
		}
	} else {
		for _, readers := range rangeRequest.readMap {
			for _, reader := range readers {
				retryMutex.Lock()
				retryRange[reader.Offset] = int64(len(reader.Content))
				retryMutex.Unlock()
			}
		}
	}
}

func (r *ObjectClient) streamReadByBinary(retryTimes int32, c chan *request.StreamReader, retryRange map[int64]int64, retryMutex *sync.RWMutex, chErrorCloseOnce *sync.Once, chError chan int, rangeRequest *RangeRequest, totalReadLen *int64, resp *http.Response) {
	rangeHeader := resp.Header.Get("Content-range")
	hyphenPos := strings.Index(rangeHeader, "-")
	partStart, err := strconv.Atoi(rangeHeader[OBS_CONTENT_RANGE_TOKEN:hyphenPos])
	if err != nil {
		logger.GetLogger().Error("obsClient.StreamReadMultiRange parse response header failed, header is: " + rangeHeader)
	}
	readers := rangeRequest.readMap[int64(partStart)]
	for i, v := range readers {
		readLen, err := io.ReadFull(resp.Body, v.Content)
		if err != nil {
			if retryTimes >= OBS_READ_RETRY_TIMES {
				logger.GetLogger().Error(fmt.Sprintf("obsClient.StreamReadMultiRange read body failed, error: %s", err))
				select {
				case c <- &request.StreamReader{Err: errno.NewError(errno.OBSClientRead, fmt.Sprintf("obsClient.StreamReadMultiRange read body failed, error: %s", err))}:
					chErrorCloseOnce.Do(func() {
						close(chError)
					})
				case <-chError:
					return
				}
			} else {
				for _, v := range readers[i:] {
					retryMutex.Lock()
					retryRange[v.Offset] = int64(len(v.Content))
					retryMutex.Unlock()
				}
			}
			return
		}
		select {
		case c <- v:
			atomic.AddInt64(totalReadLen, int64(readLen))
			continue
		case <-chError:
			return
		}
	}
}

func (r *ObjectClient) streamReadByMultiPart(retryTimes int32, c chan *request.StreamReader, retryRange map[int64]int64, retryMutex *sync.RWMutex, chErrorCloseOnce *sync.Once, chError chan int, rangeRequest *RangeRequest, totalReadLen *int64, resp *http.Response) {
	_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		logger.GetLogger().Error("obsClient.StreamReadMultiRange parse response header failed, Content-Type is: " + resp.Header.Get("Content-Type"))
	}
	mr := multipart.NewReader(resp.Body, params["boundary"])
	var part *multipart.Part
	for {
		part, err = mr.NextPart()
		if err != nil {
			break
		}
		rangeHeader := part.Header.Get("Content-range")
		hyphenPos := strings.Index(rangeHeader, "-")
		partStart, err := strconv.Atoi(rangeHeader[OBS_CONTENT_RANGE_TOKEN:hyphenPos])
		if err != nil {
			logger.GetLogger().Error("obsClient.StreamReadMultiRange parse response header failed, header is: " + rangeHeader)
		}
		readers := rangeRequest.readMap[int64(partStart)]
		for i, v := range readers {
			readLen, err := io.ReadFull(part, v.Content)
			if err != nil {
				if retryTimes >= OBS_READ_RETRY_TIMES {
					logger.GetLogger().Error(fmt.Sprintf("obsClient.StreamReadMultiRange read body failed, error: %s", err))
					select {
					case c <- &request.StreamReader{Err: errno.NewError(errno.OBSClientRead, fmt.Sprintf("obsClient.StreamReadMultiRange read body failed, error: %s", err))}:
						chErrorCloseOnce.Do(func() {
							close(chError)
						})
					case <-chError:
						return
					}
				} else {
					for _, v := range readers[i:] {
						retryMutex.Lock()
						retryRange[v.Offset] = int64(len(v.Content))
						retryMutex.Unlock()
					}
				}
				return
			}
			select {
			case c <- v:
				atomic.AddInt64(totalReadLen, int64(readLen))
				continue
			case <-chError:
				return
			}
		}
	}
}

func (r *ObjectClient) streamReadOverRetryTimes(retryTimes int32, c chan *request.StreamReader, retryRange map[int64]int64, retryMutex *sync.RWMutex, chErrorCloseOnce *sync.Once, chError chan int, rangeRequest *RangeRequest, resp *http.Response) {
	if retryTimes >= OBS_READ_RETRY_TIMES {
		body, _ := io.ReadAll(resp.Body)
		logger.GetLogger().Error(fmt.Sprintf("obsClient.StreamReadMultiRange read body failed, resp body is illegal, body: " + string(body)))
		select {
		case c <- &request.StreamReader{Err: errno.NewError(errno.OBSClientRead, fmt.Sprintf("obsClient.StreamReadMultiRange read body failed, resp body is illegal, body: "+string(body)))}:
			chErrorCloseOnce.Do(func() {
				close(chError)
			})
		case <-chError:
			return
		}
	} else {
		for _, readers := range rangeRequest.readMap {
			for _, reader := range readers {
				retryMutex.Lock()
				retryRange[reader.Offset] = int64(len(reader.Content))
				retryMutex.Unlock()
			}
		}
	}
}

func (r *ObjectClient) StreamReadRangeRequests(retryTimes int32, objectName string, c chan *request.StreamReader,
	rangeRequests []*RangeRequest, obsRangeSize int) int64 {
	var toReadLen int64 = 0
	var readLen int64 = 0
	wg := &sync.WaitGroup{}
	chError := make(chan int)
	var chErrorCloseOnce sync.Once
	var retryRange = make(map[int64]int64)
	var retryMutex sync.RWMutex
	for _, rangeRequest := range rangeRequests {
		wg.Add(1)
		toReadLen += rangeRequest.length
		go func(rangeRequest *RangeRequest, retryRange map[int64]int64, retryMutex *sync.RWMutex, waitGroup *sync.WaitGroup, totalReadLen *int64, chError chan int, c chan *request.StreamReader, chErrorCloseOnce *sync.Once, retryTimes int32) {
			defer waitGroup.Done()
			req, err := r.getHttpRequest(objectName, rangeRequest)
			if err != nil {
				logger.GetLogger().Error("obsClient.StreamReadMultiRange new request failed, url is: " + r.buildObsEndpoint(objectName))
			}
			if resp, err := r.httpClient.Do(req); err != nil {
				r.streamReadByError(retryTimes, c, retryRange, retryMutex, chErrorCloseOnce, chError, rangeRequest, err)
				return
			} else if strings.ToLower(resp.Header.Get("Content-Type")) == "binary/octet-stream" {
				r.streamReadByBinary(retryTimes, c, retryRange, retryMutex, chErrorCloseOnce, chError, rangeRequest, totalReadLen, resp)
				return
			} else if strings.HasPrefix(strings.ToLower(resp.Header.Get("Content-Type")), "multipart/") {
				r.streamReadByMultiPart(retryTimes, c, retryRange, retryMutex, chErrorCloseOnce, chError, rangeRequest, totalReadLen, resp)
				return
			} else {
				r.streamReadOverRetryTimes(retryTimes, c, retryRange, retryMutex, chErrorCloseOnce, chError, rangeRequest, resp)
				return
			}
		}(rangeRequest, retryRange, &retryMutex, wg, &readLen, chError, c, &chErrorCloseOnce, retryTimes)
	}
	wg.Wait()
	if len(retryRange) != 0 && retryTimes < OBS_READ_RETRY_TIMES {
		rangeRequests = NewObsRetryReadRequest(retryRange, obsRangeSize)
		toReadLen += r.StreamReadRangeRequests(retryTimes+1, objectName, c, rangeRequests, obsRangeSize)
	}
	if retryTimes >= OBS_READ_RETRY_TIMES && readLen != toReadLen {
		logger.GetLogger().Error(fmt.Sprintf("obsClient.StreamReadMultiRange read length mismatch, toReadLen: %d, readLen: %d", toReadLen, readLen))
	}
	return readLen
}

func (r *ObjectClient) buildObsEndpoint(objectName string) string {
	return fmt.Sprintf("http://%s.%s/%s", r.obsConf.bucketName, r.obsConf.endpoint, objectName)
}

func (r *ObjectClient) ReadAt(objectName string, off int64, size int64, dst []byte) error {
	getObjectInput := &obs.GetObjectInput{
		RangeStart: off,
		RangeEnd:   off + size - 1,
	}
	getObjectInput.Bucket = r.obsConf.bucketName
	getObjectInput.Key = objectName
	if getObjectOutput, err := r.obsClient.GetObject(getObjectInput); err != nil {
		return fmt.Errorf("obsClient.GetObject failed, error: %s", err)
	} else {
		_, err = io.ReadFull(getObjectOutput.Body, dst)
		if err != nil {
			return err
		}
		return nil
	}
}

func (r *ObjectClient) Delete(objectName string) error {
	deleteObjectInput := &obs.DeleteObjectInput{
		Bucket: r.obsConf.bucketName,
		Key:    objectName,
	}
	if _, err := r.obsClient.DeleteObject(deleteObjectInput); err != nil {
		return fmt.Errorf("obsClient.DeleteObject failed, error: %s", err)
	}
	return nil
}

func (r *ObjectClient) DeleteAll(objectName string) error {
	listObjectsInput := &obs.ListObjectsInput{
		Bucket: r.obsConf.bucketName,
	}
	listObjectsInput.Prefix = objectName

	output, err := r.obsClient.ListObjects(listObjectsInput)
	if err != nil {
		return fmt.Errorf("obsClient.DeleteAll failed, error: %s", err)
	}

	if output == nil {
		return nil
	}

	sort.Slice(output.Contents, func(i, j int) bool {
		return output.Contents[i].Key > output.Contents[j].Key
	})

	for _, content := range output.Contents {
		deleteObjectInput := &obs.DeleteObjectInput{
			Bucket: r.obsConf.bucketName,
			Key:    content.Key,
		}
		_, err = r.obsClient.DeleteObject(deleteObjectInput)
		if err != nil {
			return fmt.Errorf("obsClient.DeleteAll failed, error: %s", err)
		}
	}
	return nil
}

func (r *ObjectClient) WriteTo(objectName string, off int64, size int64, src *[]byte) error {
	if int64(len(*src)) != size {
		return fmt.Errorf("write bytes length does not equal to size")
	}
	modifyObjectInput := &obs.ModifyObjectInput{
		Bucket:        r.obsConf.bucketName,
		Key:           objectName,
		Position:      off,
		Body:          bytes.NewReader(*src),
		ContentLength: size,
	}
	if output, err := r.obsClient.ModifyObject(modifyObjectInput); err != nil {
		return fmt.Errorf("obsClient.ModifyObject failed, error: %s", err)
	} else {
		logger.GetLogger().Error(fmt.Sprintf("obsClient.ModifyObject read body failed, resp body is illegal, output: %v", output))
	}
	return nil
}

func (r *ObjectClient) Truncate(objectName string, size int64) error {
	if size < 0 {
		return fmt.Errorf("truncate size must be positive")
	}
	url := r.buildObsEndpoint(objectName)
	extension := fmt.Sprintf("?length=%d&truncate", size)
	req, err := http.NewRequest(http.MethodPut, url+extension, nil)
	if err != nil {
		return err
	}
	req.Header = http.Header{}
	dateStr := time.Now().UTC().Format(time.RFC1123)
	req.Header.Add("Authorization", headerSignature(r.obsConf.ak, r.obsConf.sk, r.obsConf.bucketName, http.MethodPut, dateStr, objectName+extension))
	req.Header.Add("Date", dateStr)
	_, err = r.httpClient.Do(req)
	if err != nil {
		return err
	}
	return nil
}

func (r *ObjectClient) Init(objectName string) error {
	putObjectInput := &obs.PutObjectInput{
		Body: bytes.NewReader([]byte{}),
	}
	putObjectInput.Bucket = r.obsConf.bucketName
	putObjectInput.Key = objectName
	if _, err := r.obsClient.PutObject(putObjectInput); err != nil {
		return fmt.Errorf("obsClient.PutObject failed, error: %s", err)
	}
	return nil
}

func (r *ObjectClient) GetLength(objectName string) (int64, error) {
	if getObjectMetadataOutput, err := r.getObjectMetadata(objectName); err != nil {
		return 0, fmt.Errorf("obsClient.GetLength failed, error: %s", err)
	} else {
		return getObjectMetadataOutput.ContentLength, nil
	}
}

func (r *ObjectClient) GetAllLength(objectName string) (int64, error) {
	listObjectsInput := &obs.ListObjectsInput{
		Bucket: r.obsConf.bucketName,
	}
	listObjectsInput.Prefix = objectName

	output, err := r.obsClient.ListObjects(listObjectsInput)
	if err != nil {
		return 0, fmt.Errorf("obsClient.GetAllLength failed, error: %s", err)
	}

	if output == nil {
		return 0, nil
	}

	var totalLength int64
	for _, content := range output.Contents {
		totalLength += content.Size
	}
	return totalLength, nil
}

func (r *ObjectClient) getObjectMetadata(objectName string) (*obs.GetObjectMetadataOutput, error) {
	getObjectMetadataInput := &obs.GetObjectMetadataInput{
		Bucket: r.obsConf.bucketName,
		Key:    objectName,
	}
	if getObjectMetadataOutput, err := r.obsClient.GetObjectMetadata(getObjectMetadataInput); err != nil {
		logger.GetLogger().Error(fmt.Sprintf("obsClient.GetObjectMetadata failed, error: %v; object: %s", err, objectName))
		return nil, fmt.Errorf("obsClient.GetObjectMetadata failed, error: %s", err)
	} else {
		return getObjectMetadataOutput, nil
	}
}

func headerSignature(ak string, sk string, bucketName string, httpMethod string, date string, objectKey string) string {
	var sb strings.Builder
	sb.WriteString(httpMethod)
	sb.WriteString("\n\n\n")
	sb.WriteString(date)
	sb.WriteString("\n")
	sb.WriteString("/")
	sb.WriteString(bucketName)
	sb.WriteString("/")
	sb.WriteString(objectKey)
	return "OBS " + ak + ":" + obs.Base64Encode(obs.HmacSha1([]byte(sk), []byte(sb.String())))
}
