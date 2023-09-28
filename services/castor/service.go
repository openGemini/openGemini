/*
Copyright 2022 Huawei Cloud Computing Technologies Co., Ltd.

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
package castor

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/services"
	"go.uber.org/zap"
)

var service *Service

type Service struct {
	services.Base

	Config  config.Castor
	closeMu sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	clientPool      []*pool
	lastPoolIdx     int
	dataChanMu      sync.Mutex
	dataChan        chan *data
	dataFailureChan chan *data
	resultChan      chan array.Record
	responseChanMap sync.Map

	alive bool
}

// GetService return service instance
func GetService() *Service {
	return service
}

// NewService return an new service instance
func NewService(c config.Castor) *Service {
	if !c.Enabled {
		return &Service{Config: c}
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Service{
		Config:          c,
		ctx:             ctx,
		cancel:          cancel,
		clientPool:      make([]*pool, len(c.PyWorkerAddr)),
		dataChan:        make(chan *data, chanBufferSize),
		dataFailureChan: make(chan *data, chanBufferSize),
		resultChan:      make(chan array.Record, chanBufferSize),
		responseChanMap: sync.Map{},
	}
	s.Init("castor", 0, s.handle)

	for i, addr := range c.PyWorkerAddr {
		s.clientPool[i] = newClientPool(
			addr, c.ConnPoolSize, s.Logger, getCliTimeout/time.Duration(len(c.PyWorkerAddr)),
			&dataChanSet{
				dataChan:   s.dataChan,
				resultChan: s.resultChan,
			},
		)
	}
	return s
}

// Open start service
func (s *Service) Open() error {
	if !s.Config.Enabled {
		return nil
	}

	if err := s.Base.Open(); err != nil {
		return err
	}
	s.wg.Add(4)
	go s.monitorConn()
	go s.handleResult()
	go s.sendData()
	go s.recordFailure()
	s.alive = true
	service = s
	return nil
}

func (s *Service) handle() {}

func (s *Service) monitorConn() {
	ticker := time.NewTicker(connCheckInterval)
	defer func() {
		s.wg.Done()
		ticker.Stop()
	}()

	for {
		select {
		case <-ticker.C:
			s.fillUpConnPool()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) recordFailure() {
	defer s.wg.Done()
	for {
		select {
		case data, ok := <-s.dataFailureChan:
			if !ok {
				return
			}
			s.Logger.Error(errno.NewError(errno.FailToProcessData, data.size()).Error())
			s.dispatchErr(data)
			data.release()
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) fillUpConnPool() {
	for _, p := range s.clientPool {
		if err := p.fillUp(); err != nil {
			s.Logger.Error(errno.NewError(errno.FailToFillUpConnPool).Error(), zap.Error(err))
		}
	}
}

func (s *Service) sendData() {
	defer s.wg.Done()
	for {
		select {
		case data, ok := <-s.dataChan:
			if !ok {
				return
			}
			if data.retryCnt >= maxSendRetry {
				data.err = errno.NewError(errno.ExceedRetryChance)
				s.dataFailureChan <- data
				continue
			}
			cli, err := s.getClient()
			if err != nil {
				s.handleRetry(data)
			} else if err := cli.send(data); err != nil {
				s.handleRetry(data)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) handleRetry(data *data) {
	s.dataChanMu.Lock()
	defer s.dataChanMu.Unlock()
	if len(s.dataChan) == cap(s.dataChan) {
		data.err = errno.NewError(errno.TaskQueueFull)
		s.dataFailureChan <- data
		return
	}
	data.retryCnt += 1
	s.dataChan <- data
}

func (s *Service) getClient() (*castorCli, *errno.Error) {
	start := time.Now()
	for {
		s.lastPoolIdx = (s.lastPoolIdx + 1) % len(s.clientPool)
		p := s.clientPool[s.lastPoolIdx]
		cli := p.get()
		if cli != nil {
			return cli, nil
		}
		if time.Since(start) >= getCliTimeout {
			return nil, errno.NewError(errno.NoAvailableClient)
		}
	}
}

func (s *Service) handleResult() {
	defer s.wg.Done()
	for {
		select {
		case res, ok := <-s.resultChan:
			if !ok {
				return
			}
			if err := s.dispatchResult(res); err != nil {
				res.Release()
				s.Logger.Error(err.Error())
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Service) getRespChan(rec array.Record) (*respChan, *errno.Error) {
	id, err := GetMetaValueFromRecord(rec, string(TaskID))
	if err != nil {
		return nil, err
	}

	nodeResultChan, exist := s.responseChanMap.Load(id)
	if !exist {
		return nil, errno.NewError(errno.ResponseTimeout)
	}
	ch, ok := nodeResultChan.(*respChan)
	if !ok {
		return nil, errno.NewError(errno.TypeAssertFail)
	}
	return ch, nil
}

func (s *Service) dispatchResult(rec array.Record) *errno.Error {
	ch, err := s.getRespChan(rec)
	if err != nil {
		return err
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if !ch.alive {
		return errno.NewError(errno.ResponseTimeout)
	}
	ch.C <- rec
	return nil
}

func (s *Service) dispatchErr(data *data) {
	ch, err2 := s.getRespChan(data.record)
	if err2 != nil {
		s.Logger.Error(err2.Error())
		return
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if !ch.alive {
		s.Logger.Error(errno.NewError(errno.ResponseTimeout).Error())
		return
	}
	if data.err != nil {
		ch.ErrCh <- data.err
		return
	}
	ch.ErrCh <- errno.NewError(errno.UnknownErr)
}

// HandleData will send data to computation node through internal client
func (s *Service) HandleData(record array.Record) {
	s.dataChanMu.Lock()
	defer s.dataChanMu.Unlock()
	data := newData(record)
	if len(s.dataChan) == cap(s.dataChan) {
		data.err = errno.NewError(errno.TaskQueueFull)
		s.dataFailureChan <- data
		return
	}
	s.dataChan <- newData(record)
}

// ResultChan store result from computation node
func (s *Service) ResultChan() chan<- array.Record {
	return s.resultChan
}

// Close release internal client and close channel, service will mark as not alive, any castor query will not be processed
func (s *Service) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if !s.alive {
		return nil
	}
	s.alive = false
	service = nil
	s.cancel()
	s.wg.Wait()

	for _, p := range s.clientPool {
		p.close()
	}

	// clear out remaining data
	for len(s.dataChan) > 0 {
		s.dataFailureChan <- <-s.dataChan
	}
	close(s.dataChan)
	for len(s.resultChan) > 0 {
		s.dataFailureChan <- newData(<-s.resultChan)
	}
	close(s.resultChan)
	for len(s.dataFailureChan) > 0 {
		data := <-s.dataFailureChan
		s.Logger.Error(errno.NewError(errno.FailToProcessData, data.size()).Error())
		data.release()
	}
	close(s.dataFailureChan)

	s.responseChanMap = sync.Map{}
	if err := s.Base.Close(); err != nil {
		return err
	}
	s.Logger.Info("shutdown castor service")
	if l := s.Logger.GetSuppressLogger(); l != nil {
		l.Close()
	}
	return nil
}

// IsAlive return service running status
func (s *Service) IsAlive() bool {
	return s.alive
}

// RegisterResultChan register an channel with id, response from computation node will be dispatched according to id
func (s *Service) RegisterResultChan(id string, ch *respChan) {
	s.responseChanMap.Store(id, ch)
}

// DeregisterResultChan deregister an channel with id, response from computation node will be dropped if correspond id not found
func (s *Service) DeregisterResultChan(id string) {
	s.responseChanMap.Delete(id)
}
