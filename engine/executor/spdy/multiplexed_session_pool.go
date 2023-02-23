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

package spdy

import (
	"crypto/tls"
	"io"
	"net"
	"sync"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
	"go.uber.org/zap"
)

type MultiplexedSessionPool struct {
	cfg       config.Spdy
	network   string
	address   string
	mspLogger *logger.Logger

	conn  *MultiplexedConnection
	queue chan *MultiplexedSession

	closed     bool
	closeGuard sync.RWMutex

	wg sync.WaitGroup

	successJob *statistics.SpdyJob
	failedJob  *statistics.SpdyJob
	closeJob   *statistics.SpdyJob
}

func NewMultiplexedSessionPool(cfg config.Spdy, network string, address string) *MultiplexedSessionPool {
	pool := &MultiplexedSessionPool{
		cfg:       cfg,
		network:   network,
		address:   address,
		conn:      nil,
		mspLogger: logger.NewLogger(errno.ModuleNetwork),
	}
	return pool
}

func (c *MultiplexedSessionPool) SetStatisticsJob(job *statistics.SpdyJob) {
	if job == nil {
		return
	}
	c.successJob = job.Clone()
	c.successJob.SetItem(statistics.SuccessCreateSessionTotal)

	c.failedJob = job.Clone()
	c.failedJob.SetItem(statistics.FailedCreateSessionTotal)

	c.closeJob = job.Clone()
	c.closeJob.SetItem(statistics.ClosedSessionTotal)
}

func (c *MultiplexedSessionPool) Dial() error {
	conn, err := c.dail()
	if err != nil {
		return err
	}

	c.wg.Add(1)
	c.conn = NewMultiplexedConnection(c.cfg, conn, true)
	go func() {
		if err := c.conn.ListenAndServed(); err != nil {
			c.mspLogger.Warn(err.Error(), zap.String("SPDY", "MultiplexedSessionPool"))
		}
	}()

	c.queue = make(chan *MultiplexedSession, c.cfg.ConcurrentAcceptSession+1)
	return nil
}

func (c *MultiplexedSessionPool) dail() (io.ReadWriteCloser, error) {
	dialer := &net.Dialer{Timeout: c.cfg.GetTCPDialTimeout()}

	if c.cfg.TLSEnable {
		config, err := c.cfg.NewClientTLSConfig()
		if err != nil {
			return nil, err
		}

		return tls.DialWithDialer(dialer, c.network, c.address, config)
	}

	return dialer.Dial(c.network, c.address)
}

func (c *MultiplexedSessionPool) Get() (*MultiplexedSession, error) {
	session, err := c.tryGet()

	if err != nil {
		return nil, err
	}

	if session == nil {
		session, err = c.create()
		if err != nil {
			return nil, err
		}
		return session, nil
	}

	return session, nil
}

func (c *MultiplexedSessionPool) tryGet() (*MultiplexedSession, error) {
	select {
	case conn, ok := <-c.queue:
		if !ok {
			return nil, errno.NewError(errno.PoolClosed)
		}
		return conn, nil
	default:
		return nil, nil
	}
}

func (c *MultiplexedSessionPool) Put(session *MultiplexedSession) {
	session.DisableDataACK()
	c.sendToQueue(session)
}

func (c *MultiplexedSessionPool) sendToQueue(session *MultiplexedSession) {
	select {
	case c.queue <- session:
	default:
		session.close()
	}
}

func (c *MultiplexedSessionPool) create() (*MultiplexedSession, error) {
	feature := c.conn.OpenSession()
	session, err := feature()
	if err != nil {
		statistics.NewSpdyStatistics().Add(c.failedJob)
		return nil, err
	}

	session.SetOnClose(func() {
		statistics.NewSpdyStatistics().Add(c.closeJob)
	})
	statistics.NewSpdyStatistics().Add(c.successJob)
	return session, nil
}

func (c *MultiplexedSessionPool) Close() {
	c.closeGuard.Lock()
	closed := c.closed
	c.closed = true
	c.closeGuard.Unlock()

	if closed {
		return
	}

	c.wg.Done()
	HandleError(c.conn.Close())
	close(c.queue)
}

func (c *MultiplexedSessionPool) IsClosed() bool {
	return c.closed
}

func (c *MultiplexedSessionPool) Available() bool {
	c.closeGuard.RLock()
	defer c.closeGuard.RUnlock()
	return !c.closed && !c.conn.IsClosed()
}

func (c *MultiplexedSessionPool) Wait() {
	c.wg.Wait()
}
