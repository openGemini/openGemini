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

package listener

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openGemini/openGemini/lib/statisticsPusher/statistics"
)

// LimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener and will drop extra connections.
func NewLimitListener(l net.Listener, n int, list string) net.Listener {
	return &limitListener{
		Listener:   l,
		listenChan: make(chan struct{}, n),
		whiteList:  strings.Split(list, ","),
		maxLimit:   n,
	}
}

// limitListener is a listener that limits the number of active connections
// at any given time.
type limitListener struct {
	net.Listener
	listenChan chan struct{}
	whiteList  []string
	maxLimit   int
}

func (l *limitListener) release() {
	<-l.listenChan
	atomic.AddInt64(&statistics.HandlerStat.ConnectionNums, -1)
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}

		remoteAddr := c.RemoteAddr().String()
		if checkInWhiteList(l.whiteList, remoteAddr) {
			return c, nil
		}

		select {
		case l.listenChan <- struct{}{}:
			atomic.AddInt64(&statistics.HandlerStat.ConnectionNums, 1)
			return &limitListenerConn{Conn: c, releaseFunc: l.release}, nil
		default:
			fmt.Printf("%s, connection exceed! Max Connection Limit is: %d\n",
				time.Now().Format(time.RFC3339Nano), l.maxLimit)
			if err := c.Close(); err != nil {
				return nil, err
			}
		}
	}
}

type limitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	releaseFunc func()
}

func (l *limitListenerConn) Close() error {
	err := l.Conn.Close()
	l.releaseOnce.Do(l.releaseFunc)
	return err
}

/*
whiteList: [127.0.0.1:8086,127.0.0.2:8086,127.0.0.3:8086]
remoteAddr: 127.0.0.1:8086
*/
func checkInWhiteList(whiteList []string, remoteAddr string) bool {
	if len(whiteList) == 0 || remoteAddr == "" {
		return false
	}

	remoteIP, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false
	}

	for _, address := range whiteList {
		localIP, _, err := net.SplitHostPort(address)
		if err != nil {
			return false
		}
		if localIP == remoteIP {
			return true
		}
	}
	return false
}
