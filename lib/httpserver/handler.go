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

package httpserver

import (
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/openGemini/openGemini/lib/logger"
	meta "github.com/openGemini/openGemini/lib/metaclient"
	"github.com/openGemini/openGemini/open_src/influx/auth"
	"github.com/openGemini/openGemini/open_src/influx/influxql"
	meta2 "github.com/openGemini/openGemini/open_src/influx/meta"
)

type Handler struct {
	Logger *logger.Logger

	MetaClient interface {
		Authenticate(username, password string) (ui meta2.User, err error)
		User(username string) (meta2.User, error)
		AdminUserExists() bool
		DataNodes() ([]meta2.DataNode, error)
		InitMetaClient(joinPeers []string, tlsEn bool, storageNodeInfo *meta.StorageNodeInfo) (uint64, uint64, error)
		CreateDataNode(httpAddr, tcpAddr string) (uint64, uint64, error)
	}

	QueryAuthorizer interface {
		AuthorizeQuery(u meta2.User, query *influxql.Query, database string) error
	}

	WriteAuthorizer interface {
		AuthorizeWrite(username, database string) error
	}

	requireAuthentication bool
	jwtSharedSecret       string
}

func NewHandler(authEn bool, jwtSharedSecret string) *Handler {
	h := &Handler{
		MetaClient:            meta.DefaultMetaClient,
		QueryAuthorizer:       auth.NewQueryAuthorizer(meta.DefaultMetaClient),
		WriteAuthorizer:       auth.NewWriteAuthorizer(meta.DefaultMetaClient),
		requireAuthentication: authEn,
		jwtSharedSecret:       jwtSharedSecret,
		Logger:                logger.NewLogger(errno.ModuleHTTP),
	}

	return h
}
