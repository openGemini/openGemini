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

package cmd

import (
	"flag"
	
	"github.com/influxdata/influxdb/client"
	"github.com/openGemini/openGemini/app/ts-cli/geminicli"
)

// the options of command line are compatible
// with influx v1.x.
type CompatibleCommand struct {
	fs *flag.FlagSet
}

func NewCompatibleCommand() *CompatibleCommand {
	return &CompatibleCommand{
		flag.NewFlagSet("openGemini CLI version "+geminicli.CLIENT_VERSION, flag.ExitOnError),
	}
}

func (c *CompatibleCommand) Parse(args []string) error {
	return c.fs.Parse(args)
}

func (c *CompatibleCommand) Args() []string {
	return c.fs.Args()
}

func (c *CompatibleCommand) Bind(config *geminicli.CommandLineConfig) {
	c.fs.StringVar(&config.Host, "host", client.DefaultHost, "openGemini host to connect to.")
	c.fs.IntVar(&config.Port, "port", client.DefaultPort, "openGemini port to connect to.")
	c.fs.StringVar(&config.UnixSocket, "socket", "", "openGemini unix socket to connect to.")
	c.fs.StringVar(&config.Username, "username", "", "Username to connect to the server.")
	c.fs.StringVar(&config.Password, "password", "", `Password to connect to the server.`)
	c.fs.StringVar(&config.Database, "database", "", "Database to connect to the server.")
	c.fs.BoolVar(&config.Ssl, "ssl", false, "Use https for connecting to cluster.")
	c.fs.BoolVar(&config.IgnoreSsl, "unsafeSsl", false, "Set this when connecting to the cluster using https and not use SSL verification.")
	c.fs.BoolVar(&config.Import, "import", false, "Import a previous database export from file")
	c.fs.StringVar(&config.ImportConfig.Path, "path", "", "Path to the file to import to OpenGemini.")
	c.fs.BoolVar(&config.ImportConfig.Compressed, "compressed", false, "Set to true when importing a compressed file.")
}

func (c *CompatibleCommand) Usage() {
	c.fs.PrintDefaults()
}

var (
	compatibleCmd = NewCompatibleCommand()
)
