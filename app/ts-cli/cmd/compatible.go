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
	c.fs.StringVar(&config.Path, "path", "", "path to the file to import to OpenGemini.")
	c.fs.StringVar(&config.Precision, "precision", "ns", "Specify the format of the timestamp: rfc3339, h, m, s, ms, u or ns. default is ns.")
	c.fs.BoolVar(&config.Export, "export", false, "Export a database from specific data file")
	c.fs.StringVar(&config.DataDir, "datadir", "", "Data storage path to export.")
	c.fs.StringVar(&config.WalDir, "waldir", "", "WAL storage path to export.")
	c.fs.StringVar(&config.Out, "out", "/tmp/openGemini/out.txt", "'-' for standard out or the destination file to export to")
	c.fs.StringVar(&config.Retentions, "retention", "", "Optional. Retention policies to export, note that it can only be used when exporting a single database. eg. rpname1,rpname2")
	c.fs.StringVar(&config.Databases, "databases", "", "Optional. Databases to export. eg. dbname1,dbname2")
	c.fs.StringVar(&config.Start, "start", "", "Optional. As the start filtering time of export data.  eg. YYYY-MM-DDTHH:MM:SSZ")
	c.fs.StringVar(&config.End, "end", "", "Optional. As the end filtering time of export data. eg. YYYY-MM-DDTHH:MM:SSZ")
	c.fs.BoolVar(&config.Compress, "compress", false, "Optional. Compress the export output.")
	c.fs.BoolVar(&config.LpOnly, "lponly", false, "Only export line protocol.")
	flag.CommandLine = c.fs
}

func (c *CompatibleCommand) Usage() {
	c.fs.PrintDefaults()
}

var (
	compatibleCmd = NewCompatibleCommand()
)
