// Copyright Huawei Cloud Computing Technologies Co., Ltd.
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

package app

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/cpu"
	"github.com/openGemini/openGemini/lib/logger"
	"github.com/openGemini/openGemini/lib/statisticsPusher"
	"github.com/openGemini/openGemini/lib/util"
	"go.uber.org/zap"
)

const METALOGO = `
 _________   ______   ____    ____  ________  _________     _       
|  _   _  |.' ____ \ |_   \  /   _||_   __  ||  _   _  |   / \      
|_/ | | \_|| (___ \_|  |   \/   |    | |_ \_||_/ | | \_|  / _ \     
    | |     _.____.    | |\  /| |    |  _| _     | |     / ___ \
   _| |_   | \____) | _| |_\/_| |_  _| |__/ |   _| |_  _/ /   \ \_
  |_____|   \______.'|_____||_____||________|  |_____||____| |____|
	
`

const SQLLOGO = `
 _________   ______    ______     ___    _____     
|  _   _  |.' ____ \ .' ____ \  .'   '.  |_   _|
|_/ | | \_|| (___ \_|| (___ \_|/  .-.  \   | |
    | |     _.____.  _.____.   | |   | |   | |   _
   _| |_   | \____) || \____) |\   -'  \_ _| |__/ |
  |_____|   \______.' \______.' \__.\__| |________|

`

const STORELOGO = `
 _________   ______    ______   _________    ___   _______     ________  
|  _   _  |.' ____ \ .' ____ \ |  _   _  | .'   '.|_   __ \   |_   __  |
|_/ | | \_|| (___ \_|| (___ \_||_/ | | \_|/  .-.  \ | |__) |    | |_ \_|
    | |     _.____'.  _.____'.     | |    | |   | | |  __ /     |  _| _
   _| |_   | \____) || \____) |   _| |_   \  '-'  /_| |  \ \_  _| |__/ |
  |_____|   \______.' \______.'  |_____|   '.___.'|____| |___||________|


`

const TSDATALOGO = `
 _________   ______   ______        _     _________     _       
|  _   _  |.' ____ \ |_   _ \.     / \   |  _   _  |   / \
|_/ | | \_|| (___ \_|  | | \. \   / _ \  |_/ | | \_|  / _ \
    | |     _.____\.   | |  | |  / ___ \     | |     / ___ \
   _| |_   | \____) | _| |_.' /_/ /   \ \_  _| |_  _/ /   \ \_
  |_____|   \______.'|______.'|____| |____||_____||____| |____|

`

const TSSERVER = `
 _________   ______    ______   ________  _______  ____   ____  ________  _______     
|  _   _  |.' ____ \ .' ____ \ |_   __  ||_   __ \|_  _| |_  _||_   __  ||_   __ \    
|_/ | | \_|| (___ \_|| (___ \_|  | |_ \_|  | |__) | \ \   / /    | |_ \_|  | |__) |   
    | |     _.____''.  _.____''.   |  _| _   |  _/   \ \ / /     |  _| _   |  __ /
   _| |_   | \____) || \____) | _| |__/ | _| |  \ \_  \ ' /     _| |__/ | _| |  \ \_
  |_____|   \______.' \______.'|________||____| |___|  \_/     |________||____| |___|

`

const MONITORLOGO = `
 _________   ______   ____    ____   ___   ____  _____  _____  _________    ___   _______     
|  _   _  |.' ____ \ |_   \  /   _|.'   '.|_   \|_   _||_   _||  _   _  | .'   '.|_   __ \    
|_/ | | \_|| (___ \_|  |   \/   | /  .-.  \ |   \ | |    | |  |_/ | | \_|/  .-.  \ | |__) |
    | |     _.____'.   | |\  /| | | |   | | | |\ \| |    | |      | |    | |   | | |  __ /    
   _| |_   | \____) | _| |_\/_| |_\  '-'  /_| |_\   |_  _| |_    _| |_   \  '-'  /_| |  \ \_
  |_____|   \______.'|_____||_____|'.___.'|_____|\____||_____|  |_____|   '.___.'|____| |___|
`

const MainUsage = `Configure and start the process of openGemini.

Usage: %s [[command]] [arguments]

The commands are:
	help            display this help message
	run             start with specified configuration
	version         display the openGemini version

"run" is the default command.

Use "%s [command] -help" for more information about a command.
`

const RunUsage = `Runs the %s server.

Usage: %s run [flags]

    -config <path>
            Set the path to the configuration file.
    -pidfile <path>
            Write process ID to a file.
`

// Version information, the value is set by the build script
var (
	Version   string
	GitCommit string
	GitBranch string
	BuildTime string
)

// FullVersion returns the full version string.
func FullVersion(app string) string {
	const format = `openGemini version info:
%s: %s
git: %s %s
os: %s
arch: %s`

	return fmt.Sprintf(format, app, Version, GitBranch, GitCommit, runtime.GOOS, runtime.GOARCH)
}

// Options represents the command line options that can be parsed.
type Options struct {
	ConfigPath string
	PIDFile    string
	Join       string
	Hostname   string
}

var (
	_ = flag.String("config", "", "-config=config file path")
	_ = flag.String("pidfile", "", "-pid=pid file path")
)

// InitParse inits the command line parse flogs
func InitParse() {
	flag.CommandLine.SetOutput(os.Stdout)
	flag.Usage = func() { println("please run the help command `ts-* help`") }
	flag.Parse()
}

func ParseFlags(usage func(), args ...string) (Options, error) {
	var options Options
	fs := flag.NewFlagSet("", flag.ExitOnError)
	fs.Usage = usage
	fs.StringVar(&options.ConfigPath, "config", "", "")
	fs.StringVar(&options.PIDFile, "pidfile", "", "")
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}
	return options, nil
}

func RemovePIDFile(pidfile string) {
	if pidfile == "" {
		return
	}

	if err := os.Remove(pidfile); err != nil {
		logger.GetLogger().Error("Remove pidfile failed", zap.Error(err))
	}
}

func WritePIDFile(pidfile string) error {
	if pidfile == "" {
		return nil
	}

	pidDir := filepath.Dir(pidfile)
	if err := os.MkdirAll(pidDir, 0700); err != nil {
		return fmt.Errorf("os.MkdirAll failed, error: %s", err)
	}

	pid := strconv.Itoa(os.Getpid())
	if err := os.WriteFile(pidfile, []byte(pid), 0600); err != nil {
		return fmt.Errorf("write pid file failed, error: %s", err)
	}

	return nil
}

func hasSensitiveWord(url string) bool {
	url = strings.ToLower(url)
	sensitiveInfo := "password"
	return strings.Contains(url, sensitiveInfo)
}

func HideQueryPassword(url string) string {
	if !hasSensitiveWord(url) {
		return url
	}
	var buf strings.Builder

	create := "with password"
	url = strings.ToLower(url)
	if strings.Contains(url, create) {
		fields := strings.Fields(url)
		for i, s := range fields {
			if s == "password" {
				buf.WriteString(strings.Join(fields[:i+1], " "))
				buf.WriteString(" [REDACTED] ")
				if i < len(fields)-2 {
					buf.WriteString(strings.Join(fields[i+2:], " "))
				}
				return buf.String()
			}
		}
	}
	set := "set password"
	if strings.Contains(url, set) {
		fields := strings.SplitAfter(url, "=")
		buf.WriteString(fields[0])
		buf.WriteString(" [REDACTED]")
		return buf.String()
	}
	return url
}

func SetStatsResponse(pusher *statisticsPusher.StatisticsPusher, w http.ResponseWriter, r *http.Request) {
	if pusher == nil {
		return
	}

	stats, err := pusher.CollectOpsStatistics()
	if err != nil {
		util.HttpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	fmt.Fprintln(w, "{")

	first := true
	uniqueKeys := make(map[string]int)
	for _, s := range stats {
		val, err := json.Marshal(s)
		if err != nil {
			continue
		}

		// Very hackily create a unique key.
		buf := bytes.NewBufferString(s.Name)
		key := buf.String()
		v := uniqueKeys[key]
		uniqueKeys[key] = v + 1
		if v > 0 {
			fmt.Fprintf(buf, ":%d", v)
			key = buf.String()
		}

		if !first {
			fmt.Fprintln(w, ",")
		}
		first = false
		fmt.Fprintf(w, "%q: ", key)
		_, err = w.Write(bytes.TrimSpace(val))
		if err != nil {
			util.HttpError(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	fmt.Fprintln(w, "\n}")
}

type ServerInfo struct {
	App       config.App
	Version   string
	Commit    string
	Branch    string
	BuildTime string
}

func (si *ServerInfo) StatVersion() string {
	return fmt.Sprintf("%s-%s:%s-%s", si.Version, si.Branch, si.Commit, si.BuildTime)
}

func (si *ServerInfo) FullVersion() string {
	return FullVersion(string(si.App))
}

func LogStarting(name string, info *ServerInfo) {
	logger.GetLogger().Info(name+" starting",
		zap.String("version", info.Version),
		zap.String("branch", info.Branch),
		zap.String("commit", info.Commit),
		zap.String("buildTime", info.BuildTime))
	logger.GetLogger().Info("Go runtime",
		zap.String("version", runtime.Version()),
		zap.Int("maxprocs", cpu.GetCpuNum()))
	//Mark start-up in extra log
	fmt.Printf("%v %s starting\n", time.Now(), name)
}
