// Copyright 2025 openGemini Authors
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

package subcmd

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openGemini/openGemini-cli/core"
	"github.com/openGemini/openGemini/lib/errno"
	"github.com/vbauerster/mpb/v7"
)

const (
	tsspFileExtension    = "tssp"
	walFileExtension     = "wal"
	csvFormatExporter    = "csv"
	txtFormatExporter    = "txt"
	remoteFormatExporter = "remote"
	resumeFilePrefix     = "resume_"
	dirNameSeparator     = "_"
)

var (
	MpbProgress         = mpb.New(mpb.WithWidth(100))
	ResumeJsonPath      string
	ProgressedFilesPath string
)

type ExportConfig struct {
	*core.CommandLineConfig `json:"-"`
	Export                  bool
	Format                  string `json:"format"`
	Out                     string `json:"out"`
	DataDir                 string `json:"data"`
	WalDir                  string `json:"wal"`
	Remote                  string `json:"remote"`
	RemoteUsername          string `json:"-"`
	RemotePassword          string `json:"-"`
	RemoteSsl               bool   `json:"remotessl"`
	DBFilter                string `json:"dbfilter"`
	RetentionFilter         string `json:"retentionfilter"`
	MeasurementFilter       string `json:"mstfilter"`
	TimeFilter              string `json:"timefilter"`
	Compress                bool   `json:"compress"`
	Resume                  bool
}

type ExportCommand struct {
	cfg       *ExportConfig
	exportCmd *Exporter
}

func (c *ExportCommand) Run(config *ExportConfig) error {
	if err := flag.CommandLine.Parse([]string{"-loggerLevel=ERROR"}); err != nil {
		return err
	}
	c.cfg = config
	c.exportCmd = NewExporter()

	return c.process()
}

func (c *ExportCommand) process() error {
	if c.cfg.Resume {
		if err := ReadLatestProgressFile(); err != nil {
			return err
		}
		oldConfig, err := getResumeConfig(c.cfg)
		if err != nil {
			return err
		}
		progressedFiles, err := getProgressedFiles()
		if err != nil {
			return err
		}
		return c.exportCmd.Export(oldConfig, progressedFiles)
	} else {
		if err := CreateNewProgressFolder(); err != nil {
			return err
		}
		return c.exportCmd.Export(c.cfg, nil)
	}
}

func getResumeConfig(options *ExportConfig) (*ExportConfig, error) {
	jsonData, err := os.ReadFile(ResumeJsonPath)
	if err != nil {
		return nil, err
	}
	var config ExportConfig
	err = json.Unmarshal(jsonData, &config)
	if err != nil {
		return nil, err
	}
	config.Resume = true
	config.RemoteUsername = options.RemoteUsername
	config.RemotePassword = options.RemotePassword
	return &config, nil
}

func getProgressedFiles() (map[string]struct{}, error) {
	file, err := os.Open(ProgressedFilesPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineSet := make(map[string]struct{})

	for scanner.Scan() {
		line := scanner.Text()
		lineSet[line] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lineSet, nil
}

// CreateNewProgressFolder init ResumeJsonPath and ProgressedFilesPath
func CreateNewProgressFolder() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	targetPath := filepath.Join(home, ".ts-cli", time.Now().Format("2006-01-02_15-04-05.000000000"))
	err = os.MkdirAll(targetPath, os.ModePerm)
	if err != nil {
		return err
	}
	// create progress.json
	progressJson := filepath.Join(targetPath, "progress.json")
	ResumeJsonPath = progressJson
	// create progressedFiles
	progressedFiles := filepath.Join(targetPath, "progressedFiles")
	ProgressedFilesPath = progressedFiles
	return nil
}

// ReadLatestProgressFile reads and processes the latest folder
func ReadLatestProgressFile() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	baseDir := filepath.Join(home, ".ts-cli")
	var dirs []string
	err = filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() || path == baseDir {
			return nil
		}
		dirs = append(dirs, path)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(dirs)
	if len(dirs) == 0 {
		return nil
	}
	latestDir := dirs[len(dirs)-1]
	// read progress.json
	ResumeJsonPath = filepath.Join(latestDir, "progress.json")
	// read progressedFiles
	ProgressedFilesPath = filepath.Join(latestDir, "progressedFiles")
	return nil
}

type dataFilter struct {
	database    string
	retention   string
	measurement string
	startTime   int64
	endTime     int64
}

func newDataFilter() *dataFilter {
	return &dataFilter{
		database:    "",
		measurement: "",
		startTime:   math.MinInt64,
		endTime:     math.MaxInt64,
	}
}

func (d *dataFilter) parseTime(clc *ExportConfig) error {
	var start, end string
	timeSlot := strings.Split(clc.TimeFilter, "~")
	if len(timeSlot) == 2 {
		start = timeSlot[0]
		end = timeSlot[1]
	} else if clc.TimeFilter != "" {
		return fmt.Errorf("invalid time filter %q", clc.TimeFilter)
	}

	if start != "" {
		st, err := convertTime(start)
		if err != nil {
			return err
		}
		d.startTime = st
	}

	if end != "" {
		ed, err := convertTime(end)
		if err != nil {
			return err
		}
		d.endTime = ed
	}

	if d.startTime > d.endTime {
		return fmt.Errorf("start time `%q` > end time `%q`", start, end)
	}

	return nil
}

func (d *dataFilter) parseDatabase(dbFilter string) {
	if dbFilter == "" {
		return
	}
	d.database = dbFilter
}

func (d *dataFilter) parseRetention(retentionFilter string) {
	if retentionFilter == "" {
		return
	}
	d.retention = retentionFilter
}

func (d *dataFilter) parseMeasurement(mstFilter string) error {
	if mstFilter == "" {
		return nil
	}
	if mstFilter != "" && d.database == "" {
		return fmt.Errorf("measurement filter %q requires database filter", mstFilter)
	}
	d.measurement = mstFilter
	return nil
}

// timeFilter [startTime, endTime]
func (d *dataFilter) timeFilter(t int64) bool {
	return t >= d.startTime && t <= d.endTime
}

func (d *dataFilter) isBelowMinTimeFilter(t int64) bool {
	return t < d.startTime
}

func (d *dataFilter) isAboveMaxTimeFilter(t int64) bool {
	return t > d.endTime
}

type DatabaseDiskInfo struct {
	dbName          string              // ie. "NOAA_water_database"
	rps             map[string]struct{} // ie. ["0:autogen","1:every_one_day"]
	dataDir         string              // ie. "/tmp/openGemini/data/data/NOAA_water_database"
	walDir          string              // ie. "/tmp/openGemini/data/wal/NOAA_water_database"
	rpToTsspDirMap  map[string]string   // ie. {"0:autogen", "/tmp/openGemini/data/data/NOAA_water_database/0/autogen"}
	rpToWalDirMap   map[string]string   // ie. {"0:autogen", "/tmp/openGemini/data/wal/NOAA_water_database/0/autogen"}
	rpToIndexDirMap map[string]string   // ie. {"0:autogen", "/tmp/openGemini/data/data/NOAA_water_database/0/autogen/index"}
}

func newDatabaseDiskInfo() *DatabaseDiskInfo {
	return &DatabaseDiskInfo{
		rps:             make(map[string]struct{}),
		rpToTsspDirMap:  make(map[string]string),
		rpToWalDirMap:   make(map[string]string),
		rpToIndexDirMap: make(map[string]string),
	}
}

func (d *DatabaseDiskInfo) init(actualDataDir string, actualWalDir string, databaseName string, retentionPolicy string) error {
	d.dbName = databaseName

	// check whether the database is in actualDataPath
	dataDir := filepath.Join(actualDataDir, databaseName)
	if _, err := os.Stat(dataDir); err != nil {
		return err
	}
	// check whether the database is in actualWalPath
	walDir := filepath.Join(actualWalDir, databaseName)
	if _, err := os.Stat(walDir); err != nil {
		return err
	}

	// ie. /tmp/openGemini/data/data/my_db  /tmp/openGemini/data/wal/my_db
	d.dataDir, d.walDir = dataDir, walDir

	ptDirs, err := os.ReadDir(d.dataDir)
	if err != nil {
		return err
	}
	for _, ptDir := range ptDirs {
		// ie. /tmp/openGemini/data/data/my_db/0
		ptTsspPath := filepath.Join(d.dataDir, ptDir.Name())
		// ie. /tmp/openGemini/data/wal/my_db/0
		ptWalPath := filepath.Join(d.walDir, ptDir.Name())

		if retentionPolicy != "" {
			ptWithRp := ptDir.Name() + ":" + retentionPolicy
			// ie. /tmp/openGemini/data/data/my_db/0/autogen
			rpTsspPath := filepath.Join(ptTsspPath, retentionPolicy)
			if _, err := os.Stat(rpTsspPath); err != nil {
				return fmt.Errorf("retention policy %q invalid : %s", retentionPolicy, err)
			} else {
				d.rps[ptWithRp] = struct{}{}
				d.rpToTsspDirMap[ptWithRp] = rpTsspPath
				d.rpToIndexDirMap[ptWithRp] = filepath.Join(rpTsspPath, "index")
			}
			// ie. /tmp/openGemini/data/wal/my_db/0/autogen
			rpWalPath := filepath.Join(ptWalPath, retentionPolicy)
			if _, err := os.Stat(rpWalPath); err != nil {
				return fmt.Errorf("retention policy %q invalid : %s", retentionPolicy, err)
			} else {
				d.rpToWalDirMap[ptWithRp] = rpWalPath
			}
			continue
		}

		rpTsspDirs, err1 := os.ReadDir(ptTsspPath)
		if err1 != nil {
			return err1
		}
		for _, rpDir := range rpTsspDirs {
			if !rpDir.IsDir() {
				continue
			}
			ptWithRp := ptDir.Name() + ":" + rpDir.Name()
			rpPath := filepath.Join(ptTsspPath, rpDir.Name())
			d.rps[ptWithRp] = struct{}{}
			d.rpToTsspDirMap[ptWithRp] = rpPath
			d.rpToIndexDirMap[ptWithRp] = filepath.Join(rpPath, "index")
		}

		rpWalDirs, err2 := os.ReadDir(ptWalPath)
		if err2 != nil {
			return err2
		}
		for _, rpDir := range rpWalDirs {
			ptWithRp := ptDir.Name() + ":" + rpDir.Name()
			if !rpDir.IsDir() {
				continue
			}
			rpPath := filepath.Join(ptWalPath, rpDir.Name())
			d.rpToWalDirMap[ptWithRp] = rpPath
		}
	}
	return nil
}

func parseShardDir(shardDirName string) (uint64, int64, int64, uint64, error) {
	shardDir := strings.Split(shardDirName, dirNameSeparator)
	if len(shardDir) != 4 {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	shardID, err := strconv.ParseUint(shardDir[0], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	dirStartTime, err := strconv.ParseInt(shardDir[1], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	dirEndTime, err := strconv.ParseInt(shardDir[2], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	indexID, err := strconv.ParseUint(shardDir[3], 10, 64)
	if err != nil {
		return 0, 0, 0, 0, errno.NewError(errno.InvalidDataDir)
	}
	return shardID, dirStartTime, dirEndTime, indexID, nil
}

func parseIndexDir(indexDirName string) (uint64, error) {
	indexDir := strings.Split(indexDirName, dirNameSeparator)
	if len(indexDir) != 3 {
		return 0, errno.NewError(errno.InvalidDataDir)
	}

	indexID, err := strconv.ParseUint(indexDir[0], 10, 64)
	if err != nil {
		return 0, errno.NewError(errno.InvalidDataDir)
	}
	return indexID, nil
}

func convertTime(input string) (int64, error) {
	t, err := time.Parse(time.RFC3339, input)
	if err == nil {
		return t.UnixNano(), nil
	}

	timestamp, err := strconv.ParseInt(input, 10, 64)
	if err == nil {
		return timestamp, nil
	}

	return 0, err
}
