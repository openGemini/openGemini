// Copyright right 2024 openGemini author.
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

package export

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/config"
	"github.com/openGemini/openGemini/lib/index"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/lib/util/lifted/influx/meta"
	"github.com/openGemini/openGemini/lib/util/lifted/vm/protoparser/influx"
	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/stretchr/testify/assert"
)

const (
	defaultDb            = "db0"
	defaultRp            = "rp0"
	defaultShardId       = uint64(1)
	defaultPtId          = uint32(0)
	defaultShardDir      = "1_1566777600000000000_1567382400000000000_1"
	defaultIndexDir      = "1_1566777600000000000_1567382400000000000"
	TxtFormatExporter    = "txt"
	CsvFormatExporter    = "csv"
	RemoteFormatExporter = "remote"
)

var (
	testDataFilePath = path.Join(GetCurrentPath(), "testData.txt")
)

var DefaultEngineOption netstorage.EngineOptions

func init() {
	DefaultEngineOption = netstorage.NewEngineOptions()
	DefaultEngineOption.WriteColdDuration = time.Second * 5000
	DefaultEngineOption.ShardMutableSizeLimit = 30 * 1024 * 1024
	DefaultEngineOption.NodeMutableSizeLimit = 1e9
	DefaultEngineOption.MaxWriteHangTime = time.Second
	DefaultEngineOption.MemDataReadEnabled = true
	DefaultEngineOption.WalSyncInterval = 100 * time.Millisecond
	DefaultEngineOption.WalEnabled = true
	DefaultEngineOption.WalReplayParallel = false
	DefaultEngineOption.WalReplayAsync = false
	DefaultEngineOption.DownSampleWriteDrop = true
}

func InitData(dir string) error {
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir)
	if err != nil {
		return err
	}
	defer func() {
		_ = closeShard(sh)
	}()

	testData, err := getTestData()
	if err != nil {
		return err
	}
	var rows []influx.Row
	for _, line := range testData {
		row, err := parseLine(line)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	err = writeData(sh, rows, true)
	if err != nil {
		return err
	}

	return nil
}

func createShard(db, rp string, ptId uint32, pathName string) (engine.Shard, error) {
	dataPath := path.Join(pathName, "data", db, strconv.Itoa(int(ptId)), rp, defaultShardDir)
	walPath := path.Join(pathName, "wal", db, strconv.Itoa(int(ptId)), rp, defaultShardDir)
	lockPath := filepath.Join(dataPath, "LOCK")
	indexPath := filepath.Join(pathName, "data", db, strconv.Itoa(int(ptId)), rp, "/index", defaultIndexDir)
	ident := &meta.IndexIdentifier{OwnerDb: db, OwnerPt: ptId, Policy: rp}
	ident.Index = &meta.IndexDescriptor{IndexID: 1, IndexGroupID: 2, TimeRange: meta.TimeRangeInfo{}}
	ltime := uint64(time.Now().Unix())
	opts := new(tsi.Options).
		Ident(ident).
		Path(indexPath).
		IndexType(index.MergeSet).
		EndTime(time.Now().Add(time.Hour)).
		Duration(time.Hour).
		LogicalClock(1).
		SequenceId(&ltime).
		Lock(&lockPath)
	indexBuilder := tsi.NewIndexBuilder(opts)
	primaryIndex, err := tsi.NewIndex(opts)
	if err != nil {
		return nil, err
	}
	primaryIndex.SetIndexBuilder(indexBuilder)
	indexRelation, err := tsi.NewIndexRelation(opts, primaryIndex, indexBuilder)
	if err != nil {
		return nil, err
	}
	indexBuilder.Relations[uint32(index.MergeSet)] = indexRelation
	err = indexBuilder.Open()
	if err != nil {
		return nil, err
	}
	shardDuration := &meta.DurationDescriptor{Tier: util.Hot, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1970-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2099-01-01T01:00:00Z")}
	shardIdent := &meta.ShardIdentifier{ShardID: defaultShardId, ShardGroupID: 1, OwnerDb: db, OwnerPt: ptId, Policy: rp}
	sh := engine.NewShard(dataPath, walPath, &lockPath, shardIdent, shardDuration, tr, DefaultEngineOption, config.TSSTORE, nil)
	sh.SetIndexBuilder(indexBuilder)
	if err := sh.OpenAndEnable(nil); err != nil {
		_ = sh.Close()
		return nil, err
	}
	return sh, nil
}

func closeShard(sh engine.Shard) error {
	if err := sh.GetIndexBuilder().Close(); err != nil {
		return err
	}
	if err := sh.Close(); err != nil {
		return err
	}
	return nil
}

func mustParseTime(layout, value string) time.Time {
	tm, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return tm
}

func getTestData() ([]string, error) {
	file, err := os.Open(testDataFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lines []string

	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

func parseLine(line string) (influx.Row, error) {
	data := influx.Row{}

	parts := strings.SplitN(line, " ", 2)
	MstAndTags := strings.Split(parts[0], ",")
	data.Name = MstAndTags[0] + "_0000"
	Tags := MstAndTags[1:]
	data.Tags = make(influx.PointTags, len(Tags))
	for i, tag := range Tags {
		keyValue := strings.Split(tag, "=")
		data.Tags[i].Key = keyValue[0]
		data.Tags[i].Value = keyValue[1]
	}

	spaceIndex := strings.LastIndex(parts[1], " ")
	fieldsPart := parts[1][:spaceIndex]
	fields := strings.Split(fieldsPart, ",")
	data.Fields = make([]influx.Field, len(fields))
	for i, field := range fields {
		keyValue := strings.Split(field, "=")
		data.Fields[i].Key = keyValue[0]
		if intValue, err := strconv.Atoi(keyValue[1]); err == nil {
			// int
			data.Fields[i].Type = influx.Field_Type_Int
			data.Fields[i].NumValue = float64(intValue)
		} else if floatValue, err := strconv.ParseFloat(keyValue[1], 64); err == nil {
			// float
			data.Fields[i].Type = influx.Field_Type_Float
			data.Fields[i].NumValue = floatValue
		} else {
			// string
			data.Fields[i].Type = influx.Field_Type_String
			data.Fields[i].StrValue, err = strconv.Unquote(keyValue[1])
		}
	}
	tm := parts[1][spaceIndex+1:]
	timeStamp, err := strconv.ParseInt(tm, 10, 64)
	if err != nil {
		return data, err
	}
	data.Timestamp = timeStamp * 1000000000
	var indexKeyPool []byte
	data.UnmarshalIndexKeys(indexKeyPool)
	err = data.UnmarshalShardKeyByTag(nil)
	if err != nil {
		return data, err
	}
	return data, nil
}

func writeData(sh engine.Shard, rs []influx.Row, forceFlush bool) error {
	var buff []byte
	var err error
	buff, err = influx.FastMarshalMultiRows(buff, rs)
	if err != nil {
		return err
	}

	for i := range rs {
		sort.Sort(&rs[i].Fields)
	}
	err = sh.WriteRows(rs, buff)
	if err != nil {
		return err
	}

	// wait index flush
	//time.Sleep(time.Second * 1)
	if forceFlush {
		// wait mem table flush
		sh.ForceFlush()
	}
	return nil
}

func CompareStrings(t *testing.T, file1, file2 io.Reader) error {

	// Read the lines from the first file, sort them, and remove empty lines
	var lines1 []string
	scanner1 := bufio.NewScanner(file1)
	for scanner1.Scan() {
		line := scanner1.Text()
		if line != "" {
			lines1 = append(lines1, line)
		}
	}
	sort.Strings(lines1)

	var lines2 []string
	scanner2 := bufio.NewScanner(file2)
	for scanner2.Scan() {
		line := scanner2.Text()
		if line != "" {
			lines2 = append(lines2, line)
		}
	}
	sort.Strings(lines2)

	// Compare the lines of the two files
	if len(lines1) != len(lines2) {
		return fmt.Errorf(fmt.Sprintf("two strings have different lines, lines1: %d, lines2: %d", len(lines1), len(lines2)))
	}

	for i := 0; i < len(lines1); i++ {
		assert.EqualValues(t, lines1[i], lines2[i], fmt.Sprintf("line %d is different", i+1))
	}

	return nil
}

func GetCurrentPath() string {
	_, filename, _, _ := runtime.Caller(1)

	return path.Dir(filename)
}

func QueryData(URL string) (string, error) {
	ip, portStr, err := net.SplitHostPort(URL)
	if err != nil {
		return "", err
	}
	port, err := strconv.ParseInt(portStr, 10, 64)
	if err != nil {
		return "", err
	}
	clientConfig := initConfig(ip, port)
	client, err := opengemini.NewClient(clientConfig)
	if err != nil {
		return "", err
	}
	q := opengemini.Query{
		Database: "db0",
		Command:  "select * from " + "average_temperature",
	}
	res, err := client.Query(q)
	if err != nil {
		return "", err
	}
	for _, r := range res.Results {
		for _, s := range r.Series {
			for _, v := range s.Values {
				line := s.Name + ","
				line += s.Columns[2] + "=" + v[2].(string) + " "
				line += s.Columns[1] + "=" + v[1].(string) + " "
				line += strconv.FormatFloat(v[0].(float64), 'f', -1, 64)
				data += line + "\n"
			}
		}
	}
	return data, nil
}

func initConfig(ip string, port int64) *opengemini.Config {
	config := &opengemini.Config{
		Addresses: []*opengemini.Address{
			{
				Host: ip,
				Port: int(port),
			},
		},
	}
	return config
}

var data = `# CONTEXT-DATABASE: db0
# CONTEXT-MEASUREMENT: average_temperature
# CONTEXT-RETENTION-POLICY: rp0
# DDL
# DML
# FROM TSSP FILE
# openGemini EXPORT: 2019-08-25T09:18:00Z - 2019-08-25T12:48:00Z
CREATE DATABASE db0
CREATE RETENTION POLICY rp0 ON db0 DURATION 0s REPLICATION 1
`
var RemoteTxt = `
# CONTEXT-DATABASE: db0
# CONTEXT-MEASUREMENT: average_temperature
# CONTEXT-RETENTION-POLICY: rp0
# DDL
# DML
# FROM TSSP FILE
# openGemini EXPORT: 2019-08-25T09:18:00Z - 2019-08-25T12:48:00Z
CREATE DATABASE db0
CREATE RETENTION POLICY rp0 ON db0 DURATION 0s REPLICATION 1
average_temperature,location=coyote_creek degrees=88 1566724680000000000
average_temperature,location=coyote_creek degrees=75 1566725040000000000
average_temperature,location=coyote_creek degrees=88 1566725400000000000
average_temperature,location=coyote_creek degrees=79 1566725760000000000
average_temperature,location=coyote_creek degrees=71 1566726120000000000
average_temperature,location=coyote_creek degrees=74 1566726480000000000
average_temperature,location=coyote_creek degrees=87 1566726840000000000
average_temperature,location=coyote_creek degrees=81 1566727200000000000
average_temperature,location=coyote_creek degrees=74 1566727560000000000
average_temperature,location=coyote_creek degrees=72 1566727920000000000
average_temperature,location=coyote_creek degrees=70 1566728280000000000
average_temperature,location=coyote_creek degrees=76 1566728640000000000
average_temperature,location=coyote_creek degrees=75 1566729000000000000
average_temperature,location=coyote_creek degrees=72 1566729360000000000
average_temperature,location=coyote_creek degrees=81 1566729720000000000
average_temperature,location=coyote_creek degrees=90 1566730080000000000
average_temperature,location=coyote_creek degrees=76 1566730440000000000
average_temperature,location=coyote_creek degrees=75 1566730800000000000
average_temperature,location=coyote_creek degrees=83 1566731160000000000
average_temperature,location=coyote_creek degrees=84 1566731520000000000
average_temperature,location=coyote_creek degrees=79 1566731880000000000
average_temperature,location=coyote_creek degrees=78 1566732240000000000
average_temperature,location=coyote_creek degrees=81 1566732600000000000
average_temperature,location=coyote_creek degrees=86 1566732960000000000
average_temperature,location=coyote_creek degrees=79 1566733320000000000
average_temperature,location=coyote_creek degrees=70 1566733680000000000
average_temperature,location=coyote_creek degrees=87 1566734040000000000
average_temperature,location=coyote_creek degrees=78 1566734400000000000
average_temperature,location=coyote_creek degrees=70 1566734760000000000
average_temperature,location=coyote_creek degrees=82 1566735120000000000
average_temperature,location=coyote_creek degrees=88 1566735480000000000
average_temperature,location=coyote_creek degrees=74 1566735840000000000
average_temperature,location=coyote_creek degrees=90 1566736200000000000
average_temperature,location=coyote_creek degrees=82 1566736560000000000
average_temperature,location=coyote_creek degrees=77 1566736920000000000
average_temperature,location=coyote_creek degrees=75 1566737280000000000
average_temperature,location=santa_monica degrees=88 1566724680000000000
average_temperature,location=santa_monica degrees=73 1566725040000000000
average_temperature,location=santa_monica degrees=87 1566725400000000000
average_temperature,location=santa_monica degrees=89 1566725760000000000
average_temperature,location=santa_monica degrees=79 1566726120000000000
average_temperature,location=santa_monica degrees=73 1566726480000000000
average_temperature,location=santa_monica degrees=74 1566726840000000000
average_temperature,location=santa_monica degrees=82 1566727200000000000
average_temperature,location=santa_monica degrees=78 1566727560000000000
average_temperature,location=santa_monica degrees=82 1566727920000000000
average_temperature,location=santa_monica degrees=83 1566728280000000000
average_temperature,location=santa_monica degrees=70 1566728640000000000
average_temperature,location=santa_monica degrees=74 1566729000000000000
average_temperature,location=santa_monica degrees=70 1566729360000000000
average_temperature,location=santa_monica degrees=71 1566729720000000000
average_temperature,location=santa_monica degrees=73 1566730080000000000
average_temperature,location=santa_monica degrees=72 1566730440000000000
average_temperature,location=santa_monica degrees=80 1566730800000000000
average_temperature,location=santa_monica degrees=72 1566731160000000000
average_temperature,location=santa_monica degrees=78 1566731520000000000
average_temperature,location=santa_monica degrees=78 1566731880000000000
average_temperature,location=santa_monica degrees=73 1566732240000000000
average_temperature,location=santa_monica degrees=82 1566732600000000000
average_temperature,location=santa_monica degrees=81 1566732960000000000
average_temperature,location=santa_monica degrees=89 1566733320000000000
average_temperature,location=santa_monica degrees=83 1566733680000000000
average_temperature,location=santa_monica degrees=70 1566734040000000000
average_temperature,location=santa_monica degrees=75 1566734400000000000
average_temperature,location=santa_monica degrees=73 1566734760000000000
average_temperature,location=santa_monica degrees=87 1566735120000000000
average_temperature,location=santa_monica degrees=75 1566735480000000000
average_temperature,location=santa_monica degrees=90 1566735840000000000
average_temperature,location=santa_monica degrees=72 1566736200000000000
average_temperature,location=santa_monica degrees=79 1566736560000000000
average_temperature,location=santa_monica degrees=81 1566736920000000000
average_temperature,location=santa_monica degrees=72 1566737280000000000
`
