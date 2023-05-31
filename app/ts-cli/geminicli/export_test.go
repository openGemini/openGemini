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
package geminicli

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openGemini/openGemini/engine"
	"github.com/openGemini/openGemini/engine/index/tsi"
	"github.com/openGemini/openGemini/lib/netstorage"
	"github.com/openGemini/openGemini/lib/util"
	"github.com/openGemini/openGemini/open_src/influx/meta"
	"github.com/openGemini/openGemini/open_src/vm/protoparser/influx"
	"github.com/stretchr/testify/assert"
)

const defaultDb = "db0"
const defaultRp = "rp0"
const defaultShardId = uint64(1)
const defaultPtId = uint32(0)
const defaultMeasurementName = "cpu"

const defaultShardDir = "1_1566777600000000000_1567382400000000000_1"
const defaultIndexDir = "1_1566777600000000000_1567382400000000000"

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

func createTestDirs(tempPath, pt, dbName string, rpNames []string) error {
	for _, rpName := range rpNames {
		if err := os.MkdirAll(path.Join(tempPath, "data", dbName, pt, rpName, defaultShardDir), os.ModePerm); err != nil {
			return err
		}
		if err := os.MkdirAll(path.Join(tempPath, "wal", dbName, pt, rpName, defaultShardDir), os.ModePerm); err != nil {
			return err
		}
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
		IndexType(tsi.MergeSet).
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
	indexBuilder.Relations[uint32(tsi.MergeSet)] = indexRelation
	err = indexBuilder.Open()
	if err != nil {
		return nil, err
	}
	shardDuration := &meta.DurationDescriptor{Tier: util.Hot, TierDuration: time.Hour}
	tr := &meta.TimeRangeInfo{StartTime: mustParseTime(time.RFC3339Nano, "1970-01-01T01:00:00Z"),
		EndTime: mustParseTime(time.RFC3339Nano, "2099-01-01T01:00:00Z")}
	shardIdent := &meta.ShardIdentifier{ShardID: defaultShardId, ShardGroupID: 1, OwnerDb: db, OwnerPt: ptId, Policy: rp}
	sh := engine.NewShard(dataPath, walPath, &lockPath, shardIdent, shardDuration, tr, DefaultEngineOption)
	sh.SetIndexBuilder(indexBuilder)
	if err := sh.OpenAndEnable(nil); err != nil {
		_ = sh.Close()
		return nil, err
	}
	return sh, nil
}

func closeShard(sh engine.Shard) error {
	if err := sh.CloseIndexBuilder(); err != nil {
		return err
	}
	if err := sh.Close(); err != nil {
		return err
	}
	return nil
}

func writeData(sh engine.Shard, rs []influx.Row, forceFlush bool) error {
	var buff []byte
	var err error
	buff, err = influx.FastMarshalMultiRows(buff, rs)
	if err != nil {
		return err
	}

	for i := range rs {
		sort.Sort(rs[i].Fields)
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

func GenDataRecord(msNames []string, seriesNum, pointNumOfPerSeries int, interval time.Duration,
	fullField, inc bool, fixBool bool, tv ...int) ([]influx.Row, int64, int64) {
	pts := make([]influx.Row, 0, seriesNum)
	names := msNames
	if len(msNames) == 0 {
		names = []string{defaultMeasurementName}
	}

	mms := func(i int) string {
		return names[i%len(names)]
	}

	var indexKeyPool []byte
	vInt, vFloat := int64(1), 1.1
	tv1, tv2, tv3, tv4 := 1, 1, 1, 1
	for _, tgv := range tv {
		tv1 = tgv
	}

	basicTime := time.Date(2023, 5, 28, 4, 24, 20, 0, time.UTC)
	random := rand.New(rand.NewSource(42))
	for i := 0; i < seriesNum; i++ {
		fields := map[string]interface{}{
			"field2_int":      vInt,
			"field3_bool":     i%2 == 0,
			`fie ld4_,fl=oat`: vFloat,
			"field1_string":   fmt.Sprintf("test-test-test-test-%d", i),
		}
		if fixBool {
			fields["field3_bool"] = i%2 == 0
		} else {
			fields["field3_bool"] = rand.Int31n(100) > 50
		}

		if !fullField {
			if i%10 == 0 {
				delete(fields, "field1_string")
			}

			if i%25 == 0 {
				delete(fields, "field4_float")
			}

			if i%35 == 0 {
				delete(fields, "field3_bool")
			}
		}

		r := influx.Row{}

		// fields init
		r.Fields = make([]influx.Field, len(fields))
		j := 0
		for k, v := range fields {
			r.Fields[j].Key = k
			switch v := v.(type) {
			case int64:
				r.Fields[j].Type = influx.Field_Type_Int
				r.Fields[j].NumValue = float64(v)
			case float64:
				r.Fields[j].Type = influx.Field_Type_Float
				r.Fields[j].NumValue = v
			case string:
				r.Fields[j].Type = influx.Field_Type_String
				r.Fields[j].StrValue = v
			case bool:
				r.Fields[j].Type = influx.Field_Type_Boolean
				if v == false {
					r.Fields[j].NumValue = 0
				} else {
					r.Fields[j].NumValue = 1
				}
			}
			j++
		}

		sort.Sort(r.Fields)

		vInt++
		vFloat += 1.1
		tags := map[string]string{
			"tagkey1":    fmt.Sprintf("tagvalue1_%d", tv1),
			`tag ke,y=2`: fmt.Sprintf("tagvalue2_%d", tv2),
			"tagkey3":    fmt.Sprintf("tagvalue3_%d", tv3),
			"tagkey4":    fmt.Sprintf(`ta valu,e=4_%d`, tv4),
		}

		// tags init
		r.Tags = make(influx.PointTags, len(tags))
		j = 0
		for k, v := range tags {
			r.Tags[j].Key = k
			r.Tags[j].Value = v
			j++
		}
		sort.Sort(&r.Tags)
		tv4++
		tv3++
		tv2++
		tv1++

		name := mms(i)
		r.Name = name

		offset := time.Duration(random.Int63n(int64(24 * time.Hour)))
		tm := basicTime.Add(offset)
		r.Timestamp = tm.UnixNano()
		r.UnmarshalIndexKeys(indexKeyPool)
		err := r.UnmarshalShardKeyByTag(nil)
		if err != nil {
			return nil, 0, 0
		}
		pts = append(pts, r)
	}
	if pointNumOfPerSeries > 1 {
		copyRs := copyPointRows(pts, pointNumOfPerSeries-1, interval, inc)
		pts = append(pts, copyRs...)
	}

	sort.Slice(pts, func(i, j int) bool {
		return pts[i].Timestamp < pts[j].Timestamp
	})

	return pts, pts[0].Timestamp, pts[len(pts)-1].Timestamp
}
func copyPointRows(rs []influx.Row, copyCnt int, interval time.Duration, inc bool) []influx.Row {
	copyRs := make([]influx.Row, 0, copyCnt*len(rs))
	for i := 0; i < copyCnt; i++ {
		cnt := int64(i + 1)
		for index, endIndex := 0, len(rs); index < endIndex; index++ {
			tmpPointRow := rs[index]
			tmpPointRow.Timestamp += int64(interval) * cnt
			// fields need regenerate
			if inc {
				tmpPointRow.Fields = make([]influx.Field, len(rs[index].Fields))
				if len(rs[index].Fields) > 0 {
					tmpPointRow.Copy(&rs[index])
					for idx, field := range rs[index].Fields {
						switch field.Type {
						case influx.Field_Type_Int:
							tmpPointRow.Fields[idx].NumValue += float64(cnt)
						case influx.Field_Type_Float:
							tmpPointRow.Fields[idx].NumValue += float64(cnt)
						case influx.Field_Type_Boolean:
							tmpPointRow.Fields[idx].NumValue = float64(cnt & 1)
						case influx.Field_Type_String:
							tmpPointRow.Fields[idx].StrValue += fmt.Sprintf("-%d", cnt)
						}
					}
				}
			}

			copyRs = append(copyRs, tmpPointRow)
		}
	}
	return copyRs
}

func compareStrings(t *testing.T, file1, file2 io.Reader) error {

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
		return fmt.Errorf("two strings have different lines")
	}
	for i := 0; i < len(lines1); i++ {
		assert.EqualValues(t, lines1[i], lines2[i])
	}

	return nil
}

func createMockDbDiskInfo(tempDir, dbName, pt string, rpNames []string) *DatabaseDiskInfo {
	a := &DatabaseDiskInfo{
		dbName:          dbName,
		rps:             make(map[string]struct{}),
		dataDir:         path.Join(tempDir, "data", dbName),
		walDir:          path.Join(tempDir, "wal", dbName),
		rpToTsspDirMap:  make(map[string]string),
		rpToWalDirMap:   make(map[string]string),
		rpToIndexDirMap: make(map[string]string),
	}
	for _, rp := range rpNames {
		key := pt + ":" + rp
		a.rps[key] = struct{}{}
		a.rpToTsspDirMap[key] = path.Join(tempDir, "data", dbName, pt, rp)
		a.rpToWalDirMap[key] = path.Join(tempDir, "wal", dbName, pt, rp)
		a.rpToIndexDirMap[key] = path.Join(tempDir, "data", dbName, pt, rp, "index")
	}
	return a
}

func TestDatabaseDiskInfo_Init(t *testing.T) {
	tempDir := t.TempDir()
	type fields struct {
		dbName              string
		rps                 map[string]struct{}
		dataDir             string
		walDir              string
		rpNameToTsspDirMap  map[string]string
		rpNameToWalDirMap   map[string]string
		rpNameToIndexDirMap map[string]string
	}
	type args struct {
		actualDataDir string
		actualWalDir  string
		databaseName  string
		rpNames       string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "T1",
			fields: fields{
				dbName:              "",
				rps:                 make(map[string]struct{}),
				dataDir:             "",
				walDir:              "",
				rpNameToTsspDirMap:  make(map[string]string),
				rpNameToWalDirMap:   make(map[string]string),
				rpNameToIndexDirMap: make(map[string]string),
			},
			args: args{
				actualDataDir: path.Join(tempDir, "data"),
				actualWalDir:  path.Join(tempDir, "wal"),
				databaseName:  "test_db2",
				rpNames:       "test_db2_rp1,test_db2_rp2",
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DatabaseDiskInfo{
				dbName:          tt.fields.dbName,
				rps:             tt.fields.rps,
				dataDir:         tt.fields.dataDir,
				walDir:          tt.fields.walDir,
				rpToTsspDirMap:  tt.fields.rpNameToTsspDirMap,
				rpToWalDirMap:   tt.fields.rpNameToWalDirMap,
				rpToIndexDirMap: tt.fields.rpNameToIndexDirMap,
			}
			err := createTestDirs(tempDir, "0", tt.args.databaseName, strings.Split(tt.args.rpNames, ","))
			assert.NoError(t, err)
			tt.wantErr(t, d.Init(tt.args.actualDataDir, tt.args.actualWalDir, tt.args.databaseName, tt.args.rpNames))
			mockDbDiskInfo := createMockDbDiskInfo(tempDir, tt.args.databaseName, "0", strings.Split(tt.args.rpNames, ","))
			assert.EqualValues(t, mockDbDiskInfo, d)
		})
	}
}

func TestExporter_Export(t *testing.T) {
	dir := t.TempDir()
	msNames := []string{"cpu"}
	sh, err := createShard(defaultDb, defaultRp, defaultPtId, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = closeShard(sh)
	}()
	rows, _, _ := GenDataRecord(msNames, 8, 5, time.Second, false, true, true)
	err = writeData(sh, rows, true)
	if err != nil {
		t.Fatal(err)
	}
	exportPath := filepath.Join(t.TempDir(), "export.txt")

	e := &Exporter{
		stderrLogger:                    log.New(os.Stderr, "uint_test", log.LstdFlags),
		stdoutLogger:                    log.New(os.Stdout, "uint_test", log.LstdFlags),
		manifest:                        make(map[string]struct{}),
		rpNameToMeasurementTsspFilesMap: make(map[string]map[string][]string),
		rpNameToIdToIndexMap:            make(map[string]map[uint64]*tsi.MergeSetIndex),
		rpNameToWalFilesMap:             make(map[string][]string),
		Stderr:                          os.Stderr,
		Stdout:                          os.Stdout,
	}
	clc := &CommandLineConfig{
		Database:   "",
		Export:     false,
		DataDir:    dir,
		WalDir:     dir,
		Out:        exportPath,
		Retentions: "",
		Start:      "",
		End:        "",
		Compress:   false,
		LpOnly:     true,
	}
	err = e.Export(clc)
	assert.NoError(t, err)
	lpOnlyReader := strings.NewReader(lpOnlyContent)
	exportFile, err := os.Open(exportPath)
	assert.NoError(t, err)
	assert.NoError(t, compareStrings(t, lpOnlyReader, exportFile))
}

func TestEscapeFieldKey(t *testing.T) {
	type args struct {
		in string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "T1",
			args: args{
				in: "fie,l d=1",
			},
			want: `fie\,l\ d\=1`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, EscapeFieldKey(tt.args.in), "EscapeFieldKey(%v)", tt.args.in)
		})
	}
}

func TestEscapeMstName(t *testing.T) {
	type args struct {
		in string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "T1",
			args: args{in: "ms tn=ame"},
			want: `ms\ tn\=ame`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, EscapeMstName(tt.args.in), "EscapeMstName(%v)", tt.args.in)
		})
	}
}

func TestEscapeTagKey(t *testing.T) {
	type args struct {
		in string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "TestEscapeTagKey",
			args: args{in: "t,a g=1"},
			want: `t\,a\ g\=1`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, EscapeTagKey(tt.args.in), "EscapeTagKey(%v)", tt.args.in)
		})
	}
}

func TestEscapeTagValue(t *testing.T) {
	type args struct {
		in string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "TestEscapeTagValue",
			args: args{in: "ta,gva lue=1"},
			want: `ta\,gva\ lue\=1`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, EscapeTagValue(tt.args.in), "EscapeTagValue(%v)", tt.args.in)
		})
	}
}

func TestExporter_writeDDL(t *testing.T) {
	tempPath := t.TempDir()
	type fields struct {
		databaseDiskInfos []*DatabaseDiskInfo
	}
	type args struct {
		dbName  string
		rpNames []string
	}
	tests := []struct {
		name             string
		fields           fields
		args             args
		wantMetaWriter   string
		wantOutputWriter string
		wantErr          assert.ErrorAssertionFunc
	}{
		{
			name: "T1",
			fields: fields{databaseDiskInfos: []*DatabaseDiskInfo{
				createMockDbDiskInfo(tempPath, "test_db2", "0", []string{"test_db2_rp1", "test_db2_rp1"}),
			}},
			args: args{
				dbName:  "test_db2",
				rpNames: []string{"test_db2_rp1", "test_db2_rp1"},
			},
			wantMetaWriter: "# DDL",
			wantOutputWriter: `CREATE DATABASE test_db2
CREATE RETENTION POLICY test_db2_rp1 ON test_db2 DURATION 0s REPLICATION 1 SHARD DURATION 168h0m0s
CREATE RETENTION POLICY test_db2_rp2 ON test_db2 DURATION 0s REPLICATION 1 SHARD DURATION 168h0m0s
`,
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := createTestDirs(tempPath, "0", tt.args.dbName, tt.args.rpNames)
			assert.NoError(t, err)

			e := &Exporter{
				databaseDiskInfos: tt.fields.databaseDiskInfos,
			}
			metaWriter := &bytes.Buffer{}
			outputWriter := &bytes.Buffer{}

			tt.wantErr(t, e.writeDDL(metaWriter, outputWriter))
			tt.wantErr(t, compareStrings(t, metaWriter, strings.NewReader(tt.wantMetaWriter)))
			tt.wantErr(t, compareStrings(t, outputWriter, strings.NewReader(tt.wantOutputWriter)))
		})
	}
}

func TestExporter_writeDML(t *testing.T) {
	type fields struct {
		manifest                        map[string]struct{}
		rpNameToMeasurementTsspFilesMap map[string]map[string][]string
		rpNameToIdToIndexMap            map[string]map[uint64]*tsi.MergeSetIndex
		rpNameToWalFilesMap             map[string][]string
		defaultLogger                   *log.Logger
	}
	tests := []struct {
		name             string
		fields           fields
		wantMetaWriter   string
		wantOutputWriter string
	}{
		{
			name: "T1",
			fields: fields{
				manifest: map[string]struct {
				}{
					"db1:rp1": struct {
					}{},
					"db2:rp2": struct {
					}{},
				},
				rpNameToMeasurementTsspFilesMap: map[string]map[string][]string{
					"db1:rp1": {
						"mst1": []string{},
						"mst2": []string{},
					},
					"db2:rp2": {
						"mst3": []string{},
						"mst4": []string{},
					},
				},
				rpNameToIdToIndexMap: map[string]map[uint64]*tsi.MergeSetIndex{
					"db1:rp1": {},
					"db2:rp2": {},
				},
				rpNameToWalFilesMap: map[string][]string{
					"db1:rp2": {},
					"db2:rp2": {},
				},
				defaultLogger: log.New(os.Stdout, "uint_test:", log.LstdFlags),
			},
			wantMetaWriter: `# DML
# CONTEXT-DATABASE:db2
# CONTEXT-RETENTION-POLICY:rp2

# FROM TSSP FILE.
# CONTEXT-MEASUREMENT mst3 
# CONTEXT-MEASUREMENT mst4 
# FROM WAL FILE.
# CONTEXT-DATABASE:db1
# CONTEXT-RETENTION-POLICY:rp1

# FROM TSSP FILE.
# CONTEXT-MEASUREMENT mst1 
# CONTEXT-MEASUREMENT mst2 `,
			wantOutputWriter: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Exporter{
				manifest:                        tt.fields.manifest,
				rpNameToMeasurementTsspFilesMap: tt.fields.rpNameToMeasurementTsspFilesMap,
				rpNameToIdToIndexMap:            tt.fields.rpNameToIdToIndexMap,
				rpNameToWalFilesMap:             tt.fields.rpNameToWalFilesMap,
				defaultLogger:                   tt.fields.defaultLogger,
			}
			metaWriter := &bytes.Buffer{}
			outputWriter := &bytes.Buffer{}
			err := e.writeDML(metaWriter, outputWriter)
			assert.NoError(t, err)
			assert.NoError(t, compareStrings(t, metaWriter, strings.NewReader(tt.wantMetaWriter)))
			assert.NoError(t, compareStrings(t, outputWriter, strings.NewReader(tt.wantOutputWriter)))
		})
	}
}

func TestLineFilter_Filter(t *testing.T) {
	type fields struct {
		startTime int64
		endTime   int64
	}
	type args struct {
		t int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "T1",
			fields: fields{
				startTime: 0,
				endTime:   100,
			},
			args: args{
				t: 30,
			},
			want: true,
		},
		{
			name: "T2",
			fields: fields{
				startTime: 0,
				endTime:   100,
			},
			args: args{
				t: 120,
			},
			want: false,
		},
		{
			name: "T3",
			fields: fields{
				startTime: math.MinInt64,
				endTime:   math.MaxInt64,
			},
			args: args{t: rand.Int63()},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &LineFilter{
				startTime: tt.fields.startTime,
				endTime:   tt.fields.endTime,
			}
			assert.Equalf(t, tt.want, l.Filter(tt.args.t), "Filter(%v)", tt.args.t)
		})
	}
}

func Test_parseShardGroupDuration(t *testing.T) {
	type args struct {
		str string
	}
	tests := []struct {
		name    string
		args    args
		want    time.Duration
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "T1",
			args: args{
				str: "1568592000000000000_1569196800000000000",
			},
			want: time.Duration(1568592000000000000 - 1569196800000000000),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return t == nil
			},
		},
		{
			name: "T2",
			args: args{
				str: "156859200xxx0000000_156919abs0000000",
			},
			want: time.Duration(0),
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return t != nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseShardGroupDuration(tt.args.str)
			if !tt.wantErr(t, err, fmt.Sprintf("parseShardGroupDuration(%v)", tt.args.str)) {
				return
			}
			assert.Equalf(t, tt.want, got, "parseShardGroupDuration(%v)", tt.args.str)
		})
	}
}

var lpOnlyContent = `cpu,tag\ ke\,y\=2=tagvalue2_4,tagkey1=tagvalue1_4,tagkey3=tagvalue3_4,tagkey4=ta\ valu\,e\=4_4 fie\ ld4_\,fl\=oat=4.4,field1_string="test-test-test-test-3",field2_int=4i,field3_bool=false 1685318046526624009
cpu,tag\ ke\,y\=2=tagvalue2_4,tagkey1=tagvalue1_4,tagkey3=tagvalue3_4,tagkey4=ta\ valu\,e\=4_4 fie\ ld4_\,fl\=oat=5.4,field1_string="test-test-test-test-3-1",field2_int=5i,field3_bool=true 1685318047526624009
cpu,tag\ ke\,y\=2=tagvalue2_4,tagkey1=tagvalue1_4,tagkey3=tagvalue3_4,tagkey4=ta\ valu\,e\=4_4 fie\ ld4_\,fl\=oat=6.4,field1_string="test-test-test-test-3-2",field2_int=6i,field3_bool=false 1685318048526624009
cpu,tag\ ke\,y\=2=tagvalue2_4,tagkey1=tagvalue1_4,tagkey3=tagvalue3_4,tagkey4=ta\ valu\,e\=4_4 fie\ ld4_\,fl\=oat=7.4,field1_string="test-test-test-test-3-3",field2_int=7i,field3_bool=true 1685318049526624009
cpu,tag\ ke\,y\=2=tagvalue2_4,tagkey1=tagvalue1_4,tagkey3=tagvalue3_4,tagkey4=ta\ valu\,e\=4_4 fie\ ld4_\,fl\=oat=8.4,field1_string="test-test-test-test-3-4",field2_int=8i,field3_bool=false 1685318050526624009
cpu,tag\ ke\,y\=2=tagvalue2_8,tagkey1=tagvalue1_8,tagkey3=tagvalue3_8,tagkey4=ta\ valu\,e\=4_8 fie\ ld4_\,fl\=oat=8.799999999999999,field1_string="test-test-test-test-7",field2_int=8i,field3_bool=false 1685278962062614208
cpu,tag\ ke\,y\=2=tagvalue2_8,tagkey1=tagvalue1_8,tagkey3=tagvalue3_8,tagkey4=ta\ valu\,e\=4_8 fie\ ld4_\,fl\=oat=9.799999999999999,field1_string="test-test-test-test-7-1",field2_int=9i,field3_bool=true 1685278963062614208
cpu,tag\ ke\,y\=2=tagvalue2_8,tagkey1=tagvalue1_8,tagkey3=tagvalue3_8,tagkey4=ta\ valu\,e\=4_8 fie\ ld4_\,fl\=oat=10.799999999999999,field1_string="test-test-test-test-7-2",field2_int=10i,field3_bool=false 1685278964062614208
cpu,tag\ ke\,y\=2=tagvalue2_8,tagkey1=tagvalue1_8,tagkey3=tagvalue3_8,tagkey4=ta\ valu\,e\=4_8 fie\ ld4_\,fl\=oat=11.799999999999999,field1_string="test-test-test-test-7-3",field2_int=11i,field3_bool=true 1685278965062614208
cpu,tag\ ke\,y\=2=tagvalue2_8,tagkey1=tagvalue1_8,tagkey3=tagvalue3_8,tagkey4=ta\ valu\,e\=4_8 fie\ ld4_\,fl\=oat=12.799999999999999,field1_string="test-test-test-test-7-4",field2_int=12i,field3_bool=false 1685278966062614208
cpu,tag\ ke\,y\=2=tagvalue2_3,tagkey1=tagvalue1_3,tagkey3=tagvalue3_3,tagkey4=ta\ valu\,e\=4_3 fie\ ld4_\,fl\=oat=3.3000000000000003,field1_string="test-test-test-test-2",field2_int=3i,field3_bool=true 1685266998101878760
cpu,tag\ ke\,y\=2=tagvalue2_3,tagkey1=tagvalue1_3,tagkey3=tagvalue3_3,tagkey4=ta\ valu\,e\=4_3 fie\ ld4_\,fl\=oat=4.300000000000001,field1_string="test-test-test-test-2-1",field2_int=4i,field3_bool=true 1685266999101878760
cpu,tag\ ke\,y\=2=tagvalue2_3,tagkey1=tagvalue1_3,tagkey3=tagvalue3_3,tagkey4=ta\ valu\,e\=4_3 fie\ ld4_\,fl\=oat=5.300000000000001,field1_string="test-test-test-test-2-2",field2_int=5i,field3_bool=false 1685267000101878760
cpu,tag\ ke\,y\=2=tagvalue2_3,tagkey1=tagvalue1_3,tagkey3=tagvalue3_3,tagkey4=ta\ valu\,e\=4_3 fie\ ld4_\,fl\=oat=6.300000000000001,field1_string="test-test-test-test-2-3",field2_int=6i,field3_bool=true 1685267001101878760
cpu,tag\ ke\,y\=2=tagvalue2_3,tagkey1=tagvalue1_3,tagkey3=tagvalue3_3,tagkey4=ta\ valu\,e\=4_3 fie\ ld4_\,fl\=oat=7.300000000000001,field1_string="test-test-test-test-2-4",field2_int=7i,field3_bool=false 1685267002101878760
cpu,tag\ ke\,y\=2=tagvalue2_2,tagkey1=tagvalue1_2,tagkey3=tagvalue3_2,tagkey4=ta\ valu\,e\=4_2 fie\ ld4_\,fl\=oat=2.2,field1_string="test-test-test-test-1",field2_int=2i,field3_bool=false 1685306996543856411
cpu,tag\ ke\,y\=2=tagvalue2_2,tagkey1=tagvalue1_2,tagkey3=tagvalue3_2,tagkey4=ta\ valu\,e\=4_2 fie\ ld4_\,fl\=oat=3.2,field1_string="test-test-test-test-1-1",field2_int=3i,field3_bool=true 1685306997543856411
cpu,tag\ ke\,y\=2=tagvalue2_2,tagkey1=tagvalue1_2,tagkey3=tagvalue3_2,tagkey4=ta\ valu\,e\=4_2 fie\ ld4_\,fl\=oat=4.2,field1_string="test-test-test-test-1-2",field2_int=4i,field3_bool=false 1685306998543856411
cpu,tag\ ke\,y\=2=tagvalue2_2,tagkey1=tagvalue1_2,tagkey3=tagvalue3_2,tagkey4=ta\ valu\,e\=4_2 fie\ ld4_\,fl\=oat=5.2,field1_string="test-test-test-test-1-3",field2_int=5i,field3_bool=true 1685306999543856411
cpu,tag\ ke\,y\=2=tagvalue2_2,tagkey1=tagvalue1_2,tagkey3=tagvalue3_2,tagkey4=ta\ valu\,e\=4_2 fie\ ld4_\,fl\=oat=6.2,field1_string="test-test-test-test-1-4",field2_int=6i,field3_bool=false 1685307000543856411
cpu,tag\ ke\,y\=2=tagvalue2_6,tagkey1=tagvalue1_6,tagkey3=tagvalue3_6,tagkey4=ta\ valu\,e\=4_6 fie\ ld4_\,fl\=oat=6.6,field1_string="test-test-test-test-5",field2_int=6i,field3_bool=false 1685303827214237261
cpu,tag\ ke\,y\=2=tagvalue2_6,tagkey1=tagvalue1_6,tagkey3=tagvalue3_6,tagkey4=ta\ valu\,e\=4_6 fie\ ld4_\,fl\=oat=7.6,field1_string="test-test-test-test-5-1",field2_int=7i,field3_bool=true 1685303828214237261
cpu,tag\ ke\,y\=2=tagvalue2_6,tagkey1=tagvalue1_6,tagkey3=tagvalue3_6,tagkey4=ta\ valu\,e\=4_6 fie\ ld4_\,fl\=oat=8.6,field1_string="test-test-test-test-5-2",field2_int=8i,field3_bool=false 1685303829214237261
cpu,tag\ ke\,y\=2=tagvalue2_6,tagkey1=tagvalue1_6,tagkey3=tagvalue3_6,tagkey4=ta\ valu\,e\=4_6 fie\ ld4_\,fl\=oat=9.6,field1_string="test-test-test-test-5-3",field2_int=9i,field3_bool=true 1685303830214237261
cpu,tag\ ke\,y\=2=tagvalue2_6,tagkey1=tagvalue1_6,tagkey3=tagvalue3_6,tagkey4=ta\ valu\,e\=4_6 fie\ ld4_\,fl\=oat=10.6,field1_string="test-test-test-test-5-4",field2_int=10i,field3_bool=false 1685303831214237261
cpu,tag\ ke\,y\=2=tagvalue2_7,tagkey1=tagvalue1_7,tagkey3=tagvalue3_7,tagkey4=ta\ valu\,e\=4_7 fie\ ld4_\,fl\=oat=7.699999999999999,field1_string="test-test-test-test-6",field2_int=7i,field3_bool=true 1685269704883513247
cpu,tag\ ke\,y\=2=tagvalue2_7,tagkey1=tagvalue1_7,tagkey3=tagvalue3_7,tagkey4=ta\ valu\,e\=4_7 fie\ ld4_\,fl\=oat=8.7,field1_string="test-test-test-test-6-1",field2_int=8i,field3_bool=true 1685269705883513247
cpu,tag\ ke\,y\=2=tagvalue2_7,tagkey1=tagvalue1_7,tagkey3=tagvalue3_7,tagkey4=ta\ valu\,e\=4_7 fie\ ld4_\,fl\=oat=9.7,field1_string="test-test-test-test-6-2",field2_int=9i,field3_bool=false 1685269706883513247
cpu,tag\ ke\,y\=2=tagvalue2_7,tagkey1=tagvalue1_7,tagkey3=tagvalue3_7,tagkey4=ta\ valu\,e\=4_7 fie\ ld4_\,fl\=oat=10.7,field1_string="test-test-test-test-6-3",field2_int=10i,field3_bool=true 1685269707883513247
cpu,tag\ ke\,y\=2=tagvalue2_7,tagkey1=tagvalue1_7,tagkey3=tagvalue3_7,tagkey4=ta\ valu\,e\=4_7 fie\ ld4_\,fl\=oat=11.7,field1_string="test-test-test-test-6-4",field2_int=11i,field3_bool=false 1685269708883513247
cpu,tag\ ke\,y\=2=tagvalue2_1,tagkey1=tagvalue1_1,tagkey3=tagvalue3_1,tagkey4=ta\ valu\,e\=4_1 fie\ ld4_\,fl\=oat=1.1,field2_int=1i 1685292814231278675
cpu,tag\ ke\,y\=2=tagvalue2_1,tagkey1=tagvalue1_1,tagkey3=tagvalue3_1,tagkey4=ta\ valu\,e\=4_1 fie\ ld4_\,fl\=oat=2.1,field2_int=2i 1685292815231278675
cpu,tag\ ke\,y\=2=tagvalue2_1,tagkey1=tagvalue1_1,tagkey3=tagvalue3_1,tagkey4=ta\ valu\,e\=4_1 fie\ ld4_\,fl\=oat=3.1,field2_int=3i 1685292816231278675
cpu,tag\ ke\,y\=2=tagvalue2_1,tagkey1=tagvalue1_1,tagkey3=tagvalue3_1,tagkey4=ta\ valu\,e\=4_1 fie\ ld4_\,fl\=oat=4.1,field2_int=4i 1685292817231278675
cpu,tag\ ke\,y\=2=tagvalue2_1,tagkey1=tagvalue1_1,tagkey3=tagvalue3_1,tagkey4=ta\ valu\,e\=4_1 fie\ ld4_\,fl\=oat=5.1,field2_int=5i 1685292818231278675
cpu,tag\ ke\,y\=2=tagvalue2_5,tagkey1=tagvalue1_5,tagkey3=tagvalue3_5,tagkey4=ta\ valu\,e\=4_5 fie\ ld4_\,fl\=oat=5.5,field1_string="test-test-test-test-4",field2_int=5i,field3_bool=true 1685309005743547657
cpu,tag\ ke\,y\=2=tagvalue2_5,tagkey1=tagvalue1_5,tagkey3=tagvalue3_5,tagkey4=ta\ valu\,e\=4_5 fie\ ld4_\,fl\=oat=6.5,field1_string="test-test-test-test-4-1",field2_int=6i,field3_bool=true 1685309006743547657
cpu,tag\ ke\,y\=2=tagvalue2_5,tagkey1=tagvalue1_5,tagkey3=tagvalue3_5,tagkey4=ta\ valu\,e\=4_5 fie\ ld4_\,fl\=oat=7.5,field1_string="test-test-test-test-4-2",field2_int=7i,field3_bool=false 1685309007743547657
cpu,tag\ ke\,y\=2=tagvalue2_5,tagkey1=tagvalue1_5,tagkey3=tagvalue3_5,tagkey4=ta\ valu\,e\=4_5 fie\ ld4_\,fl\=oat=8.5,field1_string="test-test-test-test-4-3",field2_int=8i,field3_bool=true 1685309008743547657
cpu,tag\ ke\,y\=2=tagvalue2_5,tagkey1=tagvalue1_5,tagkey3=tagvalue3_5,tagkey4=ta\ valu\,e\=4_5 fie\ ld4_\,fl\=oat=9.5,field1_string="test-test-test-test-4-4",field2_int=9i,field3_bool=false 1685309009743547657
`
