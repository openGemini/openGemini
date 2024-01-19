package influxql

// If you use the following command, you will see some variable names changed by using 'git status' and 'git diff'.
// It is caused by proto, Don't worry. This will not affect the entire project
// More detail in here https://github.com/openGemini/openGemini/lib/util/lifted/protobuf/issues/508

//go:generate protoc --gogo_out=. internal/internal.proto
