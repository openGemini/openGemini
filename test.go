package main

// go run test.go
// 执行两次会panic
import (
	"context"
	"io"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/flight"
	"github.com/apache/arrow/go/arrow/ipc"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/openGemini/openGemini/lib/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// db: db1
// create measurement mst1 (age int field default 0, height float field default 0, address string field default "", alive boolean field default false, ) with enginetype = columnstore shardkey country primarykey time sortkey time

func MockArrowRecord(size int) array.Record {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "age", Type: arrow.PrimitiveTypes.Int64},
			{Name: "height", Type: arrow.PrimitiveTypes.Float64},
			{Name: "address", Type: &arrow.StringType{}},
			{Name: "alive", Type: &arrow.BooleanType{}},
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for i := 0; i < size; i++ {
		b.Field(0).(*array.Int64Builder).AppendValues([]int64{12, 20, 3, 30}, nil)
		b.Field(1).(*array.Float64Builder).AppendValues([]float64{70.0, 80.0, 90.0, 121.0}, nil)
		b.Field(2).(*array.StringBuilder).AppendValues([]string{"shenzhen", "shanghai", "beijin", "guangzhou"}, nil)
		b.Field(3).(*array.BooleanBuilder).AppendValues([]bool{true, false, true, false}, nil)
		b.Field(4).(*array.Int64Builder).AppendValues([]int64{1629129600000000000, 1629129601000000000, 1629129602000000000, 1629129603000000000}, nil)
	}
	return b.NewRecord()
}

var Token = "token"

type clientAuth struct {
	token string
}

func (a *clientAuth) Authenticate(ctx context.Context, c flight.AuthConn) error {
	if err := c.Send(ctx.Value(Token).([]byte)); err != nil {
		return err
	}

	token, err := c.Read()
	a.token = util.Bytes2str(token)
	return err
}

func (a *clientAuth) GetToken(_ context.Context) (string, error) {
	return a.token, nil
}

func main() {
	FlightAddress := "127.0.0.1:8087"
	conn, err := grpc.Dial(FlightAddress, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = conn.Close(); err != nil {
			panic("Flight Close failed" + err.Error())
		}
	}()

	authClient := &clientAuth{}
	client, err := flight.NewFlightClient(FlightAddress, authClient, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Close(); err != nil {
			panic("Flight Client Close failed")
		}
	}()

	doPutClient, err := client.DoPut(context.Background())
	if err != nil {
		panic("Flight Client DoPut failed" + err.Error())
	}

	data := MockArrowRecord(5)
	wr := flight.NewRecordWriter(doPutClient, ipc.WithSchema(data.Schema()))
	wr.SetFlightDescriptor(&flight.FlightDescriptor{Path: []string{"{\"db\": \"db1\", \"rp\": \"autogen\", \"mst\": \"mst1\"}"}})

	for i := 0; i < 3; i++ {
		if err = wr.Write(data); err != nil {
			panic("RecordWriter Write failed" + err.Error())
		}
	}

	if err = wr.Close(); err != nil {
		panic("RecordWriter Close failed" + err.Error())
	}

	// wait for the server to ack the result
	if _, err = doPutClient.Recv(); err != nil && err != io.EOF {
		panic("doPutClient Recv failed" + err.Error())
	}

	if err = doPutClient.CloseSend(); err != nil {
		panic("doPutClient CloseSend failed" + err.Error())
	}
}
