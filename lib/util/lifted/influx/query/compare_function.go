package query

import (
	"fmt"

	"github.com/openGemini/openGemini/lib/util/lifted/influx/influxql"
)

func init() {
	RegistryCompareFunction("compare", &compareFunc{
		BaseInfo: BaseInfo{FuncType: COMPARE},
	})
}

func GetCompareFunction(name string) CompareFunc {
	compare, ok := GetFunctionFactoryInstance().FindCompareFunc(name)
	if ok && compare.GetFuncType() == COMPARE {
		return compare
	}
	return nil
}

type compareFunc struct {
	BaseInfo
}

func (s *compareFunc) CallTypeFunc(name string, args []influxql.DataType) (influxql.DataType, error) {
	return influxql.Unknown, nil
}

func (s *compareFunc) CompileFunc(expr *influxql.Call, c *compiledField) error {
	if got := len(expr.Args); got <= 1 {
		return fmt.Errorf("invalid number of arguments for %s, expected more than one arguments, got %d", expr.Name, got)
	}
	for index, arg := range expr.Args {
		if index == 0 {
			continue
		}
		if _, ok := arg.(*influxql.IntegerLiteral); !ok {
			return fmt.Errorf("invalid argument type for argument in %s(): %s", expr.Name, expr.Args[index])
		}
	}
	return compileAllStringArgs(expr, c)
}
