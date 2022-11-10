package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

func GetLinesFromFile(filename string) []string {
	var res = make([]string, 0)
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()
	buff := bufio.NewReader(file)
	for {
		data, _, eof := buff.ReadLine()
		if eof == io.EOF {
			break
		}
		str := (string)(data)
		res = append(res, str)
	}
	return res

}
