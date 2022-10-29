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
	"fmt"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(executeCmd)
}

var executeCmd = &cobra.Command{
	Use:   "execute",
	Short: "Execute a query",
	Long:  `Execute a query`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := connectCLI(); err != nil {
			return err
		}
		if len(args) > 1 {
			return fmt.Errorf("only one query can be specified")
		} else if len(args) == 0 {
			return fmt.Errorf("empty query is not allowed")
		} else {
			return cli.Execute(args[0])
		}
	},
}
