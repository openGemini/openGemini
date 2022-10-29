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
	"strings"

	"github.com/c-bata/go-prompt"
)

var cmds = []prompt.Suggest{
	{Text: "use", Description: "use <db>.[<rp>], specify the database and retention policy to be used"},
	{Text: "insert", Description: "insert datapoint to openGemini."},
	{Text: "select", Description: "query dataset from openGemini."},
}

var subcmds = map[string][]prompt.Suggest{
	"insert": {
		{Text: "into", Description: "into [<db>.<cp>], specify the database and retention policy to be written to."},
		{Text: "line protocol", Description: "<measurement>[,<tag_key>=<tag_value>[,<tag_key>=<tag_value>]] <field_key>=<field_value>[,<field_key>=<field_value>] [<time_stamp>]"},
	},
	"select": {
		{Text: "from", Description: "from [<db>.<rp>].<measurement>, specify the datasource of query."},
		{Text: "where", Description: "where <expression>[ <predicate> <expression>], specify the predicate expression of query."},
		{Text: "group by", Description: "group by <tag>[,<tag>] | <time>[,<time>] , specify aggregation dimentions of query."},
		{Text: "order by", Description: "order by <tag>[,<tag>], specify ordered tags of query."},
		{Text: "limit", Description: "limit <count>, limit the dataset ouput."},
	},
}

func (c *Completer) argumentsCompleter(d prompt.Document) []prompt.Suggest {
	args := strings.Split(d.TextBeforeCursor(), " ")

	if len(args) == 0 {
		return []prompt.Suggest{}
	}
	if len(args) == 1 {
		return prompt.FilterHasPrefix(cmds, args[0], true)
	}

	cmd := strings.ToLower(args[0])
	if w := d.GetWordBeforeCursor(); w != "" {
		switch cmd {
		case "insert":
			return c.insertCompleter(d, args)
		}
		return prompt.FilterHasPrefix(subcmds[cmd], d.GetWordBeforeCursor(), true)
	}

	return []prompt.Suggest{}
}

func (c *Completer) insertCompleter(d prompt.Document, args []string) []prompt.Suggest {
	cmd := "insert"
	if len(args) == 2 {
		return prompt.FilterHasPrefix(subcmds[cmd], "into", true)
	}
	if secondCmd := strings.ToLower(args[1]); secondCmd == "into" && len(args) == 4 {
		return prompt.FilterHasPrefix(subcmds[cmd], "line protocol", true)
	}

	return []prompt.Suggest{}
}
