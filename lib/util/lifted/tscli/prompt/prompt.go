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

package prompt

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"

	"github.com/openGemini/go-prompt"
	"github.com/openGemini/openGemini-cli/common"
)

type Prompt struct {
	completer *Completer
	instance  *prompt.Prompt
}

func NewPrompt(executor prompt.Executor) *Prompt {
	var completer = NewCompleter()
	var p = &Prompt{completer: completer}
	var instance = prompt.New(
		executor,
		completer.completer,
		prompt.OptionTitle("openGemini: interactive openGemini client"),
		prompt.OptionPrefix("> "),
		prompt.OptionPrefixTextColor(prompt.DefaultColor),
		prompt.OptionCompletionWordSeparator(string([]byte{' ', os.PathSeparator})),
		prompt.OptionAddASCIICodeBind(
			prompt.ASCIICodeBind{
				ASCIICode: []byte{0x1b, 0x62},
				Fn:        prompt.GoLeftWord,
			},
			prompt.ASCIICodeBind{
				ASCIICode: []byte{0x1b, 0x66},
				Fn:        prompt.GoRightWord,
			},
		),
		prompt.OptionAddKeyBind(
			prompt.KeyBind{
				Key: prompt.ShiftLeft,
				Fn:  prompt.GoLeftWord,
			},
			prompt.KeyBind{
				Key: prompt.ShiftRight,
				Fn:  prompt.GoRightWord,
			},
			prompt.KeyBind{
				Key: prompt.ControlC,
				Fn:  p.Destruction,
			},
		),
	)
	p.instance = instance
	return p
}

func (p *Prompt) Run() {
	fmt.Printf("openGemini CLI %s (rev-%s)\n", common.Version, common.GitCommit)
	fmt.Println("Please use `quit`, `\\q`, `exit`, `Ctrl-C` or `Ctrl-D` to exit this program.")
	defer p.Destruction(nil)
	p.instance.Run()
}

func (p *Prompt) Destruction(_ *prompt.Buffer) {
	if runtime.GOOS != "windows" {
		reset := exec.Command("stty", "-raw", "echo")
		reset.Stdin = os.Stdin
		err := reset.Run()
		if err != nil {
			fmt.Errorf("%w", err)
		}
	}
	os.Exit(0)
}

func (p *Prompt) SwitchCompleter(s bool) {
	p.completer.switchCompleter(s)
}
