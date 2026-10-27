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

package core

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/openGemini/opengemini-client-go/opengemini"
	"github.com/valyala/fastjson/fastfloat"
)

// LineProtocolState define line protocol parser fsm state
type LineProtocolState int

const (
	Measurement LineProtocolState = iota
	TagKey
	TagValue
	FieldKey
	FieldValue
	Timestamp
)

type LineProtocolParser struct {
	raw          io.Reader
	points       []*opengemini.Point
	currentPoint *opengemini.Point
	currentState LineProtocolState
	currentKey   string
	currentValue string
	currentTime  string
	escape       bool
	quota        bool
	bracket      bool
}

func NewLineProtocolParser(raw string) *LineProtocolParser {
	return &LineProtocolParser{raw: strings.NewReader(raw)}
}

func (p *LineProtocolParser) Parse(timeMultiplier int64) ([]*opengemini.Point, error) {
	scanner := bufio.NewScanner(p.raw)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lp, err := p.parse(line, timeMultiplier)
		if err != nil {
			return nil, err
		}
		if lp == nil {
			continue
		}
		p.points = append(p.points, lp)
	}
	return p.points, nil
}

// parse "average_temperature,location=coyote_creek degrees=74 1567623456"
func (p *LineProtocolParser) parse(line string, timeMultiplier int64) (*opengemini.Point, error) {
	// ignore comment
	if line[0] == '#' {
		return nil, nil
	}
	p.currentState = Measurement
	p.currentPoint = &opengemini.Point{Tags: make(map[string]string), Fields: make(map[string]interface{})}

	// parse timestamp
	fds := strings.Split(line, " ")
	if len(fds) >= 3 {
		tsp := strings.TrimSpace(fds[len(fds)-1])
		if checkIsDigit(tsp) {
			p.currentTime = tsp
		}
	}

	for _, token := range line {
		switch token {
		case '\\':
			p.escape = true
		case '=':
			if p.escape {
				p.appendToken(token)
				p.escape = false
				continue
			}
			if p.currentState == TagKey {
				p.currentState = TagValue
				continue
			}
			if p.currentState == FieldKey {
				p.currentState = FieldValue
				continue
			}
		case '"':
			if p.escape {
				p.appendToken(token)
				p.escape = false
				continue
			}
			if p.quota {
				p.quota = false
				continue
			}
			p.quota = true
		case ',':
			if p.escape {
				p.appendToken(token)
				p.escape = false
				continue
			}
			switch p.currentState {
			case Measurement:
				p.currentState = TagKey
				continue
			case TagValue:
				if p.bracket {
					p.appendToken(token)
					continue
				}
				p.currentState = TagKey
				p.appendTagOrField()
				continue
			case FieldValue:
				p.currentState = FieldKey
				p.appendTagOrField()
				continue
			}
		case ' ':
			if p.escape || p.quota {
				p.appendToken(token)
				p.escape = false
				continue
			}
			if p.currentKey != "" {
				p.appendTagOrField()
			}
			if p.currentState == Timestamp {
				break
			}
			switch p.currentState {
			case Measurement, TagKey, TagValue:
				p.currentState = FieldKey
			case FieldKey, FieldValue:
				p.currentState = Timestamp
			}
		case '[':
			if p.escape || p.quota {
				p.appendToken(token)
				p.escape = false
				continue
			}
			if p.currentState != TagValue {
				return nil, errors.New("invalid tag value token: '['")
			}
			p.bracket = true
			p.appendToken(token)
		case ']':
			if p.escape || p.quota {
				p.appendToken(token)
				p.escape = false
				continue
			}
			if p.currentState != TagValue {
				return nil, errors.New("invalid tag value token: ']'")
			}
			p.bracket = false
			p.appendToken(token)
		default:
			p.appendToken(token)
		}
	}

	if p.currentKey != "" {
		p.appendTagOrField()
	}

	if p.currentTime == "" {
		p.currentPoint.Timestamp = time.Now().UnixNano()
	} else {
		p.currentPoint.Timestamp = int64(fastfloat.ParseBestEffort(p.currentTime)) * timeMultiplier
	}
	p.currentTime = ""
	if len(p.currentPoint.Fields) == 0 {
		return nil, errors.New("no fields input")
	}

	return p.currentPoint, nil
}

func (p *LineProtocolParser) appendToken(token rune) {
	word := string(token)
	switch p.currentState {
	case Measurement:
		p.currentPoint.Measurement += word
	case TagKey, FieldKey:
		p.currentKey += word
	case TagValue, FieldValue:
		p.currentValue += word
	default:

	}
}

func (p *LineProtocolParser) appendTagOrField() {
	switch p.currentState {
	case TagKey, TagValue:
		p.currentPoint.Tags[p.currentKey] = p.currentValue
	case FieldKey, FieldValue:
		p.currentPoint.Fields[p.currentKey] = p.currentValue
	default:

	}
	p.currentKey = ""
	p.currentValue = ""
}

func checkIsDigit(s string) bool {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
