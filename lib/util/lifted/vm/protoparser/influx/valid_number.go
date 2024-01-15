package influx

/*
 * Deterministic finite automaton
 * see: https://leetcode-cn.com/problems/valid-number/
 */

type State int
type CharType int

const (
	StateNone State = iota
	StateInitial
	StateIntSign
	StateInteger
	StatePoint
	StatePointWithoutInt
	StateFraction
	StateExp
	StateExpSign
	StateExpNumber
	StateEnd
)

const (
	CharNumber CharType = iota
	CharExp
	CharPoint
	CharSign
	CharIllegal
)

func toCharType(ch byte) CharType {
	switch ch {
	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return CharNumber
	case 'e', 'E':
		return CharExp
	case '.':
		return CharPoint
	case '+', '-':
		return CharSign
	default:
		return CharIllegal
	}
}

var transfer [StateEnd][CharIllegal]State

func init() {
	transfer[StateInitial] = [CharIllegal]State{StateInteger, StateNone, StatePointWithoutInt, StateIntSign}
	transfer[StateIntSign] = [CharIllegal]State{StateInteger, StateNone, StatePointWithoutInt, StateNone}
	transfer[StateInteger] = [CharIllegal]State{StateInteger, StateExp, StatePoint, StateNone}
	transfer[StatePoint] = [CharIllegal]State{StateFraction, StateExp, StateNone, StateNone}
	transfer[StatePointWithoutInt] = [CharIllegal]State{StateFraction, StateNone, StateNone, StateNone}
	transfer[StateFraction] = [CharIllegal]State{StateFraction, StateExp, StateNone, StateNone}
	transfer[StateExp] = [CharIllegal]State{StateExpNumber, StateNone, StateNone, StateExpSign}
	transfer[StateExpSign] = [CharIllegal]State{StateExpNumber, StateNone, StateNone, StateNone}
	transfer[StateExpNumber] = [CharIllegal]State{StateExpNumber, StateNone, StateNone, StateNone}
}

func IsValidNumber(s string) bool {
	state := StateInitial
	for i := 0; i < len(s); i++ {
		typ := toCharType(s[i])
		if typ >= CharIllegal {
			return false
		}

		state = transfer[state][typ]
		if state == StateNone {
			return false
		}
	}

	return state == StateInteger || state == StatePoint || state == StateFraction || state == StateExpNumber
}
