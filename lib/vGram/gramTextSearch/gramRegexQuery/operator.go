package gramRegexQuery

func Replacei(old string, pos int, new uint8) string {
	return old[:pos] + string(new) + old[pos+1:]
}

func CanMerge(start []uint16, before []uint16, after []uint16) (bool, []uint16, []uint16) {
	newafter := make([]uint16, 0)
	newstart := make([]uint16, 0)
	for i := 0; i < len(after); i++ {
		a := after[i]
		for j := 0; j < len(before); j++ {
			if before[j] == a-1 {
				newstart = append(newstart, start[j])
				newafter = append(newafter, after[i])
				break
			}
		}
	}
	if len(newafter) > 0 {
		return true, newstart, newafter
	}
	return false, newstart, newafter
}

func AddToInOrderList(list []uint16, num uint16) []uint16 {
	index := 0
	isin := false
	for index < len(list) && num >= list[index] {
		if num == list[index] {
			isin = true
			break
		}
		index++
	}
	if !isin {
		list = append(list, 0)
		copy(list[index+1:], list[index:])
		list[index] = num
	}
	return list
}
