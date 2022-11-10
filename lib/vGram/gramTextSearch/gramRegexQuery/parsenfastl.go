package gramRegexQuery

type SubPathStack struct {
	list []*SubPath
}

func InitializeSubPathStack() *SubPathStack {
	return &SubPathStack{
		list: make([]*SubPath, 0),
	}
}

func (s *SubPathStack) Push(ele *SubPath) {
	s.list = append(s.list, ele)

}

func (s *SubPathStack) Pop() *SubPath {
	if len(s.list) < 0 {
		return nil
	} else {
		result := s.list[len(s.list)-1]
		s.list = s.list[:len(s.list)-1]
		return result
	}
}

func (s *SubPathStack) Top() *SubPath {
	if len(s.list) < 0 {
		return nil
	} else {
		return s.list[len(s.list)-1]
	}
}
