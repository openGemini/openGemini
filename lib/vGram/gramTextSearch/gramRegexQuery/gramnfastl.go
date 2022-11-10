package gramRegexQuery

type GnfaStack struct {
	list []*GnfaNode
}

func InitializeGnfaStack() *GnfaStack {
	return &GnfaStack{
		list: make([]*GnfaNode, 0),
	}
}

func (s *GnfaStack) Push(ele *GnfaNode) {
	s.list = append(s.list, ele)

}

func (s *GnfaStack) Pop() *GnfaNode {
	if len(s.list) < 0 {
		return nil
	} else {
		result := s.list[len(s.list)-1]
		s.list = s.list[:len(s.list)-1]
		return result
	}
}

func (s *GnfaStack) Top() *GnfaNode {
	if len(s.list) < 0 {
		return nil
	} else {
		return s.list[len(s.list)-1]
	}
}
