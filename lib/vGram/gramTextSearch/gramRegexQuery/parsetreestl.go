package gramRegexQuery


type ParseTreeStack struct {
	list []*ParseTreeNode
}

func InitializeParseTreeStack() *ParseTreeStack {
	return &ParseTreeStack{
		list: make([]*ParseTreeNode, 0),
	}
}

func (s *ParseTreeStack) Push(ele *ParseTreeNode) {
	s.list = append(s.list, ele)

}

func (s *ParseTreeStack) Pop() *ParseTreeNode {
	if len(s.list) < 0 {
		return nil
	} else {
		result := s.list[len(s.list)-1]
		s.list = s.list[:len(s.list)-1]
		return result
	}
}

func (s *ParseTreeStack) Top() *ParseTreeNode {
	if len(s.list) < 0 {
		return nil
	} else {
		return s.list[len(s.list)-1]
	}
}
