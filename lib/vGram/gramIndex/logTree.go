package gramIndex

type LogTree struct {
	qmax int
	root *LogTreeNode
}

func (tree *LogTree) Qmax() int {
	return tree.qmax
}

func (tree *LogTree) SetQmax(qmax int) {
	tree.qmax = qmax
}

func (tree *LogTree) Root() *LogTreeNode {
	return tree.root
}

func (tree *LogTree) SetRoot(root *LogTreeNode) {
	tree.root = root
}

func NewLogTree(qmax int) *LogTree {
	return &LogTree{
		qmax: qmax,
		root: NewLogTreeNode(""),
	}
}

func (tree *LogTree) InsertIntoTrieTreeLogTree(gram string) {
	node := tree.root
	var childIndex int8 = -1
	for i := 0; i < len(gram); i++ {
		childIndex = GetNode(node.children, gram[i])
		if childIndex == -1 {
			currentNode := NewLogTreeNode(string(gram[i]))
			node.children[gram[i]] = currentNode
			node = currentNode
		} else {
			node = node.children[uint8(childIndex)]
		}
	}
}

func (tree *LogTree) PrintTree() {
	tree.root.PrintTreeNode(0)
}
