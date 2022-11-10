package gramClvc

type TrieTree struct {
	qmin int
	qmax int
	root *TrieTreeNode
}

func (tree *TrieTree) Qmin() int {
	return tree.qmin
}

func (tree *TrieTree) SetQmin(qmin int) {
	tree.qmin = qmin
}

func (tree *TrieTree) Qmax() int {
	return tree.qmax
}

func (tree *TrieTree) SetQmax(qmax int) {
	tree.qmax = qmax
}

func (tree *TrieTree) Root() *TrieTreeNode {
	return tree.root
}

func (tree *TrieTree) SetRoot(root *TrieTreeNode) {
	tree.root = root
}

func NewTrieTree(qmin int, qmax int) *TrieTree {
	return &TrieTree{
		qmin: qmin,
		qmax: qmax,
		root: NewTrieTreeNode(""),
	}
}

// Insert gram on TrieTree
func (tree *TrieTree) InsertIntoTrieTree(gram string) {
	node := tree.root
	qmin := tree.qmin
	var childIndex int8 = -1 // The position of the child node in the ChildrenMap
	for i := 0; i < len(gram); i++ {
		childIndex = GetNode(node.children, gram[i])
		if childIndex == -1 { // There is no such node in the ChildrenMap
			currentNode := NewTrieTreeNode(string(gram[i]))
			node.children[gram[i]] = currentNode
			node = currentNode
		} else { //There is this node in the ChildrenMap, so childrenIndex is the position of the node in the ChildrenMap
			node = node.children[uint8(childIndex)]
			node.frequency++
		}
		if i >= qmin-1 { //As long as the gram length is greater than qmin - 1, it is a leaf node
			node.isleaf = true
		}
	}
}

//Pruning TrieTree
func (tree *TrieTree) PruneTree(T int) {
	tree.root.PruneNode(T)
}

func (tree *TrieTree) PrintTree() {
	tree.root.PrintTreeNode(0)
}

//Update the root node
func (tree *TrieTree) UpdateRootFrequency() {
	for _, child := range tree.root.children {
		tree.root.frequency += child.frequency
	}
	tree.root.frequency--
}

func (tree *TrieTree) SearchGramFromDicTree(gram string) bool {
	// change to array
	node := tree.root
	for i := 0; i < len(gram); i++ {
		char := gram[i]
		cnode, isfind := node.children[char]
		if isfind {
			node = cnode
		} else {
			return false
		}
	}
	// node now point to the last treenode which represent the gram
	return node.isleaf
}
