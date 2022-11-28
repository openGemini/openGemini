package mpTrie

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/vGram/gramIndex"
	"os"
)

func SerializeLogTreeToFile(tree *gramIndex.LogTree, filename string) {
	res := ""
	var dfs func(node *gramIndex.LogTreeNode, path []string)
	dfs = func(node *gramIndex.LogTreeNode, path []string) {
		if len(node.Children()) == 0 {
			temp := ""
			for _, s := range path {
				temp += s
			}
			//special char split for flag
			res += temp + SPLITFLAG
		}
		for _, child := range node.Children() {
			path = append(path, child.Data())
			dfs(child, path)
			path = path[:len(path)-1]
		}
	}
	root := tree.Root()
	path := make([]string, 0)
	dfs(root, path)
	//write string to file
	dicfile, err := os.Create(filename)
	defer dicfile.Close()
	if err != nil {
		fmt.Println("file open fail when logtree serialize.", err)
		return
	}
	_, err = dicfile.WriteString(res)
	if err != nil {
		fmt.Println("file write fail when logtree serialize", err)
		return
	}
}
