package encode

import (
	"fmt"
	"github.com/openGemini/openGemini/lib/vGram/gramDic/gramClvc"
	"github.com/openGemini/openGemini/lib/vToken/tokenDic/tokenClvc"
	"os"
	"strings"
)

const SPLITFLAG = "$"

func SerializeGramDicToFile(tree *gramClvc.TrieTree, filename string) {
	res := ""
	var dfs func(node *gramClvc.TrieTreeNode, path []string)
	dfs = func(node *gramClvc.TrieTreeNode, path []string) {
		if node.Isleaf() == true {
			temp := ""
			for _, s := range path {
				temp += s
			}
			//special char split for flag
			res += temp + SPLITFLAG
		}
		if len(node.Children()) == 0 {
			return
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
		fmt.Println("file open fail when dic serialize.", err)
		return
	}
	_, err = dicfile.WriteString(res)
	if err != nil {
		fmt.Println("file write fail when dic serialize", err)
		return
	}
}

func SerializeTokenDicToFile(tree *tokenClvc.TrieTree, filename string) {
	res := ""
	var dfs func(node *tokenClvc.TrieTreeNode, path []string)
	dfs = func(node *tokenClvc.TrieTreeNode, path []string) {
		if node.Isleaf() == true {
			var temp string
			for _, s := range path {
				temp += s + " "
			}
			temp = strings.TrimSpace(temp)
			//special char split for flag
			res += temp + SPLITFLAG
		}
		if len(node.Children()) == 0 {
			return
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
		fmt.Println("file open fail when dic serialize.", err)
		return
	}
	_, err = dicfile.WriteString(res)
	if err != nil {
		fmt.Println("file write fail when dic serialize", err)
		return
	}
}
