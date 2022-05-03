package main

import (
	"bufio"
	dmi "distributed-metadata-index/pkg"
	"flag"
	"fmt"
	"github.com/abiosoft/ishell"
	"os"
	"strings"
	"time"
)

type Client struct {
	ZookeeperClient *dmi.ZkClient
}

func main() {
	var file string

	flag.StringVar(&file, "parse", "", "To parse a txt file.")
	flag.StringVar(&file, "p", "", "To parse a txt file. (shorthand)")

	flag.Parse()

	switch {
	case file == "":
		dmi.Out.Println("Can't start a node with null file")
		return
	}

	client := Start(file)

	CLI(client)
}

func CLI(client *dmi.ZkClient) {
	shell := ishell.New()

	shell.AddCmd(&ishell.Cmd{
		Name: "help",
		Func: func(c *ishell.Context) {
			printHelp(shell)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "h",
		Func: func(c *ishell.Context) {
			printHelp(shell)
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "s",
		Func: func(c *ishell.Context) {
			timeBefore := time.Now()

			if len(c.Args) != 1 {
				c.Println("syntax error (usage: s	[regex])")
				return
			}
			regex := c.Args[0]
			tmp := strings.Split(regex, "=")
			tagKey := tmp[0]
			tagValue := tmp[1]
			results, err := client.SearchTagName(tagKey)
			if err != nil {
				fmt.Errorf("error while SearchTagName, err: %v\n", err)
			}

			fmt.Printf("%-18s %-18s %-38s\n", "tagName", "tagValue", "nodeLists")
			fmt.Printf("%-18s %-18s %-38s\n", "-------", "--------", "---------")

			for _, v := range results {
				treeb, err := dmi.GetIndex(v)
				if err != nil {
					fmt.Errorf(err.Error())
				}
				// convert bytes to TagValueIndex
				treed := dmi.DecodeBytesToTagValueIndex(treeb)

				data, err := treed.FindAllMatchedNodes(tagValue)

				for _, nodePair := range data {
					fmt.Printf("%-18s %-18s %-8v\n", v, nodePair.GetStr(), nodePair.GetNodeList())
				}
			}

			fmt.Printf("This search uses time: %d milliseconds\n", time.Now().Sub(timeBefore).Milliseconds())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "search",
		Func: func(c *ishell.Context) {
			timeBefore := time.Now()

			if len(c.Args) != 1 {
				c.Println("syntax error (usage: s	[regex])")
				return
			}
			regex := c.Args[0]
			tmp := strings.Split(regex, "=")
			tagKey := tmp[0]
			tagValue := tmp[1]
			results, err := client.SearchTagName(tagKey)
			if err != nil {
				fmt.Errorf("error while SearchTagName, err: %v\n", err)
			}

			fmt.Printf("%-18s %-18s %-38s\n", "tagName", "tagValue", "nodeLists")
			fmt.Printf("%-18s %-18s %-38s\n", "-------", "--------", "---------")

			for _, v := range results {
				treeb, err := dmi.GetIndex(v)
				if err != nil {
					fmt.Errorf(err.Error())
				}
				// convert bytes to TagValueIndex
				treed := dmi.DecodeBytesToTagValueIndex(treeb)

				data, err := treed.FindAllMatchedNodes(tagValue)

				for _, nodePair := range data {
					fmt.Printf("%-18s %-18s %-8v\n", v, nodePair.GetStr(), nodePair.GetNodeList())
				}
			}

			fmt.Printf("This search uses time: %d milliseconds\n", time.Now().Sub(timeBefore).Milliseconds())
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "q",
		Func: func(c *ishell.Context) {
			dmi.DeleteAll()
			shell.Close()
		},
	})

	shell.AddCmd(&ishell.Cmd{
		Name: "quit",
		Func: func(c *ishell.Context) {
			dmi.DeleteAll()
			shell.Close()
		},
	})

	shell.Run()
}

func Start(file string) *dmi.ZkClient {
	dmi.DeleteAll()

	client, _ := dmi.CreateZkClient()
	readFile, err := os.Open(file)

	check(err)
	fileScanner := bufio.NewScanner(readFile)

	fileScanner.Split(bufio.ScanLines)

	m := make(map[string]*dmi.TagValueIndex)
	node := 0
	for fileScanner.Scan() {
		s := fileScanner.Text()
		tags := strings.Split(s, ",")
		for _, tag := range tags {
			tmp := strings.Split(tag, "=")
			tagKey := tmp[0]
			tagValue := tmp[1]
			// fmt.Println(tagKey, tagValue)
			if _, ok := m[tagKey]; ok == false {
				client.AddTagName(tagKey)
				m[tagKey] = dmi.NewTagValueIndex()
			}
			m[tagKey].AddTagValue(tagValue, uint32(node))
		}
		node++
	}

	for tagKey, tree := range m {
		// convert TagValueIndex to bytes
		treeb := dmi.EncodeTagValueIndexToBytes(tree)
		err := dmi.PutIndex(tagKey, treeb)
		if err != nil {
			fmt.Errorf(err.Error())
		}
	}

	readFile.Close()
	return client
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func printHelp(shell *ishell.Shell) {
	shell.Println("Commands:")
	shell.Println("s <regex>                       - return search answer")
	shell.Println("search <regex>                  - return search answer")
	shell.Println("q, quit                         - quit the program")
	shell.Println("h, help                         - print out help")
}
