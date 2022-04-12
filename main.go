package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	filePath := os.Args[1]
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379", "localhost:22379", "localhost:32379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		// handle error!
		panic(err)
	}
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	/*
		_, err = cli.Put(ctx, "sample_key", "sample_value")
		if err != nil {
			// handle error!
			panic(err)
		}
		// use the response
		resp, err := cli.Get(ctx, "sample_key")
		if err != nil {
			// handle error!
			panic(err)
		}
		fmt.Printf("%s: %s", resp.Kvs[0].Key, resp.Kvs[0].Value)
	*/
	defer cancel()
	indexes := read(filePath)
	for tag, nodes := range indexes {
		nodeStr := strings.Join(nodes, ",")
		_, err = cli.Put(ctx, tag, nodeStr)
		if err != nil {
			// handle error!
			panic(err)
		}
	}

	repl(cli, ctx)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func read(filePath string) map[string][]string {
	readFile, err := os.Open(filePath)

	check(err)
	fileScanner := bufio.NewScanner(readFile)

	fileScanner.Split(bufio.ScanLines)

	res := make(map[string][]string)
	node := 0
	for fileScanner.Scan() {
		// fmt.Println(fileScanner.Text())
		s := fileScanner.Text()
		tags := strings.Split(s, ",")
		for _, tag := range tags {
			_, ok := res[tag]
			if ok {
				res[tag] = append(res[tag], strconv.Itoa(node))
			} else {
				res[tag] = []string{strconv.Itoa(node)}
			}
		}
		node++
	}

	readFile.Close()
	return res
}

func repl(cli *clientv3.Client, ctx context.Context) {
	for {
		var query string
		fmt.Printf("Enter your query (q to exit): ")
		fmt.Scanf("%s", &query)
		if query == "q" {
			return
		}
		r := regexp.MustCompile(query)
		resp, err := cli.Get(ctx, "\a", clientv3.WithRange("zzzzzzzzzzzzz"))
		check(err)
		for _, ev := range resp.Kvs {
			if r.MatchString(string(ev.Key)) {
				fmt.Printf("%s: %s\n", ev.Key, ev.Value)
			}
		}
	}
}
