package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"xbitman/entry"
	"xbitman/index"
	"xbitman/kvstore"
)

func init() {
	kvstore.Init()
	fmt.Println("kv init")
	index.Init()
	fmt.Println("index init")
}

func Close() {
	index.Close()
	kvstore.Close()
}

func Recover() {
	if err := recover(); err != nil {
		Close()
		fmt.Println(err)
		// log ...
		os.Exit(1)
	}
}

func main() {
	defer Close()
	go func() {
		http.ListenAndServe(":8787", nil)
	}()
	defer Recover()

	entry.Run()
}
