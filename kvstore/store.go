package kvstore

import (
	"xbitman/conf"
	//"xbitman/conf"
	"xbitman/kvstore/badger"
	"xbitman/kvstore/kv"
)

var KV kv.Store

func Init() {
	KV = newStore()
}

// NewStore .
func newStore() (s kv.Store) {
	c := map[string]interface{}{
		"path": conf.G.Path + "/KV",
	}
	//conf["path"] = config.G.Path + "/" + name + ".DB"
	s, err := badger.New(c)
	if err != nil {
		panic(err)
	}
	return s
}

func Close() error {
	return KV.Close()
}

// KVReader .
func KVReader() (kv.Reader, error) {
	return KV.Reader()
}

// KVWriter .
func KVWriter() (kv.Writer, error) {
	return KV.Writer()
}
