package index

import (
	"fmt"
	"sync"
	"xbitman/conf"
	bolt "xbitman/libs/bbolt"
)

type Index struct {
	mux   sync.Mutex
	Name  string   `json:"name"` // tabName_indexName
	Type  int      `json:"type"`
	Store *bolt.DB `json:"-"`
}

func NewIndex(store *bolt.DB, name string, iType int) (idx *Index) {
	idx = &Index{
		mux:   sync.Mutex{},
		Name:  name,
		Type:  iType,
		Store: store,
	}
	fmt.Println("index init", name)
	idx.Store.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name), idx.Number())
		fmt.Println("index init store", name)
		return err
	})
	return idx
}

func (idx *Index) Number() (ok bool) {
	switch idx.Type {
	case conf.TypeInt,
		conf.TypeFloat:
		return true
	}
	return false
}

func (idx *Index) IType() (i int) {
	return idx.Type
}

func (idx *Index) Remove(key []byte, uKey uint32) (err error) {
	idx.mux.Lock()
	defer idx.mux.Unlock()
	return idx.Store.Update(func(tx *bolt.Tx) error {
		return idx._remove(tx, key, uKey)
	})
}

// Dump 删除索引 todo
func (idx *Index) Dump() (err error) {
	return idx.Store.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(idx.Name))
	})
}
