package badger

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"xbitman/kvstore/kv"
)

// Reader .
type Reader struct {
	iteratorOptions badger.IteratorOptions
	store           *Store
}

// Get .
func (r *Reader) Get(key []byte) ([]byte, error) {
	txn := r.store.db.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get(key)
	if err != nil {
		return nil, nil
	}
	return item.ValueCopy(nil)
}

// MultiGet .
func (r *Reader) MultiGet(keys [][]byte) (bufs [][]byte, err error) {
	bufs = make([][]byte, 0)
	txn := r.store.db.NewTransaction(false)
	defer txn.Discard()
	for _, key := range keys {
		item, err := txn.Get(key)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				fmt.Println(err)
			}
			// log ..
			continue
		}
		buf, err := item.ValueCopy(nil)
		if err != nil {
			fmt.Println(err)
			// log ..
			continue
		}
		bufs = append(bufs, buf)
	}
	return
}

// MultiGetMap .
func (r *Reader) MultiGetMap(keys []string) (bufs map[string][]byte, err error) {
	bufs = make(map[string][]byte, 0)
	txn := r.store.db.NewTransaction(false)
	defer txn.Discard()
	for _, key := range keys {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err != badger.ErrKeyNotFound {
				fmt.Println(err)
			}
			// log ..
			continue
		}
		buf, err := item.ValueCopy(nil)
		if err != nil {
			fmt.Println(err)
			// log ..
			continue
		}
		bufs[key] = buf
	}
	return
}

// PrefixIterator initialize a new prefix iterator
func (r *Reader) PrefixIterator(k []byte) kv.Iterator {
	txn := r.store.db.NewTransaction(false)
	// defer txn.Discard() //待测试。。
	itr := txn.NewIterator(r.iteratorOptions)
	rv := Iterator{
		iterator: itr,
		prefix:   k,
	}
	rv.iterator.Seek(k)
	return &rv
}

// RangeIterator initialize a new range iterator
func (r *Reader) RangeIterator(start, end []byte) kv.Iterator {
	txn := r.store.db.NewTransaction(false)
	// defer txn.Discard() //待测试。。
	itr := txn.NewIterator(r.iteratorOptions)
	rv := Iterator{
		iterator: itr,
		start:    start,
		stop:     end,
	}
	rv.iterator.Seek(start)
	return &rv
}

// Close closes the current reader and do some cleanup
func (r *Reader) Close() error {
	return nil
}
