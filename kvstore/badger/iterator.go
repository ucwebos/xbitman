package badger

import (
	"bytes"
	"github.com/dgraph-io/badger/v3"
)

// Iterator .
type Iterator struct {
	iterator *badger.Iterator
	prefix   []byte

	start []byte
	stop  []byte
}

// Seek .
func (i *Iterator) Seek(key []byte) {
	if len(i.prefix) > 0 {
		if bytes.Compare(key, i.prefix) < 0 {
			i.iterator.Seek(i.prefix)
			return
		}
	} else if len(i.start) > 0 {
		if bytes.Compare(key, i.start) < 0 {
			i.iterator.Seek(i.start)
			return
		}
	}

	i.iterator.Seek(key)
}

// Next .
func (i *Iterator) Next() {
	i.iterator.Next()
}

// Current .
func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.Valid() {
		return i.Key(), i.Value(), true
	}
	return nil, nil, false
}

// Key .
func (i *Iterator) Key() []byte {
	return i.iterator.Item().KeyCopy(nil)
}

// Value .
func (i *Iterator) Value() []byte {
	v, _ := i.iterator.Item().ValueCopy(nil)
	return v
}

// Valid .
func (i *Iterator) Valid() bool {
	// for prefix iterator
	if len(i.prefix) > 0 {
		return i.iterator.ValidForPrefix(i.prefix)
	}

	// for range based iterator
	if !i.iterator.Valid() {
		return false
	}
	if i.stop == nil || len(i.stop) == 0 {
		return true
	}
	if bytes.Compare(i.stop, i.iterator.Item().Key()) <= 0 {
		return false
	}
	return true
}

// Close .
func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
