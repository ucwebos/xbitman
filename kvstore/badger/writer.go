package badger

import (
	"fmt"

	"github.com/dgraph-io/badger/v3"

	"xbitman/kvstore/kv"
)

// Writer implements bleve store Writer interface
type Writer struct {
	store *Store
}

// NewBatch implements NewBatch
func (w *Writer) NewBatch() kv.Batch {
	rv := Batch{
		batch: w.store.db.NewWriteBatch(),
	}
	return &rv
}

// NewBatchEx .
func (w *Writer) NewBatchEx(options kv.BatchOptions) ([]byte, kv.Batch, error) {
	return nil, w.NewBatch(), nil
}

func (w *Writer) Set(key, val []byte) (err error) {
	return w.store.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, val)
	})
}

func (w *Writer) Delete(key []byte) (err error) {
	return w.store.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// ExecuteBatch .
func (w *Writer) ExecuteBatch(b kv.Batch) (err error) {
	batch, ok := b.(*Batch)
	if ok {
		return batch.batch.Flush()
	}
	return fmt.Errorf("wrong type of batch")
}

// DropPrefix .
func (w *Writer) DropPrefix(prefix []byte) error {
	return w.store.db.DropPrefix(prefix)
}

// Flush .
func (w *Writer) Flush() error {
	return w.store.db.DropAll()
}

// Close .
func (w *Writer) Close() error {
	return nil
}
