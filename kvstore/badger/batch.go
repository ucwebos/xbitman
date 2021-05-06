package badger

import (
	"github.com/dgraph-io/badger/v3"
)

// Batch .
type Batch struct {
	batch *badger.WriteBatch
}

// Set .
func (b *Batch) Set(key, val []byte) {
	b.batch.Set(key, val)
}

// Delete .
func (b *Batch) Delete(key []byte) {
	b.batch.Delete(key)
}

// Merge .
func (b *Batch) Merge(key, val []byte) {
	return
}

// Reset .
func (b *Batch) Reset() {
	return
}

// Count .
func (b *Batch) Count() int {
	return 1
}

// Close .
func (b *Batch) Close() error {
	b.batch.Cancel()
	b.batch = nil
	return nil
}
