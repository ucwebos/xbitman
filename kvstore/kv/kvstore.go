package kv

// Store is an abstraction for working with KV stores.  Note that
// in order to be used with the bleve.registry, it must also implement
// a constructor function of the registry.KVStoreConstructor type.
type Store interface {

	// Writer returns a KVWriter which can be used to
	// make changes to the KVStore.  If a writer cannot
	// be obtained a non-nil error is returned.
	Writer() (Writer, error)

	// Reader returns a KVReader which can be used to
	// read data from the KVStore.  If a reader cannot
	// be obtained a non-nil error is returned.
	Reader() (Reader, error)

	// Close closes the KVStore
	Close() error
}

// Reader is an abstraction of an **ISOLATED** reader
// In this context isolated is defined to mean that
// writes/deletes made after the KVReader is opened
// are not observed.
// Because there is usually a cost associated with
// keeping isolated readers active, users should
// close them as soon as they are no longer needed.
type Reader interface {

	// Get returns the value associated with the key
	// If the key does not exist, nil is returned.
	// The caller owns the bytes returned.
	Get(key []byte) ([]byte, error)

	// MultiGet retrieves multiple values in one call.
	MultiGet(keys [][]byte) ([][]byte, error)

	// MultiGetMap retrieves multiple map values in one call.
	MultiGetMap(keys []string) (map[string][]byte, error)

	// PrefixIterator returns a KVIterator that will
	// visit all K/V pairs with the provided prefix
	PrefixIterator(prefix []byte) Iterator

	// RangeIterator returns a KVIterator that will
	// visit all K/V pairs >= start AND < end
	RangeIterator(start, end []byte) Iterator

	// Close closes the iterator
	Close() error
}

// Iterator is an abstraction around key iteration
type Iterator interface {

	// Seek will advance the iterator to the specified key
	Seek(key []byte)

	// Next will advance the iterator to the next key
	Next()

	// Key returns the key pointed to by the iterator
	// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
	// Continued use after that requires that they be copied.
	Key() []byte

	// Value returns the value pointed to by the iterator
	// The bytes returned are **ONLY** valid until the next call to Seek/Next/Close
	// Continued use after that requires that they be copied.
	Value() []byte

	// Valid returns whether or not the iterator is in a valid state
	Valid() bool

	// Current returns Key(),Value(),Valid() in a single operation
	Current() ([]byte, []byte, bool)

	// Close closes the iterator
	Close() error
}

// Writer is an abstraction for mutating the KVStore
// Writer does **NOT** enforce restrictions of a single writer
// if the underlying KVStore allows concurrent writes, the
// KVWriter interface should also do so, it is up to the caller
// to do this in a way that is safe and makes sense
type Writer interface {

	// NewBatch returns a KVBatch for performing batch operations on this kvstore
	NewBatch() Batch

	// NewBatchEx returns a KVBatch and an associated byte array
	// that's pre-sized based on the KVBatchOptions.  The caller can
	// use the returned byte array for keys and values associated with
	// the batch.  Once the batch is either executed or closed, the
	// associated byte array should no longer be accessed by the
	// caller.
	NewBatchEx(BatchOptions) ([]byte, Batch, error)

	// ExecuteBatch will execute the KVBatch, the provided KVBatch **MUST** have
	// been created by the same KVStore (though not necessarily the same KVWriter)
	// Batch execution is atomic, either all the operations or none will be performed
	ExecuteBatch(batch Batch) error

	Delete(key []byte) error

	Set(key, val []byte) error

	DropPrefix(prefix []byte) error

	// ...
	Flush() error

	// Close closes the writer
	Close() error
}

// BatchOptions provides the KVWriter.NewBatchEx() method with batch
// preparation and preallocation information.
type BatchOptions struct {
	// TotalBytes is the sum of key and value bytes needed by the
	// caller for the entire batch.  It affects the size of the
	// returned byte array of KVWrite.NewBatchEx().
	TotalBytes int

	// NumSets is the number of Set() calls the caller will invoke on
	// the KVBatch.
	NumSets int

	// NumDeletes is the number of Delete() calls the caller will invoke
	// on the KVBatch.
	NumDeletes int

	// NumMerges is the number of Merge() calls the caller will invoke
	// on the KVBatch.
	NumMerges int
}

// Batch is an abstraction for making multiple KV mutations at once
type Batch interface {

	// Set updates the key with the specified value
	// both key and value []byte may be reused as soon as this call returns
	Set(key, val []byte)

	// Delete removes the specified key
	// the key []byte may be reused as soon as this call returns
	Delete(key []byte)

	// Merge merges old value with the new value at the specified key
	// as prescribed by the KVStores merge operator
	// both key and value []byte may be reused as soon as this call returns
	Merge(key, val []byte)

	// Reset frees resources for this batch and allows reuse
	Reset()

	// Count .
	Count() int

	// Close frees resources
	Close() error
}
