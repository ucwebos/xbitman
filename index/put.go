package index

import (
	"github.com/RoaringBitmap/roaring"
	bolt "xbitman/libs/bbolt"
)

func (idx *Index) _append(tx *bolt.Tx, key []byte, uKeys ...uint32) (err error) {
	bm := roaring.NewBitmap()
	v := tx.Bucket([]byte(idx.Name), idx.Number()).Get(key)
	if len(v) > 0 {
		_, err = bm.FromBuffer(v)
		if err != nil {
			return err
		}
	}
	bm.AddMany(uKeys)
	bBytes, err := bm.ToBytes()
	if err != nil {
		return err
	}
	return tx.Bucket([]byte(idx.Name), idx.Number()).Put(key, bBytes)
}

func (idx *Index) _remove(tx *bolt.Tx, key []byte, uKeys ...uint32) (err error) {
	bm := roaring.NewBitmap()
	v := tx.Bucket([]byte(idx.Name), idx.Number()).Get(key)
	if len(v) > 0 {
		_, err = bm.FromBuffer(v)
		if err != nil {
			return err
		}
	}
	for _, uKey := range uKeys {
		bm.Remove(uKey)
	}
	bBytes, err := bm.ToBytes()
	if err != nil {
		return err
	}
	return tx.Bucket([]byte(idx.Name), idx.Number()).Put(key, bBytes)
}

func (idx *Index) BatchAppendWithRemove(aKeys, rKeys map[string][]uint32) (err error) {
	idx.mux.Lock()
	defer idx.mux.Unlock()
	return idx.Store.Update(func(tx *bolt.Tx) error {
		for aKey, uKeys := range aKeys {
			err = idx._append(tx, []byte(aKey), uKeys...)
			if err != nil {
				return err
			}
		}
		for rKey, uKeys := range rKeys {
			err = idx._remove(tx, []byte(rKey), uKeys...)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (idx *Index) Append(key []byte, uKey uint32) (err error) {
	idx.mux.Lock()
	defer idx.mux.Unlock()
	return idx.Store.Update(func(tx *bolt.Tx) error {
		return idx._append(tx, key, uKey)
	})
}

func (idx *Index) AppendWithRemove(aKey, rKey []byte, uKey uint32) (err error) {
	idx.mux.Lock()
	defer idx.mux.Unlock()
	return idx.Store.Update(func(tx *bolt.Tx) error {
		err = idx._append(tx, aKey, uKey)
		if err != nil {
			return err
		}
		return idx._remove(tx, rKey, uKey)
	})
}

func (idx *Index) PutBatch(data map[string][]byte) (err error) {
	length := len(data)
	if length <= 1000 {
		return idx.batch(data)
	}
	var (
		chuck = make(map[string][]byte)
	)
	for s, mBytes := range data {
		chuck[s] = mBytes
		if len(chuck) == 1000 {
			err = idx.batch(chuck)
			if err != nil {
				return err
			}
			chuck = make(map[string][]byte)
		}
	}
	return idx.batch(chuck)
}

func (idx *Index) batch(data map[string][]byte) (err error) {
	idx.mux.Lock()
	defer idx.mux.Unlock()
	tx, err := idx.Store.Begin(true)
	if err != nil {
		return err
	}
	bkt, err := tx.CreateBucketIfNotExists([]byte(idx.Name), idx.Number())
	if err != nil {
		return err
	}
	for s, mBytes := range data {
		if err = bkt.Put([]byte(s), mBytes); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}
