package index

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	bolt "xbitman/libs/bbolt"
)

const (
	eq        = "="         // =
	ne        = "!="        // !=
	gt        = ">"         // >
	lt        = "<"         // <
	ge        = ">="        // >=
	le        = "<="        // <=
	in        = "in"        // in
	nin       = "nin"       // in
	btw       = "btw"       // between
	contains  = "contains"  // contains 数组包含
	nContains = "ncontains" // ncontains 数组不包含
)

type Op struct {
	Key    string      `json:"key,omitempty"`
	SubKey string      `json:"subKey,omitempty"`
	Op     string      `json:"op,omitempty"`
	Val    interface{} `json:"val,omitempty"`
	Or     []Op        `json:"or,omitempty"`
	And    []Op        `json:"and,omitempty"`
}

type Limit struct {
	Start int `json:"start"`
	Size  int `json:"size"`
}

type Sort struct {
	Key  string `json:"key"`
	Desc bool   `json:"desc"`
}

type qs interface {
	Number() bool
	IType() (i int)
	find(key []byte) (bm *roaring.Bitmap)
	findNot(key []byte) (bm *roaring.Bitmap)
	findIn(keys [][]byte) (bm *roaring.Bitmap)
	findNotIn(keys [][]byte) (bm *roaring.Bitmap)
	findLessThan(key []byte) (bm *roaring.Bitmap)
	findLessOrEq(key []byte) (bm *roaring.Bitmap)
	findMoreThan(key []byte) (bm *roaring.Bitmap)
	findMoreOrEq(key []byte) (bm *roaring.Bitmap)
	findBetween(key []byte, key2 []byte) (bm *roaring.Bitmap)
	sort(bm *roaring.Bitmap, desc bool, cb func(x uint32) bool) (err error)
}

func (idx *Index) find(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte(idx.Name), idx.Number()).Get(key)
		if len(v) > 0 {
			bm.FromBuffer(v)
		}
		return nil
	})
	return bm
}

func (idx *Index) findNot(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(idx.Name), idx.Number())
		bkt.ForEach(func(k, v []byte) error {
			if bytes.Compare(k, key) != 0 {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					// ?? nil
					return err
				}
				bm.Or(bmt)
			}
			return nil
		})
		return nil
	})
	return
}

func (idx *Index) findIn(keys [][]byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(idx.Name), idx.Number())
		for _, key := range keys {
			v := bkt.Get(key)
			if len(v) > 0 {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					return err
				}
				bm.Or(bmt)
			}
		}
		return nil
	})
	return bm
}

func (idx *Index) findNotIn(keys [][]byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(idx.Name), idx.Number())
		return bkt.ForEach(func(k, v []byte) error {
			if notIn(k, keys) {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					// ?? nil
					return err
				}
				bm.Or(bmt)
			}
			return nil
		})
	})
	return
}

func (idx *Index) findLessThan(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(idx.Name), idx.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Prev() {
			if bytes.Compare(k, key) < 0 && len(v) > 0 {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					return err
				}
				bm.Or(bmt)
			}
		}
		return nil
	})
	return bm
}

func (idx *Index) findLessOrEq(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(idx.Name), idx.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Prev() {
			if len(v) > 0 {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					return err
				}
				bm.Or(bmt)
			}
		}
		return nil
	})
	return bm
}

func (idx *Index) findMoreThan(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(idx.Name), idx.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			if bytes.Compare(k, key) > 0 && len(v) > 0 {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					return err
				}
				bm.Or(bmt)
			}
		}
		return nil
	})
	return bm
}

func (idx *Index) findMoreOrEq(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(idx.Name), idx.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			if len(v) > 0 {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					return err
				}
				bm.Or(bmt)
			}
		}
		return nil
	})
	return bm
}

func (idx *Index) findBetween(key []byte, key2 []byte) (bm *roaring.Bitmap) {
	if bytes.Compare(key, key2) == 1 {
		key, key2 = key2, key
	}
	bm = roaring.NewBitmap()
	idx.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(idx.Name), idx.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			if len(v) > 0 {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					return err
				}
				bm.Or(bmt)
			}
			if bytes.Compare(k, key2) == 0 {
				break
			}
		}
		return nil
	})
	return bm
}

func (idx *Index) sort(bm *roaring.Bitmap, desc bool, cb func(x uint32) bool) (err error) {
	var end = false
	err = idx.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(idx.Name), idx.Number()).Cursor()
		if desc {
			for k, v := c.Last(); k != nil; k, v = c.Prev() {
				if len(v) > 0 {
					bmt := roaring.NewBitmap()
					_, err := bmt.FromBuffer(v)
					if err != nil {
						return err
					}
					bmt.And(bm)
					bmt.Iterate(func(x uint32) bool {
						if !cb(x) {
							end = true
						}
						return true
					})
					if end {
						break
					}
				}
			}
			return nil
		}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(v) > 0 {
				bmt := roaring.NewBitmap()
				_, err := bmt.FromBuffer(v)
				if err != nil {
					return err
				}
				bmt.And(bm)
				bmt.Iterate(func(x uint32) bool {
					if !cb(x) {
						end = true
					}
					return true
				})
				if end {
					break
				}
			}
		}
		return nil
	})
	return err
}
