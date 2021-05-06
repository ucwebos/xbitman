package index

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"

	"github.com/RoaringBitmap/roaring"

	"xbitman/conf"
	bolt "xbitman/libs/bbolt"
)

type PkMap struct {
	mux   sync.Mutex
	Name  string
	Type  int
	Store *bolt.DB `json:"-"`
}

func NewPkMap(store *bolt.DB, name string, iType int) (pk *PkMap) {
	pk = &PkMap{
		mux:   sync.Mutex{},
		Name:  name,
		Type:  iType,
		Store: store,
	}
	fmt.Println("pk init", name)
	fmt.Println(pk.Store)
	pk.Store.Update(func(tx *bolt.Tx) error {
		fmt.Println(tx)
		_, err := tx.CreateBucketIfNotExists([]byte(name), pk.Number())
		fmt.Println("pk init store", err)
		return err
	})
	return pk
}

func (p *PkMap) Number() (ok bool) {
	switch p.Type {
	case conf.TypeInt,
		conf.TypeFloat:
		return true
	}
	return false
}

func (p *PkMap) IType() (i int) {
	return p.Type
}

func (p *PkMap) Put(key []byte, uKey []byte) (err error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	return p.Store.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(p.Name), p.Number()).Put(key, uKey)
	})
}

func (p *PkMap) PutBatch(items map[string]uint32) (err error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	tx, err := p.Store.Begin(true)
	if err != nil {
		return err
	}
	bkt := tx.Bucket([]byte(p.Name), p.Number())
	for s, uKey := range items {
		if err = bkt.Put([]byte(s), []byte(strconv.Itoa(int(uKey)))); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (p *PkMap) get(key []byte) (uKey uint32) {
	p.Store.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte(p.Name), p.Number()).Get(key)
		uKeyI, _ := strconv.Atoi(string(v))
		uKey = uint32(uKeyI)
		return nil
	})
	return uKey
}

func (p *PkMap) find(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	p.Store.View(func(tx *bolt.Tx) error {
		v := tx.Bucket([]byte(p.Name), p.Number()).Get(key)
		uKeyI, _ := strconv.Atoi(string(v))
		uKey := uint32(uKeyI)
		bm.Add(uKey)
		return nil
	})
	return bm
}

func (p *PkMap) findNot(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	p.Store.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(p.Name), p.Number())
		bkt.ForEach(func(k, v []byte) error {
			if bytes.Compare(k, key) != 0 {
				uKeyI, _ := strconv.Atoi(string(v))
				uKey := uint32(uKeyI)
				bm.Add(uKey)
			}
			return nil
		})
		return nil
	})
	return
}

func (p *PkMap) findIn(keys [][]byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	p.Store.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(p.Name), p.Number())
		for _, key := range keys {
			v := bkt.Get(key)
			if len(v) > 0 {
				uKeyI, _ := strconv.Atoi(string(v))
				uKey := uint32(uKeyI)
				bm.Add(uKey)
			}
		}
		return nil
	})
	return bm
}

func (p *PkMap) findLessThan(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	p.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(p.Name), p.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Prev() {
			if bytes.Compare(k, key) < 0 && len(v) > 0 {
				uKeyI, _ := strconv.Atoi(string(v))
				uKey := uint32(uKeyI)
				bm.Add(uKey)
			}
		}
		return nil
	})
	return bm
}

func (p *PkMap) findLessOrEq(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	p.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(p.Name), p.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Prev() {
			if len(v) > 0 {
				uKeyI, _ := strconv.Atoi(string(v))
				uKey := uint32(uKeyI)
				bm.Add(uKey)
			}
		}
		return nil
	})
	return bm
}

func (p *PkMap) findMoreThan(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	p.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(p.Name), p.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			if bytes.Compare(k, key) > 0 && len(v) > 0 {
				uKeyI, _ := strconv.Atoi(string(v))
				uKey := uint32(uKeyI)
				bm.Add(uKey)
			}
		}
		return nil
	})
	return bm
}

func (p *PkMap) findMoreOrEq(key []byte) (bm *roaring.Bitmap) {
	bm = roaring.NewBitmap()
	p.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(p.Name), p.Number()).Cursor()
		for k, v := c.Seek(key); k != nil; k, v = c.Next() {
			if len(v) > 0 {
				uKeyI, _ := strconv.Atoi(string(v))
				uKey := uint32(uKeyI)
				bm.Add(uKey)
			}
		}
		return nil
	})
	return bm
}

func (p *PkMap) findBetween(key []byte, key2 []byte) (bm *roaring.Bitmap) {
	if bytes.Compare(key, key2) == 1 {
		key, key2 = key2, key
	}
	bm = roaring.NewBitmap()
	p.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(p.Name), p.Number()).Cursor()
		for k, v := c.Seek(key); k != nil && bytes.Compare(k, key2) <= 0; k, v = c.Next() {
			if len(v) > 0 {
				uKeyI, _ := strconv.Atoi(string(v))
				uKey := uint32(uKeyI)
				bm.Add(uKey)
			}
		}
		return nil
	})
	return bm
}

// todo 可以增加 key值判断
func (p *PkMap) sort(bm *roaring.Bitmap, desc bool, start, size int) (uKeys []uint32) {
	uKeys = make([]uint32, 0)
	offset := 0
	p.Store.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(p.Name), p.Number()).Cursor()
		if desc {
			for k, v := c.Last(); k != nil; k, v = c.Prev() {
				if len(v) > 0 {
					uKeyI, _ := strconv.Atoi(string(v))
					uKey := uint32(uKeyI)
					if !bm.CheckedAdd(uKey) {
						if offset >= start {
							uKeys = append(uKeys, uKey)
							if len(uKeys) == size {
								break
							}
						}
						offset++
					}
				}
			}
			return nil
		}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if len(v) > 0 {
				uKeyI, _ := strconv.Atoi(string(v))
				uKey := uint32(uKeyI)
				if !bm.CheckedAdd(uKey) {
					if offset >= start {
						uKeys = append(uKeys, uKey)
						if len(uKeys) == size {
							break
						}
					}
					offset++
				}
			}
		}
		return nil
	})
	return uKeys
}
