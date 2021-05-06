package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/cstockton/go-conv"

	"xbitman/conf"
	"xbitman/kvstore"
	"xbitman/libs"
	bolt "xbitman/libs/bbolt"
)

type Table struct {
	Mux     sync.Mutex        `json:"-"`
	Name    string            `json:"name"`
	UKey    uint32            `json:"uKey"`
	Store   *bolt.DB          `json:"-"`
	PkMap   *PkMap            `json:"pkMap"`
	Indexes map[string]*Index `json:"-"`
	Scheme  *conf.Table       `json:"scheme"`
}

func NewTable(store *bolt.DB, name string) *Table {
	t := &Table{
		Mux:     sync.Mutex{},
		Name:    name,
		UKey:    0,
		Store:   store,
		Indexes: make(map[string]*Index),
	}
	return t
}

func (t *Table) InitScheme(scheme *conf.Table) {
	t.Scheme = scheme
}

func (t *Table) Load(buf []byte) (err error) {
	return libs.JSON.Unmarshal(buf, &t)
}

func (t *Table) uKeyAdd() uint32 {
	t.UKey++
	return t.UKey
}

func (t *Table) Close() (err error) {
	return t.Store.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(TABLE, false)
		if bkt == nil {
			bkt, _ = tx.CreateBucket(TABLE, false)
		}
		buf, _ := libs.JSON.Marshal(t)
		return bkt.Put([]byte(t.Name), buf)
	})
}

func (t *Table) InitIndexes() {
	// pkMap
	t.PkMap = NewPkMap(t.Store, t.Name+"_0_PK", t.Scheme.PKey.Type)
	// indexes
	for _, item := range t.Scheme.Indexes {
		idx := NewIndex(t.Store, t.Name+"_1_"+item.Key, item.Type)
		t.Indexes[item.Key] = idx
	}
}

func (t *Table) Put(data map[string]interface{}) (err error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()
	kvReader, err := kvstore.KVReader()
	if err != nil {
		return err
	}
	kvWriter, err := kvstore.KVWriter()
	if err != nil {
		return err
	}
	var (
		re    = true
		uKey  uint32
		oData = make(map[string]interface{})
	)
	// find pk
	pkv, ok := data[t.Scheme.PKey.Key]
	if !ok {
		return errors.New("no pk data")
	}
	pkVal := TypeConv(t.Scheme.PKey.Type, pkv)
	uKeyB := t.PkMap.get(pkVal)
	if uKeyB == 0 {
		uKey = t.uKeyAdd()
		re = false
		err = t.PkMap.Put(pkVal, []byte(strconv.Itoa(int(uKey))))
		if err != nil {
			return err
		}
	} else {
		uKey = uKeyB
		// find kv
		raw, err := kvReader.Get(t.kvKey(uKey))
		if err != nil {
			return err
		}
		err = libs.JSON.Unmarshal(raw, &oData)
		if err != nil {
			return err
		}
	}
	// index
	for _, schemeKey := range t.Scheme.Indexes {
		iv, ok := data[schemeKey.Key]
		if !ok {
			continue
		}
		iVal := TypeConv(schemeKey.Type, iv)
		idx := t.Indexes[schemeKey.Key]
		if re {
			if ov, ok := oData[schemeKey.Key]; ok {
				rVal := TypeConv(schemeKey.Type, ov)
				idx.AppendWithRemove(iVal, rVal, uKey)
			}

			continue
		}
		idx.Append(iVal, uKey)
	}
	// set kv
	raw, _ := libs.JSON.Marshal(data)
	return kvWriter.Set(t.kvKey(uKey), raw)
}

func (t *Table) Delete(key interface{}) (err error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()
	kvReader, err := kvstore.KVReader()
	if err != nil {
		return err
	}
	kvWriter, err := kvstore.KVWriter()
	if err != nil {
		return err
	}
	var (
		uKey  uint32
		oData = make(map[string]interface{})
	)
	// find pk
	pkVal := TypeConv(t.Scheme.PKey.Type, key)
	uKey = t.PkMap.get(pkVal)
	// find kv
	raw, err := kvReader.Get(t.kvKey(uKey))
	if err != nil {
		return err
	}
	err = libs.JSON.Unmarshal(raw, &oData)
	if err != nil {
		return err
	}
	// index
	for _, schemeKey := range t.Scheme.Indexes {
		idx := t.Indexes[schemeKey.Key]
		if ov, ok := oData[schemeKey.Key]; ok {
			rVal := TypeConv(schemeKey.Type, ov)
			idx.Remove(rVal, uKey)
		}
	}
	// del kv
	return kvWriter.Delete(t.kvKey(uKey))
}

func (t *Table) DumpIndexes() (err error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()
	// 删除索引
	for _, idx := range t.Indexes {
		err = idx.Dump()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Table) DumpData() (err error) {
	t.Mux.Lock()
	defer t.Mux.Unlock()
	// 删除数据
	kvWriter, err := kvstore.KVWriter()
	if err != nil {
		return err
	}
	return kvWriter.DropPrefix([]byte(t.Name + "."))
}

func (t *Table) Get(key string) (raw []byte, err error) {
	uKey := t.PkMap.get([]byte(key))
	if uKey == 0 {
		return nil, errors.New("not found ")
	}
	kvReader, err := kvstore.KVReader()
	if err != nil {
		return nil, err
	}
	return kvReader.Get(t.kvKey(uKey))
}

func (t *Table) Gets(keys []string) (list []json.RawMessage, err error) {
	keyBytes := make([][]byte, len(keys))
	for i, key := range keys {
		keyBytes[i] = []byte(key)
	}
	uKeys := make([][]byte, 0)
	bm := t.PkMap.findIn(keyBytes)
	bm.Iterate(func(x uint32) bool {
		uKeys = append(uKeys, t.kvKey(x))
		return true
	})
	reader, err := kvstore.KVReader()
	if err != nil {
		return nil, err
	}
	data, err := reader.MultiGet(uKeys)
	if err != nil {
		return nil, err
	}
	list = make([]json.RawMessage, len(data))
	for i, datum := range data {
		list[i] = datum
	}
	return
}

func (t *Table) Query(where Op, limit *Limit, sort *Sort) (list []json.RawMessage, total int, err error) {
	bm, err := t.query(where)
	if err != nil {
		return nil, 0, err
	}
	total = int(bm.GetCardinality())

	var (
		start = 0
		size  = 1000
		uKeys = make([][]byte, 0)
	)

	if limit != nil {
		start = limit.Start
		size = limit.Size
	}
	if sort != nil && sort.Key != "" {
		// 执行排序
		uKey32s, err := t.sort(bm, sort.Key, sort.Desc, start, size)
		if err != nil {
			return nil, 0, err
		}
		for _, key32 := range uKey32s {
			uKeys = append(uKeys, t.kvKey(key32))
		}
	} else {
		offset := 0
		bm.Iterate(func(x uint32) bool {
			if offset >= start {
				uKeys = append(uKeys, t.kvKey(x))
				if len(uKeys) == size {
					return false
				}
			}
			offset++
			return true
		})
	}
	reader, err := kvstore.KVReader()
	if err != nil {
		return nil, 0, err
	}
	data, err := reader.MultiGet(uKeys)
	if err != nil {
		return nil, 0, err
	}
	list = make([]json.RawMessage, len(data))
	for i, datum := range data {
		list[i] = datum
	}
	return
}

func (t *Table) Count(where Op) (num int, err error) {
	bm, err := t.query(where)
	if err != nil {
		return num, err
	}
	num = int(bm.GetCardinality())
	return num, nil

}

func (t *Table) query(whr Op) (bm *roaring.Bitmap, err error) {
	if whr.Or != nil && len(whr.Or) > 0 {
		return t.or(whr.Or)
	}
	if whr.And != nil && len(whr.And) > 0 {
		return t.and(whr.And)
	}
	if whr.Key == t.Scheme.PKey.Key {
		return t.exec(t.PkMap, whr)
	}
	idx, ok := t.Indexes[whr.Key]
	if !ok {
		return nil, errors.New(fmt.Sprintf("not found index[%s]", whr.Key))
	}
	return t.exec(idx, whr)
}

func (t *Table) exec(q qs, whr Op) (bm *roaring.Bitmap, err error) {
	switch whr.Op {
	case eq:
		return q.find(TypeConv(q.IType(), whr.Val)), nil
	case in:
		if reflect.TypeOf(whr.Val).Kind() != reflect.Slice {
			return nil, errors.New("operator [in] must be use slice")
		}
		var list = make([][]byte, 0)
		s := reflect.ValueOf(whr.Val)
		for i := 0; i < s.Len(); i++ {
			ele := s.Index(i)
			list = append(list, TypeConv(q.IType(), ele.Interface()))
		}
		return q.findIn(list), nil
	case lt:
		return q.findLessThan(TypeConv(q.IType(), whr.Val)), nil
	case le:
		return q.findLessOrEq(TypeConv(q.IType(), whr.Val)), nil
	case gt:
		fmt.Println(string(TypeConv(q.IType(), whr.Val)))
		return q.findMoreThan(TypeConv(q.IType(), whr.Val)), nil
	case ge:
		return q.findMoreOrEq(TypeConv(q.IType(), whr.Val)), nil
	case ne:
		return q.findNot(TypeConv(q.IType(), whr.Val)), nil
	case btw:
		if reflect.TypeOf(whr.Val).Kind() != reflect.Slice {
			return nil, errors.New("operator [in] must be use slice")
		}
		var list = make([][]byte, 0)
		s := reflect.ValueOf(whr.Val)
		for i := 0; i < s.Len(); i++ {
			ele := s.Index(i)
			list = append(list, TypeConv(q.IType(), ele.Interface()))
		}
		if len(list) != 2 {
			return nil, errors.New("operator [in] must be use slice like [v1,v2]")
		}
		return q.findBetween(list[0], list[1]), nil
	default:
		return nil, errors.New(fmt.Sprintf("not found operator[%s]", whr.Op))
	}
}

func (t *Table) and(where []Op) (bm *roaring.Bitmap, err error) {
	bms := make([]*roaring.Bitmap, 0)
	for _, op := range where {
		bmi, err := t.query(op)
		if err != nil {
			return bm, err
		}
		bms = append(bms, bmi)
	}
	return roaring.FastAnd(bms...), err
}

func (t *Table) or(where []Op) (bm *roaring.Bitmap, err error) {
	bm = roaring.NewBitmap()
	for _, op := range where {
		bmi, err := t.query(op)
		if err != nil {
			return bm, err
		}
		bm.Or(bmi)
	}
	return bm, err
}

func (t *Table) sort(bm *roaring.Bitmap, key string, desc bool, start, size int) (uKeys []uint32, err error) {
	if key == t.Scheme.PKey.Key {
		return t.PkMap.sort(bm, desc, start, size), nil
	}
	idx, ok := t.Indexes[key]
	if !ok {
		return nil, errors.New(fmt.Sprintf("not found sort index[%s]", key))
	}
	return idx.sort(bm, desc, start, size), nil
}

func TypeConv(iType int, v interface{}) (buf []byte) {
	switch iType {
	case conf.TypeInt:
		i, _ := conv.Int(v)
		s := strconv.Itoa(i)
		return []byte(s)
	case conf.TypeFloat:
		i, _ := conv.Float64(v)
		s := strconv.FormatFloat(i, 'f', -1, 64)
		return []byte(s)
	case conf.TypeString:
		s, _ := conv.String(v)
		// 字符串前面加0处理空字符串问题
		return []byte("0" + s)
	case conf.TypeBool:
		i, _ := conv.Bool(v)
		s := strconv.FormatBool(i)
		return []byte(s)
	default:
		return nil
	}
}

func (t *Table) kvKey(uKey uint32) (key []byte) {
	return []byte(t.Name + "." + strconv.Itoa(int(uKey)))
}
