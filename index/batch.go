package index

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/cstockton/go-conv"
	"reflect"
	"xbitman/conf"
	"xbitman/kvstore"
	"xbitman/kvstore/kv"
	"xbitman/libs"
)

type uItem struct {
	UKey uint32
	pk   string
	Data map[string]interface{}
}

func (t *Table) idxBatchWriteData(idxBatch map[string]map[string][]uint32, item map[string]interface{}, uKey uint32) {
	for _, schemeKey := range t.Scheme.Indexes {
		iv, ok := item[schemeKey.Key]
		if !ok {
			continue
		}
		var (
			iKey = schemeKey.Key
			iVal string
		)
		switch schemeKey.Type {
		case conf.TypeSet:
			if iv == nil {
				continue
			}
			if reflect.TypeOf(iv).Kind() != reflect.Slice {
				iVal = string(TypeConv(schemeKey.Type, iv))
				t._idxBatchWriteData(idxBatch, iKey, iVal, uKey)
				continue
			}
			s := reflect.ValueOf(iv)
			for i := 0; i < s.Len(); i++ {
				ele := s.Index(i)
				iVal = string(TypeConv(schemeKey.Type, ele.Interface()))
				t._idxBatchWriteData(idxBatch, iKey, iVal, uKey)
			}
		case conf.TypeMulti:
			if iv == nil {
				continue
			}
			if reflect.TypeOf(iv).Kind() != reflect.Slice {
				continue
			}
			s := reflect.ValueOf(iv)
			for i := 0; i < s.Len(); i++ {
				sIt := s.Index(i).Interface()
				if reflect.TypeOf(sIt).Kind() != reflect.Map {
					continue
				}
				s2 := reflect.ValueOf(sIt)
				iter := s2.MapRange()
				for iter.Next() {
					k := iter.Key()
					v := iter.Value()
					kStr, _ := conv.String(k.Interface())
					for _, scheme2 := range schemeKey.SubIndexes {
						if scheme2.Key == kStr {
							i2Key := iKey + "." + kStr
							iVal = string(TypeConv(scheme2.Type, v.Interface()))
							t._idxBatchWriteData(idxBatch, i2Key, iVal, uKey)
						}
					}
				}
			}
		default:
			iVal = string(TypeConv(schemeKey.Type, iv))
			t._idxBatchWriteData(idxBatch, iKey, iVal, uKey)
		}
	}
}

func (t *Table) _idxBatchWriteData(idxBatch map[string]map[string][]uint32, iKey, iVal string, uKey uint32) {
	if _, ok := idxBatch[iKey]; !ok {
		idxBatch[iKey] = make(map[string][]uint32)
	}
	if _, ok := idxBatch[iKey][iVal]; !ok {
		idxBatch[iKey][iVal] = make([]uint32, 0)
	}
	idxBatch[iKey][iVal] = append(idxBatch[iKey][iVal], uKey)
}

func (t *Table) PutBatch(items []map[string]interface{}) (err error) {
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
	//newUKeys
	ukData, err := t.oUKeyData(items, kvReader)
	if err != nil {
		return err
	}
	var (
		idxBatchData   = make(map[string]map[string][]uint32)
		idxBatchRmData = make(map[string]map[string][]uint32)
		newUKeys       = make(map[string]uint32)
	)
	for i, item := range items {
		it := ukData[i]
		if it.UKey == 0 {
			uKey := t.uKeyAdd()
			ukData[i].UKey = uKey
			newUKeys[it.pk] = uKey
			t.idxBatchWriteData(idxBatchData, item, uKey)
			continue
		}
		// item ?????? it.Data
		var (
			aData = make(map[string]interface{})
			rData = make(map[string]interface{})
		)
		// todo ??????????????????????????????????????????
		for k, v := range item {
			if ov, ok := it.Data[k]; ok {
				idx, ok2 := t.Indexes[k]
				if !ok2 {
					continue
				}
				switch idx.IType() {
				case conf.TypeSet, conf.TypeMulti:
					var (
						b1, _ = libs.JSON.Marshal(ov)
						b2, _ = libs.JSON.Marshal(v)
					)
					if bytes.Compare(b1, b2) != 0 {
						rData[k] = ov
						aData[k] = v
					}
				default:
					if bytes.Compare(TypeConv(idx.IType(), ov), TypeConv(idx.IType(), v)) != 0 {
						rData[k] = ov
						aData[k] = v
					}
				}
			}
		}
		t.idxBatchWriteData(idxBatchRmData, rData, it.UKey)
		t.idxBatchWriteData(idxBatchData, aData, it.UKey)
	}
	// ??? pk
	err = t.PkMap.PutBatch(newUKeys)
	if err != nil {
		return err
	}
	// ??? idx
	for key, aData := range idxBatchData {
		fmt.Println(key)
		idx := t.Indexes[key]
		rData := idxBatchRmData[key]
		fmt.Println("len >>>", len(aData), len(rData))
		err = idx.BatchAppendWithRemove(aData, rData)
		if err != nil {
			return err
		}
	}
	// ???kv todo ????????????kv?????????index???????????????
	wBatch := kvWriter.NewBatch()
	for i, vData := range items {
		it := ukData[i]
		uKey := it.UKey
		// ??????put???????????????????????????????????????
		if it.Data != nil && len(it.Data) > 0 {
			for k, v := range it.Data {
				if x, ok := vData[k]; !ok || x == nil {
					vData[k] = v
				}
			}
		}
		buf, _ := libs.JSON.Marshal(vData)
		wBatch.Set(t.kvKey(uKey), buf)
	}
	err = kvWriter.ExecuteBatch(wBatch)
	if err != nil {
		return err
	}
	return wBatch.Close()
}

func (t *Table) oUKeyData(items []map[string]interface{}, kvReader kv.Reader) (ukData map[int]*uItem, err error) {
	ukData = make(map[int]*uItem, 0)
	pks := make([]string, len(items))
	for i, item := range items {
		pkv, ok := item[t.Scheme.PKey.Key]
		if !ok {
			return nil, errors.New(fmt.Sprintf("no found pkey from data[%v]", item))
		}
		pks[i] = string(TypeConv(t.Scheme.PKey.Type, pkv))
	}
	// PK to kv
	dataMap, err := t.readDataByPKeys(pks, kvReader)
	if err != nil {
		return nil, err
	}
	for i, pk := range pks {
		if v, ok := dataMap[pk]; ok {
			ukData[i] = v
			continue
		}
		ukData[i] = &uItem{
			pk: pk,
		}
	}
	return ukData, nil
}

func (t *Table) readDataByPKeys(pks []string, kvReader kv.Reader) (dataMap map[string]*uItem, err error) {
	dataMap = make(map[string]*uItem)
	uKeyMap := t.PkMap.mapGets(pks)
	if len(uKeyMap) == 0 {
		return dataMap, nil
	}
	var (
		uKvKeys = make([]string, 0)
		pkKvMap = make(map[string]string)
	)
	// to kv key
	for pk, uKey := range uKeyMap {
		uKvKey := string(t.kvKey(uKey))
		uKvKeys = append(uKvKeys, uKvKey)
		pkKvMap[pk] = uKvKey
	}
	// kv get
	uDataMap, err := kvReader.MultiGetMap(uKvKeys)
	if err != nil {
		return nil, err
	}
	for _, pk := range pks {
		if uKvKey, ok := pkKvMap[pk]; ok {
			if v, ok := uDataMap[uKvKey]; ok {
				var (
					item = &uItem{
						UKey: uKeyMap[pk],
						pk:   pk,
					}
					data = make(map[string]interface{})
				)
				err = libs.JSON.Unmarshal(v, &data)
				if err != nil {
					return nil, err
				}
				item.Data = data
				dataMap[pk] = item
			}
		}
	}
	return dataMap, nil
}
