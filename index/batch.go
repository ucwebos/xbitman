package index

import (
	"errors"
	"fmt"
	"xbitman/kvstore"
	"xbitman/kvstore/kv"
	"xbitman/libs"
)

type uKeyItem struct {
	UKey  uint32
	New   bool
	OData map[string]interface{}
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
	ukData, newUKeys, err := t.uKeyData(items, kvReader)
	if err != nil {
		return err
	}
	var (
		idxBatchData   = make(map[string]map[string][]uint32)
		idxBatchRmData = make(map[string]map[string][]uint32)
	)
	for i, item := range items {
		uKeyIt := ukData[i]
		for _, schemeKey := range t.Scheme.Indexes {
			iv, ok := item[schemeKey.Key]
			if !ok {
				continue
			}
			iVal := string(TypeConv(schemeKey.Type, iv))
			if _, ok := idxBatchData[schemeKey.Key]; !ok {
				idxBatchData[schemeKey.Key] = make(map[string][]uint32)
			}
			if _, ok := idxBatchData[schemeKey.Key][iVal]; !ok {
				idxBatchData[schemeKey.Key][iVal] = make([]uint32, 0)
			}
			idxBatchData[schemeKey.Key][iVal] = append(idxBatchData[schemeKey.Key][iVal], uKeyIt.UKey)
			if !uKeyIt.New {
				if ov, ok := uKeyIt.OData[schemeKey.Key]; ok {
					rVal := string(TypeConv(schemeKey.Type, ov))
					if _, ok := idxBatchRmData[schemeKey.Key]; !ok {
						idxBatchRmData[schemeKey.Key] = make(map[string][]uint32)
					}
					if _, ok := idxBatchRmData[schemeKey.Key][rVal]; !ok {
						idxBatchRmData[schemeKey.Key][rVal] = make([]uint32, 0)
					}
					idxBatchRmData[schemeKey.Key][rVal] = append(idxBatchRmData[schemeKey.Key][rVal], uKeyIt.UKey)
				}
			}
		}
	}

	// 写 pk
	err = t.PkMap.PutBatch(newUKeys)
	if err != nil {
		return err
	}

	// 写 idx
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
	// 写kv todo 如何保证kv数据和index数据事物性
	wBatch := kvWriter.NewBatch()
	for i, vData := range items {
		uKey := ukData[i].UKey
		buf, _ := libs.JSON.Marshal(vData)
		wBatch.Set(t.kvKey(uKey), buf)
	}
	err = kvWriter.ExecuteBatch(wBatch)
	if err != nil {
		return err
	}
	wBatch.Close()
	return err
}

func (t *Table) uKeyData(items []map[string]interface{}, kvReader kv.Reader) (ukData []uKeyItem, newUKeys map[string]uint32, err error) {
	ukData = make([]uKeyItem, len(items))
	newUKeys = make(map[string]uint32, 0)
	pks := make([]string, len(items))
	for i, item := range items {
		pkv, ok := item[t.Scheme.PKey.Key]
		if !ok {
			return nil, nil, errors.New(fmt.Sprintf("no found pkey from data[%v]", item))
		}
		pks[i] = string(TypeConv(t.Scheme.PKey.Type, pkv))
		ukData[i] = uKeyItem{
			UKey:  0,
			New:   true,
			OData: make(map[string]interface{}),
		}
	}
	pkUMap, err := kvReader.MultiGetMap(pks)
	if err != nil {
		return nil, nil, err
	}
	for i, pk := range pks {
		uKey := t.PkMap.get([]byte(pk))
		// todo 判断uKey==0 ？
		if v, ok := pkUMap[pk]; ok {
			ukData[i].New = false
			ukData[i].UKey = uKey
			oData := make(map[string]interface{})
			err = libs.JSON.Unmarshal(v, &oData)
			if err != nil {
				return nil, nil, err
			}
			ukData[i].OData = oData
			continue
		}
		uKey = t.uKeyAdd()
		newUKeys[pk] = uKey
		ukData[i].UKey = uKey
	}
	return ukData, newUKeys, nil
}
