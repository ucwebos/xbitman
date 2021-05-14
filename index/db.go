package index

import (
	"errors"
	"fmt"
	"sync"
	"xbitman/conf"
	"xbitman/libs"
	bolt "xbitman/libs/bbolt"
)

var (
	DB    *Database
	TABLE = []byte("TABLE")
)

type Database struct {
	mux    sync.Mutex
	Store  *bolt.DB `json:"-"`
	Name   string
	tables map[string]*Table
}

func NewDatabase(name string) (db *Database, err error) {
	path := conf.G.Path + "/IDX_" + name
	store, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	tables := make(map[string]*Table, 0)

	tabBufs := make(map[string][]byte, 0)
	err = store.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucketIfNotExists(TABLE, false)
		if err != nil {
			return err
		}
		err = bkt.ForEach(func(k, v []byte) error {
			tabBufs[string(k)] = v
			return nil
		})
		return err
	})
	for s, buf := range tabBufs {
		tab := NewTable(store, s)
		tab.Load(buf)
		fmt.Println("table load", s)
		tab.InitIndexes()
		tables[s] = tab
	}

	if err != nil {
		return nil, err
	}
	return &Database{
		mux:    sync.Mutex{},
		Store:  store,
		Name:   name,
		tables: tables,
	}, nil
}

func Init() {
	db, err := NewDatabase(conf.DBNAME)
	if err != nil {
		panic(err)
	}
	DB = db
}

func Close() error {
	return DB.Close()
}

func (db *Database) DeleteTable(name string) (err error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	tab, ok := db.tables[name]
	if ok {
		return nil
	}
	err = tab.DumpIndexes()
	if err != nil {
		return err
	}
	err = tab.DumpData()
	if err != nil {
		return err
	}
	delete(db.tables, name)
	return db.Store.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(TABLE, false).Delete([]byte(name))
	})
}

func (db *Database) CreateTable(name string, table *conf.Table) (err error) {
	db.mux.Lock()
	defer db.mux.Unlock()
	if _, ok := db.tables[name]; ok {
		return errors.New(fmt.Sprintf("table [%s] Exists", name))
	}
	if ok, _ := db.Exists(name); ok {
		return errors.New(fmt.Sprintf("table [%s] Exists", name))
	}
	tab := NewTable(db.Store, name)
	tab.InitScheme(table)
	tab.InitIndexes()
	buf, _ := libs.JSON.Marshal(tab)
	db.tables[name] = tab
	return db.Store.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(TABLE, false).Put([]byte(name), buf)
	})
}

func (db *Database) Table(name string) (tab *Table) {
	if tab, ok := db.tables[name]; ok {
		return tab
	}
	return nil
}

func (db *Database) Tables() (table map[string]*conf.Table) {
	tables := make(map[string]*conf.Table, len(db.tables))
	for s, t := range db.tables {
		tables[s] = t.Scheme
	}
	return tables
}

func (db *Database) Exists(name string) (ok bool, err error) {
	err = db.Store.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(TABLE, false).Get([]byte(name))
		if len(v) > 0 {
			ok = true
			return nil
		}
		return nil
	})
	return ok, err
}

func (db *Database) Close() (err error) {
	for _, table := range db.tables {
		table.Close()
	}
	return db.Store.Close()
}
