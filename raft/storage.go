package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbUtil "github.com/syndtr/goleveldb/leveldb/util"
)

type Storage interface {
	Put(entry *LogEntry) (err error)
	Get(index int) (*LogEntry, error)
	BatchGet(start int) []LogEntry
}

type Leveldb struct {
	db *leveldb.DB
}

func NewLeveldb(dbpath string) (*Leveldb, error) {
	ldb := &Leveldb{}
	db, err := leveldb.OpenFile(dbpath, nil)
	if err != nil {
		return nil, err
	}
	ldb.db = db
	return ldb, nil
}

func (ldb *Leveldb) PutLog(entry *LogEntry) (err error) {
	key := fmt.Sprintf("LogIndex_%d", entry.LogIndex)
	var encbuf bytes.Buffer
	enc := gob.NewEncoder(&encbuf)
	err = enc.Encode(*entry)
	if err != nil {
		return
	}
	ldb.db.Put([]byte(key), encbuf.Bytes(), nil)
	return
}

func (ldb *Leveldb) BatchGet(start int) []LogEntry {
	entries := make([]LogEntry, 0)
	key := fmt.Sprintf("LogIndex_%d", start)
	iter := ldb.db.NewIterator(leveldbUtil.BytesPrefix([]byte(key)), nil)
	for iter.Next() {
		decbuf := bytes.NewBuffer(iter.Value())
		dec := gob.NewDecoder(decbuf)
		entry := &LogEntry{}
		if err := dec.Decode(entry); err != nil {
			panic(err)
		}
		entries = append(entries, *entry)
	}
	iter.Release()
	return entries
}

func (ldb *Leveldb) GetLog(index int) (*LogEntry, error) {
	key := fmt.Sprintf("LogIndex_%d", index)
	buf, err := ldb.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	decbuf := bytes.NewBuffer(buf)
	dec := gob.NewDecoder(decbuf)
	entry := &LogEntry{}
	if err := dec.Decode(entry); err != nil {
		return nil, err
	}
	return entry, nil
}

func (ldb *Leveldb) Get(key string) (string, error) {
	return ldb.db.Get([]byte(key), nil)
}

func (ldb *Leveldb) DeleteLog(start int, end int) {
	startKey := fmt.Sprintf("LogIndex_%d", start)
	endKey := fmt.Sprintf("LogIndex_%d", end)
	iter := ldb.db.NewIterator(&leveldbUtil.Range{Start: []byte(startKey), Limit: []byte(endKey)}, nil)
	for iter.Next() {
		ldb.db.Delete(iter.Key())
	}
	iter.Release()
}

func (ldb *Leveldb) Put(key, value string) {
}
