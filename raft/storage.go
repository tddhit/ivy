package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbUtil "github.com/syndtr/goleveldb/leveldb/util"
)

type Storage interface {
	Put(key, value string)
	Get(key string) (string, error)
	PutLog(entry *LogEntry) (err error)
	GetLog(index int) (*LogEntry, error)
	DeleteLog(start int, end int)
	GetRangeLog(start, end int) []LogEntry
	GetFirstAndLastLog() (*LogEntry, *LogEntry)
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

func (ldb *Leveldb) Put(key, value string) {
	ldb.db.Put([]byte(key), []byte(value), nil)
}

func (ldb *Leveldb) Get(key string) (string, error) {
	value, err := ldb.db.Get([]byte(key), nil)
	return string(value), err
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

func (ldb *Leveldb) DeleteLog(start int, end int) {
	startKey := fmt.Sprintf("LogIndex_%d", start)
	endKey := fmt.Sprintf("LogIndex_%d", end)
	iter := ldb.db.NewIterator(&leveldbUtil.Range{Start: []byte(startKey), Limit: []byte(endKey)}, nil)
	for iter.Next() {
		ldb.db.Delete(iter.Key(), nil)
	}
	iter.Release()
}

func (ldb *Leveldb) GetRangeLog(start, end int) []LogEntry {
	entries := make([]LogEntry, 0)
	startKey := fmt.Sprintf("LogIndex_%d", start)
	endKey := fmt.Sprintf("LogIndex_%d", end)
	iter := ldb.db.NewIterator(&leveldbUtil.Range{Start: []byte(startKey), Limit: []byte(endKey)}, nil)
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

func (ldb *Leveldb) GetFirstAndLastLog() (*LogEntry, *LogEntry) {
	iter := ldb.db.NewIterator(leveldbUtil.BytesPrefix([]byte("LogIndex_")), nil)
	iter.First()
	decbuf := bytes.NewBuffer(iter.Value())
	dec := gob.NewDecoder(decbuf)
	firstEntry := &LogEntry{}
	if err := dec.Decode(firstEntry); err != nil {
		panic(err)
	}
	iter.Last()
	decbuf = bytes.NewBuffer(iter.Value())
	dec = gob.NewDecoder(decbuf)
	lastEntry := &LogEntry{}
	if err := dec.Decode(lastEntry); err != nil {
		panic(err)
	}
	iter.Release()
	return firstEntry, lastEntry
}
