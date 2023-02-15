package kvdatabase

import (
	"errors"
	"sync"
)

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
}

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

var ErrorNoSuchKey = errors.New("no such key")

var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

func Get(key string) (string, error) {
	store.RLock()
	v, ok := store.m[key]
	defer store.RUnlock()

	if !ok {
		return "", ErrorNoSuchKey
	}
	return v, nil
}

func Put(key string, value string) error {
	store.RLock()
	store.m[key] = value
	defer store.RUnlock()

	return nil
}

func Delete(key string) error {
	store.Lock()
	delete(store.m, key)
	defer store.Unlock()

	return nil
}
