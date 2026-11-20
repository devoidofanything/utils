package collections

import (
	"container/list"
	"sync"
)

/*
       _                 _     _
    __| | _____   _____ (_) __| |
   / _` |/ _ \ \ / / _ \| |/ _` |
  | (_| |  __/\ V / (_) | | (_| |
   \__,_|\___| \_/ \___/|_|\__,_|
*/

type indexContainer[V any] struct {
	V V
	e *list.Element // position of the key in insertion order
}

// SyncMap is a concurrent map that remembers insertion order, so the oldest
// entries can be evicted with DeleteBeginning.
type SyncMap[K comparable, V any] struct {
	m  map[K]indexContainer[V]
	ml *list.List // of K, oldest first
	mu sync.RWMutex
}

func NewSyncMap[K comparable, V any]() *SyncMap[K, V] {
	return &SyncMap[K, V]{
		m:  make(map[K]indexContainer[V]),
		ml: list.New(),
	}
}

func (s *SyncMap[K, V]) Apply(f func(k K, v V)) {
	s.mu.RLock()
	for key, value := range s.m {
		f(key, value.V)
	}
	s.mu.RUnlock()
}

func (s *SyncMap[K, V]) Add(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setUnsafe(key, value)
}

func (s *SyncMap[K, V]) AddUnsafe(key K, value V) {
	s.setUnsafe(key, value)
}

// setUnsafe inserts or replaces key, moving it to the end of the insertion order.
func (s *SyncMap[K, V]) setUnsafe(key K, value V) {
	if v, ok := s.m[key]; ok {
		s.ml.Remove(v.e)
	}
	s.m[key] = indexContainer[V]{V: value, e: s.ml.PushBack(key)}
}

func (s *SyncMap[K, V]) Has(key K) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.m[key]
	return ok
}

func (s *SyncMap[K, V]) Get(key K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	return v.V, ok
}

// HasOrCreate returns true if key already exists in the set, otherwise adds key to the set and returns false.
// If exist return true
func (s *SyncMap[K, V]) HasOrCreate(key K, value V) (V, bool) {
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	if ok {
		return v.V, true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok = s.m[key]
	if ok {
		return v.V, true
	}

	s.m[key] = indexContainer[V]{V: value, e: s.ml.PushBack(key)}
	return value, false
}

func (s *SyncMap[K, V]) Set(key K, value V) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setUnsafe(key, value)
}

func (s *SyncMap[K, V]) Delete(key K) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	if ok {
		delete(s.m, key)
		s.ml.Remove(v.e)
	}
}

func (s *SyncMap[K, V]) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m)
}

func (s *SyncMap[K, V]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m = make(map[K]indexContainer[V])
	s.ml.Init()
}

// Keys returns the keys in insertion order, oldest first.
func (s *SyncMap[K, V]) Keys() []K {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]K, 0, s.ml.Len())
	for e := s.ml.Front(); e != nil; e = e.Next() {
		keys = append(keys, e.Value.(K))
	}
	return keys
}

// DeleteBeginning removes up to limit oldest entries.
func (s *SyncMap[K, V]) DeleteBeginning(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < limit; i++ {
		e := s.ml.Front()
		if e == nil {
			return
		}
		delete(s.m, e.Value.(K))
		s.ml.Remove(e)
	}
}
