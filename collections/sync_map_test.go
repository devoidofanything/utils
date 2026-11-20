package collections

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Deleting from the middle used to leave later entries with stale slice indexes,
// which removed the wrong keys and eventually panicked with "slice bounds out of range".
func TestSyncMapDeleteKeepsOrder(t *testing.T) {
	m := NewSyncMap[int, int]()
	for i := 0; i < 10; i++ {
		m.Add(i, i)
	}
	m.Delete(2)
	m.Delete(5)
	m.Set(3, 30) // moves 3 to the end
	m.Delete(9)
	m.Delete(8)
	assert.Equal(t, []int{0, 1, 4, 6, 7, 3}, m.Keys())

	m.DeleteBeginning(2)
	assert.Equal(t, []int{4, 6, 7, 3}, m.Keys())
	assert.Equal(t, 4, m.Count())
	assert.False(t, m.Has(0))
	v, ok := m.Get(3)
	assert.True(t, ok)
	assert.Equal(t, 30, v)

	m.DeleteBeginning(100) // more than present
	assert.Equal(t, 0, m.Count())
}

func TestSyncMapChurnNoPanic(t *testing.T) {
	m := NewSyncMap[int, int]()
	for i := 0; i < 200_000; i++ {
		m.Add(i, i)
		if i%3 == 0 {
			m.Delete(i / 2)
		}
		if m.Count() >= 50_000 {
			m.DeleteBeginning(10_000)
		}
	}
	assert.Equal(t, m.Count(), len(m.Keys()))
}
