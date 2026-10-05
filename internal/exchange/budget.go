package exchange

import "sync/atomic"

type BufferBudget struct {
	Limit int64
	used  atomic.Int64
}

func (b *BufferBudget) Reserve(n int64) bool {
	if b == nil {
		return true
	}
	for {
		old := b.used.Load()
		if n < 0 || old+n > b.Limit {
			return false
		}
		if b.used.CompareAndSwap(old, old+n) {
			return true
		}
	}
}
func (b *BufferBudget) Release(n int64) {
	if b != nil {
		b.used.Add(-n)
	}
}
func (b *BufferBudget) Used() int64 {
	if b == nil {
		return 0
	}
	return b.used.Load()
}
