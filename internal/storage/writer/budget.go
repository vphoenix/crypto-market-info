package writer

import "sync"

type byteBudget struct {
	mu          sync.Mutex
	limit, used int64
}

func newByteBudget(limit int64) *byteBudget { return &byteBudget{limit: limit} }
func (b *byteBudget) TryAcquire(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || b.used+n > b.limit {
		return false
	}
	b.used += n
	return true
}
func (b *byteBudget) Release(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || n > b.used {
		panic("invalid preparation budget release")
	}
	b.used -= n
}

func (b *byteBudget) Used() int64 { b.mu.Lock(); defer b.mu.Unlock(); return b.used }
