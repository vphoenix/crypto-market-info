package bybit

import (
	"fmt"
	"sync"
)

// connectionTerminal serializes message application with terminal reader
// failures. Once fail wins the lock, no queued message can mutate state again.
type connectionTerminal struct {
	mu  sync.Mutex
	err error
}

func (t *connectionTerminal) fail(err error, invalidate func(error)) error {
	if err == nil {
		err = fmt.Errorf("Bybit websocket reader stopped")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err == nil {
		t.err = err
		if invalidate != nil {
			invalidate(err)
		}
	}
	return t.err
}

func (t *connectionTerminal) withActive(apply func() error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return t.err
	}
	return apply()
}

func (t *connectionTerminal) current() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}
