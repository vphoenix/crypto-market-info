package justlendkeeper

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

func temporaryStorage(err error) bool {
	var transient interface{ Temporary() bool }
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &transient) && transient.Temporary())
}

func (c *Collector) deferStorageRetry(err error) time.Duration {
	c.storageFailures++
	delay := min(5*time.Second<<min(c.storageFailures-1, 4), time.Minute)
	delay += time.Duration(rand.IntN(501)) * time.Millisecond
	c.storageRetryAt = c.Now().Add(delay)
	c.State.NextEnrichment = c.storageRetryAt
	c.Log(fmt.Sprintf("storage retry deferred failures=%d delay=%s error=%v", c.storageFailures, delay, err))
	return delay
}
