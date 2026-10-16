package mysql

import (
	"bytes"
	"container/list"
	"context"
	"encoding/gob"
	"fmt"
	"sync"
)

type metadataEntry struct {
	key  string
	data []byte
}
type MetadataCache struct {
	mu                                         sync.Mutex
	limit, used                                int64
	lru                                        *list.List
	entries                                    map[string]*list.Element
	hits, misses, evictions, oversized, clears uint64
}

type MetadataCacheStats struct {
	LimitBytes, UsedBytes                      int64
	Items                                      int
	Hits, Misses, Evictions, Oversized, Clears uint64
}

func (c *MetadataCache) Stats() MetadataCacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return MetadataCacheStats{c.limit, c.used, len(c.entries), c.hits, c.misses, c.evictions, c.oversized, c.clears}
}

func NewMetadataCache(limit int64) *MetadataCache {
	return &MetadataCache{limit: limit, lru: list.New(), entries: map[string]*list.Element{}}
}
func (c *MetadataCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used = 0
	c.lru.Init()
	c.entries = map[string]*list.Element{}
	c.clears++
}
func (c *MetadataCache) get(key string) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		c.misses++
		return nil
	}
	c.hits++
	c.lru.MoveToFront(e)
	return append([]byte(nil), e.Value.(metadataEntry).data...)
}
func (c *MetadataCache) put(key string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cost := int64(len(key) + len(data) + 128)
	if cost > c.limit {
		c.oversized++
		return
	}
	if e := c.entries[key]; e != nil {
		c.used -= int64(len(e.Value.(metadataEntry).key) + len(e.Value.(metadataEntry).data) + 128)
		c.lru.Remove(e)
		delete(c.entries, key)
	}
	for c.used+cost > c.limit {
		e := c.lru.Back()
		v := e.Value.(metadataEntry)
		c.used -= int64(len(v.key) + len(v.data) + 128)
		delete(c.entries, v.key)
		c.lru.Remove(e)
		c.evictions++
	}
	c.entries[key] = c.lru.PushFront(metadataEntry{key, append([]byte(nil), data...)})
	c.used += cost
}
func cachedMetadata[T any](ctx context.Context, c *Client, key string, load func(context.Context) (T, error)) (T, error) {
	key = metadataKey(c, key)
	var out T
	if c.metadata != nil {
		if data := c.metadata.get(key); data != nil {
			if e := gob.NewDecoder(bytes.NewReader(data)).Decode(&out); e == nil {
				return out, nil
			}
			c.metadata.Clear()
		}
	}
	out, e := load(ctx)
	if e != nil {
		return out, e
	} // Never cache absence or errors.
	if c.metadata != nil {
		var buf bytes.Buffer
		if e := gob.NewEncoder(&buf).Encode(out); e == nil {
			c.metadata.put(key, buf.Bytes())
		}
	}
	return out, nil
}

func metadataKey(c *Client, key string) string {
	return fmt.Sprintf("%s/%s/%d/schema1/%s", c.database, c.producer.Service, c.producer.Generation, key)
}
func cachedValue[T any](c *Client, key string) (out T, ok bool) {
	if c.metadata == nil {
		return out, false
	}
	data := c.metadata.get(metadataKey(c, key))
	if data == nil {
		return out, false
	}
	if e := gob.NewDecoder(bytes.NewReader(data)).Decode(&out); e != nil {
		c.metadata.Clear()
		return out, false
	}
	return out, true
}
func rememberValue[T any](c *Client, key string, value T) {
	if c.metadata == nil {
		return
	}
	var b bytes.Buffer
	if e := gob.NewEncoder(&b).Encode(value); e == nil {
		c.metadata.put(metadataKey(c, key), b.Bytes())
	}
}
func (c *Client) SetMetadataCache(cache *MetadataCache) { c.metadata = cache }

// WithProducer shares pools and immutable cache, with a fresh submission
// context. Concurrent workers never mutate the pool owner's producer fields.
func (c *Client) WithProducer(p Producer) *Client {
	out := &Client{db: c.db, database: c.database, readOnly: c.readOnly, family: c.family, producer: p, metadata: c.metadata}
	out.prefetchMinuteIndexes = c.prefetchMinuteIndexes
	out.discoveryEnabled.Store(c.discoveryEnabled.Load())
	return out
}
