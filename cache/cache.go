package cache

import (
	"sync"
	"time"
)

// Cache stores arbitrary data with expiration time.
type Cache[K comparable, V any] struct {
	items sync.Map
	ttl   time.Duration
	close chan struct{}
}

// An item represents arbitrary data with expiration time.
type item[V any] struct {
	data    V
	expires int64
}

// New creates a new cache that asynchronously cleans
// expired entries after the given time passes.
func New[K comparable, V any](ttl time.Duration, cleaningInterval time.Duration) *Cache[K, V] {
	cache := &Cache[K, V]{
		close: make(chan struct{}),
		ttl:   ttl,
	}

	go func() {
		ticker := time.NewTicker(cleaningInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				now := time.Now().UnixNano()

				cache.items.Range(func(key, value interface{}) bool {
					item := value.(item[V])

					if item.expires > 0 && now > item.expires {
						cache.items.Delete(key)
					}

					return true
				})

			case <-cache.close:
				return
			}
		}
	}()

	return cache
}

// Get gets the value for the given key.
func (cache *Cache[K, V]) Get(key K) (*V, bool) {
	obj, exists := cache.items.Load(key)

	if !exists {
		return nil, false
	}

	item := obj.(item[V])

	if item.expires > 0 && time.Now().UnixNano() > item.expires {
		return nil, false
	}

	return &item.data, true
}

// Set sets a value for the given key with default ttl
func (cache *Cache[K, V]) Set(key K, value V) {
	cache.SetWithTTL(key, value, cache.ttl)
}

// SetWithTTL sets a value for the given key with a given ttl
// If the ttl is 0 or less, it will be stored forever.
func (cache *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	var expires int64

	if ttl > 0 {
		expires = time.Now().Add(ttl).UnixNano()
	}

	cache.items.Store(key, item[V]{
		data:    value,
		expires: expires,
	})
}

// Delete deletes the key and its value from the cache.
func (cache *Cache[K, V]) Delete(key K) {
	cache.items.Delete(key)
}

// Close closes the cache and frees up resources.
func (cache *Cache[K, V]) Close() {
	cache.close <- struct{}{}
	cache.items = sync.Map{}
}
