package cache

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Cache stores arbitrary data with expiration time.
type Cache[K comparable, V any] struct {
	items            sync.Map
	ttl              time.Duration
	negativeTTL      time.Duration
	isCacheableError func(error) bool
	close            chan struct{}
	closeOnce        sync.Once
	group            singleflight.Group
}

// An item represents arbitrary data with expiration time. A non-nil err marks
// a negatively cached entry, in which case data holds the zero value.
type item[V any] struct {
	data    V
	err     error
	expires int64
}

// options holds the configuration applied by Option values in New.
type options struct {
	ttl              time.Duration
	negativeTTL      time.Duration
	cleaningInterval time.Duration
	isCacheableError func(error) bool
}

// An Option configures a Cache in New.
type Option func(*options)

// WithTTL sets the default time-to-live for positive entries. A value of 0 or
// less means entries never expire.
func WithTTL(ttl time.Duration) Option {
	return func(o *options) { o.ttl = ttl }
}

// WithNegativeTTL sets the time-to-live for negatively cached entries (errors
// classified as cacheable by WithCacheableError). A value of 0 or less means
// they never expire; prefer a short duration so transient absences recover
// quickly. Has no effect unless WithCacheableError is also set.
func WithNegativeTTL(ttl time.Duration) Option {
	return func(o *options) { o.negativeTTL = ttl }
}

// WithCleaningInterval sets how often expired entries are swept in the
// background. Defaults to time.Minute.
func WithCleaningInterval(interval time.Duration) Option {
	return func(o *options) { o.cleaningInterval = interval }
}

// WithCacheableError enables negative caching. When set, GetOrLoad caches the
// errors for which isCacheableError returns true for the WithNegativeTTL
// duration, so subsequent calls return the cached error without re-running
// load. If nil (the default), errors are never cached.
func WithCacheableError(isCacheableError func(error) bool) Option {
	return func(o *options) { o.isCacheableError = isCacheableError }
}

// New creates a new cache that asynchronously cleans expired entries on the
// configured cleaning interval.
func New[K comparable, V any](opts ...Option) *Cache[K, V] {
	o := options{
		cleaningInterval: time.Minute,
	}
	for _, opt := range opts {
		opt(&o)
	}

	cache := &Cache[K, V]{
		close:            make(chan struct{}),
		ttl:              o.ttl,
		negativeTTL:      o.negativeTTL,
		isCacheableError: o.isCacheableError,
	}

	go func() {
		ticker := time.NewTicker(o.cleaningInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				now := time.Now().UnixNano()

				cache.items.Range(func(key, value any) bool {
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

// getItem returns the live (non-expired) entry for the key, if any. The
// returned item may be a negative entry (item.err != nil).
func (cache *Cache[K, V]) getItem(key K) (item[V], bool) {
	obj, exists := cache.items.Load(key)

	if !exists {
		return item[V]{}, false
	}

	it := obj.(item[V])

	if it.expires > 0 && time.Now().UnixNano() > it.expires {
		return item[V]{}, false
	}

	return it, true
}

// Get gets the value for the given key. The second return value reports
// whether a live (non-expired) positive entry was found. Negatively cached
// entries are reported as not found.
func (cache *Cache[K, V]) Get(key K) (V, bool) {
	it, ok := cache.getItem(key)

	if !ok || it.err != nil {
		var zero V
		return zero, false
	}

	return it.data, true
}

// GetOrLoad returns the cached value for the given key. On a miss it calls
// load to produce the value, stores it with the default ttl, and returns it.
// Concurrent callers for the same key are deduplicated: load runs exactly
// once and all callers share its result.
//
// If load returns an error, it is returned to every caller. The error is
// cached (as a negative entry with the negative ttl) only when the cache was
// created WithCacheableError and that predicate reports the error cacheable;
// otherwise nothing is stored and load runs again on the next call.
func (cache *Cache[K, V]) GetOrLoad(key K, load func() (V, error)) (V, error) {
	if it, ok := cache.getItem(key); ok {
		return it.data, it.err
	}

	v, err, _ := cache.group.Do(fmt.Sprint(key), func() (any, error) {
		// Another flight may have populated the key while we waited.
		if it, ok := cache.getItem(key); ok {
			return it.data, it.err
		}

		v, err := load()
		if err != nil {
			if cache.isCacheableError != nil && cache.isCacheableError(err) {
				cache.setNegative(key, err)
			}
			return v, err
		}

		cache.Set(key, v)
		return v, nil
	})

	if err != nil {
		var zero V
		return zero, err
	}

	return v.(V), nil
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

// setNegative stores a negative entry for the given key with the negative ttl.
func (cache *Cache[K, V]) setNegative(key K, err error) {
	var expires int64

	if cache.negativeTTL > 0 {
		expires = time.Now().Add(cache.negativeTTL).UnixNano()
	}

	cache.items.Store(key, item[V]{
		err:     err,
		expires: expires,
	})
}

// Delete deletes the key and its value from the cache.
func (cache *Cache[K, V]) Delete(key K) {
	cache.items.Delete(key)
}

// Close closes the cache and frees up resources. It is safe to call
// multiple times.
func (cache *Cache[K, V]) Close() {
	cache.closeOnce.Do(func() {
		close(cache.close)
	})
}
