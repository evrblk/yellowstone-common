package cache

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetSet(t *testing.T) {
	cycle := 100 * time.Millisecond
	c := New[string, string](WithCleaningInterval(cycle))
	defer c.Close()

	c.Set("sticky", "forever")
	c.SetWithTTL("hello", "Hello", cycle/2)
	hello, found := c.Get("hello")

	require.True(t, found)
	require.Equal(t, "Hello", hello)

	time.Sleep(cycle / 2)

	_, found = c.Get("hello")
	require.False(t, found)

	time.Sleep(cycle)

	_, found = c.Get("404")
	require.False(t, found)

	_, found = c.Get("sticky")
	require.True(t, found)
}

func TestDelete(t *testing.T) {
	c := New[string, string](WithTTL(time.Hour), WithCleaningInterval(time.Minute))
	c.Set("hello", "Hello")
	_, found := c.Get("hello")
	require.True(t, found)

	c.Delete("hello")

	_, found = c.Get("hello")
	require.False(t, found)
}

func TestGetOrLoad(t *testing.T) {
	c := New[string, string](WithTTL(time.Hour), WithCleaningInterval(time.Minute))
	defer c.Close()

	var calls int32

	load := func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "loaded", nil
	}

	// First call loads and caches.
	v, err := c.GetOrLoad("key", load)
	require.NoError(t, err)
	require.Equal(t, "loaded", v)

	// Second call is a cache hit; load must not run again.
	v, err = c.GetOrLoad("key", load)
	require.NoError(t, err)
	require.Equal(t, "loaded", v)

	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestGetOrLoadError(t *testing.T) {
	c := New[string, string](WithTTL(time.Hour), WithCleaningInterval(time.Minute))
	defer c.Close()

	wantErr := errors.New("boom")

	v, err := c.GetOrLoad("key", func() (string, error) {
		return "", wantErr
	})
	require.ErrorIs(t, err, wantErr)
	require.Empty(t, v)

	// A failed load must not be cached.
	_, found := c.Get("key")
	require.False(t, found, "errored load should not populate the cache")
}

func TestGetOrLoadNegativeCaching(t *testing.T) {
	notFound := errors.New("not found")

	c := New[string, string](
		WithTTL(time.Hour),
		WithNegativeTTL(time.Hour),
		WithCleaningInterval(time.Minute),
		WithCacheableError(func(err error) bool {
			return errors.Is(err, notFound)
		}),
	)
	defer c.Close()

	var calls int32
	load := func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "", notFound
	}

	// First call runs load and caches the error negatively.
	_, err := c.GetOrLoad("key", load)
	require.ErrorIs(t, err, notFound)

	// Second call returns the cached error without re-running load.
	_, err = c.GetOrLoad("key", load)
	require.ErrorIs(t, err, notFound)

	require.Equal(t, int32(1), atomic.LoadInt32(&calls),
		"cacheable error should not trigger a second load")

	// A negative entry is not visible as a positive value via Get.
	_, found := c.Get("key")
	require.False(t, found)
}

func TestGetOrLoadNonCacheableError(t *testing.T) {
	cacheable := errors.New("cacheable")
	transient := errors.New("transient")

	c := New[string, string](
		WithNegativeTTL(time.Hour),
		WithCleaningInterval(time.Minute),
		WithCacheableError(func(err error) bool {
			return errors.Is(err, cacheable)
		}),
	)
	defer c.Close()

	var calls int32
	load := func() (string, error) {
		atomic.AddInt32(&calls, 1)
		return "", transient
	}

	_, err := c.GetOrLoad("key", load)
	require.ErrorIs(t, err, transient)

	// The error is not cacheable, so load must run again.
	_, err = c.GetOrLoad("key", load)
	require.ErrorIs(t, err, transient)

	require.Equal(t, int32(2), atomic.LoadInt32(&calls),
		"non-cacheable error should trigger a second load")
}

func TestGetOrLoadSingleFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New[string, string](WithTTL(time.Hour), WithCleaningInterval(time.Minute))
		defer c.Close()

		var calls int32
		release := make(chan struct{})

		load := func() (string, error) {
			atomic.AddInt32(&calls, 1)
			<-release // hold the flight open so concurrent callers pile up
			return "loaded", nil
		}

		const goroutines = 50
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				v, err := c.GetOrLoad("key", load)
				assert.NoError(t, err)
				assert.Equal(t, "loaded", v)
			}()
		}

		// Wait until every caller is durably blocked on the single flight
		// (one inside load, the rest on singleflight's WaitGroup), then release.
		synctest.Wait()
		close(release)
		wg.Wait()

		require.Equal(t, int32(1), atomic.LoadInt32(&calls),
			"thundering herd not deduplicated")
	})
}

func TestCloseIdempotent(t *testing.T) {
	c := New[string, string](WithTTL(time.Hour), WithCleaningInterval(time.Minute))
	c.Close()
	c.Close() // must not panic or block
}

func BenchmarkNew(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			New[string, string](WithCleaningInterval(5 * time.Second)).Close()
		}
	})
}

func BenchmarkGet(b *testing.B) {
	c := New[string, string](WithCleaningInterval(5 * time.Second))
	defer c.Close()
	c.Set("Hello", "World")

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Get("Hello")
		}
	})
}

func BenchmarkSet(b *testing.B) {
	c := New[string, string](WithCleaningInterval(5 * time.Second))
	defer c.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Set("Hello", "World")
		}
	})
}

func BenchmarkDelete(b *testing.B) {
	c := New[string, string](WithCleaningInterval(5 * time.Second))
	defer c.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Delete("Hello")
		}
	})
}
