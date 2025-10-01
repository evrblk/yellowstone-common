package cache

import (
	"testing"
	"time"
)

func TestGetSet(t *testing.T) {
	cycle := 100 * time.Millisecond
	c := New[string, string](0, cycle)
	defer c.Close()

	c.Set("sticky", "forever")
	c.SetWithTTL("hello", "Hello", cycle/2)
	hello, found := c.Get("hello")

	if !found {
		t.FailNow()
	}

	if *hello != "Hello" {
		t.FailNow()
	}

	time.Sleep(cycle / 2)

	_, found = c.Get("hello")

	if found {
		t.FailNow()
	}

	time.Sleep(cycle)

	_, found = c.Get("404")

	if found {
		t.FailNow()
	}

	_, found = c.Get("sticky")

	if !found {
		t.FailNow()
	}
}

func TestDelete(t *testing.T) {
	c := New[string, string](time.Hour, time.Minute)
	c.Set("hello", "Hello")
	_, found := c.Get("hello")

	if !found {
		t.FailNow()
	}

	c.Delete("hello")

	_, found = c.Get("hello")

	if found {
		t.FailNow()
	}
}

func BenchmarkNew(b *testing.B) {
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			New[string, string](0, 5*time.Second).Close()
		}
	})
}

func BenchmarkGet(b *testing.B) {
	c := New[string, string](0, 5*time.Second)
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
	c := New[string, string](0, 5*time.Second)
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
	c := New[string, string](0, 5*time.Second)
	defer c.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Delete("Hello")
		}
	})
}
