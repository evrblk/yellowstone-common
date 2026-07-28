package honey

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/evrblk/monstera/store"
	"github.com/stretchr/testify/require"
)

func TestReplicaPrefixRegistry_AssignsSequentiallyFromOne(t *testing.T) {
	s, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	defer s.Close()

	r := NewReplicaPrefixRegistry(s)

	p1, err := r.GetOrAssignPrefix("replica-a")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0x01}, p1, "first assignment must skip the reserved 0x0000")

	p2, err := r.GetOrAssignPrefix("replica-b")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0x02}, p2)

	p3, err := r.GetOrAssignPrefix("replica-c")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0x03}, p3)
}

func TestReplicaPrefixRegistry_Idempotent(t *testing.T) {
	s, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	defer s.Close()

	r := NewReplicaPrefixRegistry(s)

	first, err := r.GetOrAssignPrefix("replica-a")
	require.NoError(t, err)

	// Interleave another id, then ask for the first again.
	_, err = r.GetOrAssignPrefix("replica-b")
	require.NoError(t, err)

	again, err := r.GetOrAssignPrefix("replica-a")
	require.NoError(t, err)
	require.Equal(t, first, again, "same id must always resolve to the same prefix")
}

func TestReplicaPrefixRegistry_ReturnsCallerOwnedCopy(t *testing.T) {
	s, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	defer s.Close()

	r := NewReplicaPrefixRegistry(s)

	p, err := r.GetOrAssignPrefix("replica-a")
	require.NoError(t, err)
	p[0] = 0xff // mutating the returned slice must not corrupt stored state

	again, err := r.GetOrAssignPrefix("replica-a")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0x01}, again)
}

func TestReplicaPrefixRegistry_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	s1, err := store.NewBadgerStore(store.DefaultOptions(dir))
	require.NoError(t, err)

	r1 := NewReplicaPrefixRegistry(s1)
	pa, err := r1.GetOrAssignPrefix("replica-a")
	require.NoError(t, err)
	pb, err := r1.GetOrAssignPrefix("replica-b")
	require.NoError(t, err)
	s1.Close()

	// Reopen the same directory: assignments survive and the counter continues.
	s2, err := store.NewBadgerStore(store.DefaultOptions(dir))
	require.NoError(t, err)
	defer s2.Close()

	r2 := NewReplicaPrefixRegistry(s2)

	gotA, err := r2.GetOrAssignPrefix("replica-a")
	require.NoError(t, err)
	require.Equal(t, pa, gotA)

	gotB, err := r2.GetOrAssignPrefix("replica-b")
	require.NoError(t, err)
	require.Equal(t, pb, gotB)

	// A new id continues from where the persisted counter left off.
	pc, err := r2.GetOrAssignPrefix("replica-c")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0x03}, pc)
}

func TestReplicaPrefixRegistry_EmptyIdRejected(t *testing.T) {
	s, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	defer s.Close()

	r := NewReplicaPrefixRegistry(s)

	_, err = r.GetOrAssignPrefix("")
	require.Error(t, err)
}

func TestReplicaPrefixRegistry_AllPrefixes(t *testing.T) {
	s, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	defer s.Close()

	r := NewReplicaPrefixRegistry(s)

	pa, err := r.GetOrAssignPrefix("replica-a")
	require.NoError(t, err)
	pb, err := r.GetOrAssignPrefix("replica-b")
	require.NoError(t, err)

	all, err := r.AllPrefixes()
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{
		"replica-a": pa,
		"replica-b": pb,
	}, all)
}

func TestReplicaPrefixRegistry_Exhaustion(t *testing.T) {
	s, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	defer s.Close()

	r := NewReplicaPrefixRegistry(s)

	// Seed the counter at the last assignable value so the next new id overflows.
	maxed := make([]byte, 2)
	binary.BigEndian.PutUint16(maxed, uint16(maxAssignableReplicaPrefix))
	txn := s.Update()
	require.NoError(t, txn.Set(r.counterKey(), maxed))
	require.NoError(t, txn.Commit())

	_, err = r.GetOrAssignPrefix("replica-overflow")
	require.Error(t, err)

	// An already-assigned id still resolves even at the ceiling.
	txn = s.Update()
	require.NoError(t, txn.Set(r.assignmentKey("replica-existing"), []byte{0x12, 0x34}))
	require.NoError(t, txn.Commit())

	got, err := r.GetOrAssignPrefix("replica-existing")
	require.NoError(t, err)
	require.Equal(t, []byte{0x12, 0x34}, got)
}

func TestReplicaPrefixRegistry_ConcurrentAssignments(t *testing.T) {
	s, err := store.NewBadgerInMemoryStore()
	require.NoError(t, err)
	defer s.Close()

	r := NewReplicaPrefixRegistry(s)

	const distinctIds = 10
	const callers = 100

	var mu sync.Mutex
	seen := make(map[string]string) // id -> hex prefix

	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("replica-%d", i%distinctIds)
			p, err := r.GetOrAssignPrefix(id)
			require.NoError(t, err)

			mu.Lock()
			defer mu.Unlock()
			key := fmt.Sprintf("%x", p)
			if prev, ok := seen[id]; ok {
				require.Equal(t, prev, key, "id %s got two different prefixes", id)
			} else {
				seen[id] = key
			}
		}(i)
	}
	wg.Wait()

	// Exactly distinctIds prefixes, all unique.
	require.Len(t, seen, distinctIds)
	prefixes := make(map[string]bool)
	for _, p := range seen {
		require.False(t, prefixes[p], "duplicate prefix %s handed to two ids", p)
		prefixes[p] = true
	}
}
