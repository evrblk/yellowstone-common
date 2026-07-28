package honey

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/evrblk/monstera/store"
)

// replicaRegistryPrefix is the two-byte keyspace reserved at the front of a
// shared BadgerStore for the ReplicaPrefixRegistry. Everything the registry
// persists — the prefix assignments, the counter, and any future node-specific
// info — lives under these two bytes.
//
// It is also the prefix 0x0000 of the two-byte prefix space that the registry
// hands out: the registry keeps 0x0000 for itself and assigns 0x0001 and up to
// replicas. Anything else sharing the same store MUST NOT write keys beginning
// with 0x0000; every other range is available.
var replicaRegistryPrefix = []byte{0x00, 0x00}

// Sub-namespace bytes under replicaRegistryPrefix. A new kind of node-local
// state gets a new byte here without disturbing the existing ranges.
const (
	// replicaRegistryCategoryAssignments maps a replica id to its assigned
	// two-byte prefix.
	replicaRegistryCategoryAssignments byte = 0x01
	// replicaRegistryCategoryCounter holds the last two-byte prefix handed out.
	replicaRegistryCategoryCounter byte = 0x02
)

// maxAssignableReplicaPrefix is the largest prefix the registry can assign.
// 0x0000 is reserved for the registry itself, so assignments run 0x0001..0xffff.
const maxAssignableReplicaPrefix = 0xffff

// ReplicaPrefixRegistry is a node-local, on-disk registry that assigns a short,
// stable prefix to every replica placed on the node. Each replica namespaces
// all of its data in the shared store under its two-byte prefix, keeping
// otherwise co-located cores from colliding.
//
// It partitions the shared keyspace by replica (at runtime, per node). A core
// further partitions its own slice by table, using a leading table-prefix byte
// defined as a local constant; because each core owns a distinct replica prefix
// those table prefixes only need to be unique within a single core, and the two
// partitions never overlap in a stored key.
//
// Prefixes are assigned sequentially starting at 0x0001, persisted, and
// idempotent: the same replica id always resolves to the same prefix across
// restarts, so a replica's data is found again after a reboot. Assignments are
// node-local — a prefix means nothing outside this node and may freely collide
// with the prefixes handed out on other nodes.
//
// The registry reserves the two-byte prefix 0x0000 (see replicaRegistryPrefix)
// for its own bookkeeping and for future node-specific state. Because the shared
// store runs with conflict detection disabled, GetOrAssignPrefix serializes its
// read-modify-write under a mutex rather than relying on transaction conflicts;
// use a single ReplicaPrefixRegistry instance per store.
type ReplicaPrefixRegistry struct {
	store *store.BadgerStore
	mu    sync.Mutex
}

// NewReplicaPrefixRegistry returns a registry that persists into the given
// shared store. There must be at most one instance per store (see the type doc).
func NewReplicaPrefixRegistry(store *store.BadgerStore) *ReplicaPrefixRegistry {
	return &ReplicaPrefixRegistry{store: store}
}

// GetOrAssignPrefix returns the two-byte prefix assigned to replicaId, assigning
// the next sequential prefix on first call and persisting it. It is idempotent:
// the same id always returns the same prefix, and the returned slice is a fresh
// copy owned by the caller. It errors only on a store failure or once the prefix
// space (65535 entries) is exhausted.
func (r *ReplicaPrefixRegistry) GetOrAssignPrefix(replicaId string) ([]byte, error) {
	if replicaId == "" {
		return nil, errors.New("replica id must not be empty")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	assignmentKey := r.assignmentKey(replicaId)

	txn := r.store.Update()
	defer txn.Discard()

	// Already assigned: hand back the stored prefix unchanged.
	existing, err := txn.Get(assignmentKey)
	if err == nil {
		return append([]byte(nil), existing...), nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, fmt.Errorf("reading prefix for %q: %w", replicaId, err)
	}

	// First time seeing this id: bump the counter and record the assignment in
	// the same transaction so the two never drift apart.
	last, err := r.readCounter(txn)
	if err != nil {
		return nil, err
	}
	next := last + 1
	if next > maxAssignableReplicaPrefix {
		return nil, fmt.Errorf("replica prefix space exhausted (%d assignments)", maxAssignableReplicaPrefix)
	}

	prefix := make([]byte, 2)
	binary.BigEndian.PutUint16(prefix, uint16(next))

	if err := txn.Set(assignmentKey, prefix); err != nil {
		return nil, fmt.Errorf("writing prefix for %q: %w", replicaId, err)
	}
	if err := txn.Set(r.counterKey(), prefix); err != nil {
		return nil, fmt.Errorf("writing counter: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return nil, fmt.Errorf("committing prefix for %q: %w", replicaId, err)
	}

	return append([]byte(nil), prefix...), nil
}

// AllPrefixes returns a snapshot of every replica id → prefix assignment, for
// inspection and debugging. The prefix slices are copies owned by the caller.
func (r *ReplicaPrefixRegistry) AllPrefixes() (map[string][]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	catPrefix := r.categoryKey(replicaRegistryCategoryAssignments)
	result := make(map[string][]byte)

	txn := r.store.View()
	defer txn.Discard()

	err := txn.EachPrefix(catPrefix, func(key []byte, value []byte) (bool, error) {
		id := string(key[len(catPrefix):])
		result[id] = append([]byte(nil), value...)
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// readCounter returns the last prefix handed out, or 0 when nothing has been
// assigned yet (0x0000 is reserved for the registry, so the first assignment is
// 0x0001).
func (r *ReplicaPrefixRegistry) readCounter(txn *store.Txn) (int, error) {
	v, err := txn.Get(r.counterKey())
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("reading counter: %w", err)
	}
	return int(binary.BigEndian.Uint16(v)), nil
}

// assignmentKey builds the key holding the prefix assigned to replicaId:
// replicaRegistryPrefix | replicaRegistryCategoryAssignments | replicaId.
func (r *ReplicaPrefixRegistry) assignmentKey(replicaId string) []byte {
	key := r.categoryKey(replicaRegistryCategoryAssignments)
	return append(key, replicaId...)
}

// counterKey builds the key holding the last-assigned prefix counter.
func (r *ReplicaPrefixRegistry) counterKey() []byte {
	return r.categoryKey(replicaRegistryCategoryCounter)
}

// categoryKey builds replicaRegistryPrefix | category with spare capacity so
// callers can append an id without reallocating.
func (r *ReplicaPrefixRegistry) categoryKey(category byte) []byte {
	key := make([]byte, 0, len(replicaRegistryPrefix)+1+16)
	key = append(key, replicaRegistryPrefix...)
	key = append(key, category)
	return key
}
