package honey

import (
	"bytes"
	"encoding"
	"encoding/binary"

	"github.com/evrblk/monstera/store"
	"github.com/evrblk/monstera/utils"
)

// ptr is a generic constraint for binary-serializable messages (pointers).
// A protobuf implementation (google, gogo, etc.) or custom type implementing
// encoding.BinaryMarshaler and encoding.BinaryUnmarshaler satisfies this constraint.
type ptr[T any] interface {
	*T
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

type PaginationToken struct {
	Key     []byte
	Reverse bool
}

// table helps to work with a KV store without worrying about tableId prefixes.
//
// The tableId prefix is the table's entire key range: every key lives under it
// and nothing else does. This is the exclusive-store layout — each owning core
// nests a node-local replica prefix into its table ids, so a table id is unique
// to one core and its whole range belongs to that core. There is no in-table
// key-range restriction.
type table struct {
	// tableId is a unique prefix that is used to isolate tables on a shared Badger store.
	tableId []byte
}

func newTable(tableId []byte) table {
	if len(tableId) == 0 {
		panic("tableId must not be empty")
	}

	return table{
		tableId: tableId,
	}
}

func (t *table) get(txn *store.Txn, key []byte) ([]byte, error) {
	return txn.Get(t.getFullKey(key))
}

func (t *table) set(txn *store.Txn, key []byte, value []byte) error {
	return txn.Set(t.getFullKey(key), value)
}

func (t *table) delete(txn *store.Txn, key []byte) error {
	return txn.Delete(t.getFullKey(key))
}

func (t *table) prefixExists(txn *store.Txn, prefix []byte) (bool, error) {
	return txn.PrefixExists(t.getFullKey(prefix))
}

func (t *table) eachPrefix(txn *store.Txn, prefix []byte, fn func(key []byte, value []byte) (bool, error)) error {
	return txn.EachPrefix(t.getFullKey(prefix), func(key []byte, value []byte) (bool, error) {
		return fn(key[len(t.tableId):], value)
	})
}

func (t *table) eachPrefixKeys(txn *store.Txn, prefix []byte, fn func(key []byte) (bool, error)) error {
	return txn.EachPrefixKeys(t.getFullKey(prefix), func(key []byte) (bool, error) {
		return fn(key[len(t.tableId):])
	})
}

func (t *table) listInRange(txn *store.Txn, lowerBound []byte, upperBound []byte, reverse bool, fn func(key []byte, value []byte) (bool, error)) error {
	var lower []byte
	if lowerBound != nil {
		lower = t.getFullKey(lowerBound)
	} else {
		lower = t.tableId
	}

	var upper []byte
	if upperBound != nil {
		upper = t.getFullKey(upperBound)
	} else {
		upper = t.tableId
	}

	return txn.EachRange(lower, upper, reverse, func(key []byte, value []byte) (bool, error) {
		return fn(key[len(t.tableId):], value)
	})
}

type rawListPaginatedResult struct {
	Values                  [][]byte
	Keys                    [][]byte
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *table) listPrefixedPaginated(txn *store.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*rawListPaginatedResult, error) {
	result := &rawListPaginatedResult{
		Values: make([][]byte, 0),
		Keys:   make([][]byte, 0),
	}

	if paginationToken == nil {
		err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
			if len(result.Values) == limit {
				result.NextPaginationToken = &PaginationToken{
					Key:     bytes.Clone(key),
					Reverse: false,
				}
				return false, nil
			} else {
				result.Values = append(result.Values, value)
				result.Keys = append(result.Keys, bytes.Clone(key))
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	} else if !paginationToken.Reverse {
		err := t.listInRange(txn, nil, paginationToken.Key, true, func(key []byte, value []byte) (bool, error) {
			if !bytes.HasPrefix(key, prefix) {
				return false, nil
			}

			if bytes.Equal(key, paginationToken.Key) {
				return true, nil
			}

			result.PreviousPaginationToken = &PaginationToken{
				Key:     bytes.Clone(key),
				Reverse: true,
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}

		err = t.listInRange(txn, paginationToken.Key, nil, false, func(key []byte, value []byte) (bool, error) {
			if !bytes.HasPrefix(key, prefix) {
				return false, nil
			}

			if len(result.Values) == limit {
				result.NextPaginationToken = &PaginationToken{
					Key:     bytes.Clone(key),
					Reverse: false,
				}
				return false, nil
			} else {
				result.Values = append(result.Values, value)
				result.Keys = append(result.Keys, bytes.Clone(key))
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	} else {
		err := t.listInRange(txn, paginationToken.Key, nil, false, func(key []byte, value []byte) (bool, error) {
			if !bytes.HasPrefix(key, prefix) {
				return false, nil
			}

			if bytes.Equal(key, paginationToken.Key) {
				return true, nil
			}

			result.NextPaginationToken = &PaginationToken{
				Key:     bytes.Clone(key),
				Reverse: false,
			}
			return false, nil
		})
		if err != nil {
			return nil, err
		}

		err = t.listInRange(txn, nil, paginationToken.Key, true, func(key []byte, value []byte) (bool, error) {
			if !bytes.HasPrefix(key, prefix) {
				return false, nil
			}

			if len(result.Values) == limit {
				result.PreviousPaginationToken = &PaginationToken{
					Key:     bytes.Clone(key),
					Reverse: true,
				}
				return false, nil
			} else {
				result.Values = append(result.Values, value)
				result.Keys = append(result.Keys, bytes.Clone(key))
				return true, nil
			}
		})
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (t *table) getFullKey(key []byte) []byte {
	return utils.ConcatBytes(t.tableId, key)
}

// TableId returns the table's id prefix; every key of the table lives under
// it. Used by owners to clear a table wholesale (DropPrefix).
func (t *table) TableId() []byte {
	return t.tableId
}

// EachEntry iterates every row of the table in key order, invoking fn with the
// table-relative key (the table id prefix stripped) and the raw stored value.
// This is the raw building block for portable snapshots, which must not leak
// the table id into the stream.
func (t *table) EachEntry(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error {
	return t.listInRange(txn, nil, nil, false, fn)
}

// BinaryTable is table with a composite key: primary key PK and secondary key SK
// and generic values (of encoding.BinaryMarshaler type).
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type BinaryTable[T ptr[U], U any] struct {
	table
}

func NewBinaryTable[T ptr[U], U any](tableId []byte) *BinaryTable[T, U] {
	return &BinaryTable[T, U]{
		table: newTable(tableId),
	}
}

func (t *BinaryTable[T, U]) Get(txn *store.Txn, key []byte) (T, error) {
	value, err := t.get(txn, key)
	if err != nil {
		return nil, err
	}

	var message U
	if err := T(&message).UnmarshalBinary(value); err != nil {
		return nil, err
	}
	return &message, nil
}

func (t *BinaryTable[T, U]) Set(txn *store.Txn, key []byte, message T) error {
	value, err := message.MarshalBinary()
	if err != nil {
		return err
	}

	return t.set(txn, key, value)
}

func (t *BinaryTable[T, U]) Delete(txn *store.Txn, key []byte) error {
	return t.delete(txn, key)
}

func (t *BinaryTable[T, U]) ListAll(txn *store.Txn, prefix []byte) ([]T, error) {
	result := make([]T, 0)
	err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
		var message U
		if err := T(&message).UnmarshalBinary(value); err != nil {
			return false, err
		}

		result = append(result, T(&message))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (t *BinaryTable[T, U]) ListInRange(txn *store.Txn, lowerBound []byte, upperBound []byte, reverse bool, fn func(message T) (bool, error)) error {
	return t.listInRange(txn, lowerBound, upperBound, reverse, func(key []byte, value []byte) (bool, error) {
		var message U
		if err := T(&message).UnmarshalBinary(value); err != nil {
			return false, err
		}

		return fn(&message)
	})
}

type ListPaginatedProtobufResult[T ptr[U], U any] struct {
	Items                   []T
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *BinaryTable[T, U]) ListPaginated(txn *store.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedProtobufResult[T, U], error) {
	rawResult, err := t.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedProtobufResult[T, U]{
		Items:                   make([]T, len(rawResult.Values)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, value := range rawResult.Values {
		var message U
		if err := T(&message).UnmarshalBinary(value); err != nil {
			return nil, err
		}
		result.Items[i] = T(&message)
	}

	return result, nil
}

// StringTable is table with a composite key: primary key PK and secondary key SK
// and string values.
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type StringTable struct {
	table
}

func NewStringTable(tableId []byte) *StringTable {
	return &StringTable{
		table: newTable(tableId),
	}
}

func (t *StringTable) Get(txn *store.Txn, key []byte) (string, error) {
	value, err := t.get(txn, key)
	if err != nil {
		return "", err
	}

	return string(value), nil
}

func (t *StringTable) Set(txn *store.Txn, key []byte, value string) error {
	return t.set(txn, key, []byte(value))
}

func (t *StringTable) Delete(txn *store.Txn, key []byte) error {
	return t.delete(txn, key)
}

func (t *StringTable) ListAll(txn *store.Txn, prefix []byte) ([]string, error) {
	result := make([]string, 0)
	err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
		result = append(result, string(value))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (t *StringTable) ListInRange(txn *store.Txn, lowerBound []byte, upperBound []byte, reverse bool, fn func(value string) (bool, error)) error {
	return t.listInRange(txn, lowerBound, upperBound, reverse, func(key []byte, value []byte) (bool, error) {
		return fn(string(value))
	})
}

type ListPaginatedStringResult struct {
	Items                   []string
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *StringTable) ListPaginated(txn *store.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedStringResult, error) {
	rawResult, err := t.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedStringResult{
		Items:                   make([]string, len(rawResult.Values)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, value := range rawResult.Values {
		result.Items[i] = string(value)
	}

	return result, nil
}

// Uint64Table is table with a composite key: primary key PK and secondary key SK
// and uint64 values.
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type Uint64Table struct {
	table
}

func NewUint64Table(tableId []byte) *Uint64Table {
	return &Uint64Table{
		table: newTable(tableId),
	}
}

func (t *Uint64Table) Get(txn *store.Txn, key []byte) (uint64, error) {
	value, err := t.get(txn, key)
	if err != nil {
		return 0, err
	}

	return bytesToUint64(value), nil
}

func (t *Uint64Table) Set(txn *store.Txn, key []byte, value uint64) error {
	return t.set(txn, key, uint64ToBytes(value))
}

func (t *Uint64Table) Delete(txn *store.Txn, key []byte) error {
	return t.delete(txn, key)
}

func (t *Uint64Table) ListAll(txn *store.Txn, prefix []byte) ([]uint64, error) {
	result := make([]uint64, 0)
	err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
		result = append(result, bytesToUint64(value))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

type ListPaginatedUint64Result struct {
	Items                   []uint64
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *Uint64Table) ListPaginated(txn *store.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedUint64Result, error) {
	rawResult, err := t.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedUint64Result{
		Items:                   make([]uint64, len(rawResult.Values)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, value := range rawResult.Values {
		result.Items[i] = bytesToUint64(value)
	}

	return result, nil
}

// Uint32Table is table with a composite key: primary key PK and secondary key SK
// and uint32 values.
// Get, Set, Delete operations use PK+SK to refer records. List operation uses PK as a prefix.
type Uint32Table struct {
	table
}

func NewUint32Table(tableId []byte) *Uint32Table {
	return &Uint32Table{
		table: newTable(tableId),
	}
}

func (t *Uint32Table) Get(txn *store.Txn, key []byte) (uint32, error) {
	value, err := t.get(txn, key)
	if err != nil {
		return 0, err
	}

	return bytesToUint32(value), nil
}

func (t *Uint32Table) Set(txn *store.Txn, key []byte, value uint32) error {
	return t.set(txn, key, uint32ToBytes(value))
}

func (t *Uint32Table) Delete(txn *store.Txn, key []byte) error {
	return t.delete(txn, key)
}

func (t *Uint32Table) ListAll(txn *store.Txn, prefix []byte) ([]uint32, error) {
	result := make([]uint32, 0)
	err := t.eachPrefix(txn, prefix, func(key []byte, value []byte) (bool, error) {
		result = append(result, bytesToUint32(value))
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

type ListPaginatedUint32Result struct {
	Items                   []uint32
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (t *Uint32Table) ListPaginated(txn *store.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedUint32Result, error) {
	rawResult, err := t.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedUint32Result{
		Items:                   make([]uint32, len(rawResult.Values)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, value := range rawResult.Values {
		result.Items[i] = bytesToUint32(value)
	}

	return result, nil
}

// OneToManyUint64Index stores multiple items (uint64) per single key PK (arbitrary []byte).
type OneToManyUint64Index struct {
	table
}

func NewOneToManyUint64Index(tableId []byte) *OneToManyUint64Index {
	return &OneToManyUint64Index{
		table: newTable(tableId),
	}
}

func (i *OneToManyUint64Index) List(txn *store.Txn, pk []byte, fn func(item uint64) (bool, error)) error {
	return i.eachPrefixKeys(txn, pk, func(key []byte) (bool, error) {
		return fn(bytesToUint64(key[len(pk):]))
	})
}

func (i *OneToManyUint64Index) ListPaginated(txn *store.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedUint64Result, error) {
	rawResult, err := i.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedUint64Result{
		Items:                   make([]uint64, len(rawResult.Keys)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, key := range rawResult.Keys {
		result.Items[i] = bytesToUint64(key[len(prefix):])
	}

	return result, nil
}

func (i *OneToManyUint64Index) Add(txn *store.Txn, pk []byte, item uint64) error {
	key := utils.ConcatBytes(pk, item)
	return i.set(txn, key, nil)
}

func (i *OneToManyUint64Index) Delete(txn *store.Txn, pk []byte, item uint64) error {
	key := utils.ConcatBytes(pk, item)
	return i.delete(txn, key)
}

func (i *OneToManyUint64Index) NotEmpty(txn *store.Txn, pk []byte) (bool, error) {
	return i.prefixExists(txn, pk)
}

// OneToManySortedIndex stores multiple items (arbitrary []byte) per single key PK (arbitrary []byte).
type OneToManySortedIndex struct {
	table
}

func NewOneToManySortedIndex(tableId []byte) *OneToManySortedIndex {
	return &OneToManySortedIndex{
		table: newTable(tableId),
	}
}

func (i *OneToManySortedIndex) ListAll(txn *store.Txn, pk []byte, fn func(item []byte) (bool, error)) error {
	return i.eachPrefixKeys(txn, pk, func(key []byte) (bool, error) {
		return fn(key[len(pk):])
	})
}

func (i *OneToManySortedIndex) ListInRange(txn *store.Txn, pk []byte, lowerBound []byte, upperBound []byte, fn func(item []byte) (bool, error)) error {
	lower := utils.ConcatBytes(pk, lowerBound)
	upper := utils.ConcatBytes(pk, upperBound)

	return i.listInRange(txn, lower, upper, false, func(key []byte, value []byte) (bool, error) {
		return fn(key)
	})
}

type ListPaginatedBytesResult struct {
	Items                   [][]byte
	NextPaginationToken     *PaginationToken
	PreviousPaginationToken *PaginationToken
}

func (i *OneToManySortedIndex) ListPaginated(txn *store.Txn, prefix []byte, paginationToken *PaginationToken, limit int) (*ListPaginatedBytesResult, error) {
	rawResult, err := i.listPrefixedPaginated(txn, prefix, paginationToken, limit)
	if err != nil {
		return nil, err
	}

	result := &ListPaginatedBytesResult{
		Items:                   make([][]byte, len(rawResult.Keys)),
		NextPaginationToken:     rawResult.NextPaginationToken,
		PreviousPaginationToken: rawResult.PreviousPaginationToken,
	}

	for i, key := range rawResult.Keys {
		result.Items[i] = key[len(prefix):]
	}

	return result, nil
}

func (i *OneToManySortedIndex) Add(txn *store.Txn, pk []byte, item []byte) error {
	key := utils.ConcatBytes(pk, item)
	return i.set(txn, key, nil)
}

func (i *OneToManySortedIndex) Delete(txn *store.Txn, pk []byte, item []byte) error {
	key := utils.ConcatBytes(pk, item)
	return i.delete(txn, key)
}

func (i *OneToManySortedIndex) NotEmpty(txn *store.Txn, pk []byte) (bool, error) {
	return i.prefixExists(txn, pk)
}

// SortedIndex stores multiple sorted items (arbitrary []byte) without any PK (global index)
type SortedIndex struct {
	table
}

func NewSortedIndex(tableId []byte) *SortedIndex {
	return &SortedIndex{
		table: newTable(tableId),
	}
}

func (i *SortedIndex) ListInRange(txn *store.Txn, lowerBound []byte, upperBound []byte, fn func(item []byte) (bool, error)) error {
	return i.listInRange(txn, lowerBound, upperBound, false, func(key []byte, value []byte) (bool, error) {
		return fn(key)
	})
}

func (i *SortedIndex) Add(txn *store.Txn, item []byte) error {
	return i.set(txn, item, nil)
}

func (i *SortedIndex) Delete(txn *store.Txn, item []byte) error {
	return i.delete(txn, item)
}

func (i *SortedIndex) NotEmpty(txn *store.Txn, pk []byte) (bool, error) {
	return i.prefixExists(txn, pk)
}

// Converts bytes to uint64
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// Converts uint64 to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

// Converts bytes to uint32
func bytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

// Converts uint32 to a byte slice
func uint32ToBytes(u uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, u)
	return buf
}
