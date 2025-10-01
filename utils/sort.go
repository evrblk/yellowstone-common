package utils

import (
	"sort"

	"golang.org/x/exp/constraints"
)

func SortBy[T any, R constraints.Ordered](collection []T, sortBy func(item T) R) []T {
	result := make([]T, len(collection))
	copy(result, collection)

	sort.Slice(result, func(i, j int) bool {
		return sortBy(result[i]) < sortBy(result[j])
	})

	return result
}

func SortByWithComparator[T any, R any](collection []T, sortBy func(item T) R, comparator func(a, b R) bool) []T {
	result := make([]T, len(collection))
	copy(result, collection)

	sort.Slice(result, func(i, j int) bool {
		return comparator(sortBy(result[i]), sortBy(result[j]))
	})

	return result
}
