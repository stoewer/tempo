package parquetquery

import (
	"sync"

	"github.com/parquet-go/parquet-go"
)

var DefaultPool = NewResultPool[any](10)

type ResultPool[T any] struct {
	pool *sync.Pool
	cap  int
}

// NewResultPool creates a pool for reusing IteratorResults. New items are created
// with the given default capacity.  Using different pools is helpful to keep
// items of similar sizes together which reduces slice allocations.
func NewResultPool[T any](defaultCapacity int) *ResultPool[T] {
	return &ResultPool[T]{
		pool: &sync.Pool{},
		cap:  defaultCapacity,
	}
}

func (p *ResultPool[T]) Get() *TypedIteratorResult[T] {
	if x := p.pool.Get(); x != nil {
		return x.(*TypedIteratorResult[T])
	}

	return &TypedIteratorResult[T]{
		Entries: make([]struct {
			Key   string
			Value parquet.Value
		}, 0, p.cap),
		OtherEntries: make([]struct {
			Key   string
			Value T
		}, 0, p.cap),
	}
}

func (p *ResultPool[T]) Release(r *TypedIteratorResult[T]) {
	r.Reset()
	p.pool.Put(r)
}
