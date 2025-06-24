package vparquet4

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/parquet-go/parquet-go"

	pq "github.com/grafana/tempo/pkg/parquetquery"
)

var _ pq.Predicate = (*StringEqualPredicate)(nil)

type StringEqualPredicate struct {
	value []byte
}

func NewStringEqualPredicate(val []byte) StringEqualPredicate {
	return StringEqualPredicate{value: val}
}

func (p StringEqualPredicate) String() string {
	return fmt.Sprintf("StringEqualPredicate{%s}", p.value)
}

func (p StringEqualPredicate) KeepColumnChunk(col *pq.ColumnChunkHelper) bool {
	minVal, maxVal, ok := col.Bounds()
	if !ok {
		return true
	}
	if bytes.Compare(p.value, minVal.ByteArray()) >= 0 && bytes.Compare(p.value, maxVal.ByteArray()) <= 0 {
		return true
	}
	return false
}

func (p StringEqualPredicate) KeepPage(page parquet.Page) bool {
	minVal, maxVal, ok := page.Bounds()
	if !ok {
		return true
	}
	if bytes.Compare(p.value, minVal.ByteArray()) >= 0 && bytes.Compare(p.value, maxVal.ByteArray()) <= 0 {
		return true
	}
	return false
}

func (p StringEqualPredicate) KeepValue(val parquet.Value) bool {
	vv := val.ByteArray()
	return bytes.Equal(vv, p.value)
}

var _ io.ReaderAt = &benchReaderAt{}

type benchReaderAt struct {
	Reader  io.ReaderAt
	Delay   time.Duration
	Count   int64
	CountFn func()
}

func (b *benchReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	time.Sleep(b.Delay)
	if b.CountFn != nil {
		b.CountFn()
	}
	b.Count++
	return b.Reader.ReadAt(p, off)
}
