package vparquet4

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"

	pq "github.com/grafana/tempo/pkg/parquetquery"
)

const (
	indexColKey                     = "Key"
	indexColScopeMask               = "ScopeMask"
	indexColVal                     = "ValuesString.list.element.Value"
	indexColStringValRowNumbersLvl1 = "ValuesString.list.element.RowNumbers.Lvl01"
	indexColStringValRowNumbersLvl2 = "ValuesString.list.element.RowNumbers.Lvl02"
	indexColStringValRowNumbersLvl3 = "ValuesString.list.element.RowNumbers.Lvl03"
	indexColStringValRowNumbersLvl4 = "ValuesString.list.element.RowNumbers.Lvl04"

	rowNumLvl1 = "Lvl01"
	rowNumLvl2 = "Lvl02"
	rowNumLvl3 = "Lvl03"
	rowNumLvl4 = "Lvl04"

	entryResultKey    = "Result"
	entryValueKey     = "Value"
	entryRowNumberKey = "RowNumber"
)

var indexResultPool = sync.Pool{
	New: func() interface{} {
		return &indexResult{
			RowNumbers: make([]pq.RowNumber, 0, 64),
		}
	},
}

func putIndexResult(r *indexResult) {
	r.RowNumbers = r.RowNumbers[:0]
	indexResultPool.Put(r)
}

func getIndexResult() *indexResult {
	return indexResultPool.Get().(*indexResult)
}

type indexResult struct {
	Key        string
	Value      string
	ScopeMask  int64
	RowNumbers []pq.RowNumber
}

var _ pq.GroupPredicate = (*indexCollector)(nil)

type indexCollector struct{}

func (i indexCollector) String() string {
	return "indexCollector{}"
}

func (i indexCollector) KeepGroup(res *pq.IteratorResult) bool {
	var idx *indexResult

	// Look for existing indexResult first
	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(*indexResult); ok {
			idx = v
			break
		}
	}

	// If not found, create a new one
	if idx == nil {
		idx = getIndexResult()
	}

	// Extract data from the result entries
	for _, e := range res.Entries {
		switch e.Key {
		case indexColKey:
			idx.Key = string(e.Value.ByteArray())
		case indexColScopeMask:
			idx.ScopeMask = e.Value.Int64()
		case entryValueKey:
			idx.Value = string(e.Value.ByteArray())
		}
	}

	// Get row numbers from rowNumberCollector
	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(*pq.RowNumber); ok {
			idx.RowNumbers = append(idx.RowNumbers, *v)
			putRowNumbers(v)
		}
	}

	// Reset the result and add the indexResult to OtherEntries
	res.Reset()
	res.AppendOtherValue(entryResultKey, idx)

	return true
}

var rowNumbersPool = sync.Pool{
	New: func() interface{} {
		r := pq.EmptyRowNumber()
		return &r
	},
}

func putRowNumbers(r *pq.RowNumber) {
	for i := range len(r) {
		r[i] = -1
	}
	rowNumbersPool.Put(r)
}

func getRowNumbers() *pq.RowNumber {
	return rowNumbersPool.Get().(*pq.RowNumber)
}

var _ pq.GroupPredicate = (*rowNumberCollector)(nil)

type rowNumberCollector struct{}

func (r rowNumberCollector) String() string {
	return "rowNumberCollector{}"
}

func (r rowNumberCollector) KeepGroup(res *pq.IteratorResult) bool {
	var row *pq.RowNumber

	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(*pq.RowNumber); ok {
			row = v
			break
		}
	}

	if row == nil {
		row = getRowNumbers()
	}

	for _, e := range res.Entries {
		switch e.Key {
		case rowNumLvl1:
			row[0] = e.Value.Int32()
		case rowNumLvl2:
			row[1] = e.Value.Int32()
		case rowNumLvl3:
			row[2] = e.Value.Int32()
		case rowNumLvl4:
			row[3] = e.Value.Int32()
		}
	}

	res.Entries = res.Entries[:0]
	res.AppendOtherValue(entryRowNumberKey, row)
	//putRowNumbers(row)

	return true
}

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

func createRowNumberIterator(makeIter makeIterFn) pq.Iterator {
	inner := []pq.Iterator{
		makeIter(indexColStringValRowNumbersLvl1, nil, rowNumLvl1),
		makeIter(indexColStringValRowNumbersLvl2, nil, rowNumLvl2),
		makeIter(indexColStringValRowNumbersLvl3, nil, rowNumLvl3),
		makeIter(indexColStringValRowNumbersLvl4, nil, rowNumLvl4),
	}

	return pq.NewJoinIterator(2, inner, &rowNumberCollector{})
}

func createIndexIterator(makeIter makeIterFn, key, value string) pq.Iterator {
	inner := []pq.Iterator{
		makeIter(indexColKey, NewStringEqualPredicate([]byte(key)), indexColKey),
		makeIter(indexColVal, NewStringEqualPredicate([]byte(value)), entryValueKey),
		makeIter(indexColScopeMask, nil, indexColScopeMask),
		createRowNumberIterator(makeIter),
	}

	return pq.NewJoinIterator(0, inner, &indexCollector{})
}
