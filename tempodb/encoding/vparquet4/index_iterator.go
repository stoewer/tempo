package vparquet4

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"

	pq "github.com/grafana/tempo/pkg/parquetquery"
	"github.com/grafana/tempo/pkg/traceql"
)

const (
	indexColKey                     = "Key"
	indexColScop                    = "Scopes.list.element.Scope"
	indexColVal                     = "Scopes.list.element.ValuesString.list.element.Value"
	indexColStringValRowNumbersLvl1 = "Scopes.list.element.ValuesString.list.element.RowNumbers.Lvl01"
	indexColStringValRowNumbersLvl2 = "Scopes.list.element.ValuesString.list.element.RowNumbers.Lvl02"
	indexColStringValRowNumbersLvl3 = "Scopes.list.element.ValuesString.list.element.RowNumbers.Lvl03"
	indexColStringValRowNumbersLvl4 = "Scopes.list.element.ValuesString.list.element.RowNumbers.Lvl04"

	rowNumLvl1 = "Lvl01"
	rowNumLvl2 = "Lvl02"
	rowNumLvl3 = "Lvl03"
	rowNumLvl4 = "Lvl04"

	entryResultKey    = "Result"
	entryValueKey     = "Value"
	entryScopeKey     = "Scope"
	entryRowNumberKey = "RowNumber"
)

var indexResultPool = sync.Pool{
	New: func() interface{} {
		return &IndexResult{
			RowNumbers: make([]pq.RowNumber, 0, 1024),
		}
	},
}

func putIndexResult(r *IndexResult) {
	r.RowNumbers = r.RowNumbers[:0]
	indexResultPool.Put(r)
}

func getIndexResult() *IndexResult {
	return indexResultPool.Get().(*IndexResult)
}

type IndexResult struct {
	Key        string
	Value      string
	Scope      traceql.AttributeScope
	RowNumbers []pq.RowNumber
}

func NewIndexIterator(makeIter makeIterFn, maxRowNums int, scope, key, value string) *IndexIterator {
	scopeInt := int64(traceql.AttributeScopeFromString(scope))

	rnIter := []pq.Iterator{
		makeIter(indexColStringValRowNumbersLvl1, nil, entryRowNumberKey),
		makeIter(indexColStringValRowNumbersLvl2, nil, entryRowNumberKey),
	}
	if scope != "resource" {
		rnIter = append(rnIter,
			makeIter(indexColStringValRowNumbersLvl3, nil, entryRowNumberKey),
			makeIter(indexColStringValRowNumbersLvl4, nil, entryRowNumberKey),
		)
	}

	return &IndexIterator{
		keyIter:       makeIter(indexColKey, NewStringEqualPredicate([]byte(key)), indexColKey),
		valIter:       makeIter(indexColVal, NewStringEqualPredicate([]byte(value)), entryValueKey),
		scopeIter:     makeIter(indexColScop, pq.NewIntEqualPredicate(scopeInt), entryScopeKey),
		rowNumberIter: rnIter,
		maxRowNums:    maxRowNums,
		pos:           pq.EmptyRowNumber(),
		last: struct {
			pos pq.RowNumber
			row pq.RowNumber
		}{
			pos: pq.EmptyRowNumber(),
			row: pq.EmptyRowNumber(),
		},
	}
}

type IndexIterator struct {
	keyIter       pq.Iterator
	valIter       pq.Iterator
	scopeIter     pq.Iterator
	rowNumberIter []pq.Iterator
	maxRowNums    int

	// state
	pos  pq.RowNumber // position of last match
	last struct {
		pos pq.RowNumber // position of the last read row number
		row pq.RowNumber // the last read row number
	}
}

func (ii *IndexIterator) Next() (*IndexResult, error) {
	var ires IndexResult

	res, err := ii.keyIter.Next()
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	for _, e := range res.Entries {
		if e.Key == indexColKey {
			ires.Key = string(e.Value.ByteArray())
			break
		}
	}

	// Important: Currently this only works if there is a matching value in that row
	res, err = ii.scopeIter.SeekTo(res.RowNumber, 0)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	for _, e := range res.Entries {
		if e.Key == entryScopeKey {
			ires.Scope = traceql.AttributeScope(e.Value.Int64())
			break
		}
	}

	res, err = ii.valIter.SeekTo(res.RowNumber, 1)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	ii.pos = res.RowNumber
	for _, e := range res.Entries {
		if e.Key == entryValueKey {
			ires.Value = string(e.Value.ByteArray())
			break
		}
	}

	allocRN := ii.maxRowNums
	if allocRN == 0 {
		allocRN = 1024
	}
	ires.RowNumbers = make([]pq.RowNumber, 0, allocRN)

	if ii.maxRowNums == 0 || len(ires.RowNumbers) < ii.maxRowNums {
		var row pq.RowNumber

		for i, ri := range ii.rowNumberIter {
			res, err = ri.SeekTo(ii.pos, 2)
			if err != nil {
				return nil, err
			}
			if res == nil {
				return &ires, nil
			}
			for _, e := range res.Entries {
				if e.Key == entryRowNumberKey {
					row[i] = e.Value.Int32()
					break
				}
			}
		}

		ii.last.pos = res.RowNumber
		ii.last.row = row

		if pq.CompareRowNumbers(1, ii.pos, ii.last.pos) != 0 {
			return &ires, nil
		}

		ires.RowNumbers = append(ires.RowNumbers, ii.last.row)
	}

	for ii.maxRowNums == 0 || len(ires.RowNumbers) < ii.maxRowNums {
		var row pq.RowNumber

		for i, ri := range ii.rowNumberIter {
			res, err = ri.Next()
			if err != nil {
				return nil, err
			}
			if res == nil {
				return &ires, nil
			}
			for _, e := range res.Entries {
				if e.Key == entryRowNumberKey {
					row[i] = e.Value.Int32()
					break
				}
			}
		}

		ii.last.pos = res.RowNumber
		ii.last.row = row

		if pq.CompareRowNumbers(2, ii.pos, ii.last.pos) != 0 {
			return &ires, nil
		}

		ires.RowNumbers = append(ires.RowNumbers, ii.last.row)
	}

	return &ires, nil
}

func (ii *IndexIterator) Close() {
	ii.keyIter.Close()
	ii.valIter.Close()
	ii.scopeIter.Close()
	for _, iter := range ii.rowNumberIter {
		iter.Close()
	}
}

var _ pq.GroupPredicate = (*indexCollector)(nil)

type indexCollector struct{}

func (i indexCollector) String() string {
	return "indexCollector{}"
}

func (i indexCollector) KeepGroup(res *pq.IteratorResult) bool {
	var idx *IndexResult

	// Look for existing IndexResult first
	for _, e := range res.OtherEntries {
		if v, ok := e.Value.(*IndexResult); ok {
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
		case entryScopeKey:
			idx.Scope = traceql.AttributeScope(e.Value.Int64())
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

	// Reset the result and add the IndexResult to OtherEntries
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
	Reader      io.ReaderAt
	Delay       time.Duration
	BytesRead   int
	ReadCount   int64
	ReadCountFn func()
}

func (b *benchReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	time.Sleep(b.Delay)
	if b.ReadCountFn != nil {
		b.ReadCountFn()
	}
	b.ReadCount++

	n, err = b.Reader.ReadAt(p, off)
	b.BytesRead += n
	return n, err
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
		makeIter(indexColScop, nil, indexColScop),
		createRowNumberIterator(makeIter),
	}

	return pq.NewJoinIterator(0, inner, &indexCollector{})
}

func writeRowNumbers(w io.Writer, rows []pq.RowNumber) error {
	for _, row := range rows {
		_, err := fmt.Fprintf(w, "%d,%d,%d,%d\n", row[0], row[1], row[2], row[3])
		if err != nil {
			return err
		}
	}
	return nil
}

func readRowNumbers(r io.Reader) ([]pq.RowNumber, error) {
	var rows []pq.RowNumber
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) != 4 {
			return nil, fmt.Errorf("invalid format: expected 4 values, got %d", len(parts))
		}

		var row pq.RowNumber
		for i := 0; i < 4; i++ {
			val, err := strconv.ParseInt(parts[i], 10, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid number at position %d at line %s: %v", i, line, err)
			}
			row[i] = int32(val)
		}
		rows = append(rows, row)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return rows, nil
}

func loadRowNumbersFromFile(path string) ([]pq.RowNumber, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rows, err := readRowNumbers(f)
	if err != nil {
		return nil, err
	}

	return rows, err
}
