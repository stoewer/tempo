package parquetquery

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

type makeTestIterFn func(pf *parquet.File, idx int, filter Predicate, selectAs string) Iterator

var iterTestCases = []struct {
	name     string
	makeIter makeTestIterFn
}{
	{"async", func(pf *parquet.File, idx int, filter Predicate, selectAs string) Iterator {
		return NewColumnIterator(context.TODO(), pf.RowGroups(), idx, selectAs, 1000, filter, selectAs)
	}},
	{"sync", func(pf *parquet.File, idx int, filter Predicate, selectAs string) Iterator {
		return NewSyncIterator(context.TODO(), pf.RowGroups(), idx, selectAs, 1000, filter, selectAs)
	}},
}

func TestColumnIterator(t *testing.T) {
	for _, tc := range iterTestCases {
		t.Run(tc.name, func(t *testing.T) {
			testColumnIterator(t, tc.makeIter)
		})
	}
}

func testColumnIterator(t *testing.T, makeIter makeTestIterFn) {
	count := 100_000
	pf := createTestFile(t, count)

	idx, _ := GetColumnIndexByPath(pf, "A")
	iter := makeIter(pf, idx, nil, "A")
	defer iter.Close()

	for i := 0; i < count; i++ {
		res, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, res, "i=%d", i)
		require.Equal(t, RowNumber{int32(i), -1, -1, -1, -1, -1, -1, -1}, res.RowNumber)
		require.Equal(t, int64(i), res.ToMap()["A"][0].Int64())
	}

	res, err := iter.Next()
	require.NoError(t, err)
	require.Nil(t, res)
}

func TestColumnIteratorSeek(t *testing.T) {
	for _, tc := range iterTestCases {
		t.Run(tc.name, func(t *testing.T) {
			testColumnIteratorSeek(t, tc.makeIter)
		})
	}
}

func testColumnIteratorSeek(t *testing.T, makeIter makeTestIterFn) {
	count := 10_000
	pf := createTestFile(t, count)

	idx, _ := GetColumnIndexByPath(pf, "A")
	iter := makeIter(pf, idx, nil, "A")
	defer iter.Close()

	seekTos := []int32{
		100,
		1234,
		4567,
		5000,
		7890,
	}

	for _, seekTo := range seekTos {
		rn := EmptyRowNumber()
		rn[0] = seekTo
		res, err := iter.SeekTo(rn, 0)
		require.NoError(t, err)
		require.NotNil(t, res, "seekTo=%v", seekTo)
		require.Equal(t, RowNumber{seekTo, -1, -1, -1, -1, -1, -1, -1}, res.RowNumber)
		require.Equal(t, seekTo, res.ToMap()["A"][0].Int32())
	}
}

func TestColumnIteratorPredicate(t *testing.T) {
	for _, tc := range iterTestCases {
		t.Run(tc.name, func(t *testing.T) {
			testColumnIteratorPredicate(t, tc.makeIter)
		})
	}
}

func testColumnIteratorPredicate(t *testing.T, makeIter makeTestIterFn) {
	count := 10_000
	pf := createTestFile(t, count)

	pred := NewIntBetweenPredicate(7001, 7003)

	idx, _ := GetColumnIndexByPath(pf, "A")
	iter := makeIter(pf, idx, pred, "A")
	defer iter.Close()

	expectedResults := []int32{
		7001,
		7002,
		7003,
	}

	for _, expectedResult := range expectedResults {
		res, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, RowNumber{expectedResult, -1, -1, -1, -1, -1, -1, -1}, res.RowNumber)
		require.Equal(t, expectedResult, res.ToMap()["A"][0].Int32())
	}
}

func TestColumnIteratorExitEarly(t *testing.T) {
	type T struct{ A int }

	rows := []T{}
	count := 10_000
	for i := 0; i < count; i++ {
		rows = append(rows, T{i})
	}

	pf := createFileWith(t, rows)
	idx, _ := GetColumnIndexByPath(pf, "A")
	readSize := 1000

	readIter := func(iter Iterator) (int, error) {
		received := 0
		for {
			res, err := iter.Next()
			if err != nil {
				return received, err
			}
			if res == nil {
				break
			}
			received++
		}
		return received, nil
	}

	t.Run("cancelledEarly", func(t *testing.T) {
		// Cancel before iterating
		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		iter := NewColumnIterator(ctx, pf.RowGroups(), idx, "", readSize, nil, "A")
		count, err := readIter(iter)
		require.ErrorContains(t, err, "context canceled")
		require.Equal(t, 0, count)
	})

	t.Run("cancelledPartial", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.TODO())
		iter := NewColumnIterator(ctx, pf.RowGroups(), idx, "", readSize, nil, "A")

		// Read some results
		_, err := iter.Next()
		require.NoError(t, err)

		// Then cancel
		cancel()

		// Read again = context cancelled
		_, err = readIter(iter)
		require.ErrorContains(t, err, "context canceled")
	})

	t.Run("closedEarly", func(t *testing.T) {
		// Close before iterating
		iter := NewColumnIterator(context.TODO(), pf.RowGroups(), idx, "", readSize, nil, "A")
		iter.Close()
		count, err := readIter(iter)
		require.NoError(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("closedPartial", func(t *testing.T) {
		iter := NewColumnIterator(context.TODO(), pf.RowGroups(), idx, "", readSize, nil, "A")

		// Read some results
		_, err := iter.Next()
		require.NoError(t, err)

		// Then close
		iter.Close()

		// Read again = should close early
		res2, err := readIter(iter)
		require.NoError(t, err)
		require.Less(t, readSize+res2, count)
	})
}

func BenchmarkColumnIterator(b *testing.B) {
	for _, tc := range iterTestCases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkColumnIterator(b, tc.makeIter)
		})
	}
}

func benchmarkColumnIterator(b *testing.B, makeIter makeTestIterFn) {
	count := 100_000
	pf := createTestFile(b, count)

	idx, _ := GetColumnIndexByPath(pf, "A")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter := makeIter(pf, idx, nil, "A")
		actualCount := 0
		for {
			res, err := iter.Next()
			require.NoError(b, err)
			if res == nil {
				break
			}
			actualCount++
		}
		iter.Close()
		require.Equal(b, count, actualCount)
	}
}

func createTestFile(t testing.TB, count int) *parquet.File {
	type T struct{ A int }

	rows := []T{}
	for i := 0; i < count; i++ {
		rows = append(rows, T{i})
	}

	pf := createFileWith(t, rows)
	return pf
}

func createFileWith[T any](t testing.TB, rows []T) *parquet.File {
	f, err := os.CreateTemp(t.TempDir(), "data.parquet")
	require.NoError(t, err)

	half := len(rows) / 2

	w := parquet.NewGenericWriter[T](f)
	_, err = w.Write(rows[0:half])
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	_, err = w.Write(rows[half:])
	require.NoError(t, err)
	require.NoError(t, w.Flush())

	require.NoError(t, w.Close())

	stat, err := f.Stat()
	require.NoError(t, err)

	pf, err := parquet.OpenFile(f, stat.Size())
	require.NoError(t, err)

	return pf
}

func TestEqualRowNumber(t *testing.T) {
	r1 := RowNumber{1, 2, 3, 4, 5, 6}
	r2 := RowNumber{1, 2, 3, 5, 7, 9}

	require.True(t, EqualRowNumber(0, r1, r2))
	require.True(t, EqualRowNumber(1, r1, r2))
	require.True(t, EqualRowNumber(2, r1, r2))
	require.False(t, EqualRowNumber(3, r1, r2))
	require.False(t, EqualRowNumber(4, r1, r2))
	require.False(t, EqualRowNumber(5, r1, r2))
}

func BenchmarkEqualRowNumber(b *testing.B) {
	r1 := RowNumber{1, 2, 3, 4, 5, 6, 7, 8}
	r2 := RowNumber{1, 2, 3, 5, 7, 9, 11, 13}

	for d := 0; d <= MaxDefinitionLevel; d++ {
		b.Run(strconv.Itoa(d), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				EqualRowNumber(d, r1, r2)
			}
		})
	}
}

func assertPanic(t *testing.T, f func()) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("no panic")
		}
	}()
	f()
}
