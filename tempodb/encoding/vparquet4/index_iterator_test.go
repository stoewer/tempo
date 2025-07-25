package vparquet4

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	pq "github.com/grafana/tempo/pkg/parquetquery"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

func BenchmarkIndexIterators(b *testing.B) {
	ctx := context.TODO()
	opts := common.DefaultSearchOptions()

	block := blockForBenchmarks(b)

	pf, r := openIndexForSearch(b, block, opts)

	rgs := pf.RowGroups()
	rgs = rgs[:]

	var predicates []*pq.InstrumentedPredicate
	makeIterInternal := makeIterFunc(ctx, rgs, pf, pq.SyncIteratorOptUseSeekTo(true))
	makeIter := func(columnName string, predicate pq.Predicate, selectAs string) pq.Iterator {
		pred := &pq.InstrumentedPredicate{
			Pred: predicate,
		}
		predicates = append(predicates, pred)
		return makeIterInternal(columnName, pred, selectAs)
	}

	b.ResetTimer()

	var res *IndexResult
	var err error

	for range b.N {
		iter := NewIndexIterator(makeIter, 0, "aws_region", "us_east_1")
		r.Count = 0

		res, err = iter.Next()
		if err != nil {
			panic(err)
		}

		var (
			results        int
			rowNumberCount int
		)
		if res != nil {
			results++
			rowNumberCount = len(res.RowNumbers)
		}

		iter.Close()
		b.ReportMetric(float64(r.Count), "reads/op")
		b.ReportMetric(float64(results), "results")
		b.ReportMetric(float64(rowNumberCount), "row_numbers")

		if len(predicates) > 0 {
			pred := predicates[0]
			b.ReportMetric(float64(pred.InspectedValues), "vals")
			b.ReportMetric(float64(pred.KeptValues), "vals_kept")
		}
	}

	//if res == nil {
	//	return
	//}
	//
	//f, err := os.OpenFile("row-numbers.txt", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	//if err != nil {
	//	panic(err)
	//}
	//defer f.Close()
	//
	//err = writeRowNumbers(f, res.RowNumbers)
	//if err != nil {
	//	panic(err)
	//}
}

func openIndexForSearch(b *testing.B, block *backendBlock, searchOpts common.SearchOptions) (*parquet.File, *benchReaderAt) {
	blockPath, ok := os.LookupEnv("BENCH_PATH")
	if !ok {
		b.Fatal("BENCH_PATH is not set. These benchmarks are designed to run against a block on local disk. Set BENCH_PATH to the root of the backend such that the block to benchmark is at <BENCH_PATH>/<BENCH_TENANTID>/<BENCH_BLOCKID>.")
	}
	indexPath := filepath.Join(blockPath, block.meta.TenantID, block.meta.BlockID.String(), "index.parquet")

	fileSize, footerSize, err := parquetFileAndFooterSize(indexPath)
	require.NoError(b, err)

	opts := []parquet.FileOption{
		parquet.SkipBloomFilters(true),
		parquet.SkipPageIndex(false),
		parquet.FileReadMode(parquet.ReadModeSync),
	}
	readBufferSize := searchOpts.ReadBufferSize
	if readBufferSize <= 0 {
		readBufferSize = parquet.DefaultFileConfig().ReadBufferSize
	}
	opts = append(opts, parquet.ReadBufferSize(readBufferSize))

	backendReaderAt := NewBackendReaderAt(context.Background(), block.r, "index.parquet", block.meta)

	cachedReader := newCachedReaderAt(backendReaderAt, readBufferSize, fileSize, footerSize)
	benchReader := &benchReaderAt{Delay: time.Millisecond * 50, Reader: cachedReader}

	pf, err := parquet.OpenFile(benchReader, fileSize, opts...)

	return pf, benchReader
}

func parquetFileAndFooterSize(path string) (int64, uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	// Get file stats to get the total file size
	stat, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	fileSize := stat.Size()

	// Read the last 8 bytes (4 bytes footer length + 4 bytes PAR1 magic string)
	buff := make([]byte, 8)
	_, err = f.ReadAt(buff, fileSize-8)
	if err != nil {
		return 0, 0, err
	}
	if string(buff[4:]) != "PAR1" {
		return 0, 0, fmt.Errorf("invalid parquet magic footer: %x", buff[4:])
	}

	footerSize := binary.LittleEndian.Uint32(buff[:4])

	return fileSize, footerSize, nil
}

func BenchmarkBackendBlockQueryRangeIndex(b *testing.B) {
	// benchmark config
	indexLookup := false
	attrScope := "span"
	attrKey := "aws_region"
	attrValue := "us_east_1"

	ctx := context.TODO()
	opts := common.DefaultSearchOptions()

	block := blockForBenchmarks(b)
	pf, r := openIndexForSearch(b, block, opts)
	_, _, err := block.openForSearch(ctx, opts)
	require.NoError(b, err)

	var predicates []*pq.InstrumentedPredicate
	makeIterInternal := makeIterFunc(ctx, pf.RowGroups(), pf, pq.SyncIteratorOptUseSeekTo(true))
	makeIter := func(columnName string, predicate pq.Predicate, selectAs string) pq.Iterator {
		pred := &pq.InstrumentedPredicate{
			Pred: predicate,
		}
		predicates = append(predicates, pred)
		return makeIterInternal(columnName, pred, selectAs)
	}

	engine := traceql.NewEngine()
	fetcher := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
		return block.Fetch(ctx, req, opts)
	})

	// Setup time range
	minutes := 9
	start := block.meta.StartTime
	end := start.Add(time.Duration(minutes) * time.Minute)

	req := &tempopb.QueryRangeRequest{
		Query:     fmt.Sprintf(`{ %s.%s = "%s" } | rate()`, attrScope, attrKey, attrValue),
		Step:      uint64(time.Minute),
		Start:     uint64(start.UnixNano()),
		End:       uint64(end.UnixNano()),
		MaxSeries: 1000,
	}

	eval, err := engine.CompileMetricsQueryRange(req, 2, 0, false)
	require.NoError(b, err)

	// reset counter
	block.count = 0
	r.Count = 0
	rnCount := 0
	b.ResetTimer()

	for range b.N {
		// Index lookup
		var res *IndexResult
		if indexLookup {
			iter := NewIndexIterator(makeIter, 0, attrKey, attrValue)

			res, err = iter.Next()
			if err != nil {
				b.Fatal(err)
			}

			iter.Close()
		}

		// Inject row numbers if present
		if res != nil {
			rnCount = len(res.RowNumbers)
			block.rowNumbers = &rowNumberIterator{
				rowNumbers: res.RowNumbers,
				entry: &struct {
					Key   string
					Value parquet.Value
				}{Key: attrKey, Value: parquet.ValueOf(attrValue)},
			}
		}

		// TraceQL metrics query
		err = eval.Do(ctx, fetcher, uint64(block.meta.StartTime.UnixNano()), uint64(block.meta.EndTime.UnixNano()), int(req.MaxSeries))
		require.NoError(b, err)
	}

	// Report metrics
	bytes, spansTotal, _ := eval.Metrics()
	b.ReportMetric(float64(bytes)/1024.0/1024.0, "MB_IO/op")
	b.ReportMetric(float64(spansTotal/uint64(b.N)), "spans/op")
	b.ReportMetric(float64(spansTotal)/b.Elapsed().Seconds(), "spans/s")
	b.ReportMetric(float64((block.count+r.Count)/int64(b.N)), "reads/op")
	b.ReportMetric(float64(rnCount), "index_rn")
}

func TestReadWriteRowNumbers(t *testing.T) {
	testRows := []pq.RowNumber{
		{100, 22, -1, -1, 0, 0, 0, 0},
		{1010, 0, 3, -1, 0, 0, 0, 0},
		{2, 2000, 987, 8, 0, 0, 0, 0},
	}

	var buf bytes.Buffer
	err := writeRowNumbers(&buf, testRows)
	require.NoError(t, err)

	expected := "100,22,-1,-1\n1010,0,3,-1\n2,2000,987,8\n"
	require.Equal(t, expected, buf.String())

	readRows, err := readRowNumbers(&buf)
	require.NoError(t, err)

	require.Equal(t, len(testRows), len(readRows))
	for i, row := range testRows {
		for j := 0; j < 4; j++ {
			require.Equal(t, row[j], readRows[i][j])
		}
	}
}
