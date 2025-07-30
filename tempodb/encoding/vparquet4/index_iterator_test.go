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

	var res *IndexResult
	var err error

	// reset counter
	b.ResetTimer()
	r.BytesRead = 0
	r.ReadCount = 0
	rowNumberCount := 0
	results := 0

	for range b.N {
		iter := NewIndexIterator(makeIter, 0, "span", "aws_region", "us_east_1")

		res, err = iter.Next()
		if err != nil {
			panic(err)
		}

		if res != nil {
			results++
			rowNumberCount += len(res.RowNumbers)
		}

		iter.Close()
	}

	b.ReportMetric(float64(r.BytesRead)/float64(b.N)/1000/1000, "MB_io/op")
	b.ReportMetric(float64(r.ReadCount)/float64(b.N), "reads/op")
	b.ReportMetric(float64(rowNumberCount)/float64(b.N), "index_rn")

	b.ReportMetric(float64(results)/float64(b.N), "results/op")
	if len(predicates) > 0 {
		pred := predicates[0]
		b.ReportMetric(float64(pred.InspectedValues), "vals")
		b.ReportMetric(float64(pred.KeptValues), "vals_kept")
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

func BenchmarkBackendBlockQueryRangeIndex(b *testing.B) {
	// benchmark config
	indexLookup := true
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
	r.ReadCount = 0
	r.BytesRead = 0
	rnCount := 0
	b.ResetTimer()

	for range b.N {
		// Index lookup
		var res *IndexResult
		if indexLookup {
			iter := NewIndexIterator(makeIter, 0, attrScope, attrKey, attrValue)

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
	bytesRead, spansTotal, _ := eval.Metrics()
	totalByes := int(bytesRead) + r.BytesRead
	// b.SetBytes(int64(totalByes / b.N))
	b.ReportMetric(float64(totalByes)/float64(b.N)/1000/1000, "MB_io/op")
	totalCount := block.count + r.ReadCount
	b.ReportMetric(float64(totalCount)/float64(b.N), "reads/op")
	b.ReportMetric(float64(spansTotal)/float64(b.N), "spans/op")
	b.ReportMetric(float64(spansTotal)/b.Elapsed().Seconds(), "spans/s")
	b.ReportMetric(float64(rnCount)/float64(b.N), "index_rn")
}

func BenchmarkBackendBlockTraceQLIndex(b *testing.B) {
	// benchmark config
	indexLookup := true
	attrScope := "span"
	attrKey := "aws_region"
	attrValue := "us_east_1"

	ctx := context.TODO()
	opts := common.DefaultSearchOptions()
	opts.StartPage = 0
	//opts.TotalPages = 5

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

	// TraceQL query
	query := fmt.Sprintf("{ %s.%s=`%s` }", attrScope, attrKey, attrValue)

	// counter and metrics
	block.count = 0
	r.ReadCount = 0
	rnCount := 0
	bytesRead := 0
	spansMatched := 0
	tracesMatched := 0
	b.ResetTimer()

	for range b.N {
		// Index lookup
		var res *IndexResult
		if indexLookup {
			iter := NewIndexIterator(makeIter, 0, attrScope, attrKey, attrValue)

			res, err = iter.Next()
			if err != nil {
				b.Fatal(err)
			}

			iter.Close()
		}

		// Inject row numbers if present
		if res != nil {
			rnCount += len(res.RowNumbers)
			block.rowNumbers = &rowNumberIterator{
				rowNumbers: res.RowNumbers,
				entry: &struct {
					Key   string
					Value parquet.Value
				}{Key: attrKey, Value: parquet.ValueOf(attrValue)},
			}
		}

		// TraceQL search query
		e := traceql.NewEngine()
		resp, err := e.ExecuteSearch(ctx, &tempopb.SearchRequest{Query: query}, traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
			return block.Fetch(ctx, req, opts)
		}))
		require.NoError(b, err)
		require.NotNil(b, resp)

		// Collect metrics
		bytesRead += int(resp.Metrics.InspectedBytes)
		for _, t := range resp.Traces {
			tracesMatched++
			for _, s := range t.SpanSets {
				spansMatched += int(s.Matched)
			}
		}
	}

	// Report metrics
	totalBytes := bytesRead + r.BytesRead
	// b.SetBytes(int64(totalBytes / b.N))
	b.ReportMetric(float64(totalBytes)/float64(b.N)/1000.0/1000.0, "MB_io/op")
	totalCount := block.count + r.ReadCount
	b.ReportMetric(float64(totalCount)/float64(b.N), "reads/op")
	b.ReportMetric(float64(spansMatched)/float64(b.N), "spans/op")
	b.ReportMetric(float64(tracesMatched)/float64(b.N), "traces/op")
	b.ReportMetric(float64(rnCount/b.N), "index_rn")
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
