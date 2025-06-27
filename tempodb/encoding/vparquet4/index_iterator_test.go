package vparquet4

import (
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
	makeIterInternal := makeIterFunc(ctx, rgs, pf, pq.SyncIteratorOptUsePageIndex(true))
	makeIter := func(columnName string, predicate pq.Predicate, selectAs string) pq.Iterator {
		pred := &pq.InstrumentedPredicate{
			Pred: predicate,
		}
		predicates = append(predicates, pred)
		return makeIterInternal(columnName, pred, selectAs)
	}

	b.ResetTimer()
	for range b.N {
		//keys := makeIter("Key", pq.NewStringEqualPredicate([]byte("k8s.cluster.name")), "key")
		//vals := makeIter("ValuesString.list.element.Value", pq.NewStringEqualPredicate([]byte("prod-au-southeast-0")), "value")
		//iter := pq.NewJoinIterator(0, []pq.Iterator{keys, vals}, nil)
		iter := createIndexIterator(makeIter, "k8s.cluster.name", "prod-au-southeast-0")
		r.Count = 0

		res, err := iter.Next()
		if err != nil {
			panic(err)
		}

		var (
			results        int
			rowNumberCount int
		)
		if res != nil {
			for _, e := range res.OtherEntries {
				if v, ok := e.Value.(*indexResult); ok {
					rowNumberCount += len(v.RowNumbers)
					results++
					putIndexResult(v)
				}
			}
		}

		iter.Close()
		b.ReportMetric(float64(r.Count), "reads/op")
		b.ReportMetric(float64(results), "results")
		b.ReportMetric(float64(rowNumberCount), "row_numbers")

		if len(predicates) > 0 {
			pred := predicates[0]
			//b.ReportMetric(float64(pred.InspectedColumnChunks), "stats_cc")
			//b.ReportMetric(float64(pred.KeptColumnChunks), "stats_cc_kept")
			//b.ReportMetric(float64(pred.InspectedPages), "stats_ip")
			//b.ReportMetric(float64(pred.KeptPages), "stats_ip_kept")
			b.ReportMetric(float64(pred.InspectedValues), "vals")
			b.ReportMetric(float64(pred.KeptValues), "vals_kept")
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
