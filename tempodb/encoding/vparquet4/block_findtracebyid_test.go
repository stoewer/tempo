package vparquet4

import (
	"bytes"
	"context"
	"os"
	"sort"
	"testing"

	"github.com/grafana/tempo/pkg/util"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tempo_io "github.com/grafana/tempo/pkg/io"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

func TestBackendBlockFindTraceByID(t *testing.T) {
	rawR, rawW, _, err := local.New(&local.Config{
		Path: t.TempDir(),
	})
	require.NoError(t, err)

	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)
	ctx := context.Background()

	cfg := &common.BlockConfig{
		BloomFP:             0.01,
		BloomShardSizeBytes: 100 * 1024,
	}

	// Test data - sorted by trace ID
	// Find trace by ID uses the column and page bounds,
	// which by default only stores 16 bytes, which is the first
	// half of the trace ID (which is stored as 32 hex text)
	// Therefore it is important that the test data here has
	// full-length trace IDs.
	var traces []*Trace
	for i := 0; i < 16; i++ {
		bar := "bar"
		traces = append(traces, &Trace{
			TraceID: test.ValidTraceID(nil),
			ResourceSpans: []ResourceSpans{
				{
					Resource: Resource{
						ServiceName: "s",
					},
					ScopeSpans: []ScopeSpans{
						{
							Spans: []Span{
								{
									Name: "hello",
									Attrs: []Attribute{
										attr("foo", bar),
									},
									SpanID:       []byte{},
									ParentSpanID: []byte{},
								},
							},
						},
					},
				},
			},
		})
	}

	// Sort
	sort.Slice(traces, func(i, j int) bool {
		return bytes.Compare(traces[i].TraceID, traces[j].TraceID) == -1
	})

	meta := backend.NewBlockMeta("fake", uuid.New(), VersionString, backend.EncNone, "")
	meta.TotalObjects = int64(len(traces))
	s := newStreamingBlock(ctx, cfg, meta, r, w, tempo_io.NewBufferedWriter)

	// Write test data, occasionally flushing (cutting new row group)
	rowGroupSize := 5
	for _, tr := range traces {
		err := s.Add(tr, 0, 0)
		require.NoError(t, err)
		if s.CurrentBufferedObjects() >= rowGroupSize {
			_, err = s.Flush()
			require.NoError(t, err)
		}
	}
	_, err = s.Complete()
	require.NoError(t, err)

	b := newBackendBlock(s.meta, r)

	// Now find and verify all test traces
	for _, tr := range traces {
		wantProto := parquetTraceToTempopbTrace(meta, tr)

		gotProto, err := b.FindTraceByID(ctx, tr.TraceID, common.DefaultSearchOptions())
		require.NoError(t, err)
		require.Equal(t, wantProto, gotProto)
	}
}

func TestBackendBlockFindTraceByID_TestData(t *testing.T) {
	rawR, _, _, err := local.New(&local.Config{
		Path: "./test-data",
	})
	require.NoError(t, err)

	r := backend.NewReader(rawR)
	ctx := context.Background()

	blocks, _, err := r.Blocks(ctx, "single-tenant")
	require.NoError(t, err)
	assert.Len(t, blocks, 1)

	meta, err := r.BlockMeta(ctx, blocks[0], "single-tenant")
	require.NoError(t, err)

	b := newBackendBlock(meta, r)

	iter, err := b.rawIter(context.Background(), newRowPool(10))
	require.NoError(t, err)

	sch := parquet.SchemaOf(new(Trace))
	for {
		_, row, err := iter.Next(context.Background())
		require.NoError(t, err)

		if row == nil {
			break
		}

		tr := &Trace{}
		err = sch.Reconstruct(tr, row)
		require.NoError(t, err)

		protoTr, err := b.FindTraceByID(ctx, tr.TraceID, common.DefaultSearchOptions())
		require.NoError(t, err)
		require.NotNil(t, protoTr)
	}
}

/*func genIndex(t require.TestingT, block *backendBlock) *index {
	pf, _, err := block.openForSearch(context.TODO(), common.DefaultSearchOptions())
	require.NoError(t, err)

	i := &index{}

	for j := range pf.RowGroups() {
		iter := parquetquery.NewSyncIterator(context.TODO(), pf.RowGroups()[j:j+1], 0, "", 1000, nil, "TraceID")
		defer iter.Close()

		for {
			v, err := iter.Next()
			require.NoError(t, err)
			if v == nil {
				break
			}

			i.Add(v.Entries[0].Value.ByteArray())
		}
		i.Flush()
	}

	return i
}*/

func BenchmarkFindTraceByID(b *testing.B) {
	var (
		ctx      = context.TODO()
		tenantID = "1"
		blockID  = uuid.MustParse("782b8848-61dc-4618-aa35-67a9bf527489")
		path     = "/home/astoewer/Develop/tempo/bench-data/blocks/vp4"
	)

	r, _, _, err := local.New(&local.Config{
		Path: path,
	})
	require.NoError(b, err)

	rr := backend.NewReader(r)

	meta, err := rr.BlockMeta(ctx, blockID, tenantID)
	require.NoError(b, err)

	traceIDs := []string{
		"1009d20f3b553",
		"452d994356711",
		"1240aef94269c82b",
		"4385f3b04b9789ab",
		"c081117606e97b2ed8d8fc04297760d9",
		"62a76b52b6cf352473f3f10fb8cfc8e5",
		"3735077ed900a2369ab112e9afd3afd8",
		"178a61f7e0106a231227df793f528c76",
		"cfdfd178b7fd954845ffff2878246a2",
		"178b466976c65e0b",
	}
	traceIDsBytes := make([][]byte, len(traceIDs))
	for i, id := range traceIDs {
		traceIDsBytes[i], err = util.HexStringToTraceID(id)
		require.NoError(b, err)
	}
	block := newBackendBlock(meta, rr)

	for _, tc := range []string{"0", EnvVarIndexEnabledValue} {
		b.Run(EnvVarIndexName+"="+tc, func(b *testing.B) {
			os.Setenv(EnvVarIndexName, tc)
			b.ResetTimer()

			for i := range b.N {
				tr, err := block.FindTraceByID(ctx, traceIDsBytes[i%len(traceIDsBytes)], common.DefaultSearchOptions())
				require.NoError(b, err)
				require.NotNil(b, tr)
			}
		})
	}
}
