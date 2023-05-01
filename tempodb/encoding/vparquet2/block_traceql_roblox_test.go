package vparquet2

import (
	"context"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

func BenchmarkRobloxBlockTraceQL(b *testing.B) {
	testCases := []struct {
		name    string
		req     traceql.FetchSpansRequest
		noMatch bool
	}{
		// span
		{"dedicated01NoMatch", traceql.MustExtractFetchSpansRequest("{ .db.instance = `does-not-exist` }"), true},
		{"dedicated01Match01", traceql.MustExtractFetchSpansRequest("{ .db.instance = `GameSearchUITreatments` }"), false},
		{"dedicated01Match02", traceql.MustExtractFetchSpansRequest("{ .db.instance = `FriendsMcrouterGroup` }"), false},
		{"dedicated02Match01", traceql.MustExtractFetchSpansRequest("{ .memcache.keys = `p1-Unv:4027164587` }"), false},
		{"dedicated02Match02", traceql.MustExtractFetchSpansRequest("{ .memcache.keys = `p1-BundleId:667` }"), false},
		{"attr01NoMatch", traceql.MustExtractFetchSpansRequest("{ .action.route = `does-not-exist` }"), true},
		{"attr01Match01", traceql.MustExtractFetchSpansRequest("{ .action.route = `XboxLive.GetAccountInfo` }"), false},
		{"attr01Match02", traceql.MustExtractFetchSpansRequest("{ .action.route = `Voice.InitiateSubscriptions` }"), false},
		{"attr02Match01", traceql.MustExtractFetchSpansRequest("{ .sampler.type = `lowerbound` }"), false},
		{"attr02Match02", traceql.MustExtractFetchSpansRequest("{ .sampler.type = `probabilistic` }"), false},
	}

	ctx := context.TODO()
	tenantID := "357703"
	blockID := uuid.MustParse("002ea495-2323-45dc-ad03-432ea188d1aa")

	r, _, _, err := local.New(&local.Config{
		Path: path.Join("../../../bench-data/vparquet2"),
	})
	require.NoError(b, err)

	rr := backend.NewReader(r)
	meta, err := rr.BlockMeta(ctx, blockID, tenantID)
	require.NoError(b, err)

	opts := common.DefaultSearchOptions()
	opts.StartPage = 10
	opts.TotalPages = 10

	block := newBackendBlock(meta, rr)
	_, _, err = block.openForSearch(ctx, opts)
	require.NoError(b, err)

	for _, tc := range testCases {

		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			bytesRead := 0

			for i := 0; i < b.N; i++ {
				resp, err := block.Fetch(ctx, tc.req, opts)
				require.NoError(b, err)
				require.NotNil(b, resp)

				// Read first 20 results (if any)
				var count int
				for count < 20 {
					ss, err := resp.Results.Next(ctx)
					require.NoError(b, err)
					if ss == nil {
						break
					}
					count += len(ss.Spans)
				}
				if tc.noMatch {
					require.Equal(b, 0, count)
				} else {
					require.GreaterOrEqual(b, count, 1)
				}

				bytesRead += int(resp.Bytes())
			}
			b.SetBytes(int64(bytesRead) / int64(b.N))
			b.ReportMetric(float64(bytesRead)/float64(b.N)/1000.0/1000.0, "MB_io/op")
		})
	}
}
