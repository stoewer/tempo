package tracing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/pkg/tempopb"
	v1common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1res "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1trace "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vparquet3"
)

var rnd *rand.Rand

func init() {
	rnd = rand.New(rand.NewSource(0))
}

func TestAttributeAnalyzer_TopAttributes(t *testing.T) {
	trace := &tempopb.Trace{
		Batches: []*v1trace.ResourceSpans{{
			ScopeSpans: []*v1trace.ScopeSpans{{
				Spans: []*v1trace.Span{
					{
						Attributes: []*v1common.KeyValue{
							// attributes in the order of their combined weight
							// weight: 50
							attr("attr-0", "10aaaaaaaa"),
							attr("attr-0", "10aaaaaaaa"),
							attr("attr-0", "10aaaaaaaa"),
							attr("attr-0", "10aaaaaaaa"),
							attr("attr-0", "10aaaaaaaa"),
							// weight: 30
							attr("attr-1", "15aaaaaaaabbbbb"),
							attr("attr-1", "15aaaaaaaabbbbb"),
							// weight: 25
							attr("attr-2", "25aaaaaaaabbbbbbbbbbccccc"),
						},
					},
					{
						Attributes: []*v1common.KeyValue{
							// weight: 20
							attr("attr-3", "10aaaaaaaa"),
							attr("attr-3", "10aaaaaaaa"),
							// weight: 15
							attr("attr-4", "15aaaaaaaabbbbb"),
							// weight: 14
							attr("attr-5", "14aaaaaaaabbbb"),
							// weight: 13
							attr("attr-6", "07aaaaa"),
							attr("attr-6", "06aaaa"),
							// weight: 12
							attr("attr-7", "12aaaaaaaabb"),
							// weight: 10
							attr("attr-8", "10aaaaaaaa"),
						},
					},
					{
						Attributes: []*v1common.KeyValue{
							// weight: 8
							attr("attr-9", "02"),
							attr("attr-9", "02"),
							attr("attr-9", "02"),
							attr("attr-9", "02"),
							// weight: 7
							attr("attr-10", "04aa"),
							attr("attr-10", "03a"),
							// rest
							attr("attr-11", "05aaa"),
							attr("attr-12", "04aa"),
							attr("attr-13", "02"),
						},
					},
				},
			}},
		}},
	}

	expected := []string{"attr-0", "attr-1", "attr-2", "attr-3", "attr-4", "attr-5", "attr-6", "attr-7", "attr-8", "attr-9"}

	eval := NewAttributeAnalyzer(10, spanAttrIterator(extractStringWeight))
	eval.Analyze(trace)
	topAttrs := eval.TopAttributes()
	assert.Equal(t, expected, topAttrs)
}

func TestAttributeAnalyzer_IsReady(t *testing.T) {
	const (
		maxRoundsToConverge = 1_000_000
		parallelism         = 10
	)

	tests := []struct {
		attrCount    int
		topAttrCount int
		attrParams   []stringAttributeParams
	}{
		{attrCount: 20, topAttrCount: 5},
		{attrCount: 20, topAttrCount: 10},
		{attrCount: 50, topAttrCount: 10},
		{attrCount: 50, topAttrCount: 15},
		{attrCount: 100, topAttrCount: 10},
		{attrCount: 100, topAttrCount: 15},
		{attrCount: 200, topAttrCount: 10},
		{attrCount: 200, topAttrCount: 15},
	}

	for i, tt := range tests {
		tests[i].attrParams = randomStringAttributesParams(tt.attrCount)
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("count-%d-top-%d", tt.attrCount, tt.topAttrCount), func(t *testing.T) {
			evaluators := make([]*AttributeAnalyzer, parallelism)
			for i := 0; i < parallelism; i++ {
				evaluators[i] = NewAttributeAnalyzer(tt.topAttrCount, spanAttrIterator(extractStringWeight))
			}

			var isReadyCount, rounds, uniqueTopAttrs int
			for isReadyCount < parallelism && rounds < maxRoundsToConverge {
				rounds++

				eval := evaluators[rounds%parallelism]
				eval.Analyze(traceWithRandomStringAttributes(tt.attrParams))

				var topAttrs []string
				if rounds%parallelism == 0 {
					isReadyCount = 0
					equalTopAttrs := map[string]struct{}{}
					for _, e := range evaluators {
						if e.IsReady() {
							isReadyCount++

							topAttrs = e.TopAttributes()
							equalTopAttrs[strings.Join(topAttrs, ",")] = struct{}{}
						}
					}
					uniqueTopAttrs = len(equalTopAttrs)
				}

				if rounds%10_000 == 0 {
					t.Log("rounds:", rounds, " ready:", isReadyCount, " unique:", uniqueTopAttrs, " len:", len(topAttrs))
				}
			}

			equalTopAttrs := map[string]struct{}{}
			for _, eval := range evaluators {
				topAttrs := eval.TopAttributes()
				equalTopAttrs[strings.Join(topAttrs, ",")] = struct{}{}
			}

			assert.Equal(t, parallelism, isReadyCount, "not all evaluators have converged")
			assert.LessOrEqual(t, uniqueTopAttrs, 2, "not enough evaluators have the same top attrs")
		})
	}
}

func TestAttributeAnalyzer_IsReadyLocal(t *testing.T) {
	// t.Skip("local test")

	const (
		basePath     = "/home/astoewer/Downloads/Blocks/vparquet3"
		topAttrCount = 10
	)

	tests := []struct {
		parallelism  int
		topAttrCount int
		tenant       string
		block        string
	}{
		{parallelism: 10, tenant: "1", block: "5adfcf69-0a0f-4576-85f0-54b2410cd596"},
		{parallelism: 20, tenant: "1", block: "5adfcf69-0a0f-4576-85f0-54b2410cd596"},
		//{parallelism: 20, tenant: "1", block: "ed3be876-7341-4270-bcbe-a3f7bba08738"},
		//{parallelism: 10, tenant: "151892", block: "2d9ad36a-cf60-415e-aa22-ad07a3eec379"},
		//{parallelism: 20, tenant: "151892", block: "2d9ad36a-cf60-415e-aa22-ad07a3eec379"},
		{parallelism: 10, tenant: "230482", block: "cf919455-29f3-4c4d-87ec-38495651fe3d"},
		{parallelism: 20, tenant: "230482", block: "cf919455-29f3-4c4d-87ec-38495651fe3d"},
		// too short {parallelism: 10, tenant: "23604", block: "3208c7b1-e2e6-4fe6-ba60-6ef7b74bd03d"},
		// too short {parallelism: 20, tenant: "23604", block: "3208c7b1-e2e6-4fe6-ba60-6ef7b74bd03d"},
		{parallelism: 10, tenant: "357703", block: "c4a60a84-6425-4bdd-b5b1-7f82479365d4"},
		{parallelism: 20, tenant: "357703", block: "c4a60a84-6425-4bdd-b5b1-7f82479365d4"},
		{parallelism: 10, tenant: "41449", block: "2daff472-e5ec-4477-bd19-536f3336efdc"},
		{parallelism: 20, tenant: "41449", block: "2daff472-e5ec-4477-bd19-536f3336efdc"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("tenant-%s-block-%s-parallel-%d", tt.tenant, tt.block, tt.parallelism), func(t *testing.T) {
			evaluators := make([]*AttributeAnalyzer, tt.parallelism)
			for i := 0; i < tt.parallelism; i++ {
				evaluators[i] = NewAttributeAnalyzer(topAttrCount, spanAttrIterator(extractStringWeight))
			}

			blockDir := filepath.Join(basePath, tt.tenant, tt.block)

			iter, err := newParquetIterator(blockDir)
			require.NoError(t, err)
			defer iter.Close()

			var isReadyCount, processedTraces, uniqueTopAttrs int
			for tr, err := iter.Next(); err == nil && tr != nil && isReadyCount < tt.parallelism; tr, err = iter.Next() {
				processedTraces++

				eval := evaluators[processedTraces%tt.parallelism]
				eval.Analyze(tr)

				var topAttrs []string
				if processedTraces%tt.parallelism == 0 {
					isReadyCount = 0
					equalTopAttrs := map[string]struct{}{}
					for _, e := range evaluators {
						if e.IsReady() {
							isReadyCount++

							topAttrs = e.TopAttributes()
							equalTopAttrs[strings.Join(topAttrs, ",")] = struct{}{}
						}
					}
					uniqueTopAttrs = len(equalTopAttrs)
				}

				if processedTraces%10_000 == 0 {
					t.Log("traces:", processedTraces, " ready:", isReadyCount, " unique:", uniqueTopAttrs, " len:", len(topAttrs))
				}
			}
			if err != nil && !errors.Is(err, io.EOF) {
				require.NoError(t, err)
			}

			equalTopAttrs := map[string]struct{}{}
			for _, eval := range evaluators {
				topAttrs := eval.TopAttributes()
				equalTopAttrs[strings.Join(topAttrs, ",")] = struct{}{}
			}

			assert.Equal(t, tt.parallelism, isReadyCount, "not all evaluators have converged")
			assert.LessOrEqual(t, uniqueTopAttrs, 2, "not enough evaluators have the same top attrs")
		})
	}
}

func Test_spanAttrIterator(t *testing.T) {
	trace := &tempopb.Trace{
		Batches: []*v1trace.ResourceSpans{
			{
				ScopeSpans: []*v1trace.ScopeSpans{
					{
						Spans: []*v1trace.Span{
							{
								Attributes: []*v1common.KeyValue{
									attr("attr-0", "x"),
									attr("attr-1", "xx"),
									attr("attr-2", "xxx"),
									attr("ignore-0", 0),
								},
							},
							{
								Attributes: []*v1common.KeyValue{
									attr("attr-3", "xxxx"),
								},
							},
							{
								Attributes: []*v1common.KeyValue{
									attr("attr-4", "xxxxx"),
									attr("ignore-1", 1),
								},
							},
						},
					},
					{
						Spans: []*v1trace.Span{
							{
								Attributes: []*v1common.KeyValue{
									attr("attr-5", "xxxxxx"),
									attr("attr-6", "xxxxxxx"),
								},
							},
						},
					},
				},
			},
			{
				ScopeSpans: []*v1trace.ScopeSpans{
					{
						Spans: []*v1trace.Span{
							{
								Attributes: []*v1common.KeyValue{
									attr("attr-7", "xxxxxxxx"),
								},
							},
						},
					},
					{
						Spans: []*v1trace.Span{
							{
								Attributes: []*v1common.KeyValue{
									attr("attr-8", "xxxxxxxxx"),
									attr("ignore-2", 2),
									attr("attr-9", "xxxxxxxxxx"),
								},
							},
						},
					},
				},
			},
		},
	}

	iterator := spanAttrIterator(extractStringWeight)

	count := 0
	iterator(trace, func(attr *WeightedAttribute) bool {
		assert.Equal(t, fmt.Sprintf("attr-%d", count), attr.Name, "name does not match")
		assert.Equal(t, count+1, attr.Weight, "weight does not match")
		count++
		return true
	})

	assert.Equal(t, 10, count, "count does not match")
}

func Test_resourceAttrIterator(t *testing.T) {
	trace := &tempopb.Trace{
		Batches: []*v1trace.ResourceSpans{
			{
				Resource: &v1res.Resource{
					Attributes: []*v1common.KeyValue{
						attr("attr-0", "x"),
						attr("attr-1", "xx"),
						attr("ignore-0", 0),
						attr("attr-2", "xxx"),
					},
				},
			},
			{
				Resource: &v1res.Resource{
					Attributes: []*v1common.KeyValue{
						attr("attr-3", "xxxx"),
						attr("attr-4", "xxxxx"),
						attr("ignore-1", 0),
					},
				},
			},
		},
	}

	iterator := resourceAttrIterator(extractStringWeight)

	count := 0
	iterator(trace, func(attr *WeightedAttribute) bool {
		assert.Equal(t, fmt.Sprintf("attr-%d", count), attr.Name, "name does not match")
		assert.Equal(t, count+1, attr.Weight, "weight does not match")
		count++
		return true
	})

	assert.Equal(t, 5, count, "count does not match")
}

var benchStrAttrs []string

func BenchmarkAttributeAnalyzer(b *testing.B) {
	benchmarks := []struct {
		numAttrs   int
		topAttrs   int
		attrParams []stringAttributeParams
	}{
		{numAttrs: 20, topAttrs: 5},
		{numAttrs: 20, topAttrs: 10},
		{numAttrs: 50, topAttrs: 10},
		{numAttrs: 50, topAttrs: 20},
		{numAttrs: 100, topAttrs: 10},
		{numAttrs: 100, topAttrs: 20},
		{numAttrs: 100, topAttrs: 50},
		{numAttrs: 200, topAttrs: 10},
		{numAttrs: 200, topAttrs: 20},
		{numAttrs: 200, topAttrs: 50},
		{numAttrs: 300, topAttrs: 10},
		{numAttrs: 300, topAttrs: 20},
		{numAttrs: 300, topAttrs: 50},
	}

	for i, tt := range benchmarks {
		benchmarks[i].attrParams = randomStringAttributesParams(tt.numAttrs)
	}

	b.ResetTimer()
	for _, bm := range benchmarks {
		b.Run(fmt.Sprintf("count-%d-top-%d", bm.numAttrs, bm.topAttrs), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				eval := NewAttributeAnalyzer(bm.topAttrs, spanAttrIterator(extractStringWeight))
				for i := 0; !eval.IsReady(); i++ {
					eval.Analyze(traceWithRandomStringAttributes(bm.attrParams))
				}
				benchStrAttrs = eval.TopAttributes()

				assert.False(b, eval.isSaturated, "eval is saturated")
				require.Equal(b, bm.numAttrs, len(benchStrAttrs), "length does not match")
			}
		})
	}
}

type stringAttributeParams struct {
	name           string
	incidence      float64
	weight         int32
	weightVariance float64
}

func randomStringAttributesParams(n int) []stringAttributeParams {
	params := make([]stringAttributeParams, n)
	for i := 0; i < n; i++ {
		params[i] = stringAttributeParams{
			name:           fmt.Sprintf("attr-%d", i),
			incidence:      rnd.Float64(),
			weight:         rnd.Int31n(100),
			weightVariance: rnd.Float64(),
		}
	}
	return params
}

func traceWithRandomStringAttributes(params []stringAttributeParams) *tempopb.Trace {
	var attrs []*v1common.KeyValue
	for _, p := range params {
		if t := rnd.Float64(); t >= p.incidence {
			continue
		}

		weightVar := p.weightVariance * float64(p.weight)
		weightMod := weightVar/2 - (rnd.Float64() * weightVar)
		weight := p.weight + int32(weightMod)

		attrs = append(attrs, attr(p.name, randomString(int(weight))))
	}
	rnd.Shuffle(len(attrs), func(i, j int) { attrs[i], attrs[j] = attrs[j], attrs[i] })

	var spans []*v1trace.Span
	for i := 0; i < len(attrs); i += 10 {
		spans = append(spans, &v1trace.Span{Attributes: attrs[i:min(i+10, len(attrs))]})
	}

	return &tempopb.Trace{
		Batches: []*v1trace.ResourceSpans{
			{ScopeSpans: []*v1trace.ScopeSpans{{Spans: spans}}},
		},
	}
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = letters[rnd.Intn(len(letters))]
	}
	return string(b)
}

func attr(name string, value any) *v1common.KeyValue {
	var v *v1common.AnyValue

	switch value := value.(type) {
	case string:
		v = &v1common.AnyValue{Value: &v1common.AnyValue_StringValue{StringValue: value}}
	case int:
		v = &v1common.AnyValue{Value: &v1common.AnyValue_IntValue{IntValue: int64(value)}}
	default:
		panic(fmt.Sprintf("unsupported value type %T", value))
	}

	return &v1common.KeyValue{Key: name, Value: v}
}

func newParquetIterator(blockPath string) (*parquetIterator, error) {
	f1, err := os.Open(filepath.Join(blockPath, "meta.json"))
	if err != nil {
		return nil, err
	}
	defer f1.Close()

	var meta backend.BlockMeta
	err = json.NewDecoder(f1).Decode(&meta)
	if err != nil {
		return nil, err
	}

	f2, err := os.Open(filepath.Join(blockPath, "data.parquet"))
	if err != nil {
		return nil, err
	}

	i := &parquetIterator{
		r:      parquet.NewGenericReader[*vparquet3.Trace](f2),
		m:      &meta,
		buffer: make([]*vparquet3.Trace, 1_000),
	}

	return i, nil
}

type parquetIterator struct {
	r      *parquet.GenericReader[*vparquet3.Trace]
	m      *backend.BlockMeta
	i      int
	n      int
	buffer []*vparquet3.Trace
}

func (i *parquetIterator) Next() (*tempopb.Trace, error) {
	if i.i >= i.n {
		n, err := i.r.Read(i.buffer)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n == 0 {
			return nil, io.EOF
		}

		i.i = 0
		i.n = n
	}

	t := vparquet3.ParquetTraceToTempopbTrace(i.m, i.buffer[i.i])
	i.i++

	return t, nil
}

func (i *parquetIterator) Close() {
	_ = i.r.Close()
}
