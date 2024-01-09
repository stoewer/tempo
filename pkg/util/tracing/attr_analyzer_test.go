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
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	res_v1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	trace_v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vparquet3"
)

var rnd *rand.Rand

func init() {
	rnd = rand.New(rand.NewSource(0))
}

func TestTraceAttrAnalyzer(t *testing.T) {
	resAttrs := []*v1.KeyValue{
		// attributes in the order of their combined weight
		// weight: 50
		attr("res-attr-0", "10aaaaaaaa"),
		attr("res-attr-0", "10aaaaaaaa"),
		attr("res-attr-0", "10aaaaaaaa"),
		attr("res-attr-0", "10aaaaaaaa"),
		attr("res-attr-0", "10aaaaaaaa"),
		// weight: 30
		attr("res-attr-1", "15aaaaaaaabbbbb"),
		attr("res-attr-1", "15aaaaaaaabbbbb"),
		// weight: 25
		attr("res-attr-2", "25aaaaaaaabbbbbbbbbbccccc"),
		// weight: 20
		attr("res-attr-3", "10aaaaaaaa"),
		attr("res-attr-3", "10aaaaaaaa"),
		// weight: 15
		attr("res-attr-4", "15aaaaaaaabbbbb"),
		// weight: 14
		attr("res-attr-5", "14aaaaaaaabbbb"),
		// weight: 13
		attr("res-attr-6", "07aaaaa"),
		attr("res-attr-6", "06aaaa"),
		// weight: 12
		attr("res-attr-7", "12aaaaaaaabb"),
		// weight: 10
		attr("res-attr-8", "10aaaaaaaa"),
		// weight: 8
		attr("res-attr-9", "02"),
		attr("res-attr-9", "02"),
		attr("res-attr-9", "02"),
		attr("res-attr-9", "02"),
		// weight: 7
		attr("res-attr-10", "04aa"),
		attr("res-attr-10", "03a"),
		// rest
		attr("res-attr-11", "05aaa"),
		attr("res-attr-12", "04aa"),
		attr("res-attr-13", "02"),
		// non strings
		attr("res-ignore-0", 0),
		attr("res-ignore-0", 1),
		attr("res-ignore-0", 2),
	}
	expectedResAttrs := []string{"res-attr-0", "res-attr-1", "res-attr-2", "res-attr-3", "res-attr-4", "res-attr-5", "res-attr-6", "res-attr-7", "res-attr-8", "res-attr-9"}

	spanAttrs := []*v1.KeyValue{
		// attributes in the order of their combined weight
		// weight: 50
		attr("span-attr-0", "10aaaaaaaa"),
		attr("span-attr-0", "10aaaaaaaa"),
		attr("span-attr-0", "10aaaaaaaa"),
		attr("span-attr-0", "10aaaaaaaa"),
		attr("span-attr-0", "10aaaaaaaa"),
		// weight: 30
		attr("span-attr-1", "15aaaaaaaabbbbb"),
		attr("span-attr-1", "15aaaaaaaabbbbb"),
		// weight: 25
		attr("span-attr-2", "25aaaaaaaabbbbbbbbbbccccc"),
		// weight: 20
		attr("span-attr-3", "10aaaaaaaa"),
		attr("span-attr-3", "10aaaaaaaa"),
		// weight: 15
		attr("span-attr-4", "15aaaaaaaabbbbb"),
		// weight: 14
		attr("span-attr-5", "14aaaaaaaabbbb"),
		// weight: 13
		attr("span-attr-6", "07aaaaa"),
		attr("span-attr-6", "06aaaa"),
		// weight: 12
		attr("span-attr-7", "12aaaaaaaabb"),
		// weight: 10
		attr("span-attr-8", "10aaaaaaaa"),
		// weight: 8
		attr("span-attr-9", "02"),
		attr("span-attr-9", "02"),
		attr("span-attr-9", "02"),
		attr("span-attr-9", "02"),
		// weight: 7
		attr("span-attr-10", "04aa"),
		attr("span-attr-10", "03a"),
		// rest
		attr("span-attr-11", "05aaa"),
		attr("span-attr-12", "04aa"),
		attr("span-attr-13", "02"),
		// non strings
		attr("span-ignore-0", 0),
		attr("span-ignore-0", 1),
		attr("span-ignore-0", 2),
	}
	expectedSpanAttrs := []string{"span-attr-0", "span-attr-1", "span-attr-2", "span-attr-3", "span-attr-4", "span-attr-5", "span-attr-6", "span-attr-7", "span-attr-8", "span-attr-9"}

	rnd.Shuffle(len(spanAttrs), func(i, j int) { spanAttrs[i], spanAttrs[j] = spanAttrs[j], spanAttrs[i] })
	rnd.Shuffle(len(resAttrs), func(i, j int) { resAttrs[i], resAttrs[j] = resAttrs[j], resAttrs[i] })

	trace := traceFromAttributes(resAttrs, spanAttrs)

	analyzer := NewTraceAttrAnalyzer(10, 10*time.Second)
	analyzer.Analyze(trace)

	assert.Equal(t, expectedResAttrs, analyzer.TopResourceAttributes())
	assert.Equal(t, expectedSpanAttrs, analyzer.TopSpanAttributes())
}

func TestTraceAttrAnalyzer_IsReady(t *testing.T) {
	const (
		maxRoundsToConverge = 1_000_000
		parallelism         = 10
	)

	tests := []struct {
		attrCount         int
		topAttrCount      int
		spanAttrParam     []stringAttributeParams
		resourceAttrParam []stringAttributeParams
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
		tests[i].spanAttrParam = randomStringAttributesParams(tt.attrCount)
		tests[i].resourceAttrParam = randomStringAttributesParams(tt.attrCount)
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("count-%d-top-%d", tt.attrCount, tt.topAttrCount), func(t *testing.T) {
			analyzers := make([]*TraceAttrAnalyzer, parallelism)
			for i := 0; i < parallelism; i++ {
				analyzers[i] = NewTraceAttrAnalyzer(tt.topAttrCount, 10*time.Second)
			}

			var isReadyCount, rounds, uniqueResourceAttrs, uniqueSpanAttrs int
			for isReadyCount < parallelism && rounds < maxRoundsToConverge {
				rounds++

				analyzer := analyzers[rounds%parallelism]
				trace := traceFromAttributes(randomStringAttributes(tt.resourceAttrParam), randomStringAttributes(tt.spanAttrParam))
				analyzer.Analyze(trace)

				var topResourceAttrs, topSpanAttrs []string
				if rounds%parallelism == 0 {
					isReadyCount = 0
					equalResourceAttrs := map[string]struct{}{}
					equalSpanAttrs := map[string]struct{}{}
					for _, e := range analyzers {
						if e.IsReadySpan() && e.IsReadyResource() {
							isReadyCount++

							topResourceAttrs = e.TopResourceAttributes()
							equalResourceAttrs[strings.Join(topResourceAttrs, ",")] = struct{}{}

							topSpanAttrs = e.TopSpanAttributes()
							equalSpanAttrs[strings.Join(topSpanAttrs, ",")] = struct{}{}
						}
					}

					uniqueResourceAttrs = len(equalResourceAttrs)
					uniqueSpanAttrs = len(equalSpanAttrs)
				}

				if rounds%10_000 == 0 {
					t.Log("rounds:", rounds, " ready:", isReadyCount, " unique res:", uniqueResourceAttrs, " unique span:", uniqueSpanAttrs)
				}
			}

			assert.Equal(t, parallelism, isReadyCount, "not all analyzers are ready")
			assert.LessOrEqual(t, uniqueResourceAttrs, 2, "not enough analyzers have the same top resource attrs")
			assert.LessOrEqual(t, uniqueSpanAttrs, 2, "not enough analyzers have the same top span attrs")
		})
	}
}

func TestAttrAnalyzer(t *testing.T) {
	attrs := []*v1.KeyValue{
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
		// weight: 10attr
		attr("attr-8", "10aaaaaaaa"),
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
	}

	expected := []string{"attr-0", "attr-1", "attr-2", "attr-3", "attr-4", "attr-5", "attr-6", "attr-7", "attr-8", "attr-9"}

	rnd.Shuffle(len(attrs), func(i, j int) { attrs[i], attrs[j] = attrs[j], attrs[i] })
	trace := traceFromAttributes(nil, attrs)

	analyzer := newAttrAnalyzer(10, spanAttrIterator(extractStringWeight))
	analyzer.Analyze(trace)
	topAttrs := analyzer.TopAttributes()
	assert.Equal(t, expected, topAttrs)
}

func TestAttrAnalyzer_IsReady(t *testing.T) {
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
			analyzers := make([]*attrAnalyzer, parallelism)
			for i := 0; i < parallelism; i++ {
				analyzers[i] = newAttrAnalyzer(tt.topAttrCount, spanAttrIterator(extractStringWeight))
			}

			var isReadyCount, rounds, uniqueTopAttrs int
			for isReadyCount < parallelism && rounds < maxRoundsToConverge {
				rounds++

				analyzer := analyzers[rounds%parallelism]
				analyzer.Analyze(traceFromAttributes(nil, randomStringAttributes(tt.attrParams)))

				var topAttrs []string
				if rounds%parallelism == 0 {
					isReadyCount = 0
					equalTopAttrs := map[string]struct{}{}
					for _, e := range analyzers {
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

			assert.Equal(t, parallelism, isReadyCount, "not all analyzers are ready")
			assert.LessOrEqual(t, uniqueTopAttrs, 2, "not enough analyzers have the same top attrs")
		})
	}
}

func TestAttrAnalyzer_IsReadyLocal(t *testing.T) {
	// t.Skip("local test data needed")

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
		{parallelism: 10, tenant: "230482", block: "cf919455-29f3-4c4d-87ec-38495651fe3d"},
		{parallelism: 20, tenant: "230482", block: "cf919455-29f3-4c4d-87ec-38495651fe3d"},
		{parallelism: 10, tenant: "357703", block: "c4a60a84-6425-4bdd-b5b1-7f82479365d4"},
		{parallelism: 20, tenant: "357703", block: "c4a60a84-6425-4bdd-b5b1-7f82479365d4"},
		{parallelism: 10, tenant: "41449", block: "2daff472-e5ec-4477-bd19-536f3336efdc"},
		{parallelism: 20, tenant: "41449", block: "2daff472-e5ec-4477-bd19-536f3336efdc"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("tenant-%s-block-%s-parallel-%d", tt.tenant, tt.block, tt.parallelism), func(t *testing.T) {
			analyzers := make([]*attrAnalyzer, tt.parallelism)
			for i := 0; i < tt.parallelism; i++ {
				analyzers[i] = newAttrAnalyzer(topAttrCount, spanAttrIterator(extractStringWeight))
			}

			blockDir := filepath.Join(basePath, tt.tenant, tt.block)

			iter, err := newParquetIterator(blockDir)
			require.NoError(t, err)
			defer iter.Close()

			var isReadyCount, processedTraces, uniqueTopAttrs int
			for tr, err := iter.Next(); err == nil && tr != nil && isReadyCount < tt.parallelism; tr, err = iter.Next() {
				processedTraces++

				analyzer := analyzers[processedTraces%tt.parallelism]
				analyzer.Analyze(tr)

				var topAttrs []string
				if processedTraces%tt.parallelism == 0 {
					isReadyCount = 0
					equalTopAttrs := map[string]struct{}{}
					for _, e := range analyzers {
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

			assert.Equal(t, tt.parallelism, isReadyCount, "not all analyzers are ready")
			assert.LessOrEqual(t, uniqueTopAttrs, 2, "not enough analyzers have the same top attrs")
		})
	}
}

func Test_spanAttrIterator(t *testing.T) {
	trace := &tempopb.Trace{
		Batches: []*trace_v1.ResourceSpans{
			{
				ScopeSpans: []*trace_v1.ScopeSpans{
					{
						Spans: []*trace_v1.Span{
							{
								Attributes: []*v1.KeyValue{
									attr("attr-0", "x"),
									attr("attr-1", "xx"),
									attr("attr-2", "xxx"),
									attr("ignore-0", 0),
								},
							},
							{
								Attributes: []*v1.KeyValue{
									attr("attr-3", "xxxx"),
								},
							},
							{
								Attributes: []*v1.KeyValue{
									attr("attr-4", "xxxxx"),
									attr("ignore-1", 1),
								},
							},
						},
					},
					{
						Spans: []*trace_v1.Span{
							{
								Attributes: []*v1.KeyValue{
									attr("attr-5", "xxxxxx"),
									attr("attr-6", "xxxxxxx"),
								},
							},
						},
					},
				},
			},
			{
				ScopeSpans: []*trace_v1.ScopeSpans{
					{
						Spans: []*trace_v1.Span{
							{
								Attributes: []*v1.KeyValue{
									attr("attr-7", "xxxxxxxx"),
								},
							},
						},
					},
					{
						Spans: []*trace_v1.Span{
							{
								Attributes: []*v1.KeyValue{
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
	iterator(trace, func(attr *weightedAttribute) bool {
		assert.Equal(t, fmt.Sprintf("attr-%d", count), attr.Name, "name does not match")
		assert.Equal(t, count+1, attr.Weight, "weight does not match")
		count++
		return true
	})

	assert.Equal(t, 10, count, "count does not match")
}

func Test_resourceAttrIterator(t *testing.T) {
	trace := &tempopb.Trace{
		Batches: []*trace_v1.ResourceSpans{
			{
				Resource: &res_v1.Resource{
					Attributes: []*v1.KeyValue{
						attr("attr-0", "x"),
						attr("attr-1", "xx"),
						attr("ignore-0", 0),
						attr("attr-2", "xxx"),
					},
				},
			},
			{
				Resource: &res_v1.Resource{
					Attributes: []*v1.KeyValue{
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
	iterator(trace, func(attr *weightedAttribute) bool {
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
				analyzer := newAttrAnalyzer(bm.topAttrs, spanAttrIterator(extractStringWeight))
				for i := 0; !analyzer.IsReady(); i++ {
					analyzer.Analyze(traceFromAttributes(nil, randomStringAttributes(bm.attrParams)))
				}
				benchStrAttrs = analyzer.TopAttributes()

				assert.False(b, analyzer.isSaturated, "analyzer is saturated")
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

func randomStringAttributes(params []stringAttributeParams) []*v1.KeyValue {
	// preallocate attribute data in order to avoid allocations during benchmark
	attrAlloc := make([]v1.KeyValue, len(params))
	valAlloc := make([]v1.AnyValue, len(attrAlloc))
	strValAlloc := make([]v1.AnyValue_StringValue, len(attrAlloc))

	attrs := make([]*v1.KeyValue, 0, len(params))
	for _, p := range params {
		if t := rnd.Float64(); t >= p.incidence {
			continue
		}

		weightVar := p.weightVariance * float64(p.weight)
		weightMod := weightVar/2 - (rnd.Float64() * weightVar)
		weight := p.weight + int32(weightMod)

		strVal := strValAlloc[len(attrs)]
		strVal.StringValue = randomString(int(weight))

		val := valAlloc[len(attrs)]
		val.Value = &strVal

		a := attrAlloc[len(attrs)]
		a.Key = p.name
		a.Value = &val

		attrs = append(attrs, &a)
	}

	rnd.Shuffle(len(attrs), func(i, j int) { attrs[i], attrs[j] = attrs[j], attrs[i] })
	return attrs
}

func traceFromAttributes(resourceAttrs, spanAttrs []*v1.KeyValue) *tempopb.Trace {
	// preallocate trace data in order to avoid allocations during benchmark
	spanAlloc := make([]trace_v1.Span, len(spanAttrs)/10+1)
	batchAlloc := make([]trace_v1.ResourceSpans, len(resourceAttrs)/10+1)
	resAlloc := make([]res_v1.Resource, len(batchAlloc))
	ssAlloc := make([]trace_v1.ScopeSpans, len(batchAlloc))

	batches := make([]*trace_v1.ResourceSpans, 0, len(batchAlloc))
	for i := 0; i < len(batchAlloc); i++ {
		batch := batchAlloc[i]
		batch.Resource = &resAlloc[i]
		batch.Resource.Attributes = resourceAttrs[i*10 : min((i+1)*10, len(resourceAttrs))]

		spans := make([]*trace_v1.Span, 0, 10)
		for j := 0; j < len(spanAlloc); j++ {
			span := spanAlloc[j]
			span.Attributes = spanAttrs[j*10 : min((j+1)*10, len(spanAttrs))]
			spans = append(spans, &span)
		}

		ss := ssAlloc[i]
		ss.Spans = spans
		batch.ScopeSpans = []*trace_v1.ScopeSpans{&ss}
		batches = append(batches, &batch)
	}

	return &tempopb.Trace{Batches: batches}
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = letters[rnd.Intn(len(letters))]
	}
	return string(b)
}

func attr(name string, value any) *v1.KeyValue {
	var v *v1.AnyValue

	switch value := value.(type) {
	case string:
		v = &v1.AnyValue{Value: &v1.AnyValue_StringValue{StringValue: value}}
	case int:
		v = &v1.AnyValue{Value: &v1.AnyValue_IntValue{IntValue: int64(value)}}
	default:
		panic(fmt.Sprintf("unsupported value type %T", value))
	}

	return &v1.KeyValue{Key: name, Value: v}
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
