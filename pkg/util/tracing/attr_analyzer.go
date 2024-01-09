package tracing

import (
	"math"
	"slices"
	"time"

	"gopkg.in/yaml.v2"

	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
)

const (
	convergedThreshold    = 30
	recalculationInterval = 10_000
)

type TraceAttrAnalyzer struct {
	minAnalyzerLifespan time.Duration
	span                struct {
		current  *attrAnalyzer
		next     *attrAnalyzer
		lastSwap time.Time
	}
	resource struct {
		current  *attrAnalyzer
		next     *attrAnalyzer
		lastSwap time.Time
	}
}

func NewTraceAttrAnalyzer(topAttrCount int, minAnalyzerLifespan time.Duration) *TraceAttrAnalyzer {
	ta := TraceAttrAnalyzer{
		minAnalyzerLifespan: minAnalyzerLifespan,
	}

	now := time.Now()
	ta.span.current = newAttrAnalyzer(topAttrCount, spanAttrIterator(extractStringWeight))
	ta.span.next = newAttrAnalyzer(topAttrCount, spanAttrIterator(extractStringWeight))
	ta.span.lastSwap = now
	ta.resource.current = newAttrAnalyzer(topAttrCount, resourceAttrIterator(extractStringWeight))
	ta.resource.next = newAttrAnalyzer(topAttrCount, resourceAttrIterator(extractStringWeight))
	ta.resource.lastSwap = now

	return &ta
}

func (ta *TraceAttrAnalyzer) Analyze(trace *tempopb.Trace) {
	ta.span.current.Analyze(trace)
	ta.span.next.Analyze(trace)
	ta.resource.current.Analyze(trace)
	ta.resource.next.Analyze(trace)
	return
}

func (ta *TraceAttrAnalyzer) IsReadySpan() bool {
	return ta.span.current.IsReady()
}

func (ta *TraceAttrAnalyzer) TopSpanAttributes() []string {
	ta.swapAnalyzers()
	return ta.span.current.TopAttributes()
}

func (ta *TraceAttrAnalyzer) IsReadyResource() bool {
	return ta.resource.current.IsReady()
}

func (ta *TraceAttrAnalyzer) TopResourceAttributes() []string {
	ta.swapAnalyzers()
	return ta.resource.current.TopAttributes()
}

type traceAttrAnalyzerState struct {
	Span     *attrAnalyzerState `yaml:"span"`
	Resource *attrAnalyzerState `yaml:"resource"`
}

func (ta *TraceAttrAnalyzer) RetrieveState() ([]byte, error) {
	state := traceAttrAnalyzerState{
		Span:     ta.span.current.RetrieveState(),
		Resource: ta.resource.current.RetrieveState(),
	}

	return yaml.Marshal(state)
}

func (ta *TraceAttrAnalyzer) RestoreState(b []byte) error {
	var state traceAttrAnalyzerState
	err := yaml.Unmarshal(b, &state)
	if err != nil {
		return err
	}

	ta.span.current.RestoreState(state.Span)
	ta.span.next.Reset()

	ta.resource.current.RestoreState(state.Resource)
	ta.resource.next.Reset()

	return nil
}

func (ta *TraceAttrAnalyzer) swapAnalyzers() {
	now := time.Now()
	if ta.span.next.IsReady() && now.Sub(ta.span.lastSwap) > ta.minAnalyzerLifespan {
		tmp := ta.span.current
		ta.span.current = ta.span.next
		tmp.Reset()
		ta.span.next = tmp
		ta.span.lastSwap = now
	}
	if ta.resource.next.IsReady() && now.Sub(ta.resource.lastSwap) > ta.minAnalyzerLifespan {
		tmp := ta.resource.current
		ta.resource.current = ta.resource.next
		tmp.Reset()
		ta.resource.next = tmp
		ta.resource.lastSwap = now
	}
}

type weightedAttribute struct {
	Name   string `yaml:"name"`   // the attributes name
	Weight int    `yaml:"weight"` // an abstract indicator of the attribute's occurrence and size
}

type weightedAttrIterator func(trace *tempopb.Trace, callback func(attr *weightedAttribute) bool) bool

func newAttrAnalyzer(topAttrCount int, weightedAttrIterator weightedAttrIterator) *attrAnalyzer {
	return &attrAnalyzer{
		weightedAttrIterator: weightedAttrIterator,
		topAttrCount:         topAttrCount,
		attrs:                make(map[string]*weightedAttribute, topAttrCount),
		attrBuffer:           make([]*weightedAttribute, 0, topAttrCount),
	}
}

// attrAnalyzer is used to determine the top attributes in the overall trace data.
type attrAnalyzer struct {
	weightedAttrIterator  weightedAttrIterator
	isSaturated           bool
	isReady               bool
	attrs                 map[string]*weightedAttribute
	attrBuffer            []*weightedAttribute
	attrCount             int
	attrsNotEvaluated     int
	topAttrCount          int
	topAttrUnchangedCount int
	topAttrs              []string
}

// Analyze analyzes the attributes of a given trace
func (a *attrAnalyzer) Analyze(trace *tempopb.Trace) {
	if a.isSaturated {
		return
	}

	a.weightedAttrIterator(trace, a.analyzeAttribute)

	if a.recalculationRequired() {
		a.recalculateTopAttrs()
	}
}

// IsReady returns true if the analysis is good enough to be used
func (a *attrAnalyzer) IsReady() bool {
	return a.isReady
}

// Reset resets the analyzer while trying to reuse the allocated memory
func (a *attrAnalyzer) Reset() {
	a.isSaturated = false
	a.isReady = false
	clear(a.attrs)
	a.attrBuffer = a.attrBuffer[:0]
	a.attrCount = 0
	a.attrsNotEvaluated = 0
	a.topAttrCount = 0
	a.topAttrUnchangedCount = 0
	a.topAttrs = nil
}

type attrAnalyzerState struct {
	Attrs        []weightedAttribute `yaml:"attrs"`
	AttrCount    int                 `yaml:"attr_count"`
	TopAttrCount int                 `yaml:"top_attr_count"`
	IsReady      bool                `yaml:"is_ready"`
	IsSaturated  bool                `yaml:"is_saturated"`
}

func (a *attrAnalyzer) RetrieveState() *attrAnalyzerState {
	attrs := make([]weightedAttribute, 0, len(a.attrs))
	return &attrAnalyzerState{
		Attrs:        attrs,
		AttrCount:    a.attrCount,
		TopAttrCount: a.topAttrCount,
		IsReady:      a.isReady,
		IsSaturated:  a.isSaturated,
	}
}

func (a *attrAnalyzer) RestoreState(state *attrAnalyzerState) {
	a.Reset()
	a.attrs = make(map[string]*weightedAttribute, len(state.Attrs))
	for _, attr := range state.Attrs {
		a.attrs[attr.Name] = &attr
	}
	a.attrCount = state.AttrCount
	a.topAttrCount = state.TopAttrCount
	a.isReady = state.IsReady
	a.isSaturated = state.IsSaturated
}

// TopAttributes returns the top attributes by overall weight
func (a *attrAnalyzer) TopAttributes() []string {
	if a.topAttrs == nil {
		a.recalculateTopAttrs()
	}
	return a.topAttrs[:min(len(a.topAttrs), a.topAttrCount)]
}

func (a *attrAnalyzer) analyzeAttribute(attr *weightedAttribute) bool {
	a.attrCount++
	a.attrsNotEvaluated++

	stat, ok := a.attrs[attr.Name]
	if !ok {
		a.attrs[attr.Name] = attr
		return true
	}

	if stat.Weight >= math.MaxInt-attr.Weight || a.attrCount == math.MaxInt {
		a.attrsNotEvaluated = 0
		a.isSaturated = true
		return false
	}

	stat.Weight += attr.Weight
	return true
}

func (a *attrAnalyzer) recalculationRequired() bool {
	if a.attrCount > 1_000_000 || a.isReady {
		return a.attrsNotEvaluated > recalculationInterval*10
	}

	// we need to analyze at least convergedThreshold * recalculationInterval attributes to converge
	return a.attrsNotEvaluated > recalculationInterval
}

func (a *attrAnalyzer) recalculateTopAttrs() {
	topWeightedAttrs := topAttributesSort(a.attrs, a.topAttrCount, a.attrBuffer)

	topAttrs := make([]string, 0, len(topWeightedAttrs))
	for _, attr := range topWeightedAttrs {
		topAttrs = append(topAttrs, attr.Name)
	}
	slices.Sort(topAttrs)

	if slices.Equal(a.topAttrs, topAttrs) {
		a.topAttrUnchangedCount++
		if a.topAttrUnchangedCount > convergedThreshold {
			a.isReady = true
		}
	} else {
		a.topAttrUnchangedCount = 0
	}

	a.topAttrs = topAttrs
	a.attrsNotEvaluated = 0
}

// topAttributesSort is a simple implementation of topAttributes that uses slices.SortFunc to determine top attributes.
// This implementation is used for benchmarking and testing purposes.
func topAttributesSort(attrs map[string]*weightedAttribute, maxAttrs int, buffer []*weightedAttribute) []*weightedAttribute {
	buffer = buffer[:0]
	for _, attr := range attrs {
		buffer = append(buffer, attr)
	}

	slices.SortFunc(buffer, weightedAttributeSortFn)
	return buffer[:min(len(attrs), maxAttrs)]
}

func weightedAttributeSortFn(a, b *weightedAttribute) int {
	return b.Weight - a.Weight
}

type extractWeightFn func(v *v1.AnyValue) (int, bool)

func extractStringWeight(v *v1.AnyValue) (int, bool) {
	if s, ok := v.Value.(*v1.AnyValue_StringValue); ok {
		return len(s.StringValue), true
	}
	return 0, false
}

func resourceAttrIterator(extractWeight extractWeightFn) weightedAttrIterator {
	return func(trace *tempopb.Trace, callback func(attr *weightedAttribute) bool) bool {
		for _, b := range trace.Batches {
			if b.Resource != nil {
				for _, attr := range b.Resource.Attributes {
					weight, ok := extractWeight(attr.Value)
					if !ok {
						continue
					}

					wantNext := callback(&weightedAttribute{Name: attr.Key, Weight: weight})
					if !wantNext {
						return false
					}
				}
			}
		}
		return true
	}
}

func spanAttrIterator(extractWeight extractWeightFn) weightedAttrIterator {
	return func(trace *tempopb.Trace, callback func(attr *weightedAttribute) bool) bool {
		for _, b := range trace.Batches {
			for _, ss := range b.ScopeSpans {
				for _, span := range ss.Spans {
					for _, attr := range span.Attributes {
						weight, ok := extractWeight(attr.Value)
						if !ok {
							continue
						}

						wantNext := callback(&weightedAttribute{Name: attr.Key, Weight: weight})
						if !wantNext {
							return false
						}
					}
				}
			}
		}
		return true
	}
}
