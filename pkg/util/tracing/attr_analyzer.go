package tracing

import (
	"math"
	"slices"

	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
)

const (
	convergedThreshold    = 30
	recalculationInterval = 10_000
)

type weightedAttribute struct {
	Name   string // the attributes name
	Weight int    // an abstract indicator of the attribute's occurrence and size
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
