package traceql

import (
	"fmt"
	"math"
	"regexp"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
)

type Element interface {
	fmt.Stringer
	validate() error
}

type metricsFirstStageElement interface {
	Element
	extractConditions(request *FetchSpansRequest)
	init(req *tempopb.QueryRangeRequest, mode AggregateMode)
	observe(Span)                        // TODO - batching?
	observeSeries([]*tempopb.TimeSeries) // Re-entrant metrics on the query-frontend.  Using proto version for efficiency
	result() SeriesSet
}

type pipelineElement interface {
	Element
	extractConditions(request *FetchSpansRequest)
	evaluate([]*Spanset) ([]*Spanset, error)
}

type typedExpression interface {
	impliedType() StaticType
}

type RootExpr struct {
	Pipeline        Pipeline
	MetricsPipeline metricsFirstStageElement
	Hints           *Hints
}

func newRootExpr(e pipelineElement) *RootExpr {
	p, ok := e.(Pipeline)
	if !ok {
		p = newPipeline(e)
	}

	return &RootExpr{
		Pipeline: p,
	}
}

func newRootExprWithMetrics(e pipelineElement, m metricsFirstStageElement) *RootExpr {
	p, ok := e.(Pipeline)
	if !ok {
		p = newPipeline(e)
	}

	return &RootExpr{
		Pipeline:        p,
		MetricsPipeline: m,
	}
}

func (r *RootExpr) withHints(h *Hints) *RootExpr {
	r.Hints = h
	return r
}

// **********************
// Pipeline
// **********************

type Pipeline struct {
	Elements []pipelineElement
}

// nolint: revive
func (Pipeline) __scalarExpression() {}

// nolint: revive
func (Pipeline) __spansetExpression() {}

func newPipeline(i ...pipelineElement) Pipeline {
	return Pipeline{
		Elements: i,
	}
}

func (p Pipeline) addItem(i pipelineElement) Pipeline {
	p.Elements = append(p.Elements, i)
	return p
}

func (p Pipeline) impliedType() StaticType {
	if len(p.Elements) == 0 {
		return TypeSpanset
	}

	finalItem := p.Elements[len(p.Elements)-1]
	aggregate, ok := finalItem.(Aggregate)
	if ok {
		return aggregate.impliedType()
	}

	return TypeSpanset
}

func (p Pipeline) extractConditions(req *FetchSpansRequest) {
	for _, element := range p.Elements {
		element.extractConditions(req)
	}
	// TODO this needs to be fine-tuned a bit, e.g. { .foo = "bar" } | by(.namespace), AllConditions can still be true
	if len(p.Elements) > 1 {
		req.AllConditions = false
	}
}

func (p Pipeline) evaluate(input []*Spanset) (result []*Spanset, err error) {
	result = input

	for _, element := range p.Elements {
		result, err = element.evaluate(result)
		if err != nil {
			return nil, err
		}

		if len(result) == 0 {
			return []*Spanset{}, nil
		}
	}

	return result, nil
}

type GroupOperation struct {
	Expression FieldExpression

	groupBuffer map[Static]*Spanset
}

func newGroupOperation(e FieldExpression) GroupOperation {
	return GroupOperation{
		Expression:  e,
		groupBuffer: make(map[Static]*Spanset),
	}
}

func (o GroupOperation) extractConditions(request *FetchSpansRequest) {
	o.Expression.extractConditions(request)
}

type CoalesceOperation struct{}

func newCoalesceOperation() CoalesceOperation {
	return CoalesceOperation{}
}

func (o CoalesceOperation) extractConditions(*FetchSpansRequest) {
}

type SelectOperation struct {
	attrs []Attribute
}

func newSelectOperation(exprs []Attribute) SelectOperation {
	return SelectOperation{
		attrs: exprs,
	}
}

// **********************
// Scalars
// **********************
type ScalarExpression interface {
	// pipelineElement
	Element
	typedExpression
	__scalarExpression()

	extractConditions(request *FetchSpansRequest)
}

type ScalarOperation struct {
	Op  Operator
	LHS ScalarExpression
	RHS ScalarExpression
}

func newScalarOperation(op Operator, lhs, rhs ScalarExpression) ScalarOperation {
	return ScalarOperation{
		Op:  op,
		LHS: lhs,
		RHS: rhs,
	}
}

// nolint: revive
func (ScalarOperation) __scalarExpression() {}

func (o ScalarOperation) impliedType() StaticType {
	if o.Op.isBoolean() {
		return TypeBoolean
	}

	// remaining operators will be based on the operands
	// opAdd, opSub, opDiv, opMod, opMult
	t := o.LHS.impliedType()
	if t != TypeAttribute {
		return t
	}

	return o.RHS.impliedType()
}

func (o ScalarOperation) extractConditions(request *FetchSpansRequest) {
	o.LHS.extractConditions(request)
	o.RHS.extractConditions(request)
	request.AllConditions = false
}

type Aggregate struct {
	op AggregateOp
	e  FieldExpression
}

func newAggregate(agg AggregateOp, e FieldExpression) Aggregate {
	return Aggregate{
		op: agg,
		e:  e,
	}
}

// nolint: revive
func (Aggregate) __scalarExpression() {}

func (a Aggregate) impliedType() StaticType {
	if a.op == aggregateCount || a.e == nil {
		return TypeInt
	}

	return a.e.impliedType()
}

func (a Aggregate) extractConditions(request *FetchSpansRequest) {
	if a.e != nil {
		a.e.extractConditions(request)
	}
}

// **********************
// Spansets
// **********************
type SpansetExpression interface {
	pipelineElement
	__spansetExpression()
}

type SpansetOperation struct {
	Op                  Operator
	LHS                 SpansetExpression
	RHS                 SpansetExpression
	matchingSpansBuffer []Span
}

func (o SpansetOperation) extractConditions(request *FetchSpansRequest) {
	switch o.Op {
	case OpSpansetDescendant, OpSpansetAncestor, OpSpansetNotDescendant, OpSpansetNotAncestor, OpSpansetUnionDescendant, OpSpansetUnionAncestor:
		request.Conditions = append(request.Conditions, Condition{
			Attribute: NewIntrinsic(IntrinsicStructuralDescendant),
		})
	case OpSpansetChild, OpSpansetParent, OpSpansetNotChild, OpSpansetNotParent, OpSpansetUnionChild, OpSpansetUnionParent:
		request.Conditions = append(request.Conditions, Condition{
			Attribute: NewIntrinsic(IntrinsicStructuralChild),
		})
	case OpSpansetSibling, OpSpansetNotSibling, OpSpansetUnionSibling:
		request.Conditions = append(request.Conditions, Condition{
			Attribute: NewIntrinsic(IntrinsicStructuralSibling),
		})
	}

	o.LHS.extractConditions(request)
	o.RHS.extractConditions(request)

	request.AllConditions = false
}

func newSpansetOperation(op Operator, lhs, rhs SpansetExpression) SpansetOperation {
	return SpansetOperation{
		Op:  op,
		LHS: lhs,
		RHS: rhs,
	}
}

// nolint: revive
func (SpansetOperation) __spansetExpression() {}

type SpansetFilter struct {
	Expression          FieldExpression
	matchingSpansBuffer []Span
}

func newSpansetFilter(e FieldExpression) *SpansetFilter {
	return &SpansetFilter{
		Expression: e,
	}
}

// nolint: revive
func (*SpansetFilter) __spansetExpression() {}

func (f *SpansetFilter) evaluate(input []*Spanset) ([]*Spanset, error) {
	var outputBuffer []*Spanset

	for _, ss := range input {
		if len(ss.Spans) == 0 {
			continue
		}

		f.matchingSpansBuffer = f.matchingSpansBuffer[:0]

		for _, s := range ss.Spans {
			result, err := f.Expression.execute(s)
			if err != nil {
				return nil, err
			}

			if result.Type != TypeBoolean {
				continue
			}

			if !result.B {
				continue
			}

			f.matchingSpansBuffer = append(f.matchingSpansBuffer, s)
		}

		if len(f.matchingSpansBuffer) == 0 {
			continue
		}

		if len(f.matchingSpansBuffer) == len(ss.Spans) {
			// All matched, so we return the input as-is
			// and preserve the local buffer.
			outputBuffer = append(outputBuffer, ss)
			continue
		}

		matchingSpanset := ss.clone()
		matchingSpanset.Spans = append([]Span(nil), f.matchingSpansBuffer...)
		outputBuffer = append(outputBuffer, matchingSpanset)
	}

	return outputBuffer, nil
}

type ScalarFilter struct {
	op  Operator
	lhs ScalarExpression
	rhs ScalarExpression
}

func newScalarFilter(op Operator, lhs, rhs ScalarExpression) ScalarFilter {
	return ScalarFilter{
		op:  op,
		lhs: lhs,
		rhs: rhs,
	}
}

// nolint: revive
func (ScalarFilter) __spansetExpression() {}

func (f ScalarFilter) extractConditions(request *FetchSpansRequest) {
	f.lhs.extractConditions(request)
	f.rhs.extractConditions(request)
	request.AllConditions = false
}

// **********************
// Expressions
// **********************
type FieldExpression interface {
	Element
	typedExpression

	// referencesSpan returns true if this field expression has any attributes or intrinsics. i.e. it references the span itself
	referencesSpan() bool
	__fieldExpression()

	extractConditions(request *FetchSpansRequest)
	execute(span Span) (Static, error)
}

type BinaryOperation struct {
	Op  Operator
	LHS FieldExpression
	RHS FieldExpression

	compiledExpression *regexp.Regexp
}

func newBinaryOperation(op Operator, lhs, rhs FieldExpression) FieldExpression {
	binop := &BinaryOperation{
		Op:  op,
		LHS: lhs,
		RHS: rhs,
	}

	if !binop.referencesSpan() && binop.validate() == nil {
		if simplified, err := binop.execute(nil); err == nil {
			return simplified
		}
	}

	return binop
}

// nolint: revive
func (BinaryOperation) __fieldExpression() {}

func (o *BinaryOperation) impliedType() StaticType {
	if o.Op.isBoolean() {
		return TypeBoolean
	}

	// remaining operators will be based on the operands
	// opAdd, opSub, opDiv, opMod, opMult
	t := o.LHS.impliedType()
	if t != TypeAttribute {
		return t
	}

	return o.RHS.impliedType()
}

func (o *BinaryOperation) referencesSpan() bool {
	return o.LHS.referencesSpan() || o.RHS.referencesSpan()
}

type UnaryOperation struct {
	Op         Operator
	Expression FieldExpression
}

func newUnaryOperation(op Operator, e FieldExpression) FieldExpression {
	unop := UnaryOperation{
		Op:         op,
		Expression: e,
	}

	if !unop.referencesSpan() && unop.validate() == nil {
		if simplified, err := unop.execute(nil); err == nil {
			return simplified
		}
	}

	return unop
}

// nolint: revive
func (UnaryOperation) __fieldExpression() {}

func (o UnaryOperation) impliedType() StaticType {
	// both operators (opPower and opNot) will just be based on the operand type
	return o.Expression.impliedType()
}

func (o UnaryOperation) referencesSpan() bool {
	return o.Expression.referencesSpan()
}

// **********************
// Statics
// **********************
type Static struct {
	Type   StaticType
	N      int
	F      float64
	S      string
	B      bool
	D      time.Duration
	Status Status // todo: can we just use the N member for status and kind?
	Kind   Kind
}

// nolint: revive
func (Static) __fieldExpression() {}

// nolint: revive
func (Static) __scalarExpression() {}

func (Static) referencesSpan() bool {
	return false
}

func (s Static) impliedType() StaticType {
	return s.Type
}

func (s Static) Equals(other Static) bool {
	// if they are different number types. compare them as floats. however, if they are the same type just fall through to
	// a normal comparison which should be more efficient
	differentNumberTypes := (s.Type == TypeInt || s.Type == TypeFloat || s.Type == TypeDuration) &&
		(other.Type == TypeInt || other.Type == TypeFloat || other.Type == TypeDuration) &&
		s.Type != other.Type
	if differentNumberTypes {
		return s.asFloat() == other.asFloat()
	}

	eitherIsTypeStatus := (s.Type == TypeStatus && other.Type == TypeInt) || (other.Type == TypeStatus && s.Type == TypeInt)
	if eitherIsTypeStatus {
		if s.Type == TypeStatus {
			return s.Status == Status(other.N)
		}
		return Status(s.N) == other.Status
	}

	// no special cases, just compare directly
	return s == other
}

func (s Static) compare(other *Static) int {
	if s.Type != other.Type {
		if s.asFloat() > other.asFloat() {
			return 1
		} else if s.asFloat() < other.asFloat() {
			return -1
		}

		return 0
	}

	switch s.Type {
	case TypeInt:
		if s.N > other.N {
			return 1
		} else if s.N < other.N {
			return -1
		}
	case TypeFloat:
		if s.F > other.F {
			return 1
		} else if s.F < other.F {
			return -1
		}
	case TypeDuration:
		if s.D > other.D {
			return 1
		} else if s.D < other.D {
			return -1
		}
	case TypeString:
		if s.S > other.S {
			return 1
		} else if s.S < other.S {
			return -1
		}
	case TypeBoolean:
		if s.B && !other.B {
			return 1
		} else if !s.B && other.B {
			return -1
		}
	case TypeStatus:
		if s.Status > other.Status {
			return 1
		} else if s.Status < other.Status {
			return -1
		}
	case TypeKind:
		if s.Kind > other.Kind {
			return 1
		} else if s.Kind < other.Kind {
			return -1
		}
	}

	return 0
}

func (s Static) Add(o *Static) *Static {
	switch s.Type {
	case TypeInt:
		s.N += o.N
	case TypeFloat:
		s.F += o.F
	case TypeDuration:
		s.D += o.D
	}
	return &s
}

func (s Static) Divide(f float64) *Static {
	switch s.Type {
	case TypeInt:
		s = NewStaticFloat(float64(s.N) / f) // there's no integer division in traceql
	case TypeFloat:
		s.F /= f
	case TypeDuration:
		s.D /= time.Duration(f)
	}
	return &s
}

func (s Static) asFloat() float64 {
	switch s.Type {
	case TypeInt:
		return float64(s.N)
	case TypeFloat:
		return s.F
	case TypeDuration:
		return float64(s.D.Nanoseconds())
	default:
		return math.NaN()
	}
}

func NewStaticInt(n int) Static {
	return Static{
		Type: TypeInt,
		N:    n,
	}
}

func NewStaticFloat(f float64) Static {
	return Static{
		Type: TypeFloat,
		F:    f,
	}
}

func NewStaticString(s string) Static {
	return Static{
		Type: TypeString,
		S:    s,
	}
}

func NewStaticBool(b bool) Static {
	return Static{
		Type: TypeBoolean,
		B:    b,
	}
}

func NewStaticNil() Static {
	return Static{
		Type: TypeNil,
	}
}

func NewStaticDuration(d time.Duration) Static {
	return Static{
		Type: TypeDuration,
		D:    d,
	}
}

func NewStaticStatus(s Status) Static {
	return Static{
		Type:   TypeStatus,
		Status: s,
	}
}

func NewStaticKind(k Kind) Static {
	return Static{
		Type: TypeKind,
		Kind: k,
	}
}

// **********************
// Attributes
// **********************

type Attribute struct {
	Scope     AttributeScope
	Parent    bool
	Name      string
	Intrinsic Intrinsic
}

// NewAttribute creates a new attribute with the given identifier string.
func NewAttribute(att string) Attribute {
	return Attribute{
		Scope:     AttributeScopeNone,
		Parent:    false,
		Name:      att,
		Intrinsic: IntrinsicNone,
	}
}

// nolint: revive
func (Attribute) __fieldExpression() {}

func (a Attribute) impliedType() StaticType {
	switch a.Intrinsic {
	case IntrinsicDuration:
		return TypeDuration
	case IntrinsicChildCount:
		return TypeInt
	case IntrinsicName:
		return TypeString
	case IntrinsicStatus:
		return TypeStatus
	case IntrinsicStatusMessage:
		return TypeString
	case IntrinsicKind:
		return TypeKind
	case IntrinsicEventName:
		return TypeString
	case IntrinsicLinkTraceID:
		return TypeString
	case IntrinsicLinkSpanID:
		return TypeString
	case IntrinsicParent:
		return TypeNil
	case IntrinsicTraceDuration:
		return TypeDuration
	case IntrinsicTraceRootService:
		return TypeString
	case IntrinsicTraceRootSpan:
		return TypeString
	case IntrinsicNestedSetLeft:
		return TypeInt
	case IntrinsicNestedSetRight:
		return TypeInt
	case IntrinsicNestedSetParent:
		return TypeInt
	case IntrinsicTraceID:
		return TypeString
	case IntrinsicSpanID:
		return TypeString
	}

	return TypeAttribute
}

func (Attribute) referencesSpan() bool {
	return true
}

// NewScopedAttribute creates a new scopedattribute with the given identifier string.
// this handles parent, span, and resource scopes.
func NewScopedAttribute(scope AttributeScope, parent bool, att string) Attribute {
	intrinsic := IntrinsicNone
	// if we are explicitly passed a resource or span scopes then we shouldn't parse for intrinsic
	if scope == AttributeScopeNone && !parent {
		intrinsic = intrinsicFromString(att)
	}

	return Attribute{
		Scope:     scope,
		Parent:    parent,
		Name:      att,
		Intrinsic: intrinsic,
	}
}

func NewIntrinsic(n Intrinsic) Attribute {
	return Attribute{
		Scope:     AttributeScopeNone,
		Parent:    false,
		Name:      n.String(),
		Intrinsic: n,
	}
}

var (
	_ pipelineElement = (*Pipeline)(nil)
	_ pipelineElement = (*Aggregate)(nil)
	_ pipelineElement = (*SpansetOperation)(nil)
	_ pipelineElement = (*SpansetFilter)(nil)
	_ pipelineElement = (*CoalesceOperation)(nil)
	_ pipelineElement = (*ScalarFilter)(nil)
	_ pipelineElement = (*GroupOperation)(nil)
)

// MetricsAggregate is a placeholder in the AST for a metrics aggregation
// pipeline element. It has a superset of the properties of them all, and
// builds them later via init() so that appropriate buffers can be allocated
// for the query time range and step, and different implementations for
// shardable and unshardable pipelines.
type MetricsAggregate struct {
	op        MetricsAggregateOp
	by        []Attribute
	attr      Attribute
	floats    []float64
	agg       SpanAggregator
	seriesAgg SeriesAggregator
}

func newMetricsAggregate(agg MetricsAggregateOp, by []Attribute) *MetricsAggregate {
	return &MetricsAggregate{
		op: agg,
		by: by,
	}
}

func newMetricsAggregateQuantileOverTime(attr Attribute, qs []float64, by []Attribute) *MetricsAggregate {
	return &MetricsAggregate{
		op:     metricsAggregateQuantileOverTime,
		floats: qs,
		attr:   attr,
		by:     by,
	}
}

func newMetricsAggregateHistogramOverTime(attr Attribute, by []Attribute) *MetricsAggregate {
	return &MetricsAggregate{
		op:   metricsAggregateHistogramOverTime,
		by:   by,
		attr: attr,
	}
}

func (a *MetricsAggregate) extractConditions(request *FetchSpansRequest) {
	switch a.op {
	case metricsAggregateRate, metricsAggregateCountOverTime:
		// No extra conditions, start time is already enough
	case metricsAggregateQuantileOverTime, metricsAggregateHistogramOverTime:
		if !request.HasAttribute(a.attr) {
			request.SecondPassConditions = append(request.SecondPassConditions, Condition{
				Attribute: a.attr,
			})
		}
	}

	for _, b := range a.by {
		if !request.HasAttribute(b) {
			request.SecondPassConditions = append(request.SecondPassConditions, Condition{
				Attribute: b,
			})
		}
	}
}

func (a *MetricsAggregate) init(q *tempopb.QueryRangeRequest, mode AggregateMode) {
	switch mode {
	case AggregateModeSum:
		a.initSum(q)
		return

	case AggregateModeFinal:
		a.initFinal(q)
		return
	}

	// Raw mode:

	var innerAgg func() VectorAggregator
	var byFunc func(Span) (Static, bool)
	var byFuncLabel string

	switch a.op {
	case metricsAggregateCountOverTime:
		innerAgg = func() VectorAggregator { return NewCountOverTimeAggregator() }

	case metricsAggregateRate:
		innerAgg = func() VectorAggregator { return NewRateAggregator(1.0 / time.Duration(q.Step).Seconds()) }

	case metricsAggregateHistogramOverTime:
		// Histograms are implemented as count_over_time() by(2^log2(attr)) for now
		// This is very similar to quantile_over_time except the bucket values are the true
		// underlying value in scale, i.e. a duration of 500ms will be in __bucket==0.512s
		// The difference is that quantile_over_time has to calculate the final quantiles
		// so in that case the log2 bucket number is more useful.  We can clean it up later
		// when updating quantiles to be smarter and more customizable range of buckets.
		innerAgg = func() VectorAggregator { return NewCountOverTimeAggregator() }
		byFuncLabel = internalLabelBucket
		switch a.attr {
		case IntrinsicDurationAttribute:
			// Optimal implementation for duration attribute
			byFunc = func(s Span) (Static, bool) {
				d := s.DurationNanos()
				if d < 2 {
					return Static{}, false
				}
				// Bucket is log2(nanos) converted to float seconds
				return NewStaticFloat(Log2Bucketize(d) / float64(time.Second)), true
			}
		default:
			// Basic implementation for all other attributes
			byFunc = func(s Span) (Static, bool) {
				v, ok := s.AttributeFor(a.attr)
				if !ok {
					return Static{}, false
				}

				// TODO(mdisibio) - Add support for floats, we need to map them into buckets.
				// Because of the range of floats, we need a native histogram approach.
				if v.Type != TypeInt {
					return Static{}, false
				}

				if v.N < 2 {
					return Static{}, false
				}
				// Bucket is the value rounded up to the nearest power of 2
				return NewStaticFloat(Log2Bucketize(uint64(v.N))), true
			}
		}

	case metricsAggregateQuantileOverTime:
		// Quantiles are implemented as count_over_time() by(log2(attr)) for now
		innerAgg = func() VectorAggregator { return NewCountOverTimeAggregator() }
		byFuncLabel = internalLabelBucket
		switch a.attr {
		case IntrinsicDurationAttribute:
			// Optimal implementation for duration attribute
			byFunc = func(s Span) (Static, bool) {
				d := s.DurationNanos()
				if d < 2 {
					return Static{}, false
				}
				// Bucket is in seconds
				return NewStaticFloat(Log2Bucketize(d) / float64(time.Second)), true
			}
		default:
			// Basic implementation for all other attributes
			byFunc = func(s Span) (Static, bool) {
				v, ok := s.AttributeFor(a.attr)
				if !ok {
					return Static{}, false
				}

				// TODO(mdisibio) - Add support for floats, we need to map them into buckets.
				// Because of the range of floats, we need a native histogram approach.
				if v.Type != TypeInt {
					return Static{}, false
				}

				if v.N < 2 {
					return Static{}, false
				}
				return NewStaticFloat(Log2Bucketize(uint64(v.N))), true
			}
		}
	}

	a.agg = NewGroupingAggregator(a.op.String(), func() RangeAggregator {
		return NewStepAggregator(q.Start, q.End, q.Step, innerAgg)
	}, a.by, byFunc, byFuncLabel)
}

func (a *MetricsAggregate) initSum(q *tempopb.QueryRangeRequest) {
	// Currently all metrics are summed by job to produce
	// intermediate results. This will change when adding min/max/topk/etc
	a.seriesAgg = NewSimpleAdditionCombiner(q)
}

func (a *MetricsAggregate) initFinal(q *tempopb.QueryRangeRequest) {
	switch a.op {
	case metricsAggregateQuantileOverTime:
		a.seriesAgg = NewHistogramAggregator(q, a.floats)
	default:
		// These are simple additions by series
		a.seriesAgg = NewSimpleAdditionCombiner(q)
	}
}

func (a *MetricsAggregate) observe(span Span) {
	a.agg.Observe(span)
}

func (a *MetricsAggregate) observeSeries(ss []*tempopb.TimeSeries) {
	a.seriesAgg.Combine(ss)
}

func (a *MetricsAggregate) result() SeriesSet {
	if a.agg != nil {
		return a.agg.Series()
	}

	// In the frontend-version the results come from
	// the job-level aggregator
	return a.seriesAgg.Results()
}

func (a *MetricsAggregate) validate() error {
	switch a.op {
	case metricsAggregateCountOverTime:
	case metricsAggregateRate:
	case metricsAggregateHistogramOverTime:
		if len(a.by) >= maxGroupBys {
			// We reserve a spot for the bucket so quantile has 1 less group by
			return newUnsupportedError(fmt.Sprintf("metrics group by %v values", len(a.by)))
		}
	case metricsAggregateQuantileOverTime:
		if len(a.by) >= maxGroupBys {
			// We reserve a spot for the bucket so quantile has 1 less group by
			return newUnsupportedError(fmt.Sprintf("metrics group by %v values", len(a.by)))
		}
		for _, q := range a.floats {
			if q < 0 || q > 1 {
				return fmt.Errorf("quantile must be between 0 and 1: %v", q)
			}
		}
	default:
		return newUnsupportedError(fmt.Sprintf("metrics aggregate operation (%v)", a.op))
	}

	if len(a.by) > maxGroupBys {
		return newUnsupportedError(fmt.Sprintf("metrics group by %v values", len(a.by)))
	}

	return nil
}

var _ metricsFirstStageElement = (*MetricsAggregate)(nil)
