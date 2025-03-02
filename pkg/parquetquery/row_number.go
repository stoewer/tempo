package parquetquery

import (
	"fmt"
	"math"
)

const MaxDefinitionLevel = 7

// RowNumber is the sequence of row numbers uniquely identifying a value
// in a tree of nested columns, starting at the top-level and including
// another row number for each level of nesting. -1 is a placeholder
// for undefined at lower levels.  RowNumbers can be compared for full
// equality using the == operator, or can be compared partially, looking
// for equal lineages down to a certain level.
// For example given the following tree, the row numbers would be:
//
//	A          0, -1, -1
//	  B        0,  0, -1
//	  C        0,  1, -1
//	    D      0,  1,  0
//	  E        0,  2, -1
//
// Currently supports 8 levels of nesting which should be enough for anybody. :)
type RowNumber [8]int32

// EmptyRowNumber creates an empty invalid row number.
func EmptyRowNumber() RowNumber {
	return RowNumber{-1, -1, -1, -1, -1, -1, -1, -1}
}

// MaxRowNumber is a helper that represents the maximum(-ish) representable value.
func MaxRowNumber() RowNumber {
	return RowNumber{math.MaxInt32}
}

// CompareRowNumbers compares the sequences of row numbers in
// a and b for partial equality, descending from top-level
// through the given definition level.
// For example, definition level 1 means that row numbers are compared
// at two levels of nesting, the top-level and 1 level of nesting
// below.
func CompareRowNumbers(upToDefinitionLevel int, a, b RowNumber) int {
	for i := 0; i <= upToDefinitionLevel; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// EqualRowNumber compares the sequences of row numbers in a and b
// for partial equality. A little faster than CompareRowNumbers(d,a,b)==0
func EqualRowNumber(upToDefinitionLevel int, a, b RowNumber) bool {
	for i := 0; i <= upToDefinitionLevel; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func truncateRowNumberSlow(definitionLevelToKeep int, t RowNumber) RowNumber {
	n := EmptyRowNumber()
	for i := 0; i <= definitionLevelToKeep; i++ {
		n[i] = t[i]
	}
	return n
}

func TruncateRowNumber(definitionLevelToKeep int, t RowNumber) RowNumber {
	switch definitionLevelToKeep {
	case 0:
		return RowNumber{t[0], -1, -1, -1, -1, -1, -1, -1}
	case 1:
		return RowNumber{t[0], t[1], -1, -1, -1, -1, -1, -1}
	case 2:
		return RowNumber{t[0], t[1], t[2], -1, -1, -1, -1, -1}
	case 3:
		return RowNumber{t[0], t[1], t[2], t[3], -1, -1, -1, -1}
	case 4:
		return RowNumber{t[0], t[1], t[2], t[3], t[4], -1, -1, -1}
	case 5:
		return RowNumber{t[0], t[1], t[2], t[3], t[4], t[5], -1, -1}
	case 6:
		return RowNumber{t[0], t[1], t[2], t[3], t[4], t[5], t[6], -1}
	case 7:
		return RowNumber{t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7]}
	default:
		panic(fmt.Sprintf("definition level out of bound: should be [0:7] but got %d", definitionLevelToKeep))
	}
}

func (t *RowNumber) Valid() bool {
	return t[0] >= 0
}

func (t *RowNumber) Next(repetitionLevel, definitionLevel int) {
	rowNumberNext(t, repetitionLevel, definitionLevel)
}

// nextSlow is the original implementation of next. it is kept to test against
// the unrolled version above
//
//go:noinline
func (t *RowNumber) nextSlow(repetitionLevel, definitionLevel int) {
	t[repetitionLevel]++

	// New children up through the definition level
	for i := repetitionLevel + 1; i <= definitionLevel; i++ {
		t[i] = 0
	}

	// // Children past the definition level are undefined
	for i := definitionLevel + 1; i < len(t); i++ {
		t[i] = -1
	}
}

func (t *RowNumber) nextUnrolled(rep, def int) {
	t[rep]++

	// the following is nextSlow() unrolled
	switch rep {
	case 0:
		switch def {
		case 0:
			t[1] = -1
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 1:
			t[1] = 0
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 2:
			t[1] = 0
			t[2] = 0
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 3:
			t[1] = 0
			t[2] = 0
			t[3] = 0
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 4:
			t[1] = 0
			t[2] = 0
			t[3] = 0
			t[4] = 0
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 5:
			t[1] = 0
			t[2] = 0
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = -1
			t[7] = -1
		case 6:
			t[1] = 0
			t[2] = 0
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = 0
			t[7] = -1
		case 7:
			t[1] = 0
			t[2] = 0
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = 0
			t[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 1:
		switch def {
		case 0:
			t[1] = -1
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 1:
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 2:
			t[2] = 0
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 3:
			t[2] = 0
			t[3] = 0
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 4:
			t[2] = 0
			t[3] = 0
			t[4] = 0
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 5:
			t[2] = 0
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = -1
			t[7] = -1
		case 6:
			t[2] = 0
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = 0
			t[7] = -1
		case 7:
			t[2] = 0
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = 0
			t[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 2:
		switch def {
		case 0:
			t[1] = -1
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 1:
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 2:
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 3:
			t[3] = 0
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 4:
			t[3] = 0
			t[4] = 0
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 5:
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = -1
			t[7] = -1
		case 6:
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = 0
			t[7] = -1
		case 7:
			t[3] = 0
			t[4] = 0
			t[5] = 0
			t[6] = 0
			t[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 3:
		switch def {
		case 0:
			t[1] = -1
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 1:
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 2:
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 3:
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 4:
			t[4] = 0
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 5:
			t[4] = 0
			t[5] = 0
			t[6] = -1
			t[7] = -1
		case 6:
			t[4] = 0
			t[5] = 0
			t[6] = 0
			t[7] = -1
		case 7:
			t[4] = 0
			t[5] = 0
			t[6] = 0
			t[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 4:
		switch def {
		case 0:
			t[1] = -1
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 1:
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 2:
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 3:
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 4:
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 5:
			t[5] = 0
			t[6] = -1
			t[7] = -1
		case 6:
			t[5] = 0
			t[6] = 0
			t[7] = -1
		case 7:
			t[5] = 0
			t[6] = 0
			t[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 5:
		switch def {
		case 0:
			t[1] = -1
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 1:
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 2:
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 3:
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 4:
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 5:
			t[6] = -1
			t[7] = -1
		case 6:
			t[6] = 0
			t[7] = -1
		case 7:
			t[6] = 0
			t[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 6:
		switch def {
		case 0:
			t[1] = -1
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 1:
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 2:
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 3:
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 4:
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 5:
			t[6] = -1
			t[7] = -1
		case 6:
			t[7] = -1
		case 7:
			t[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 7:
		switch def {
		case 0:
			t[1] = -1
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 1:
			t[2] = -1
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 2:
			t[3] = -1
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 3:
			t[4] = -1
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 4:
			t[5] = -1
			t[6] = -1
			t[7] = -1
		case 5:
			t[6] = -1
			t[7] = -1
		case 6:
			t[7] = -1
		case 7:
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	}
}

// Skip rows at the root-level.
func (t *RowNumber) Skip(numRows int64) {
	t[0] += int32(numRows)
	for i := 1; i < len(t); i++ {
		t[i] = -1
	}
}

// Preceding returns the largest representable row number that is immediately prior to this
// one. Think of it like math.NextAfter but for segmented row numbers. Examples:
//
//		RowNumber 1000.0.0 (defined at 3 levels) is preceded by 999.max.max
//	    RowNumber 1000.-1.-1 (defined at 1 level) is preceded by 999.-1.-1
func (t RowNumber) Preceding() RowNumber {
	for i := len(t) - 1; i >= 0; i-- {
		switch t[i] {
		case -1:
			continue
		case 0:
			t[i] = math.MaxInt32
		default:
			t[i]--
			return t
		}
	}
	return t
}
