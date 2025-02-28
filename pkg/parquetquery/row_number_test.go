package parquetquery

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowNumberNext(t *testing.T) {
	tests := []struct {
		name     string
		input    RowNumber
		repLvl   int
		defLvl   int
		expected RowNumber
	}{
		{
			name:     "increment root level",
			input:    RowNumber{2, -1, -1, -1, -1, -1, -1, -1},
			repLvl:   0,
			defLvl:   3,
			expected: RowNumber{3, 0, 0, 0, -1, -1, -1, -1},
		},
		{
			name:     "rep-1-def-2",
			input:    RowNumber{3, 2, 1, 0, -1, -1, -1, -1},
			repLvl:   1,
			defLvl:   2,
			expected: RowNumber{3, 3, 0, -1, -1, -1, -1, -1},
		},
		{
			name:     "rep-2-def-4",
			input:    RowNumber{3, 2, 1, 1, -1, -1, -1, -1},
			repLvl:   2,
			defLvl:   4,
			expected: RowNumber{3, 2, 2, 0, 0, -1, -1, -1},
		},
		{
			name:     "max level",
			input:    RowNumber{1, 1, 1, 1, 1, 1, 1, 1},
			repLvl:   7,
			defLvl:   7,
			expected: RowNumber{1, 1, 1, 1, 1, 1, 1, 2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := tc.input
			row.Next(tc.repLvl, tc.defLvl)

			rowSlow := tc.input
			rowSlow.nextSlow(tc.repLvl, tc.defLvl)

			assert.Equalf(t, tc.expected, row, "row: %v", row)
			assert.Equalf(t, tc.expected, rowSlow, "rowSlow: %v", rowSlow)
		})
	}
}

// TestNext compares the unrolled Next() with the original nextSlow() to
// prevent drift
func TestRowNumberNext_CompareImpl(t *testing.T) {
	rn1 := RowNumber{0, 0, 0, 0, 0, 0, 0, 0}
	rn2 := RowNumber{0, 0, 0, 0, 0, 0, 0, 0}

	for i := 0; i < 1000; i++ {
		r := rand.Intn(MaxDefinitionLevel + 1)
		d := rand.Intn(MaxDefinitionLevel + 1)

		rn1.Next(r, d)
		rn2.nextSlow(r, d)

		require.Equal(t, rn1, rn2)
	}
}

func TestRowNumberNext_Steps(t *testing.T) {
	tr := EmptyRowNumber()
	require.Equal(t, RowNumber{-1, -1, -1, -1, -1, -1, -1, -1}, tr)

	steps := []struct {
		repetitionLevel int
		definitionLevel int
		expected        RowNumber
	}{
		// Name.Language.Country examples from the Dremel whitepaper
		{0, 3, RowNumber{0, 0, 0, 0, -1, -1, -1, -1}},
		{2, 2, RowNumber{0, 0, 1, -1, -1, -1, -1, -1}},
		{1, 1, RowNumber{0, 1, -1, -1, -1, -1, -1, -1}},
		{1, 3, RowNumber{0, 2, 0, 0, -1, -1, -1, -1}},
		{0, 1, RowNumber{1, 0, -1, -1, -1, -1, -1, -1}},
	}

	for _, step := range steps {
		tr.Next(step.repetitionLevel, step.definitionLevel)
		require.Equal(t, step.expected, tr)
	}
}

func TestRowNumberNext_InvalidLevel(t *testing.T) {
	t.Skip()
	t.Run("Next -1", func(t *testing.T) {
		assertPanic(t, func() {
			rn := RowNumber{1, 2, 3, 4, 5, 6, 7, 8}
			r := 0
			d := -1
			rn.Next(r, d)
		})
	})
	t.Run("Next Max+1", func(t *testing.T) {
		assertPanic(t, func() {
			rn := RowNumber{1, 2, 3, 4, 5, 6, 7, 8}
			r := 0
			d := MaxDefinitionLevel + 1
			rn.Next(r, d)
		})
	})
}

// TestTruncate compares the unrolled TruncateRowNumber() with the original truncateRowNumberSlow() to
// prevent drift
func TestTruncateRowNumber(t *testing.T) {
	for i := 0; i <= MaxDefinitionLevel; i++ {
		rn := RowNumber{1, 2, 3, 4, 5, 6, 7, 8}

		newR := TruncateRowNumber(i, rn)
		oldR := truncateRowNumberSlow(i, rn)

		require.Equal(t, newR, oldR)
	}
}

func TestTruncateRowNumber_InvalidLevel(t *testing.T) {
	t.Run("TruncateRowNumber -1", func(t *testing.T) {
		assertPanic(t, func() {
			rn := RowNumber{1, 2, 3, 4, 5, 6, 7, 8}
			d := -1
			TruncateRowNumber(d, rn)
		})
	})
	t.Run("TruncateRowNumber Max+1", func(t *testing.T) {
		assertPanic(t, func() {
			rn := RowNumber{1, 2, 3, 4, 5, 6, 7, 8}
			d := MaxDefinitionLevel + 1
			TruncateRowNumber(d, rn)
		})
	})
}

func TestRowNumber(t *testing.T) {
	tr := EmptyRowNumber()
	require.Equal(t, RowNumber{-1, -1, -1, -1, -1, -1, -1, -1}, tr)

	steps := []struct {
		repetitionLevel int
		definitionLevel int
		expected        RowNumber
	}{
		// Name.Language.Country examples from the Dremel whitepaper
		{0, 3, RowNumber{0, 0, 0, 0, -1, -1, -1, -1}},
		{2, 2, RowNumber{0, 0, 1, -1, -1, -1, -1, -1}},
		{1, 1, RowNumber{0, 1, -1, -1, -1, -1, -1, -1}},
		{1, 3, RowNumber{0, 2, 0, 0, -1, -1, -1, -1}},
		{0, 1, RowNumber{1, 0, -1, -1, -1, -1, -1, -1}},
	}

	for _, step := range steps {
		tr.Next(step.repetitionLevel, step.definitionLevel)
		require.Equal(t, step.expected, tr)
	}
}

func TestCompareRowNumbers(t *testing.T) {
	testCases := []struct {
		a, b     RowNumber
		expected int
	}{
		{RowNumber{-1}, RowNumber{0}, -1},
		{RowNumber{0}, RowNumber{0}, 0},
		{RowNumber{1}, RowNumber{0}, 1},

		{RowNumber{0, 1}, RowNumber{0, 2}, -1},
		{RowNumber{0, 2}, RowNumber{0, 1}, 1},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.expected, CompareRowNumbers(MaxDefinitionLevel, tc.a, tc.b))
	}
}

func TestRowNumberPreceding(t *testing.T) {
	testCases := []struct {
		start, preceding RowNumber
	}{
		{RowNumber{1000, -1, -1, -1, -1, -1, -1, -1}, RowNumber{999, -1, -1, -1, -1, -1, -1, -1}},
		{RowNumber{1000, 0, 0, 0, 0, 0, 0, 0}, RowNumber{999, math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32, math.MaxInt32}},
	}

	for _, tc := range testCases {
		require.Equal(t, tc.preceding, tc.start.Preceding())
	}
}

var rowNumber RowNumber

func BenchmarkRowNumberNext(b *testing.B) {
	// Define test cases for various repetition and definition levels.
	testCases := []struct {
		name    string
		initial RowNumber
		repLvl  int
		defLvl  int
	}{
		{"root level", RowNumber{0, -1, -1, -1, -1, -1, -1, -1}, 0, 3},
		{"mid level", RowNumber{3, 2, 1, 0, -1, -1, -1, -1}, 2, 4},
		{"max level", RowNumber{1, 1, 1, 1, 1, 1, 1, 1}, 6, 7},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			rowNumber := tc.initial
			b.ResetTimer()

			for b.Loop() {
				rowNumber.nextSlow(tc.repLvl, tc.defLvl)
			}
		})
	}
}
