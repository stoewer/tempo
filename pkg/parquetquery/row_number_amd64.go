//go:build amd64

package parquetquery

var (
	indexArray    = [8]int32{0, 1, 2, 3, 4, 5, 6, 7}
	zeroArray     = [8]int32{0, 0, 0, 0, 0, 0, 0, 0}
	minusOneArray = [8]int32{-1, -1, -1, -1, -1, -1, -1, -1}
)

func rowNumberNext(row *RowNumber, rep, def int)
