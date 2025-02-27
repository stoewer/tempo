//go:build !amd64

package parquetquery

func rowNumberNext(row *RowNumber, rep, def int) {
	row[rep]++

	// the following is nextSlow() unrolled
	switch rep {
	case 0:
		switch def {
		case 0:
			row[1] = -1
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 1:
			row[1] = 0
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 2:
			row[1] = 0
			row[2] = 0
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 3:
			row[1] = 0
			row[2] = 0
			row[3] = 0
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 4:
			row[1] = 0
			row[2] = 0
			row[3] = 0
			row[4] = 0
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 5:
			row[1] = 0
			row[2] = 0
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = -1
			row[7] = -1
		case 6:
			row[1] = 0
			row[2] = 0
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = 0
			row[7] = -1
		case 7:
			row[1] = 0
			row[2] = 0
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = 0
			row[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 1:
		switch def {
		case 0:
			row[1] = -1
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 1:
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 2:
			row[2] = 0
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 3:
			row[2] = 0
			row[3] = 0
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 4:
			row[2] = 0
			row[3] = 0
			row[4] = 0
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 5:
			row[2] = 0
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = -1
			row[7] = -1
		case 6:
			row[2] = 0
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = 0
			row[7] = -1
		case 7:
			row[2] = 0
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = 0
			row[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 2:
		switch def {
		case 0:
			row[1] = -1
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 1:
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 2:
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 3:
			row[3] = 0
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 4:
			row[3] = 0
			row[4] = 0
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 5:
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = -1
			row[7] = -1
		case 6:
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = 0
			row[7] = -1
		case 7:
			row[3] = 0
			row[4] = 0
			row[5] = 0
			row[6] = 0
			row[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 3:
		switch def {
		case 0:
			row[1] = -1
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 1:
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 2:
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 3:
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 4:
			row[4] = 0
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 5:
			row[4] = 0
			row[5] = 0
			row[6] = -1
			row[7] = -1
		case 6:
			row[4] = 0
			row[5] = 0
			row[6] = 0
			row[7] = -1
		case 7:
			row[4] = 0
			row[5] = 0
			row[6] = 0
			row[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 4:
		switch def {
		case 0:
			row[1] = -1
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 1:
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 2:
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 3:
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 4:
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 5:
			row[5] = 0
			row[6] = -1
			row[7] = -1
		case 6:
			row[5] = 0
			row[6] = 0
			row[7] = -1
		case 7:
			row[5] = 0
			row[6] = 0
			row[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 5:
		switch def {
		case 0:
			row[1] = -1
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 1:
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 2:
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 3:
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 4:
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 5:
			row[6] = -1
			row[7] = -1
		case 6:
			row[6] = 0
			row[7] = -1
		case 7:
			row[6] = 0
			row[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 6:
		switch def {
		case 0:
			row[1] = -1
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 1:
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 2:
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 3:
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 4:
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 5:
			row[6] = -1
			row[7] = -1
		case 6:
			row[7] = -1
		case 7:
			row[7] = 0
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	case 7:
		switch def {
		case 0:
			row[1] = -1
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 1:
			row[2] = -1
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 2:
			row[3] = -1
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 3:
			row[4] = -1
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 4:
			row[5] = -1
			row[6] = -1
			row[7] = -1
		case 5:
			row[6] = -1
			row[7] = -1
		case 6:
			row[7] = -1
		case 7:
		default:
			panicWhenInvalidDefinitionLevel(def)
		}
	}
}
