//go:build amd64

#include "textflag.h"

// func rowNumberNext(row *RowNumber, rep, def int)
TEXT ·rowNumberNext(SB), NOSPLIT, $0

    // load all parameters to registers
    MOVQ    row+0(FP), AX     // load pointer to RowNumber array (row)
    MOVQ    rep+8(FP), BX     // load repetition level (rep)
    MOVQ    def+16(FP), CX    // load definition level (def)

    // check ranges (no need to check for < 0, as MOVQ is an unsigned instruction)
    CMPQ    BX, $7            // Check if rep > 7
    JG      PANIC
    CMPQ    CX, $7            // Check if def > 7
    JG      PANIC

    // row[rep]++
    MOVL    (AX)(BX*4), DX    // move row[rep] to DX
    ADDL    $1, DX            // increment DX
    MOVL    DX, (AX)(BX*4)    // move DX back to row[rep]

    // move the row and predefined masks to 265bit registers
    VMOVDQU (AX), Y0                  // move incremented row number to Y0
    VMOVDQU ·indexArray(SB), Y2       // move indexArray to Y2
    VMOVDQU ·minusOneArray(SB), Y3    // move minusOneArray to Y3

    // zero all elements in Y0 after rep+1
    ADDL $1, BX                       // BX = rep+1
    VMOVD BX, X1
    VPBROADCASTD X1, Y1               // broadcast rep+1 to Y1
    VPCMPGTD Y2, Y1, Y1               // generate mask for elements >= rep+1
    VPAND Y3, Y1, Y1                  // mask zero with the complement and logical AND
    VPAND Y1, Y0, Y0                  //

    // set all elements in Y0 to -1 after def
    VMOVD CX, X1
    VPBROADCASTD X1, Y1               // broadcast def+1 to Y1
    VPCMPGTD Y1, Y2, Y1               // generate mask for elements <= def
    VPOR Y1, Y0, Y0

    VMOVDQU Y0, (AX)       // move the result back to row/AX

    VZEROUPPER
    RET

PANIC:
    CALL runtime·gopanic(SB)





