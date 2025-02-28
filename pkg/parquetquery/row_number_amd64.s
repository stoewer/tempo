//go:build amd64

#include "textflag.h"

// func rowNumberNext(row *RowNumber, rep, def int)
TEXT ·rowNumberNext(SB), NOSPLIT, $0

    // load all parameters to registers
    MOVQ    row+0(FP), AX     // load pointer to RowNumber array (row)
    MOVQ    rep+8(FP), BX     // load repetition level (rep)
    MOVQ    def+16(FP), CX    // load definition level (def)

    // row[rep]++
    // MOVL    (AX)(BX*4), DX    // move row[rep] to DX
    // ADDL    $1, DX            // increment DX
    // MOVL    DX, (AX)(BX*4)    // move DX back to row[rep]
    LEAQ (AX)(BX*4), DX          // Address of row[rep]
    ADDL $1, (DX)                // Increment row[rep]

    // move the row and predefined masks to 265bit registers
    VMOVDQU (AX), Y0                  // move incremented row number to Y0
    VMOVDQU indexArray(SB), Y2        // move indexArray to Y2
    VMOVDQU minusOneArray(SB), Y3     // move minusOneArray to Y3

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

// Define literals for indexArray and minusOneArray as constants
// indexArray = [0, 1, 2, 3, 4, 5, 6, 7]
// minusOneArray = [-1, -1, -1, -1, -1, -1, -1, -1]
DATA indexArray+0(SB)/4, $0
DATA indexArray+4(SB)/4, $1
DATA indexArray+8(SB)/4, $2
DATA indexArray+12(SB)/4, $3
DATA indexArray+16(SB)/4, $4
DATA indexArray+20(SB)/4, $5
DATA indexArray+24(SB)/4, $6
DATA indexArray+28(SB)/4, $7
GLOBL indexArray(SB), RODATA, $32

DATA minusOneArray+0(SB)/4, $-1
DATA minusOneArray+4(SB)/4, $-1
DATA minusOneArray+8(SB)/4, $-1
DATA minusOneArray+12(SB)/4, $-1
DATA minusOneArray+16(SB)/4, $-1
DATA minusOneArray+20(SB)/4, $-1
DATA minusOneArray+24(SB)/4, $-1
DATA minusOneArray+28(SB)/4, $-1
GLOBL minusOneArray(SB), RODATA, $32
