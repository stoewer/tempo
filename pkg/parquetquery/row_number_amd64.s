//go:build amd64

#include "textflag.h"

// func rowNumberNext(row *RowNumber, rep, def int)
TEXT ·rowNumberNext(SB), NOSPLIT, $0
    // load all parameters to registers
    MOVQ    row+0(FP), AX         // load pointer to RowNumber array (row)
    MOVQ    rep+8(FP), BX         // load repetition level (rep)
    MOVQ    def+16(FP), CX        // load definition level (def)

    // row[rep]++
    LEAQ (AX)(BX*4), DX           // address of row[rep]
    ADDL $1, (DX)                 // ancrement row[rep]
    VMOVDQU (AX), Y0              // move incremented row number to Y0

    // get the mask at maskZeroGT[rep] that contains zeros for elements > rep
    MOVQ $maskZeroGT(SB), R8      // mov address of maskZeroGT[0] to R8
    SHLQ $5, BX                   // rep = rep * 8 * 4
    ADDQ BX, R8                   // mov address of maskZeroGT[rep] to R8
    // set all elements in Y0 to 0 after rep
    VPAND (R8), Y0, Y0            // calculate logical AND between row and mask

    // get the mask at maskZeroLT[def] that contains zeros for elements <= def
    MOVQ $maskZeroLT(SB), R8      // mov address of maskZeroLT[0] to R8
    SHLQ $5, CX                   // def = def * 8 * 4
    ADDQ CX, R8                   // mov address of maskZeroLT[def] to R8
    // set all elements in Y0 to -1 after def
    VPOR (R8), Y0, Y0             // calculate logical OR between row and mask

    VMOVDQU Y0, (AX)       // move the result back to row/AX

    VZEROUPPER
    RET

// 8x8 matrix of int32 where maskZero[i][j] is -1 for j>i
DATA maskZeroGT+0(SB)/4,	$-1
DATA maskZeroGT+4(SB)/4,	$0
DATA maskZeroGT+8(SB)/4,	$0
DATA maskZeroGT+12(SB)/4,	$0
DATA maskZeroGT+16(SB)/4,	$0
DATA maskZeroGT+20(SB)/4,	$0
DATA maskZeroGT+24(SB)/4,	$0
DATA maskZeroGT+28(SB)/4,	$0
DATA maskZeroGT+32(SB)/4,	$-1
DATA maskZeroGT+36(SB)/4,	$-1
DATA maskZeroGT+40(SB)/4,	$0
DATA maskZeroGT+44(SB)/4,	$0
DATA maskZeroGT+48(SB)/4,	$0
DATA maskZeroGT+52(SB)/4,	$0
DATA maskZeroGT+56(SB)/4,	$0
DATA maskZeroGT+60(SB)/4,	$0
DATA maskZeroGT+64(SB)/4,	$-1
DATA maskZeroGT+68(SB)/4,	$-1
DATA maskZeroGT+72(SB)/4,	$-1
DATA maskZeroGT+76(SB)/4,	$0
DATA maskZeroGT+80(SB)/4,	$0
DATA maskZeroGT+84(SB)/4,	$0
DATA maskZeroGT+88(SB)/4,	$0
DATA maskZeroGT+92(SB)/4,	$0
DATA maskZeroGT+96(SB)/4,	$-1
DATA maskZeroGT+100(SB)/4,	$-1
DATA maskZeroGT+104(SB)/4,	$-1
DATA maskZeroGT+108(SB)/4,	$-1
DATA maskZeroGT+112(SB)/4,	$0
DATA maskZeroGT+116(SB)/4,	$0
DATA maskZeroGT+120(SB)/4,	$0
DATA maskZeroGT+124(SB)/4,	$0
DATA maskZeroGT+128(SB)/4,	$-1
DATA maskZeroGT+132(SB)/4,	$-1
DATA maskZeroGT+136(SB)/4,	$-1
DATA maskZeroGT+140(SB)/4,	$-1
DATA maskZeroGT+144(SB)/4,	$-1
DATA maskZeroGT+148(SB)/4,	$0
DATA maskZeroGT+152(SB)/4,	$0
DATA maskZeroGT+156(SB)/4,	$0
DATA maskZeroGT+160(SB)/4,	$-1
DATA maskZeroGT+164(SB)/4,	$-1
DATA maskZeroGT+168(SB)/4,	$-1
DATA maskZeroGT+172(SB)/4,	$-1
DATA maskZeroGT+176(SB)/4,	$-1
DATA maskZeroGT+180(SB)/4,	$-1
DATA maskZeroGT+184(SB)/4,	$0
DATA maskZeroGT+188(SB)/4,	$0
DATA maskZeroGT+192(SB)/4,	$-1
DATA maskZeroGT+196(SB)/4,	$-1
DATA maskZeroGT+200(SB)/4,	$-1
DATA maskZeroGT+204(SB)/4,	$-1
DATA maskZeroGT+208(SB)/4,	$-1
DATA maskZeroGT+212(SB)/4,	$-1
DATA maskZeroGT+216(SB)/4,	$-1
DATA maskZeroGT+220(SB)/4,	$0
DATA maskZeroGT+224(SB)/4,	$-1
DATA maskZeroGT+228(SB)/4,	$-1
DATA maskZeroGT+232(SB)/4,	$-1
DATA maskZeroGT+236(SB)/4,	$-1
DATA maskZeroGT+240(SB)/4,	$-1
DATA maskZeroGT+244(SB)/4,	$-1
DATA maskZeroGT+248(SB)/4,	$-1
DATA maskZeroGT+252(SB)/4,	$-1
GLOBL maskZeroGT(SB), RODATA, $256

// 8x8 matrix of int32 where maskZero[i][j] is -1 for j<=i
DATA maskZeroLT+0(SB)/4,	$0
DATA maskZeroLT+4(SB)/4,	$-1
DATA maskZeroLT+8(SB)/4,	$-1
DATA maskZeroLT+12(SB)/4,	$-1
DATA maskZeroLT+16(SB)/4,	$-1
DATA maskZeroLT+20(SB)/4,	$-1
DATA maskZeroLT+24(SB)/4,	$-1
DATA maskZeroLT+28(SB)/4,	$-1
DATA maskZeroLT+32(SB)/4,	$0
DATA maskZeroLT+36(SB)/4,	$0
DATA maskZeroLT+40(SB)/4,	$-1
DATA maskZeroLT+44(SB)/4,	$-1
DATA maskZeroLT+48(SB)/4,	$-1
DATA maskZeroLT+52(SB)/4,	$-1
DATA maskZeroLT+56(SB)/4,	$-1
DATA maskZeroLT+60(SB)/4,	$-1
DATA maskZeroLT+64(SB)/4,	$0
DATA maskZeroLT+68(SB)/4,	$0
DATA maskZeroLT+72(SB)/4,	$0
DATA maskZeroLT+76(SB)/4,	$-1
DATA maskZeroLT+80(SB)/4,	$-1
DATA maskZeroLT+84(SB)/4,	$-1
DATA maskZeroLT+88(SB)/4,	$-1
DATA maskZeroLT+92(SB)/4,	$-1
DATA maskZeroLT+96(SB)/4,	$0
DATA maskZeroLT+100(SB)/4,	$0
DATA maskZeroLT+104(SB)/4,	$0
DATA maskZeroLT+108(SB)/4,	$0
DATA maskZeroLT+112(SB)/4,	$-1
DATA maskZeroLT+116(SB)/4,	$-1
DATA maskZeroLT+120(SB)/4,	$-1
DATA maskZeroLT+124(SB)/4,	$-1
DATA maskZeroLT+128(SB)/4,	$0
DATA maskZeroLT+132(SB)/4,	$0
DATA maskZeroLT+136(SB)/4,	$0
DATA maskZeroLT+140(SB)/4,	$0
DATA maskZeroLT+144(SB)/4,	$0
DATA maskZeroLT+148(SB)/4,	$-1
DATA maskZeroLT+152(SB)/4,	$-1
DATA maskZeroLT+156(SB)/4,	$-1
DATA maskZeroLT+160(SB)/4,	$0
DATA maskZeroLT+164(SB)/4,	$0
DATA maskZeroLT+168(SB)/4,	$0
DATA maskZeroLT+172(SB)/4,	$0
DATA maskZeroLT+176(SB)/4,	$0
DATA maskZeroLT+180(SB)/4,	$0
DATA maskZeroLT+184(SB)/4,	$-1
DATA maskZeroLT+188(SB)/4,	$-1
DATA maskZeroLT+192(SB)/4,	$0
DATA maskZeroLT+196(SB)/4,	$0
DATA maskZeroLT+200(SB)/4,	$0
DATA maskZeroLT+204(SB)/4,	$0
DATA maskZeroLT+208(SB)/4,	$0
DATA maskZeroLT+212(SB)/4,	$0
DATA maskZeroLT+216(SB)/4,	$0
DATA maskZeroLT+220(SB)/4,	$-1
DATA maskZeroLT+224(SB)/4,	$0
DATA maskZeroLT+228(SB)/4,	$0
DATA maskZeroLT+232(SB)/4,	$0
DATA maskZeroLT+236(SB)/4,	$0
DATA maskZeroLT+240(SB)/4,	$0
DATA maskZeroLT+244(SB)/4,	$0
DATA maskZeroLT+248(SB)/4,	$0
DATA maskZeroLT+252(SB)/4,	$0
GLOBL maskZeroLT(SB), RODATA, $256
