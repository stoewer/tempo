#!/bin/bash

export GOWORK=off
export GOMEMLIMIT=10GiB

BENCH_ID=4
BENCH_COUNT=6
BENCH_TIME=3s
BENCH_TIMEOUT=1h

BENCH_CMD="go test -mod=vendor -run='^#' -timeout=$BENCH_TIMEOUT -benchtime=$BENCH_TIME -count=$BENCH_COUNT "

$BENCH_CMD -bench=BenchmarkRobloxBlockTraceQL/dedi ./tempodb/encoding/vparquet  | tee bench-dedi-1-$BENCH_ID.txt
$BENCH_CMD -bench=BenchmarkRobloxBlockTraceQL/dedi ./tempodb/encoding/vparquet2 | tee bench-dedi-2-$BENCH_ID.txt

$BENCH_CMD -bench=BenchmarkRobloxBlockTraceQL/attr ./tempodb/encoding/vparquet  | tee bench-attr-1-$BENCH_ID.txt
$BENCH_CMD -bench=BenchmarkRobloxBlockTraceQL/attr ./tempodb/encoding/vparquet2 | tee bench-attr-2-$BENCH_ID.txt

