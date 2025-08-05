package main

import (
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"text/tabwriter"
	"unsafe"

	"github.com/parquet-go/parquet-go"

	pq "github.com/grafana/tempo/pkg/parquetquery"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/tempodb/backend"
	vp4 "github.com/grafana/tempo/tempodb/encoding/vparquet4"
)

// attrIndexCmd represents a command to generate attribute indices from a parquet block.
//
// This command is highly experimental and meant to facilitate experimentation with different
// kinds of indexes.
type attrIndexCmd struct {
	In            string   `arg:"" help:"The input parquet block to read from."`
	AddIntrinsics bool     `help:"Add some intrinsic attributes to the index like name, kind, status, etc."`
	IndexTypes    []string `enum:"rows,codes" help:"The type of index to generate (rows | codes | rows,codes)" default:"rows,codes"`
	dedicatedRes  []string `kong:"-"`
	dedicatedSpan []string `kong:"-"`
}

func (cmd *attrIndexCmd) Run(_ *globalOptions) error {
	cmd.In = getPathToBlockDir(cmd.In)
	fmt.Printf("Analyzing parquet block from %s\n", cmd.In)

	meta, err := readBlockMeta(cmd.In)
	if err != nil {
		return err
	}
	if meta.Version != vp4.VersionString {
		return fmt.Errorf("unsupported parquet version %s", meta.Version)
	}

	cmd.readDedicatedAttributes(meta)

	stats, err := cmd.collectAttributeStats()
	if err != nil {
		return err
	}
	stats.printStats()

	rowsPerRowGroup := estimateRowsPerRowGroup(stats)
	opts := []parquet.WriterOption{
		parquet.MaxRowsPerRowGroup(rowsPerRowGroup),
		parquet.SkipPageBounds("Scopes", "list", "element", "ValuesString", "list", "element", "RowNumbers"),
		parquet.SkipPageBounds("Scopes", "list", "element", "ValuesInt", "list", "element", "RowNumbers"),
		parquet.SkipPageBounds("Scopes", "list", "element", "ValuesFloat", "list", "element", "RowNumbers"),
		parquet.SkipPageBounds("Scopes", "list", "element", "ValuesBool", "list", "element", "RowNumbers"),
	}

	if len(cmd.IndexTypes) == 0 || len(cmd.IndexTypes) == 2 {
		index := generateCombinedIndex(stats)

		fmt.Printf("Generating combined index with %d rows and %d rows per row group\n", len(index), rowsPerRowGroup)
		err = writeAttributeIndex(cmd.In, index, opts)
	} else if len(cmd.IndexTypes) == 1 {
		if cmd.IndexTypes[0] == "rows" {
			index := generateRowsIndex(stats)
			fmt.Printf("Generating inverted index with %d rows and %d rows per row group\n", len(index), rowsPerRowGroup)

			err = writeAttributeIndex(cmd.In, index, opts)
		} else if cmd.IndexTypes[0] == "codes" {
			index := generateCodesIndex(stats)

			fmt.Printf("Generating index with key/value codes with %d rows and %d rows per row group\n", len(index), rowsPerRowGroup)
			err = writeAttributeIndex(cmd.In, index, opts)
		}
	}
	if err != nil {
		return err
	}

	fmt.Printf("\nSuccessfully generated attribute index in %s/index.parquet\n", cmd.In)
	return nil
}

func (cmd *attrIndexCmd) readDedicatedAttributes(meta *backend.BlockMeta) {
	for _, ded := range meta.DedicatedColumns {
		switch ded.Scope {
		case backend.DedicatedColumnScopeResource:
			cmd.dedicatedRes = append(cmd.dedicatedRes, ded.Name)
		case backend.DedicatedColumnScopeSpan:
			cmd.dedicatedSpan = append(cmd.dedicatedSpan, ded.Name)
		}
	}
}

func (cmd *attrIndexCmd) collectAttributeStats() (*fileStats, error) {
	stats := fileStats{
		Attributes: make(map[string]attributeInfo, 200),
	}

	in, pf, err := openParquetFile(cmd.In)
	if err != nil {
		return nil, err
	}
	defer in.Close()

	reader := parquet.NewGenericReader[vp4.Trace](pf)
	defer reader.Close()

	var (
		traceBuffer = make([]vp4.Trace, 1024)
		readCount   int
	)

	for {
		readCount, err = reader.Read(traceBuffer)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			break
		}
		runtime.GC() // after reading the new traces to the buffer, GC can free the old ones

		if readCount > 0 {
			cmd.collectAttributeStatsForTraces(&stats, traceBuffer[:readCount])
		}
	}
	if readCount > 0 {
		cmd.collectAttributeStatsForTraces(&stats, traceBuffer[:readCount])
	}

	return &stats, nil
}

func (cmd *attrIndexCmd) collectAttributeStatsForTraces(stats *fileStats, traces []vp4.Trace) {
	row := pq.EmptyRowNumber()
	row.Skip(int64(stats.Traces))

	stats.Traces += len(traces)
	for _, tr := range traces {
		stats.Resources += len(tr.ResourceSpans)
		row.Next(0, 0, 3)

		for _, rs := range tr.ResourceSpans {
			row.Next(1, 1, 3)

			res := rs.Resource
			stats.addAttributes(row, traceql.AttributeScopeResource, res.Attrs)
			stats.addDedicatedAttributes(row, traceql.AttributeScopeResource, cmd.dedicatedRes, &res.DedicatedAttributes)

			stats.addAttribute(row, traceql.AttributeScopeResource, "service.name", res.ServiceName)
			stats.addAttribute(row, traceql.AttributeScopeResource, "cluster", res.Cluster)
			stats.addAttribute(row, traceql.AttributeScopeResource, "namespace", res.Namespace)
			stats.addAttribute(row, traceql.AttributeScopeResource, "pod", res.Pod)
			stats.addAttribute(row, traceql.AttributeScopeResource, "container", res.Container)
			stats.addAttribute(row, traceql.AttributeScopeResource, "k8s.cluster.name", res.K8sClusterName)
			stats.addAttribute(row, traceql.AttributeScopeResource, "k8s.namespace.name", res.K8sNamespaceName)
			stats.addAttribute(row, traceql.AttributeScopeResource, "k8s.pod.name", res.K8sPodName)
			stats.addAttribute(row, traceql.AttributeScopeResource, "k8s.container.name", res.K8sContainerName)
			for _, ss := range rs.ScopeSpans {
				row.Next(2, 2, 3)

				scope := ss.Scope
				stats.Spans += len(ss.Spans)

				stats.addAttributes(row, traceql.AttributeScopeInstrumentation, scope.Attrs)
				if cmd.AddIntrinsics {
					// adding scope to distinguish from span.name
					stats.addAttribute(row, traceql.AttributeScopeInstrumentation, "scope.name", scope.Name)
					stats.addAttribute(row, traceql.AttributeScopeInstrumentation, "version", scope.Version)
				}
				for _, sp := range ss.Spans {
					row.Next(3, 3, 3)

					stats.Events += len(sp.Events)
					stats.Links += len(sp.Links)

					stats.addAttributes(row, traceql.AttributeScopeSpan, sp.Attrs)
					stats.addDedicatedAttributes(row, traceql.AttributeScopeSpan, cmd.dedicatedSpan, &sp.DedicatedAttributes)
					stats.addAttribute(row, traceql.AttributeScopeSpan, "http.method", sp.HttpMethod)
					stats.addAttribute(row, traceql.AttributeScopeSpan, "http.url", sp.HttpUrl)
					stats.addAttribute(row, traceql.AttributeScopeSpan, "http.status_code", sp.HttpStatusCode)
					if cmd.AddIntrinsics {
						stats.addAttribute(row, traceql.AttributeScopeSpan, "name", sp.Name)
						stats.addAttribute(row, traceql.AttributeScopeSpan, "kind", sp.Kind)
						stats.addAttribute(row, traceql.AttributeScopeSpan, "status.code", sp.StatusCode)
						stats.addAttribute(row, traceql.AttributeScopeSpan, "status.message", sp.StatusMessage)
					}
					for _, ev := range sp.Events {
						stats.addAttributes(row, traceql.AttributeScopeEvent, ev.Attrs)
						if cmd.AddIntrinsics {
							// adding scope to distinguish from span.name
							stats.addAttribute(row, traceql.AttributeScopeEvent, "event.name", ev.Name)
						}
					}
					for _, ln := range sp.Links {
						stats.addAttributes(row, traceql.AttributeScopeLink, ln.Attrs)
					}
				}
			}
		}
	}
}

func (fs *fileStats) printStats() {
	fmt.Println("File stats:")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.DiscardEmptyColumns)
	tmpl := "%s\t%d\n"
	_, _ = fmt.Fprintf(w, tmpl, "Traces", fs.Traces)
	_, _ = fmt.Fprintf(w, tmpl, "Resources", fs.Resources)
	_, _ = fmt.Fprintf(w, tmpl, "Spans", fs.Spans)
	_, _ = fmt.Fprintf(w, tmpl, "Events", fs.Events)
	_, _ = fmt.Fprintf(w, tmpl, "Links", fs.Links)
	_, _ = fmt.Fprintf(w, tmpl, "Arrays", fs.Arrays)
	_ = w.Flush()

	// sort attributes by scope and count
	attrs := make([]attributeInfo, 0, len(fs.Attributes))
	for _, attr := range fs.Attributes {
		attrs = append(attrs, attr)
	}
	sort.Slice(attrs, func(i, j int) bool {
		var iCount, jCount int
		for _, s := range attrs[i].Scopes {
			iCount += s.Count
		}
		for _, s := range attrs[j].Scopes {
			jCount += s.Count
		}
		return iCount > jCount
	})

	const maxAttrPrints = 500

	fmt.Printf("\nAttribute stats (%d most frequent):\n", maxAttrPrints)
	w = tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', tabwriter.DiscardEmptyColumns)
	_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", "Name", "Scope", "Count", "Cardinality", "Most common value", "Occurrence")
	tmpl = "%s\t%s\t%d\t%d\t%v\t%d\n"

	var attrCount int
	for _, a := range attrs {
		if attrCount > maxAttrPrints {
			break
		}
		attrCount++
		for _, s := range a.Scopes {
			var (
				mostCommonVal any
				occurrence    int
			)

			for _, v := range s.ValuesString {
				if len(v.RowNumbers) > occurrence {
					mostCommonVal = v.Value[0]
					occurrence = len(v.RowNumbers)
				}
			}
			if mostCommonVal != nil {
				str := mostCommonVal.(string)
				str = strings.ReplaceAll(str, "\n", " ")
				if len(str) > 50 {
					str = fmt.Sprintf("%s...", str[:47])
				}
				mostCommonVal = str
			}
			if mostCommonVal == nil {
				for _, v := range s.ValuesInt {
					if len(v.RowNumbers) > occurrence {
						mostCommonVal = v.Value[0]
						occurrence = len(v.RowNumbers)
					}
				}
			}
			if mostCommonVal == nil {
				for _, v := range s.ValuesFloat {
					if len(v.RowNumbers) > occurrence {
						mostCommonVal = v.Value[0]
						occurrence = len(v.RowNumbers)
					}
				}
			}
			if mostCommonVal == nil {
				for _, v := range s.ValuesBool {
					if len(v.RowNumbers) > occurrence {
						mostCommonVal = v.Value[0]
						occurrence = len(v.RowNumbers)
					}
				}
			}
			_, _ = fmt.Fprintf(w, tmpl, a.Key, s.Scope.String(), s.Count, len(s.ValuesString)+len(s.ValuesInt)+len(s.ValuesFloat)+len(s.ValuesBool), mostCommonVal, occurrence)
		}
	}
	_ = w.Flush()

	fmt.Printf("\n\n")
}

func generateCombinedIndex(stats *fileStats) []indexedAttrCombined {
	var (
		index   = make([]indexedAttrCombined, 0, len(stats.Attributes))
		keyCode int64
	)

	for _, attr := range stats.Attributes {
		keyCode++

		a := indexedAttrCombined{
			Key:     attr.Key,
			KeyCode: keyCode,
			Scopes:  make([]indexedScopeCombined, 0, len(attr.Scopes)),
		}

		for _, scope := range attr.Scopes {
			s := indexedScopeCombined{
				Scope: int64(scope.Scope),
			}

			if len(scope.ValuesString) > 0 {
				s.ValuesString = make([]indexedValCombined[string], 0, len(scope.ValuesString))

				for _, v := range scope.ValuesString {
					rn, _ := vp4.RowNumbersEncode(make([]byte, 0, len(v.RowNumbers)), v.RowNumbers)
					s.ValuesString = append(s.ValuesString, indexedValCombined[string]{
						Value:      v.Value,
						RowNumbers: rn,
					})
				}

				sort.Slice(s.ValuesString, func(i, j int) bool {
					return cmpSlice(s.ValuesString[i].Value, s.ValuesString[j].Value) < 0
				})

				var valueCode int64
				for i := range s.ValuesString {
					valueCode++
					s.ValuesString[i].ValueCode = valueCode
				}
			}

			if len(scope.ValuesInt) > 0 {
				s.ValuesInt = make([]indexedValCombined[int64], 0, len(scope.ValuesInt))

				for _, v := range scope.ValuesInt {
					rn, _ := vp4.RowNumbersEncode(make([]byte, 0, len(v.RowNumbers)), v.RowNumbers)
					s.ValuesInt = append(s.ValuesInt, indexedValCombined[int64]{
						Value:      v.Value,
						RowNumbers: rn,
					})
				}

				sort.Slice(s.ValuesInt, func(i, j int) bool {
					return cmpSlice(s.ValuesInt[i].Value, s.ValuesInt[j].Value) < 0
				})

				var valueCode int64
				for i := range s.ValuesInt {
					valueCode++
					s.ValuesInt[i].ValueCode = valueCode
				}
			}

			if len(scope.ValuesFloat) > 0 {
				s.ValuesFloat = make([]indexedValCombined[float64], 0, len(scope.ValuesFloat))

				for _, v := range scope.ValuesFloat {
					rn, _ := vp4.RowNumbersEncode(make([]byte, 0, len(v.RowNumbers)), v.RowNumbers)
					s.ValuesFloat = append(s.ValuesFloat, indexedValCombined[float64]{
						Value:      v.Value,
						RowNumbers: rn,
					})
				}

				sort.Slice(s.ValuesFloat, func(i, j int) bool {
					return cmpSlice(s.ValuesFloat[i].Value, s.ValuesFloat[j].Value) < 0
				})

				var valueCode int64
				for i := range s.ValuesFloat {
					valueCode++
					s.ValuesFloat[i].ValueCode = valueCode
				}
			}

			if len(scope.ValuesBool) > 0 {
				s.ValuesBool = make([]indexedValCombined[bool], 0, len(scope.ValuesBool))

				for _, v := range scope.ValuesBool {
					rn, _ := vp4.RowNumbersEncode(make([]byte, 0, len(v.RowNumbers)), v.RowNumbers)
					s.ValuesBool = append(s.ValuesBool, indexedValCombined[bool]{
						Value:      v.Value,
						RowNumbers: rn,
					})
				}

				sort.Slice(s.ValuesBool, func(i, j int) bool {
					return cmpSliceBool(s.ValuesBool[i].Value, s.ValuesBool[j].Value) < 0
				})

				var valueCode int64
				for i := range s.ValuesBool {
					valueCode++
					s.ValuesBool[i].ValueCode = valueCode
				}
			}

			a.Scopes = append(a.Scopes, s)
		}

		sort.Slice(a.Scopes, func(i, j int) bool {
			return a.Scopes[i].Scope < a.Scopes[j].Scope
		})

		index = append(index, a)
	}

	sort.Slice(index, func(i, j int) bool {
		return strings.Compare(index[i].Key, index[j].Key) < 0
	})

	return index
}

func generateRowsIndex(stats *fileStats) []indexedAttrRows {
	var (
		index   = make([]indexedAttrRows, 0, len(stats.Attributes))
		keyCode int64
	)

	for _, attr := range stats.Attributes {
		keyCode++

		a := indexedAttrRows{
			Key:    attr.Key,
			Scopes: make([]indexedScopeRows, 0, len(attr.Scopes)),
		}

		for _, scope := range attr.Scopes {
			s := indexedScopeRows{
				Scope: int64(scope.Scope),
			}

			if len(scope.ValuesString) > 0 {
				s.ValuesString = make([]indexedValRows[string], 0, len(scope.ValuesString))

				for _, v := range scope.ValuesString {
					rn, _ := vp4.RowNumbersEncode(make([]byte, 0, len(v.RowNumbers)), v.RowNumbers)
					s.ValuesString = append(s.ValuesString, indexedValRows[string]{
						Value:      v.Value,
						RowNumbers: rn,
					})
				}

				sort.Slice(s.ValuesString, func(i, j int) bool {
					return cmpSlice(s.ValuesString[i].Value, s.ValuesString[j].Value) < 0
				})
			}

			if len(scope.ValuesInt) > 0 {
				s.ValuesInt = make([]indexedValRows[int64], 0, len(scope.ValuesInt))

				for _, v := range scope.ValuesInt {
					rn, _ := vp4.RowNumbersEncode(make([]byte, 0, len(v.RowNumbers)), v.RowNumbers)
					s.ValuesInt = append(s.ValuesInt, indexedValRows[int64]{
						Value:      v.Value,
						RowNumbers: rn,
					})
				}

				sort.Slice(s.ValuesInt, func(i, j int) bool {
					return cmpSlice(s.ValuesInt[i].Value, s.ValuesInt[j].Value) < 0
				})
			}

			if len(scope.ValuesFloat) > 0 {
				s.ValuesFloat = make([]indexedValRows[float64], 0, len(scope.ValuesFloat))

				for _, v := range scope.ValuesFloat {
					rn, _ := vp4.RowNumbersEncode(make([]byte, 0, len(v.RowNumbers)), v.RowNumbers)
					s.ValuesFloat = append(s.ValuesFloat, indexedValRows[float64]{
						Value:      v.Value,
						RowNumbers: rn,
					})
				}

				sort.Slice(s.ValuesFloat, func(i, j int) bool {
					return cmpSlice(s.ValuesFloat[i].Value, s.ValuesFloat[j].Value) < 0
				})
			}

			if len(scope.ValuesBool) > 0 {
				s.ValuesBool = make([]indexedValRows[bool], 0, len(scope.ValuesBool))

				for _, v := range scope.ValuesBool {
					rn, _ := vp4.RowNumbersEncode(make([]byte, 0, len(v.RowNumbers)), v.RowNumbers)
					s.ValuesBool = append(s.ValuesBool, indexedValRows[bool]{
						Value:      v.Value,
						RowNumbers: rn,
					})
				}

				sort.Slice(s.ValuesBool, func(i, j int) bool {
					return cmpSliceBool(s.ValuesBool[i].Value, s.ValuesBool[j].Value) < 0
				})
			}

			a.Scopes = append(a.Scopes, s)
		}

		sort.Slice(a.Scopes, func(i, j int) bool {
			return a.Scopes[i].Scope < a.Scopes[j].Scope
		})

		index = append(index, a)
	}

	sort.Slice(index, func(i, j int) bool {
		return strings.Compare(index[i].Key, index[j].Key) < 0
	})

	return index
}

func generateCodesIndex(stats *fileStats) []indexedAttrCodes {
	var (
		index   = make([]indexedAttrCodes, 0, len(stats.Attributes))
		keyCode int64
	)

	for _, attr := range stats.Attributes {
		keyCode++

		a := indexedAttrCodes{
			Key:     attr.Key,
			KeyCode: keyCode,
			Scopes:  make([]indexScopeCodes, 0, len(attr.Scopes)),
		}

		for _, scope := range attr.Scopes {
			s := indexScopeCodes{
				Scope: int64(scope.Scope),
			}

			if len(scope.ValuesString) > 0 {
				s.ValuesString = make([]indexedValCodes[string], 0, len(scope.ValuesString))

				for _, v := range scope.ValuesString {
					s.ValuesString = append(s.ValuesString, indexedValCodes[string]{
						Value: v.Value,
					})
				}

				sort.Slice(s.ValuesString, func(i, j int) bool {
					return cmpSlice(s.ValuesString[i].Value, s.ValuesString[j].Value) < 0
				})

				var valueCode int64
				for i := range s.ValuesString {
					valueCode++
					s.ValuesString[i].ValueCode = valueCode
				}
			}

			if len(scope.ValuesInt) > 0 {
				s.ValuesInt = make([]indexedValCodes[int64], 0, len(scope.ValuesInt))

				for _, v := range scope.ValuesInt {
					s.ValuesInt = append(s.ValuesInt, indexedValCodes[int64]{
						Value: v.Value,
					})
				}

				sort.Slice(s.ValuesInt, func(i, j int) bool {
					return cmpSlice(s.ValuesInt[i].Value, s.ValuesInt[j].Value) < 0
				})

				var valueCode int64
				for i := range s.ValuesInt {
					valueCode++
					s.ValuesInt[i].ValueCode = valueCode
				}
			}

			if len(scope.ValuesFloat) > 0 {
				s.ValuesFloat = make([]indexedValCodes[float64], 0, len(scope.ValuesFloat))

				for _, v := range scope.ValuesFloat {
					s.ValuesFloat = append(s.ValuesFloat, indexedValCodes[float64]{
						Value: v.Value,
					})
				}

				sort.Slice(s.ValuesFloat, func(i, j int) bool {
					return cmpSlice(s.ValuesFloat[i].Value, s.ValuesFloat[j].Value) < 0
				})

				var valueCode int64
				for i := range s.ValuesFloat {
					valueCode++
					s.ValuesFloat[i].ValueCode = valueCode
				}
			}

			if len(scope.ValuesBool) > 0 {
				s.ValuesBool = make([]indexedValCodes[bool], 0, len(scope.ValuesBool))

				for _, v := range scope.ValuesBool {
					s.ValuesBool = append(s.ValuesBool, indexedValCodes[bool]{
						Value: v.Value,
					})
				}

				sort.Slice(s.ValuesBool, func(i, j int) bool {
					return cmpSliceBool(s.ValuesBool[i].Value, s.ValuesBool[j].Value) < 0
				})

				var valueCode int64
				for i := range s.ValuesBool {
					valueCode++
					s.ValuesBool[i].ValueCode = valueCode
				}
			}

			a.Scopes = append(a.Scopes, s)
		}

		sort.Slice(a.Scopes, func(i, j int) bool {
			return a.Scopes[i].Scope < a.Scopes[j].Scope
		})

		index = append(index, a)
	}

	sort.Slice(index, func(i, j int) bool {
		return strings.Compare(index[i].Key, index[j].Key) < 0
	})

	return index
}

type indexedAttrCombined struct {
	Key     string                 `parquet:",snappy"`
	KeyCode int64                  `parquet:",snappy,delta"`
	Scopes  []indexedScopeCombined `parquet:",list"`
}

type indexedScopeCombined struct {
	Scope        int64                         `parquet:",snappy,delta"`
	ValuesString []indexedValCombined[string]  `parquet:",list"`
	ValuesInt    []indexedValCombined[int64]   `parquet:",list"`
	ValuesFloat  []indexedValCombined[float64] `parquet:",list"`
	ValuesBool   []indexedValCombined[bool]    `parquet:",list"`
}

type indexedValCombined[T comparable] struct {
	Value      []T    `parquet:",snappy"`
	ValueCode  int64  `parquet:",snappy,delta"`
	RowNumbers []byte `parquet:",snappy"`
}

type indexedAttrRows struct {
	Key    string             `parquet:",snappy"`
	Scopes []indexedScopeRows `parquet:",list"`
}

type indexedScopeRows struct {
	Scope        int64                     `parquet:",snappy,delta"`
	ValuesString []indexedValRows[string]  `parquet:",list"`
	ValuesInt    []indexedValRows[int64]   `parquet:",list"`
	ValuesFloat  []indexedValRows[float64] `parquet:",list"`
	ValuesBool   []indexedValRows[bool]    `parquet:",list"`
}

type indexedValRows[T comparable] struct {
	Value      []T    `parquet:",snappy"`
	RowNumbers []byte `parquet:",snappy"`
}

type indexedAttrCodes struct {
	Key     string            `parquet:",snappy"`
	KeyCode int64             `parquet:",snappy,delta"`
	Scopes  []indexScopeCodes `parquet:",list"`
}

type indexScopeCodes struct {
	Scope        int64                      `parquet:",snappy,delta"`
	ValuesString []indexedValCodes[string]  `parquet:",list"`
	ValuesInt    []indexedValCodes[int64]   `parquet:",list"`
	ValuesFloat  []indexedValCodes[float64] `parquet:",list"`
	ValuesBool   []indexedValCodes[bool]    `parquet:",list"`
}

type indexedValCodes[T comparable] struct {
	Value     []T   `parquet:",snappy"`
	ValueCode int64 `parquet:",snappy,delta"`
}

func writeAttributeIndex[T any](in string, index []T, opts []parquet.WriterOption) error {
	stat, err := os.Stat(filepath.Join(in, "data.parquet"))
	if err != nil {
		return err
	}

	out, err := os.OpenFile(filepath.Join(in, "index.parquet"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, stat.Mode())
	if err != nil {
		return err
	}
	defer out.Close()

	writer := parquet.NewGenericWriter[T](out, opts...)
	defer writer.Close()

	n, err := writer.Write(index)
	if err != nil {
		return err
	}
	if n != len(index) {
		return fmt.Errorf("expected to write %d rows, got %d", len(index), n)
	}

	err = writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

type fileStats struct {
	Traces     int
	Resources  int
	Spans      int
	Events     int
	Links      int
	Arrays     int
	Attributes map[string]attributeInfo
}

type attributeInfo struct {
	Key    string
	Scopes map[traceql.AttributeScope]attributeInfoScope
}

type attributeInfoScope struct {
	Scope        traceql.AttributeScope
	Count        int
	ValuesString map[uint64]valueInfo[string]
	ValuesInt    map[uint64]valueInfo[int64]
	ValuesFloat  map[uint64]valueInfo[float64]
	ValuesBool   map[uint64]valueInfo[bool]
}

type valueInfo[T comparable] struct {
	Value      []T
	RowNumbers []pq.RowNumber
}

func (fs *fileStats) addAttributes(row pq.RowNumber, scope traceql.AttributeScope, attrs []vp4.Attribute) {
	for _, attr := range attrs {
		if attr.IsArray {
			fs.addAttribute(row, scope, attr.Key, attr.Value)
			fs.Arrays++
		} else if len(attr.Value) > 0 {
			fs.addAttribute(row, scope, attr.Key, attr.Value[0])
		} else if len(attr.ValueInt) > 0 {
			fs.addAttribute(row, scope, attr.Key, attr.ValueInt[0])
		} else if len(attr.ValueDouble) > 0 {
			fs.addAttribute(row, scope, attr.Key, attr.ValueDouble[0])
		} else if len(attr.ValueBool) > 0 {
			fs.addAttribute(row, scope, attr.Key, attr.ValueBool[0])
		}
	}
}

func (fs *fileStats) addDedicatedAttributes(row pq.RowNumber, scope traceql.AttributeScope, columns []string, attrs *vp4.DedicatedAttributes) {
	if attrs == nil {
		return
	}

	if attrs.String01 != nil && len(columns) > 0 {
		fs.addAttribute(row, scope, columns[0], attrs.String01)
	}
	if attrs.String02 != nil && len(columns) > 1 {
		fs.addAttribute(row, scope, columns[1], attrs.String02)
	}
	if attrs.String03 != nil && len(columns) > 2 {
		fs.addAttribute(row, scope, columns[2], attrs.String03)
	}
	if attrs.String04 != nil && len(columns) > 3 {
		fs.addAttribute(row, scope, columns[3], attrs.String04)
	}
	if attrs.String05 != nil && len(columns) > 4 {
		fs.addAttribute(row, scope, columns[4], attrs.String05)
	}
	if attrs.String06 != nil && len(columns) > 5 {
		fs.addAttribute(row, scope, columns[5], attrs.String06)
	}
	if attrs.String07 != nil && len(columns) > 6 {
		fs.addAttribute(row, scope, columns[6], attrs.String07)
	}
	if attrs.String08 != nil && len(columns) > 7 {
		fs.addAttribute(row, scope, columns[7], attrs.String08)
	}
	if attrs.String09 != nil && len(columns) > 8 {
		fs.addAttribute(row, scope, columns[8], attrs.String09)
	}
	if attrs.String10 != nil && len(columns) > 9 {
		fs.addAttribute(row, scope, columns[9], attrs.String10)
	}
}

func (fs *fileStats) addAttribute(row pq.RowNumber, scope traceql.AttributeScope, key string, value any) {
	attrInfo, ok := fs.Attributes[key]
	if !ok {
		attrInfo = attributeInfo{
			Key:    key,
			Scopes: make(map[traceql.AttributeScope]attributeInfoScope),
		}
	}

	scopeInfo, ok := attrInfo.Scopes[scope]
	if !ok {
		scopeInfo = attributeInfoScope{
			Scope: scope,
		}
	}

	scopeInfo.Count++

	switch value := value.(type) {
	case string, *string, []string:
		s := toSlice[string](value)
		if len(s) == 0 {
			return
		}
		if scopeInfo.ValuesString == nil {
			scopeInfo.ValuesString = make(map[uint64]valueInfo[string], 10)
		}

		sum := fnvStrings(s)
		info, ok := scopeInfo.ValuesString[sum]
		if !ok {
			info = valueInfo[string]{
				Value:      s,
				RowNumbers: make([]pq.RowNumber, 0, 10),
			}
		}

		info.RowNumbers = append(info.RowNumbers, row)
		scopeInfo.ValuesString[sum] = info
	case int64, *int64, []int64:
		s := toSlice[int64](value)
		if len(s) == 0 {
			return
		}
		if scopeInfo.ValuesInt == nil {
			scopeInfo.ValuesInt = make(map[uint64]valueInfo[int64], 10)
		}

		sum := fnvInts(s)
		v, ok := scopeInfo.ValuesInt[sum]
		if !ok {
			v = valueInfo[int64]{
				Value:      s,
				RowNumbers: make([]pq.RowNumber, 0, 1),
			}
		}
		v.RowNumbers = append(v.RowNumbers, row)
		scopeInfo.ValuesInt[sum] = v
	case float64, *float64, []float64:
		s := toSlice[float64](value)
		if len(s) == 0 {
			return
		}
		if scopeInfo.ValuesFloat == nil {
			scopeInfo.ValuesFloat = make(map[uint64]valueInfo[float64])
		}

		sum := fnvFloats(s)
		v, ok := scopeInfo.ValuesFloat[sum]
		if !ok {
			v = valueInfo[float64]{
				Value:      s,
				RowNumbers: make([]pq.RowNumber, 0, 1),
			}
		}
		v.RowNumbers = append(v.RowNumbers, row)
		scopeInfo.ValuesFloat[sum] = v
	case bool, *bool, []bool:
		s := toSlice[bool](value)
		if len(s) == 0 {
			return
		}
		if scopeInfo.ValuesBool == nil {
			scopeInfo.ValuesBool = make(map[uint64]valueInfo[bool])
		}

		sum := fnvBools(s)
		v, ok := scopeInfo.ValuesBool[sum]
		if !ok {
			v = valueInfo[bool]{
				Value:      s,
				RowNumbers: make([]pq.RowNumber, 0, 1),
			}
		}
		v.RowNumbers = append(v.RowNumbers, row)
		scopeInfo.ValuesBool[sum] = v
	}

	attrInfo.Scopes[scope] = scopeInfo
	fs.Attributes[key] = attrInfo
}

func toSlice[T any](val any) []T {
	switch v := val.(type) {
	case []T:
		return v
	case T:
		return []T{v}
	case *T:
		if v == nil {
			return nil
		}
		return []T{*v}
	default:
		panic(fmt.Sprintf("unexpected type %T", v))
	}
}

func fnvStrings(values []string) uint64 {
	h := fnv.New64a()
	for _, v := range values {
		_, _ = h.Write(unsafe.Slice(unsafe.StringData(v), len(v)))
	}
	return h.Sum64()
}

func fnvInts(values []int64) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for _, v := range values {
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		_, _ = h.Write(buf[:])
	}
	return h.Sum64()
}

func fnvFloats(values []float64) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for _, v := range values {
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
		_, _ = h.Write(buf[:])
	}
	return h.Sum64()
}

func fnvBools(values []bool) uint64 {
	h := fnv.New64a()
	var buf [1]byte
	for _, v := range values {
		if v {
			buf[0] = 1
		} else {
			buf[0] = 0
		}
		_, _ = h.Write(buf[:])
	}
	return h.Sum64()
}

func cmpSlice[T cmp.Ordered](a, b []T) int {
	for i := range min(len(a), len(b)) {
		if n := cmp.Compare(a[i], b[i]); n != 0 {
			return n
		}
	}
	return cmp.Compare(len(a), len(b))
}

func cmpSliceBool(a, b []bool) int {
	for i := range min(len(a), len(b)) {
		if !a[i] && b[i] {
			return -1
		}
		if a[i] && !b[i] {
			return 1
		}
	}
	return cmp.Compare(len(a), len(b))
}

const (
	magicUncompressedPageSize int64 = 4_500_000
	minRowGroups              int64 = 3
)

func estimateRowsPerRowGroup(stats *fileStats) int64 {
	var uncompressedValueSize int64

	for _, attr := range stats.Attributes {
		for _, scope := range attr.Scopes {
			for _, vals := range scope.ValuesString {
				for _, val := range vals.Value {
					if uncompressedValueSize%100_000 == 0 {
						uncompressedValueSize++
					}
					uncompressedValueSize += int64(len(val))
				}
			}
		}
	}

	// estimate 5 pages per RG
	uncompressedRowGroupSize := magicUncompressedPageSize * 5
	rgCount := max(uncompressedValueSize/uncompressedRowGroupSize, minRowGroups)

	return (int64(len(stats.Attributes)) / rgCount) + 1
}
