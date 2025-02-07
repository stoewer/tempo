package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"

	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/vparquet4"
)

const (
	metaFilename  = "meta.json"
	blockFilename = "data.parquet"
)

type convertParquet4to4 struct {
	In  string `arg:"" help:"The input parquet block to read from."`
	Out string `arg:"" help:"The output folder to write block to." default:"./out" optional:""`
}

func (cmd *convertParquet4to4) Run() error {
	// sanitize input path
	cmd.In = getPathToBlockDir(cmd.In)

	// open the input metadata file
	meta, err := readBlockMeta(cmd.In)
	if err != nil {
		return err
	}

	// sanitize output path
	cmd.Out = getPathToBlockDir(cmd.Out)
	if last := filepath.Base(cmd.Out); last != meta.BlockID.String() {
		if last == meta.TenantID {
			cmd.Out = filepath.Join(cmd.Out, meta.BlockID.String())
		} else {
			cmd.Out = filepath.Join(cmd.Out, meta.TenantID, meta.BlockID.String())
		}
	}

	// prepare output dir
	err = os.MkdirAll(cmd.Out, 0o755)
	if err != nil {
		return err
	}

	// convert block
	err = cmd.convertBlock(meta)
	if err != nil {
		return fmt.Errorf("failed to convert block: %w", err)
	}

	newMeta, err := cmd.writeNewBlockMeta(meta)
	if err != nil {
		return fmt.Errorf("failed to write new block meta: %w", err)
	}
	err = cmd.copyRemainingFiles()
	if err != nil {
		return fmt.Errorf("failed to copy remaining files: %w", err)
	}

	fmt.Printf("Successfully created block with size=%d and footerSize=%d\n", newMeta.Size_, newMeta.FooterSize)
	return nil
}

func (cmd *convertParquet4to4) convertBlock(meta *backend.BlockMeta) error {
	// open the input parquet file
	in, pf, err := openParquetFile(cmd.In)
	if err != nil {
		return err
	}
	defer in.Close()

	inStat, err := in.Stat()
	if err != nil {
		return err
	}

	printPath, err := filepath.Abs(cmd.Out)
	if err != nil {
		printPath = cmd.Out
	}
	fmt.Printf("Creating vParquet4 block in %s\n", printPath)

	// create output parquet file
	out, err := os.OpenFile(filepath.Join(cmd.Out, blockFilename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, inStat.Mode())
	if err != nil {
		return err
	}
	defer out.Close()

	writer := parquet.NewGenericWriter[vparquet4.Trace](out)

	readBuffer := make([]vparquet4.Trace, 500)
	writeBuffer := make([]vparquet4.Trace, 500)

	rowGroups := pf.RowGroups()
	fmt.Printf("Total rowgroups: %d\n", len(rowGroups))

	// copy row groups
	for i, rowGroup := range rowGroups {
		fmt.Printf("Converting rowgroup: %d\n", i+1)
		reader := parquet.NewGenericRowGroupReader[vparquet4.Trace](rowGroup)

		for {
			readCount, err := reader.Read(readBuffer)
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			if readCount == 0 {
				err = writer.Flush()
				if err != nil {
					return err
				}
				break
			}

			for j := 0; j < readCount; j++ {
				t := vparquet4.ParquetTraceToTempopbTrace(meta, &readBuffer[j])
				vparquet4.TraceToParquet(meta, readBuffer[j].TraceID, t, &writeBuffer[j])
			}

			writeCount := 0
			for writeCount < readCount {
				n, err := writer.Write(writeBuffer[writeCount:readCount])
				if err != nil {
					return err
				}
				writeCount += n
			}
		}
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}

func (cmd *convertParquet4to4) writeNewBlockMeta(meta *backend.BlockMeta) (*backend.BlockMeta, error) {
	out, err := os.Open(filepath.Join(cmd.Out, blockFilename))
	if err != nil {
		return nil, err
	}
	defer out.Close()

	// read file size
	stat, err := out.Stat()
	if err != nil {
		return nil, err
	}

	metaNew := *meta
	metaNew.Size_ = uint64(stat.Size())

	// read footer size
	buf := make([]byte, 8)
	n, err := out.ReadAt(buf, stat.Size()-8)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if n < 4 {
		return nil, errors.New("not enough bytes read to determine footer size")
	}
	metaNew.FooterSize = binary.LittleEndian.Uint32(buf[0:4])

	// write vParquet4 meta
	outMeta, err := os.OpenFile(filepath.Join(cmd.Out, metaFilename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, stat.Mode())
	if err != nil {
		return nil, err
	}
	defer outMeta.Close()

	err = json.NewEncoder(outMeta).Encode(&metaNew)
	if err != nil {
		return nil, err
	}

	return &metaNew, nil
}

func (cmd *convertParquet4to4) copyRemainingFiles() error {
	items, err := os.ReadDir(cmd.In)
	if err != nil {
		return err
	}

	for _, item := range items {
		if item.IsDir() {
			continue
		}
		if item.Name() == blockFilename || item.Name() == metaFilename {
			continue
		}

		err = copyFile(filepath.Join(cmd.In, item.Name()), filepath.Join(cmd.Out, item.Name()))
		if err != nil {
			return err
		}
	}

	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	inStat, err := in.Stat()
	if err != nil {
		return err
	}

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, inStat.Mode())
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
