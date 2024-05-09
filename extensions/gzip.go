package extensions

import (
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"os"
)

type Options struct {
	Filename string
	Level    int
}

func NewGzip(level int) *Options {
	if level == 0 {
		level = flate.BestCompression
	}
	return &Options{
		Level: level,
	}
}

// Compress reads the file stream to write compressed data
// use io.Pipe and a goroutine to create reader
// on data written by the appliation.
//
// Then copy file through gzip to pipe writer
// with chosen compression algorithm level
// This uses CloseWithError to propgate errors back to
// the main goroutine.
// Then flush to the writer stream
func (opts *Options) Compress() error {
	f, err := os.Open(opts.Filename)
	if err != nil {
		return err
	}
	defer f.Close()

	r, w := io.Pipe()
	go func() {
		gzw, _ := gzip.NewWriterLevel(w, opts.Level)
		if _, err = io.Copy(gzw, f); err != nil {
			w.CloseWithError(err)
			return
		}
		w.CloseWithError(gzw.Close())
	}()

	gf, err := os.Create(fmt.Sprintf("%s.gz", opts.Filename))
	if err != nil {
		return err
	}
	defer gf.Close()

	if _, err = io.Copy(gf, r); err != nil {
		return err
	}
	if err = opts.clean(); err != nil {
		return err
	}
	return nil
}

func (opts *Options) clean() error {
	return os.Remove(opts.Filename)
}
