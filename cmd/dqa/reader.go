package main

import "io"

// UniversalReader wraps an io.Reader to replace carriage returns with newlines.
// This is used with the csv.Reader so it can properly delimit lines.
type UniversalReader struct {
	r io.Reader
}

func (r *UniversalReader) Read(buf []byte) (int, error) {
	n, err := r.r.Read(buf)

	// Replace carriage returns with newlines
	for i, b := range buf {
		if b == '\r' {
			buf[i] = '\n'
		}
	}

	return n, err
}
