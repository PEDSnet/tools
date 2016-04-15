package results

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/PEDSnet/tools/cmd/dqa/uni"
)

// ReadFromDir reads all files in a directory and returns reports for each.
func ReadFromDir(dir string) (map[string]*File, error) {
	fns, err := ioutil.ReadDir(dir)

	if err != nil {
		return nil, err
	}

	var (
		path string
		f    *os.File
	)

	reports := make(map[string]*File)

	// Iterate over each CSV file in the directory.
	for _, fi := range fns {
		if fi.IsDir() {
			continue
		}

		if filepath.Ext(fi.Name()) != ".csv" {
			continue
		}

		path = filepath.Join(dir, fi.Name())

		if f, err = os.Open(path); err != nil {
			return nil, err
		}

		report := &File{}
		reports[fi.Name()] = report

		_, err := report.Read(f)

		f.Close()

		// Presumably not a valid file.
		if err != nil {
			return nil, err
		}
	}

	return reports, nil
}

// Reader reads a DQA exposing a header with mapped positions
// to the field.
type Reader struct {
	head *FileHeader
	csv  *csv.Reader
}

// Read reads and parses a result from the underlying reader.
func (r *Reader) Read() (*Result, error) {
	row, err := r.csv.Read()

	if err != nil {
		return nil, err
	}

	var rank Rank

	switch row[r.head.Rank] {
	case "High":
		rank = HighRank
	case "Medium":
		rank = MediumRank
	case "Low":
		rank = LowRank
	}

	// Using the head struct to select the corresponding value
	// in the input row to the result.
	return &Result{
		Model:            row[r.head.Model],
		ModelVersion:     row[r.head.ModelVersion],
		DataVersion:      row[r.head.DataVersion],
		DQAVersion:       row[r.head.DQAVersion],
		Table:            row[r.head.Table],
		Field:            row[r.head.Field],
		Goal:             row[r.head.Goal],
		IssueCode:        row[r.head.IssueCode],
		IssueDescription: row[r.head.IssueDescription],
		Finding:          row[r.head.Finding],
		Prevalence:       row[r.head.Prevalence],
		Rank:             rank,
		rank:             row[r.head.Rank],
		SiteResponse:     row[r.head.SiteResponse],
		Cause:            row[r.head.Cause],
		Status:           row[r.head.Status],
		Reviewer:         row[r.head.Reviewer],
		GithubID:         row[r.head.GithubID],
	}, nil
}

// ReadAll reads all results from the reader.
func (cr *Reader) ReadAll() ([]*Result, error) {
	var results []*Result

	for {
		r, err := cr.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		results = append(results, r)
	}

	return results, nil
}

// NewReader initializes a new results reader.
func NewReader(r io.Reader) (*Reader, error) {
	cr := csv.NewReader(uni.New(r))

	cr.FieldsPerRecord = len(FileHeaderFields)
	cr.Comment = '#'
	cr.LazyQuotes = true
	cr.TrimLeadingSpace = true

	// Read the header.
	row, err := cr.Read()
	if err != nil {
		return nil, err
	}

	head, err := ParseFileHeader(row)
	if err != nil {
		return nil, err
	}

	return &Reader{
		head: head,
		csv:  cr,
	}, nil
}

// Writer writes results to a file.
type Writer struct {
	csv  *csv.Writer
	head bool
}

// Write writes a result to the underlying writer.
func (w *Writer) Write(r *Result) error {
	if !w.head {
		if err := w.csv.Write(FileHeaderFields); err != nil {
			return err
		}

		w.head = true
	}

	return w.csv.Write(r.Row())
}

// WriteAll writes all results in a slice.
func (w *Writer) WriteAll(results []*Result) error {
	var err error

	for _, r := range results {
		if err = w.Write(r); err != nil {
			return err
		}
	}

	return nil
}

// Flush flushes the written results to the underlying writer.
func (w *Writer) Flush() error {
	w.csv.Flush()
	return w.csv.Error()
}

// NewWriter initializes a new writer for results.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		csv: csv.NewWriter(w),
	}
}
