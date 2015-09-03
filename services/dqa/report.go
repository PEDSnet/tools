package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

const reportUsage = `usage: dqa generate-feedback-for-sites [options] <path>...

Generates a Markdown report of issues found in DQA results.

Example:

  dqa generate-feedback-for-sites -out chop-etlv4.md path/to/CHOP/results

Options:
`

var (
	reportfs *flag.FlagSet

	i2b2   bool
	output string
)

func init() {
	reportfs = flag.NewFlagSet("report", flag.ExitOnError)

	reportfs.Usage = func() {
		fmt.Fprintln(os.Stderr, reportUsage)
		reportfs.PrintDefaults()
		fmt.Printf("\nBuild: %s\n", buildVersion)
		os.Exit(1)
	}

	reportfs.StringVar(&output, "out", "-", "Path to output file.")
	reportfs.BoolVar(&i2b2, "i2b2", false, "Render a report only containing i2b2-related issues.")
}

func reportMain(args []string) {
	reportfs.Parse(args)

	args = reportfs.Args()

	if len(args) < 1 {
		reportfs.Usage()
	}

	// Gather all of the files.
	var files []string

	for _, path := range args {
		fi, err := os.Stat(path)

		if err != nil {
			fmt.Print(err)
			os.Exit(1)
		}

		// Get a list of all files in the directory.
		if fi.IsDir() {
			fis, _ := ioutil.ReadDir(path)

			for _, fi := range fis {
				if !fi.IsDir() {
					files = append(files, filepath.Join(path, fi.Name()))
				}
			}
		} else {
			files = append(files, path)
		}
	}

	var (
		err error
		f   *os.File
	)

	report := NewReport("")

	// Toggle i2b2 mode.
	report.I2b2 = i2b2

	for _, name := range files {
		if f, err = os.Open(name); err != nil {
			fmt.Printf("cannot open file %s: %s\n", name, err)
		}

		if _, err = ReadResults(report, &universalReader{f}); err != nil {
			fmt.Println(err)
		}

		f.Close()
	}

	var w io.Writer

	// Render the output.
	if output == "-" {
		w = os.Stdout
	} else {
		if f, err = os.Create(output); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		defer f.Close()

		w = f
	}

	Render(w, report)
}
