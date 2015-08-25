package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

var buildVersion string

const usage = `dqa-issues [options] <path>...

Generates a Markdown report of issues found in DQA results.

Example:

  dqa-issues -o chop-etlv4.md path/to/CHOP/results

Options:
`

func init() {
	// Usage prints the usage of the command.
	flag.Usage = func() {
		fmt.Println(usage)
		flag.PrintDefaults()
		fmt.Printf("\nBuild: %s\n", buildVersion)
		os.Exit(1)
	}
}

func main() {
	var output string

	flag.StringVar(&output, "o", "-", "Path to output file.")

	flag.Parse()

	args := flag.Args()

	if len(args) < 1 {
		flag.Usage()
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

	report := NewReport()

	for _, name := range files {
		if f, err = os.Open(name); err != nil {
			fmt.Printf("cannot open file %s: %s\n", name, err)
		}

		if _, err = ReadResults(report, &universalReader{f}); err != nil {
			fmt.Println(err)
		}

		f.Close()
	}

	// Render the output.
	if output == "-" {
		RenderMarkdown(os.Stdout, report)
	} else {
		if f, err = os.Create(output); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		RenderMarkdown(f, report)
		f.Close()
	}
}
