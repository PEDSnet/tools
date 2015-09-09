package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var feedbackCmd = &cobra.Command{
	Use: "generate-feedback-for-sites <path>...",

	Short: "Generates a Markdown report of issues found in DQA results.",

	Example: `
  pedsnet-dqa generate-feedback-for-site --out=chop-etlv4.md path/to/CHOP/results`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			cmd.Usage()
		}

		i2b2 := viper.GetBool("feedback.i2b2")
		output := viper.GetString("feedback.output")

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
	},
}

func init() {
	flags := feedbackCmd.Flags()

	// Define the flags.
	flags.String("out", "-", "Path to output file.")
	flags.Bool("i2b2", false, "Render a report only containing i2b2-related issues.")

	// Bind them to configuration.
	viper.BindPFlag("feedback.out", flags.Lookup("out"))
	viper.BindPFlag("feedback.i2b2", flags.Lookup("i2b2"))
}
