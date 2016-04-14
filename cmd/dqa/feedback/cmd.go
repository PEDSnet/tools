package feedback

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/PEDSnet/tools/cmd/dqa/results"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Cmd = &cobra.Command{
	Use: "generate-feedback-for-sites <path>...",

	Short: "Generates a Markdown report of issues found in DQA results.",

	Example: `
  pedsnet-dqa generate-feedback-for-sites --out=chop-etlv4.md SecondaryReports/CHOP/ETLv4`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			cmd.Usage()
			os.Exit(0)
		}

		i2b2 := viper.GetBool("feedback.i2b2")
		output := viper.GetString("feedback.out")

		// Gather all of the files.
		var files []string

		for _, path := range args {
			f, err := os.Stat(path)
			if err != nil {
				cmd.Printf("Error inspecting file: %s\n", err)
				os.Exit(1)
			}

			// Get a list of all files in the directory.
			if f.IsDir() {
				fis, _ := ioutil.ReadDir(path)

				for _, f := range fis {
					if f.IsDir() {
						continue
					}

					name := f.Name()

					if filepath.Ext(name) != ".csv" {
						continue
					}

					files = append(files, filepath.Join(path, name))
				}
			} else {
				files = append(files, path)
			}
		}

		// Initialize a report that combines the data from multiple input files.
		file := results.NewFile("")
		file.I2b2 = i2b2

		for _, name := range files {
			f, err := os.Open(name)
			if err != nil {
				cmd.Printf("Cannot open file %s: %s\n", name, err)
				os.Exit(1)
			}
			defer f.Close()

			if _, err = file.Read(f); err != nil {
				cmd.Printf("Error reading results from %s: %s\n", name, err)
				os.Exit(1)
			}
		}

		// Render the output.
		var w io.Writer

		if output == "-" {
			w = os.Stdout
		} else {
			f, err := os.Create(output)
			if err != nil {
				cmd.Printf("Error creating output file: %s\n", err)
				os.Exit(1)
			}
			defer f.Close()

			w = f
		}

		report := results.NewMarkdownReport(file)
		if err := report.Render(w); err != nil {
			cmd.Printf("Error generating report: %s", err)
		}
	},
}

func init() {
	flags := Cmd.Flags()

	// Define the flags.
	flags.String("out", "-", "Path to output file.")
	flags.Bool("i2b2", false, "Render a report only containing i2b2-related issues.")

	// Bind them to configuration.
	viper.BindPFlag("feedback.out", flags.Lookup("out"))
	viper.BindPFlag("feedback.i2b2", flags.Lookup("i2b2"))
}
