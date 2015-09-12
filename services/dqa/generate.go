package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var generateCmd = &cobra.Command{
	Use: "generate-templates <site> <extract>",

	Short: "Generates a set of DQA files for the site and extract version.",

	Example: `
  pedsnet-dqa generate-templates --root=SecondaryReports/CHOP/ETLv5 CHOP ETLv5`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			cmd.Usage()
			return
		}

		model := viper.GetString("generate.model")
		version := viper.GetString("generate.version")
		root := viper.GetString("generate.root")
		url := viper.GetString("generate.url")

		dqa := NewResultsTemplate(model, version, args[0], args[1])

		dir := filepath.Join(root, dqa.SiteName, dqa.Extract)

		// Create the necessary directories to write the files to.
		if err := os.MkdirAll(dir, os.ModeDir|0775); err != nil {
			log.Fatal("mkdir:", err)
		}

		m, err := FetchModel(url, dqa.Model, dqa.ModelVersion)

		if err != nil {
			log.Fatal("fetch:", err)
		}

		var (
			p   string
			f   *os.File
			w   *csv.Writer
			row = make([]string, len(ResultsTemplateHeader))
		)

		// Model level fields.
		row[0] = m.Name
		row[1] = m.Version
		row[2] = dqa.DataVersion
		row[3] = dqa.Version

		// Create a file per table.
		for _, table := range m.Tables {
			if _, ok := ExcludedTables[table.Name]; ok {
				continue
			}

			p = filepath.Join(dir, fmt.Sprintf("%s.csv", table.Name))

			if f, err = os.Create(p); err != nil {
				log.Fatal("create:", err)
			}

			// Initialize CSV writer and start with the header.
			w = csv.NewWriter(f)
			w.Write(ResultsTemplateHeader)

			// Table level fields.
			row[4] = table.Name

			for _, field := range table.Fields {
				row[5] = field.Name

				for _, goal := range Goals {
					row[6] = goal
					w.Write(row)
				}

			}

			w.Flush()
			f.Close()
		}

		fmt.Printf("Wrote files to '%s' for model '%s/%s'\n", dir, m.Name, m.Version)
	},
}

func init() {
	flags := generateCmd.Flags()

	flags.String("root", "", "Root directory of output directory.")
	flags.String("model", "pedsnet", "The model the DQA files are generated for.")
	flags.String("version", "2.0.0", "The version of the model the DQA files are generated for.")
	flags.String("url", "http://data-models.origins.link", "URL to a DataModels service.")

	viper.BindPFlag("generate.root", flags.Lookup("root"))
	viper.BindPFlag("generate.model", flags.Lookup("model"))
	viper.BindPFlag("generate.version", flags.Lookup("version"))
	viper.BindPFlag("generate.url", flags.Lookup("url"))
}
