package migrate

import (
	"bytes"
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/PEDSnet/tools/cmd/dqa/uni"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Cmd = &cobra.Command{
	Use: "migrate-reports <dir>...",

	Short: "Migrates one of more reports to the latest structure.",

	Example: `
  pedsnet-dqa migrate-reports SecondaryReports/CHOP/*`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			cmd.Usage()
			os.Exit(0)
		}

		dryrun := viper.GetBool("migrate.dryrun")

		for _, dir := range args {
			stat, err := os.Stat(dir)

			if err != nil {
				cmd.Printf("Error inspecting directory: %s\n", err)
				continue
			}

			if !stat.IsDir() {
				cmd.Printf("Ignoring file '%s'\n", stat.Name())
				return
			}

			migrateReport(cmd, dir, dryrun)
		}
	},
}

func migrateReport(cmd *cobra.Command, dir string, dryrun bool) {
	cmd.Printf("Migrating files in '%s'\n", dir)

	// Get a list of all files in the directory.
	files, _ := ioutil.ReadDir(dir)

	if len(files) == 0 {
		cmd.Printf("* No files in directory.")
		return
	}

	for _, stat := range files {
		name := stat.Name()

		if filepath.Ext(name) != ".csv" {
			continue
		}

		path := filepath.Join(dir, name)

		f, err := os.Open(path)
		if err != nil {
			cmd.Printf("* Error opening '%s'\n", name)
			continue
		}

		cr := csv.NewReader(uni.New(f))
		rows, err := cr.ReadAll()

		f.Close()
		if err != nil {
			cmd.Printf("* Error reading '%s': %s\n", name, err)
			continue
		}

		var changed bool

		rows, changed, err = migrateGithubColumn(rows)

		if err != nil {
			cmd.Printf("* Error migrating '%s': %s\n", name, err)
			continue
		}

		// If none of migrations resulted in a change, ignore.
		if !changed {
			cmd.Printf("* '%s' already migrated.\n", name)
			continue
		}

		var w io.Writer

		if dryrun {
			w = bytes.NewBuffer(nil)
		} else {
			f, err := os.Create(path)
			if err != nil {
				cmd.Printf("* Error opening file for write: %s\n", err)
				continue
			}
			w = f
			defer f.Close()
		}

		wr := csv.NewWriter(w)
		if err := wr.WriteAll(rows); err != nil {
			cmd.Printf("* Error writing results: %s\n", err)
			continue
		}
		wr.Flush()

		if dryrun {
			cmd.Printf("* Fake migrated '%s'\n", name)
		} else {
			cmd.Printf("* Migrated '%s'\n", name)
		}
	}

	cmd.Println("")
}

// Add github column to the results.
func migrateGithubColumn(rows [][]string) ([][]string, bool, error) {
	head := rows[0]
	if head[len(head)-1] == "Github ID" {
		return rows, false, nil
	}

	for i, row := range rows {
		if i == 0 {
			row = append(row, "Github ID")
		} else {
			row = append(row, "")
		}

		rows[i] = row
	}

	return rows, true, nil
}

func init() {
	flags := Cmd.Flags()

	// Define the flags.
	flags.Bool("dryrun", false, "Test the migration without actual writing the output.")

	// Bind them to configuration.
	viper.BindPFlag("migrate.dryrun", flags.Lookup("dryrun"))
}
