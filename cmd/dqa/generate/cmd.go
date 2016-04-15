package generate

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/PEDSnet/tools/cmd/dqa/results"
	dms "github.com/chop-dbhi/data-models-service/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Cmd = &cobra.Command{
	Use: "generate-templates <site> <cycle>",

	Short: "Generates a Secondary Report template for a site and data cycle.",

	Long: `The Secondary Report is a set of files that contain placeholders for
putting results of a data quality assessment. Reports will vary over time based
the data cycle, data model version, and ranking rules that used during the
assessment.

The typical process is to generate a new template per site and derive subsequent
reports from the previous. This can be done using the --copy-persistent
flag which ensures all persistent issues are copied to the new template.
`,

	Example: `Generate a new Secondary Report template:
  pedsnet-dqa generate-templates --root=SecondaryReports/CHOP/ETLv5 CHOP ETLv5`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			cmd.Usage()
			return
		}

		// Positional.
		siteName := args[0]
		dataCycle := args[1]

		// Options.
		modelName := viper.GetString("generate.model")
		modelVersion := viper.GetString("generate.version")
		dqaVersion := viper.GetString("generate.dqa-version")
		outDir := viper.GetString("generate.root")
		serviceUrl := viper.GetString("generate.url")
		copyPersistent := viper.GetString("generate.copy-persistent")

		if modelVersion == "" {
			cmd.Println("Model version required. Specify using the --version option.")
			os.Exit(1)
		}

		// Derived value.
		dataVersion := fmt.Sprintf("%s-%s-%s-%s", modelName, modelVersion, siteName, dataCycle)

		var files map[string]*results.File

		// Load the previous set of results.
		if copyPersistent != "" {
			var err error
			files, err = results.ReadFromDir(copyPersistent)

			if err != nil {
				cmd.Println(err)
				os.Exit(1)
			}
		} else {
			// Initialize to prevent lookup panics below.
			files = make(map[string]*results.File)
		}

		// Create the necessary directories to write the files to.
		if err := os.MkdirAll(outDir, os.ModeDir|0775); err != nil {
			cmd.Printf("Error creating output directory '%s': %s", outDir, err)
			os.Exit(1)
		}

		client, err := dms.New(serviceUrl)
		if err != nil {
			cmd.Printf("Bad service URL: %s", err)
			os.Exit(1)
		}

		model, err := client.ModelRevision(modelName, modelVersion)
		if err != nil {
			cmd.Printf("Error fetching model revision '%s/%s': %s", modelName, modelVersion, err)
		}

		// Create a file per table.
		for _, table := range model.Tables.List() {
			// Ignore certain tables from the template file.
			if _, ok := results.ExcludedTables[table.Name]; ok {
				continue
			}

			// Build an index of persistent and outstanding issues.
			var index prevIssues

			// Check if there is an existing file being copied.
			if file, ok := files[fmt.Sprintf("%s.csv", table.Name)]; ok {
				index = indexPreviousIssues(file)
			}

			// Initialize the file.
			var file *os.File
			var err error

			// Path to output file.
			path := filepath.Join(outDir, fmt.Sprintf("%s.csv", table.Name))

			if file, err = os.Create(path); err != nil {
				cmd.Printf("Error creating output file: %s", err)
				os.Exit(1)
			}

			w := results.NewWriter(file)

			for _, field := range table.Fields.List() {
				for _, goal := range results.Goals {
					res := &results.Result{
						Model:        modelName,
						ModelVersion: modelVersion,
						DataVersion:  dataVersion,
						DQAVersion:   dqaVersion,
						Table:        table.Name,
						Field:        field.Name,
						Goal:         goal,
					}

					// No copying needed.
					if copyPersistent == "" {
						w.Write(res)
						continue
					}

					// Find persistent issues for the field/goal and copy them.
					// Note there may be multiple for the same field and goal.
					if l, ok := index[[2]string{field.Name, goal}]; ok {
						for _, r := range l {
							res.IssueCode = r.IssueCode
							res.IssueDescription = r.IssueDescription
							res.Finding = r.Finding
							res.Prevalence = r.Prevalence
							res.Rank = r.Rank
							res.SiteResponse = r.SiteResponse
							res.Cause = r.Cause
							res.Status = r.Status
							res.Reviewer = r.Reviewer

							w.Write(res)
						}

						continue
					}

					// No persistent issues found, write the default field/goal.
					w.Write(res)
				}
			}

			w.Flush()
			file.Close()
		}

		cmd.Printf("Wrote files to '%s' for model '%s/%s'\n", outDir, modelName, modelVersion)

		if copyPersistent != "" {
			cmd.Printf("Copied persistent issues from '%s'\n", copyPersistent)
		}
	},
}

// Index of field/goal pairs to a set of results.
type prevIssues map[[2]string][]*results.Result

// Index persistent issues by field and goal. Multiple issues can be present
// so a slice is used here.
func indexPreviousIssues(f *results.File) prevIssues {
	index := make(prevIssues)

	for _, r := range f.Results {
		status := strings.ToLower(r.Status)
		if status == "persistent" || status == "under review" {
			results := index[[2]string{r.Field, r.Goal}]
			results = append(results, r)
			index[[2]string{r.Field, r.Goal}] = results
		}
	}

	return index
}

func init() {
	flags := Cmd.Flags()

	flags.String("root", ".", "Directory to the write the files to.")
	flags.String("model", "pedsnet", "The model the DQA files are generated for.")
	flags.String("version", "", "The version of the model the DQA files are generated for.")
	flags.String("dqa-version", "0", "The DQA version.")
	flags.String("url", dms.DefaultServiceURL, "Data models service URL.")
	flags.String("copy-persistent", "", "Copies issues in the specified path with a status of 'persistent' from an existing Secondary Report.")

	viper.BindPFlag("generate.root", flags.Lookup("root"))
	viper.BindPFlag("generate.model", flags.Lookup("model"))
	viper.BindPFlag("generate.version", flags.Lookup("version"))
	viper.BindPFlag("generate.dqa-version", flags.Lookup("dqa-version"))
	viper.BindPFlag("generate.url", flags.Lookup("url"))
	viper.BindPFlag("generate.copy-persistent", flags.Lookup("copy-persistent"))
}
