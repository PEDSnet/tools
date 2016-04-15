package feedback

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/PEDSnet/tools/cmd/dqa/results"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Cmd = &cobra.Command{
	Use: "generate-feedback-for-sites <path>",

	Short: "Generates a Markdown report of issues found in DQA results.",

	Example: `
  pedsnet-dqa generate-feedback-for-sites --token=abc123 SecondaryReports/CHOP/ETLv4`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			cmd.Usage()
			os.Exit(0)
		}

		post := viper.GetBool("feedback.post")
		token := viper.GetString("feedback.token")
		dataCycle := viper.GetString("feedback.cycle")
		printSummary := viper.GetBool("feedback.print-summary")

		if dataCycle == "" {
			cmd.Println("The data cycle could not be detected. Please supply it using the --cycle option.")
			os.Exit(1)
		}

		if post && token == "" {
			cmd.Println("A token is required to post issues to GitHub.")
			os.Exit(1)
		}

		dir := args[0]
		files, err := results.ReadFromDir(dir)
		if err != nil {
			cmd.Printf("Error reading files in '%s'\n", err)
			os.Exit(1)
		}

		// TODO: check if a summary has already been created.

		gr := NewGitHubReport("", "", dataCycle, token)

		// Iterate over each file and incrementally post the issues.
		for name, file := range files {
			var newIssues results.Results

			for _, result := range file.Results {
				if gr.Site == "" {
					gr.Site = result.SiteName()
					gr.ETLVersion = result.ETLVersion()
				}

				// Not in an issue. This will not be included in the summary report.
				if !result.IsIssue() {
					continue
				}

				newIssues = append(newIssues, result)

				ir, err := gr.NewIssue(result)
				if err != nil {
					cmd.Printf("Error creating issue request: %s\n", err)
					os.Exit(1)
				}

				// Only post if it does not already have a GitHub ID.
				if post {
					if result.GithubID == "" {
						issue, err := gr.PostIssue(ir)
						if err != nil {
							cmd.Printf("Error posting issue to GitHub: %s\n", err)
							continue
						}

						result.GithubID = fmt.Sprintf("%d", *issue.Number)
					}
				}
			}

			if len(newIssues) == 0 {
				cmd.Printf("No new issues for '%s'\n", name)
				continue
			}

			cmd.Printf("%d issues found in '%s'\n", len(newIssues), name)

			//
			if post {
				success := true
				f, err := os.Create(filepath.Join(dir, name))

				// File opened successfully.
				if err == nil {
					defer f.Close()
					w := results.NewWriter(f)

					if err := w.WriteAll(file.Results); err != nil {
						success = false
						cmd.Printf("Error writing results to file.")
					}

					if err := w.Flush(); err != nil {
						success = false
						cmd.Printf("Error flushing results to file.")
					}

					cmd.Printf("Saved new issue IDs to '%s'\n", name)
				} else {
					success = false
					cmd.Printf("Error opening file to write issue IDs: %s\n", err)
				}

				// Fallback to writing to standard out.
				if !success {
					cmd.Printf("Falling back to printing the results so they can be copy and pasted into '%s'.", name)
					// Only print the new issues to stdout.
					w := results.NewWriter(os.Stdout)
					w.WriteAll(newIssues)
					w.Flush()
					continue
				}
			}
		}

		if gr.Len() == 0 {
			fmt.Println("No issues to report.")
			return
		}

		// Build the file summary issue and post it.
		ir, err := gr.BuildSummaryIssue()
		if err != nil {
			cmd.Printf("Error building summary issue: %s\n", err)
			cmd.Println("Note: This can be safely retried without duplicating issues.")
			os.Exit(1)
		}

		if !post || printSummary {
			fmt.Println(*ir.Body)
		} else {
			issue, err := gr.PostIssue(ir)
			if err != nil {
				cmd.Printf("Error posting summary issue to GitHub: %s\n", err)
				cmd.Println("Note: This can be safely retried without duplicating issues.")
				os.Exit(1)
			}

			fmt.Printf("Summary issue URL: %s\n", *issue.HTMLURL)
		}
	},
}

func init() {
	flags := Cmd.Flags()

	// Define the flags.
	flags.Bool("post", false, "Posts the issues to GitHub.")
	flags.String("token", "", "Token used to authenticate with GitHub.")
	flags.String("cycle", "", "The data cycle for this report.")
	flags.Bool("print-summary", false, "Print the summary to stdout rather than posting it.")

	// Bind them to configuration.
	viper.BindPFlag("feedback.post", flags.Lookup("post"))
	viper.BindPFlag("feedback.token", flags.Lookup("token"))
	viper.BindPFlag("feedback.cycle", flags.Lookup("cycle"))
	viper.BindPFlag("feedback.print-summary", flags.Lookup("print-summary"))
}
