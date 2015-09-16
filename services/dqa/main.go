package main

import "github.com/spf13/cobra"

var buildVersion string

var mainCmd = &cobra.Command{
	Use: "pedsnet-dqa",

	Short: "Commands for the data quality analysis of PEDSnet data.",

	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func main() {
	mainCmd.AddCommand(generateCmd)
	mainCmd.AddCommand(feedbackCmd)
	mainCmd.AddCommand(rankIssuesCmd)
	mainCmd.AddCommand(queryCmd)

	mainCmd.Execute()
}
