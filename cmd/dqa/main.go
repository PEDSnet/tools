package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var buildVersion string

var mainCmd = &cobra.Command{
	Use: "pedsnet-dqa",

	Short: "Commands for the data quality analysis of PEDSnet data.",

	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var versionCmd = &cobra.Command{
	Use: "version",

	Short: "Prints the version of the program.",

	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(os.Stdout, "%s\n", buildVersion)
	},
}

func main() {
	mainCmd.AddCommand(versionCmd)
	mainCmd.AddCommand(generateCmd)
	mainCmd.AddCommand(feedbackCmd)
	mainCmd.AddCommand(rankIssuesCmd)
	mainCmd.AddCommand(queryCmd)

	mainCmd.Execute()
}
