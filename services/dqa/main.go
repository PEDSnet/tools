package main

import (
	"fmt"
	"os"
)

var buildVersion string

const usage = `usage: dqa <command> [options]

Commands:

    generate    Generate a set of result files for a site extract.
    report      Outputs a feedback report of issues.
`

func printUsage() {
	fmt.Fprintln(os.Stderr, usage)
	fmt.Printf("Build: %s\n", buildVersion)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
	}

	args := os.Args[2:]

	// Command.
	switch os.Args[1] {
	case "generate":
		generateMain(args)
	case "report":
		reportMain(args)
	case "help":
		helpMain(args)
	default:
		printUsage()
	}
}

func helpMain(args []string) {
	if len(args) == 0 {
		printUsage()
	}

	switch args[0] {
	case "generate":
		genfs.Usage()
	case "report":
		reportfs.Usage()
	default:
		printUsage()
	}
}
