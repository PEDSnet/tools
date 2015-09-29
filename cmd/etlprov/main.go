package main

import (
	"flag"
	"fmt"
	"os"

	dms "github.com/chop-dbhi/data-models-service/client"
)

const (
	entitiesFile = "entities.csv"
	stepsFile    = "steps.csv"
	peopleFile   = "people.csv"
	toolsFile    = "tools.csv"
	sourcesFile  = "sources.csv"
)

func main() {
	var (
		dir     string
		model   string
		version string
		service string
	)

	flag.StringVar(&model, "model", "pedsnet", "Name of the data model to validate against.")
	flag.StringVar(&version, "version", "2.0.0", "Version of the data model to validate against.")
	flag.StringVar(&service, "service", "http://data-models.origins.link", "URL to the data models service.")

	flag.Parse()

	args := flag.Args()

	if len(args) == 0 {
		dir = "."
	} else {
		dir = args[0]
	}

	// Ensure the directory exists.
	stat, err := os.Stat(dir)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if !stat.IsDir() {
		fmt.Fprintf(os.Stderr, "%s not a directory\n", stat.Name())
		os.Exit(1)
	}

	client, err := dms.New(service)

	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if err = client.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "error communicating with service: %s\n", err)
		os.Exit(1)
	}

	dm, err := client.ModelRevision(model, version)

	if err != nil {
		fmt.Fprintf(os.Stderr, "problem fetching model data: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Validating against model '%s/%s'\n", model, version)

	// Start in a valid state.
	valid := true

	// Everything driven by the entities.
	entityParser := &EntityParser{
		model: dm,
	}

	entities, err := parseFile(dir, entitiesFile, entityParser)

	if err != nil {
		valid = false
	}

	// Steps file depends on the entityParser
	stepParser := &StepParser{
		entityParser: entityParser,
	}

	steps, err := parseFile(dir, stepsFile, stepParser)

	if err != nil {
		valid = false
	}

	toolParser := &ToolParser{
		stepParser: stepParser,
	}

	tools, err := parseFile(dir, toolsFile, toolParser)

	if err != nil {
		valid = false
	}

	sourceParser := &SourceParser{
		stepParser: stepParser,
	}

	sources, err := parseFile(dir, sourcesFile, sourceParser)

	if err != nil {
		valid = false
	}

	personParser := &PersonParser{
		stepParser: stepParser,
	}

	people, err := parseFile(dir, peopleFile, personParser)

	if err != nil {
		valid = false
	}

	fmt.Printf("Found %d entities\n", len(entities))
	fmt.Printf("Found %d steps\n", len(steps))
	fmt.Printf("Found %d tools\n", len(tools))
	fmt.Printf("Found %d sources\n", len(sources))
	fmt.Printf("Found %d persons\n", len(people))

	if !valid {
		os.Exit(1)
	}
}
