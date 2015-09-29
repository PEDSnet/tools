package main

type Tool struct {
	Name    string
	Steps   []int
	Usage   string
	Version string
}

type Step struct {
	ID           int
	Description  string
	Entities     []string
	PreviousStep int
	Time         string
}

type Source struct {
	Name    string
	Steps   []int
	Usage   string
	Version string
}

type Person struct {
	Name  string
	Email string
	Role  string
	Steps []int
}

type Entity struct {
	Name         string
	Availability string
	Transmitting bool
	Comment      string
	Truncation   string
	Limit        string
}
