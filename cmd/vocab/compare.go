package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
)

type Index map[int]*Concept

func GenerateIndex(r *ConceptReader) (Index, error) {
	idx := make(Index)

	for {
		c, err := r.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			return nil, err
		}

		if _, ok := idx[c.ConceptID]; ok {
			return nil, fmt.Errorf("Duplicate concept: %d", c.ConceptID)
		} else {
			idx[c.ConceptID] = c
		}
	}

	return idx, nil
}

type Diff struct {
	Added   []*Concept
	Removed []*Concept
	Changed [][2]*Concept
}

var compareCmd = &cobra.Command{
	Use: "compare <before> <after>",

	Short: "Compares two concept.csv files.",

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			cmd.Usage()
			os.Exit(1)
		}

		fb, err := os.Open(args[0])

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		fa, err := os.Open(args[1])

		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		rb := NewConceptReader(fb)
		ra := NewConceptReader(fa)

		ia, err := GenerateIndex(ra)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error indexing: %s", err)
		}

		ib, err := GenerateIndex(rb)

		if err != nil {
			fmt.Fprintf(os.Stderr, "Error indexing: %s", err)
		}

		d := Diff{}
		seen := make(map[int]struct{})

		// Iterate over new concepts.
		for ak, ac := range ia {
			seen[ac.ConceptID] = struct{}{}

			// New concept.
			bc, ok := ib[ak]

			if !ok {
				d.Added = append(d.Added, ac)
				continue
			}

			// Compare.
			if ac.ConceptName != bc.ConceptName {
				d.Changed = append(d.Changed, [2]*Concept{bc, ac})
			}
		}

		for bk, bc := range ib {
			if _, ok := seen[bk]; ok {
				continue
			}

			d.Removed = append(d.Removed, bc)
		}

		fmt.Printf("%d Added\n", len(d.Added))
		fmt.Printf("%d Removed\n", len(d.Removed))
		fmt.Printf("%d Changed\n", len(d.Changed))
	},
}
