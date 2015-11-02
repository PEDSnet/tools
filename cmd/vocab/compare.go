package main

import (
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

var (
	nonAlphaNum   = regexp.MustCompile(`[^a-zA-Z0-9\s]+`)
	dupWhitespace = regexp.MustCompile(`\s+`)
)

func normalizeString(s string) string {
	s = strings.ToLower(s)
	s = nonAlphaNum.ReplaceAllString(s, " ")
	return dupWhitespace.ReplaceAllString(s, " ")
}

func conceptsEqual(a, b *Concept) bool {
	// Check for exact match first since this is fastest.
	if a.ConceptName == b.ConceptName {
		return true
	}

	// Do some cleaning.
	an := normalizeString(a.ConceptName)
	bn := normalizeString(b.ConceptName)

	return an == bn
}

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

		rb, err := NewConceptReader(fb)

		if err != nil {
			fmt.Fprintln(os.Stderr, "Error reading file:", err)
			os.Exit(1)
		}

		ra, err := NewConceptReader(fa)

		if err != nil {
			fmt.Fprintln(os.Stderr, "Error reading file:", err)
			os.Exit(1)
		}

		var (
			ia, ib     Index
			aerr, berr error
		)

		// Parallelize index building.
		wg := &sync.WaitGroup{}
		wg.Add(2)

		go func() {
			ia, aerr = GenerateIndex(ra)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error indexing %s\n%s\n", fa.Name(), err)
			}

			wg.Done()
		}()

		go func() {
			ib, berr = GenerateIndex(rb)

			if err != nil {
				fmt.Fprintf(os.Stderr, "Error indexing %s\n%s\n", fb.Name(), err)
			}

			wg.Done()
		}()

		wg.Wait()

		if aerr != nil || berr != nil {
			os.Exit(1)
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
			if !conceptsEqual(ac, bc) {
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
