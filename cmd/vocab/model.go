package main

import "time"

type Concept struct {
	ConceptID       int
	ConceptName     string
	DomainID        string
	VocabularyID    string
	ConceptClassID  string
	StandardConcept string
	ConceptCode     string
	ValidStartDate  time.Time
	ValidEndDate    time.Time
	InvalidReason   string
}
