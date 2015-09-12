package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

// FetchModel fetches a model from the data models service.
func FetchModel(base, model, version string) (*Model, error) {
	u, err := url.Parse(base)

	if err != nil {
		return nil, err
	}

	u.Path = fmt.Sprintf("models/%s/%s", model, version)

	req, err := http.NewRequest("GET", u.String(), nil)

	if err != nil {
		return nil, err
	}

	// We want the JSON output.
	req.Header.Add("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var m Model

	if err = json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}

	return &m, nil
}
