package main

import (
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var queryCmd = &cobra.Command{
	Use: "query ( - | <sql> ) <path>...",

	Short: "Executes a SQL query against one or more sets of results.",

	Example: `
Inline:

  $ pedsnet-dqa query "select * from results limit 10" SecondaryReports/CHOP/ETLv4

Use - to read from stdin:

  $ pedsnet-dqa query - ./ETLv1 ./ETLv2 ./ETLv3 ./ETLv4
  select data_version, "table", field issue_code, rank, site_response
  from results
  where status = 'persistent'
  order by data_version, "table", field
  ^D

Read from a file:

  $ pedsnet-dqa query - ./ETLv1 ./ETLv2 ./ETLv3 ./ETLv4 < query.sql
`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 2 {
			cmd.Usage()
			return
		}

		stmt := args[0]

		// Read the SQL from stdin
		if stmt == "-" {
			b, err := ioutil.ReadAll(os.Stdin)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			stmt = string(b)
		}

		db, err := newDatabase()

		if err != nil {
			panic(err)
		}

		for _, dir := range args[1:] {
			reports, err := ReadResultsFromDir(dir, false)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			for _, r := range reports {
				if err = loadResults(db, r.Results); err != nil {
					panic(err)
				}
			}
		}

		err = queryDatabase(db, stmt, os.Stdout)

		if err != nil {
			fmt.Printf("query error: %s\n", err)
			os.Exit(1)
		}
	},
}

func newDatabase() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")

	if err != nil {
		return nil, err
	}

	cols := make([]string, len(ResultsTemplateHeader))

	for i, f := range ResultsTemplateHeader {
		cols[i] = fmt.Sprintf("\"%s\" TEXT", strings.Replace(strings.ToLower(f), " ", "_", -1))
	}

	stmt := fmt.Sprintf("CREATE TABLE results (%s)", strings.Join(cols, ",\n"))

	_, err = db.Exec(stmt)

	return db, err
}

func loadResults(db *sql.DB, results []*Result) error {
	params := make([]string, len(ResultsTemplateHeader))

	for i, _ := range params {
		params[i] = "?"
	}

	sql := fmt.Sprintf("insert into results values (%s)", strings.Join(params, ","))

	for _, r := range results {
		row := make([]interface{}, len(params))

		for i, c := range r.Row() {
			// Use null values for empty strings
			if c == "" {
				row[i] = nil
			} else {
				row[i] = c
			}
		}

		_, err := db.Exec(sql, row...)

		if err != nil {
			return err
		}
	}

	return nil
}

func queryDatabase(db *sql.DB, stmt string, w io.Writer) error {
	rows, err := db.Query(stmt)

	if err != nil {
		return err
	}

	defer rows.Close()

	cols, err := rows.Columns()

	if err != nil {
		return err
	}

	tw := tablewriter.NewWriter(os.Stdout)
	tw.SetHeader(cols)

	row := make([]interface{}, len(cols))
	out := make([]string, len(row))

	for i, _ := range row {
		row[i] = new(sql.NullString)
	}

	for rows.Next() {
		if err = rows.Scan(row...); err != nil {
			return err
		}

		for i, v := range row {
			x := v.(*sql.NullString)

			if x.Valid {
				out[i] = x.String
			} else {
				out[i] = ""
			}
		}

		tw.Append(out)
	}

	tw.Render()

	return rows.Err()
}
