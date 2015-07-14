# DQA Template Files

Run:

```bash
dqa-files [-version <version>]
          [-model <model>]
          [-root <root>]
          [-data-models <url>]

          <site-name> <dqa-version>
```

This will output files in the directory `<site-name>/<dqa-version>` in the specified `<root>` directory (defaults to the current working directory). 

This program requires network access for the `-data-models` service (defaults to http://data-models.origins.link).
