# ETL Convention Service

Service that exposes a single endpoint for fetching and parsing the [ETL conventions document](https://github.com/PEDSnet/Data_Models/blob/master/PEDSnet/V2/docs/Pedsnet_CDM_V2_OMOPV5_ETL_Conventions.md).

## Run

Options:

- `host`
- `port`
- `token`, alternately the `GITHUB_AUTH_TOKEN` environment variable can be set.

```
docker build -t pedsnet/etlconv .

# Token required since the file is in a private repository.
docker run pedsnet/etlconv --token=GITHUB_TOKEN
```

## Usage

Perform a GET request to the root endpoint. See `tests/expected.json` for example output.
