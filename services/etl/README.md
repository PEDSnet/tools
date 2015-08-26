# ETL Conventions Service

Service that exposes a single endpoint for fetching and parsing ETL conventions documents:

- [PEDSnet 2.0.0](https://github.com/PEDSnet/Data_Models/blob/master/PEDSnet/V2/docs/Pedsnet_CDM_V2_OMOPV5_ETL_Conventions.md)
- [i2b2 2.0.0](https://github.com/PEDSnet/Data_Models/blob/master/i2b2/V2/docs/i2b2_pedsnet_v2_etl_conventions.md)

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
