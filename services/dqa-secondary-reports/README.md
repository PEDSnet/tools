# Service for DQA secondary report information

Service that exposes a endpoint for fetching PEDSnet DQA secondary report information for different ETL versions:

```
/pedsnet/ETLv1
/pedsnet/ETLv2
/pedsnet/ETLv3
/pedsnet/ETLv4
/pedsnet/ETLv5
```

## Installation

To install the prerequisites, run
```
pip3 install -r requirements.txt 
```

## Run

```
main.py [--token=<token>] [--host=<host>] [--port=<port>] [--debug]
```

Options:

- `host`
- `port`
- `token`, alternately the `GITHUB_AUTH_TOKEN` environment variable can be set.
