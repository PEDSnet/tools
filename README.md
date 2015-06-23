# PEDSnet Origins

The service's job is to integrate the data from these sources and make the information accessible through an API. A client will also be created to consume that API and present the information is addressable which enables it to be linked to by other webpages.

Project layout:

- `web/` - Web client files that consume the service and render views.
- `service/` - Service that exposes endpoints for monitoring the data.
- `generators/` - Custom Origins fact generators for PEDSnet-specific pedsnet.

### Generators

- CSV-based DQA results
- ETL conventions document annotations
- Vocabulary concepts

## Development

Requires a working [Go](http://golang.org) installation.

```
make install
```

Build generators.

```
make build-generators
```
