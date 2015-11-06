# PEDSnet ETL Provenance Validator

## Usage

```bash
$ pedsnet-etlprov [-model <model>] [-version <version>] [-truncate=false] [-ignore <entities>] [-service <service>] [-v] <dir>
```

Option | Description
---|---
-model <model> | Specify a data model other than 'pedsnet', e.g. 'i2b2_pedsnet'
-version <version> | Specify PEDSnet CDM version, e.g. 2.0.0 or 2.1.0
-truncate=false | Show all errors, even redundant or excessive ones
-service <service> | Specify model service other than http://data-models.origins.link (not useful unless you run your own model service)
-ignore <entities> | Ignore a comma-separated list of entities (not normally used)
-v | Show the version of this tool


### Example

```bash
$ pedsnet-etlprov -model i2b2_pedsnet -version 2.0.0 ./files
Validating against model 'i2b2_pedsnet/2.0.0'
Scanning files in '/path/to/files/'
---
8 errors have been detected for 'steps.csv'
* Error parsing previous step 'none'
* Entity 'patient_dimension.gestational_age_num' not defined
* Entity 'patient_dimension.gestational_age_num' not defined
* Entity 'visit_dimension.admit_src_destcd' not defined
* Entity 'patient_dimension.gestational_age_num' not defined
* Entity 'visit_dimension.admit_src_destcd' not defined
* Entity 'provider_dimension' not defined
* Step 70 does not exist
---
2 entities are missing from the model
* provider_dimension.provider_id
* provider_dimension.provider_path
---
95 entities
18 steps
10 tools
2 sources
3 persons
```
