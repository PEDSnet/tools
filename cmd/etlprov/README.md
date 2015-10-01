# PEDSnet ETL Provenance Validator

## Usage

```bash
$ pedsnet-etlprov [-model <model>] [-version <version>] [-service <service>] <dir>
```


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
