# PEDSnet ETL Provenance Validator

## Usage

```bash
$ pedsnet-etlprov [-model <model>] [-version <version>] [-service <service>] <dir>
```


### Example

```bash
$ pedsnet-etlprov path/to/files
Validating against model 'pedsnet/1.0.0'
File entities.csv looks good!
File steps.csv looks good!
File tools.csv looks good!
File sources.csv looks good!
File people.csv looks good!
Found 58 entities
Found 14 steps
Found 1 tools
Found 2 sources
Found 1 persons
```

If there are errors, a set of messages will be printed out.

```bash
A few problems have been detected with the entities.csv file:
* Unknown entity `oraganization` for pedsnet data model
* Unknown entity `osbervation.relevant_condition_concept_id` for pedsnet data model
* Unknown entity `osbervation.unit_source_value` for pedsnet data model
```
