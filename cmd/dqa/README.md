# PEDSnet DQA CLI

The command-line interface for various DQA tools.

To get the full set of options for any command, use the `help` command.

```
pedsnet-dqa help <command>
```

## Generate Template

The `generate-templates` command generates a new set of files to be filled out. The `--copy-persistent` option can be used to copy persistent issues from the previous version of results.

Create the v4 result set for CHOP in the `ETLv4/` with the persistent issues in v3 copied over.

```
$ pedsnet-dqa generate-templates --copy-persistent=ETLv3 --root=ETLv4 CHOP ETLv4
Wrote files to 'ETLv4' for model 'pedsnet/2.0.0'
Copied persistent issues from 'ETLv3'
```

## Rank Issues

The `assign-rank-to-issues` command assigns a rank to issues based on a set of pre-determined rules. The set of rules are listed maintained [here](https://github.com/PEDSnet/Data-Quality/tree/master/SecondaryReports/Ranking). The rules are fetched dynamically which requires authorization against the repository (since it is private). This is done by supplying a [GitHub access token](https://help.github.com/articles/creating-an-access-token-for-command-line-use/) with the `--token` option.

Do a *dry run* on a set of results:

```
$ pedsnet-dqa assign-rank-to-issues --dryrun --token=abc123 ./ETLv4
+----------------+-------------------+-------------------------------+------------+------------+----------+----------+---------+
|      TYPE      |       TABLE       |             FIELD             | ISSUE CODE | PREVALENCE | NEW RANK | OLD RANK | CHANGED |
+----------------+-------------------+-------------------------------+------------+------------+----------+----------+---------+
| Administrative | care_site         | care_site_name                | G4-002     | full       | Medium   | Low      | Yes     |
| Administrative | care_site         | place_of_service_source_value | G4-002     | full       | Medium   | Low      | Yes     |
| Administrative | care_site         | specialty_source_value        | G4-002     | full       | Medium   | High     | Yes     |
| Administrative | location          | location_id                   | G2-013     | medium     | Low      | Low      | No      |
| Administrative | provider          | care_site_id                  | G2-013     | high       | Medium   | Low      | Yes     |
| Administrative | provider          | provider_id                   | G2-013     | high       | Medium   | Medium   | No      |
| Demographic    | death             | cause_source_value            | G4-002     | full       | Medium   | Medium   | No      |
| Demographic    | death             | person_id                     | G2-013     | low        | Medium   | Medium   | No      |
| Demographic    | person            | day_of_birth                  | G4-002     | full       | High     | High     | No      |
| Demographic    | person            | person_id                     | G2-013     | medium     | High     | High     | No      |
| Demographic    | person            | provider_id                   | G2-005     | high       | Low      | Low      | No      |
| Fact           | drug_exposure     | drug_exposure_start_date      | G2-009     | low        | Medium   | Medium   | No      |
| Fact           | drug_exposure     | drug_source_concept_id        | G4-002     | full       | High     | High     | No      |
| Fact           | drug_exposure     | person_id                     | G2-005     | high       | Medium   | High     | Yes     |
| Fact           | drug_exposure     | visit_occurrence_id           | G2-005     | high       | Medium   | Medium   | No      |
| Fact           | fact_relationship | relationship_concept_id       | G4-001     | unknown    | High     | High     | No      |
| Fact           | measurement       | measurement_date              | G2-010     | low        | Low      | Low      | No      |
| Fact           | measurement       | measurement_date              | G2-009     | low        | Medium   | Medium   | No      |
| Fact           | measurement       | person_id                     | G2-005     | high       | Medium   | Medium   | No      |
| Fact           | measurement       | visit_occurrence_id           | G2-005     | high       | Medium   | Medium   | No      |
| Fact           | observation       | observation_concept_id        | G2-013     | high       | High     | High     | No      |
| Fact           | observation       | person_id                     | G2-005     | medium     | Medium   | Medium   | No      |
| Fact           | visit_occurrence  | provider_id                   | G4-002     | low        | Low      | Low      | No      |
+----------------+-------------------+-------------------------------+------------+------------+----------+----------+---------+
```

## Site Feedback

The `generate-feedback-for-sites` command outputs a Markdown file with the list of issues for a set of results. The output is written to stdout, so it should be redirected to a file to save it.

```
$ pedsnet-dqa generate-feedback-for-sites ./ETLv4 > CHOP_ETLv4.md
```

## Query Issues

The `query` subcommand enables querying across the DQA results using SQL.

Given a file with the query named `persistent_fields.sql`:

```sql
select "table", field, count(*)
from results
where status = 'persistent'
group by "table", field
having count(*) > 1
order by count(*) desc, "table", field
```

The query can be read from stdin against multiple result sets.

```
$ pedsnet-dqa query - ./ETLv1 ./ETLv2 ./ETLv3 ./ETLv4 < persistent_fields.sql
+----------------------+------------------------+----------+
|        TABLE         |         FIELD          | COUNT(*) |
+----------------------+------------------------+----------+
| visit_occurrence     | visit_end_date         | 5        |
| observation          | value_as_number        | 4        |
| person               | pn_gestational_age     | 4        |
| person               | provider_id            | 4        |
| person               | year_of_birth          | 4        |
| visit_occurrence     | provider_id            | 4        |
| condition_occurrence | stop_reason            | 3        |
| measurement          | value_as_number        | 3        |
| visit_occurrence     | person_id              | 3        |
| condition_occurrence | condition_end_time     | 2        |
| condition_occurrence | person_id              | 2        |
| observation          | qualifier_source_value | 2        |
| observation          | unit_source_value      | 2        |
| person               | gender_source_value    | 2        |
| person               | time_of_birth          | 2        |
| procedure_occurrence | modifier_concept_id    | 2        |
| procedure_occurrence | modifier_source_value  | 2        |
| provider             | specialty_concept_id   | 2        |
| provider             | specialty_source_value | 2        |
| visit_occurrence     | visit_start_date       | 2        |
+----------------------+------------------------+----------+
```

## Validate Results

Checks the values in the DQA result files to be consistent. The validator checks the:

- `model version` is a valid semantic version.
- `goal` is one of the pre-defined choices.
- `prevalence` is one of the pre-defined choices.
- `rank` is one of the pre-defined choices.
- `cause` is one of the pre-defined choices.
- `status` is one of the pre-defined choices.

The set of *pre-defined choices* for each field are defined in the [SecondaryReports](https://github.com/PEDSnet/Data-Quality/tree/master/SecondaryReports#format-for-secondary-reports) repository.

The command can take one or more directories that contain the report files. For example, the following will evaluate all ETL versions for the CHOP site.

```
$ pedsnet-dqa validate ./CHOP/*
```

A recommended workflow is to produce a report for all sites and pipe it to `more` so the results can be paged through incrementally as the issues are fixed.

```
$ pedsnet-dqa validate \
    Boston/* \
    CCHMC/* \
    CHOP/* \
    Colorado/* \
    Nationwide/* \
    Nemours/* \
    Seattle/* \
    StLouis/* | more
```
