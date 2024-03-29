# Safety and Efficacy Metadata

## Forms

The "safety and efficacy" forms are described in [this google doc](https://docs.google.com/document/d/1MJeQreVlvfTfDb8kWwox5kaACd3dY4dVq62i6akbaaE/edit#heading=h.1gtnxmy3zejc). The "suite" of forms consists of the below:

- “V0 form” (no metadata)
- “V1-4 Safety form”
- “V1-3 Safety new members”
- “V1-7 Efficacy form”
- “V2-V13 Pregnancy follow up form (PFU)”

This document describes the metadata that the last 4 forms consume.

## Metadata tables

The aforementioned forms consume "metadata", meaning csv files which are uploaded to ODK Central so as to accompany the XML form definitions. These metadata consist of two tables:

1. `individual_data.csv`
2. `household_data.csv`

These tables are updated frequently. An updated to a table should be accompanied by an XML form definition update and version increment, even if the changes are only to the metadata (the form increment/xml update forces a push of the metadata to the ODK Collect client application).

## Metadata variables

### `individual_data`

The individual data table, `individual_data.csv`, is a one row per individual dataframe consisting of the following variables:

```
 firstname
 lastname
 fullname_dob
 fullname_id
 dob
 sex
 hhid
 extid
 intervention
 village
 ward
 cluster
 starting_safety_status
 starting_weight
 starting_height
 pk_preselected
 migrated
 efficacy_preselected
 starting_efficacy_status
 efficacy_absent_most_recent_visit
 efficacy_most_recent_visit_present
 efficacy_visits_done
 starting_pregnancy_status
 pregnancy_consecutive_absences
 pregnancy_most_recent_visit_present
 pregnancy_visits_done
```

### `household_data`

The household data table, `household_data.csv`, is a one row per household dataframe consisting of the following variables:

```
hhid	
roster	
num_members	
cluster	
intervention	
household_head
visits_done
pfu_members
```


## Values

### `individual_data`

What follows are the acceptable values / formats for each variable in the `individual_data.csv` table.


- `firstname`: string
- `lastname`: string
- `fullname_dob`: the full name in order of `firstname`, then space, then `lastname`, followed by a seperator (space + "|" + space), followed by the date of birth in ISO format (YYYY-MM-DD)
- `fullname_id`: the full name in order of `firstname`, then space, then `lastname`, followed by a space, then the `extid` wrapped in parentheses
- `dob`: the date of birth in ISO format (YYYY-MM-DD)
- `sex`: string; either "Male" or "Female"
- `hhid`: the household ID
- `extid`: the person ID
- `intervention`: string; either "Treatment" or "Control"
- `village`: string value of village name
- `ward`: string value of ward name
- `cluster`: numeric value of cluster number
- `starting_safety_status`: string; all lowercase; one of "icf", "in", "out", "refusal", "eos", "completion". This variable is derived from the "safety_status" variable from SE V0 form and then the most recent "safety_status" variable of Safety form
- `starting_weight`: numeric
- `starting_height`: numeric
- `pk_preselected`: numeric representing a boolean; one of 0 or 1; 0 = no; 1 = yes
- `migrated`: numeric representing a boolean; one of 0 or 1; 0 = no; 1 = yes
- `efficacy_preselected`: numeric representing a boolean; one of 0 or 1; 0 = no; 1 = yes
- `starting_efficacy_status`: string; all lowercase; one of "in", "out", "eos", "completion"
- `efficacy_absent_most_recent_visit`: numeric representing a boolean; one of 0 or 1; 0 = no; 1 = yes
- `efficacy_most_recent_visit_present`: a numeric value of 1, 2, 3, 4, 5, 6, or 7 representing the most recent Efficacy visit in which the participant was present
- `efficacy_visits_done`: string; the comma-separated Efficacy visits carried out to date (for example, "V2", or "V2,V3")
- `starting_pregnancy_status`: string; all lowercase; one of "in", "out", "eos".
- `pregnancy_consecutive_absences`: integer of 0 or above
- `pregnancy_most_recent_visit_present`: a numeric value of ranging from 2-13 representing the most recent PFU visit in which the participant was present
- `pregnancy_visits_done`: string, the comma-separated Pregnancy visits carried out to date (for example, "V2", or "V2,V3,V4")

### `household_data`

What follows are the acceptable values / formats for each variable in the `household_data.csv` table.


- `hhid`: an integer	
- `roster`: string; a comma+space (", ") separated list of members, wherein each element consists of the concatenation of `firstname` + space + `lastname` + space + `extid` wrapped in parentheses	
- `num_members`: an integer	
- `cluster`: an integer	
- `intervention`: String. "Treatment" or "Control"	
- `household_head`: string; the full name of the household head in order of `firstname`, then space, then `lastname`
- `visits_done`: string, the comma-separated visits carried out to date (for example, "V1", or "V1,V3")
- `pfu_mebmers`: string; a comma-separated list of all members of the household whose `starting_pregnancy_status` contains the value "in"

