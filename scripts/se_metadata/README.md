# Safety and Efficacy metadata

## Forms

The "safety and efficacy" forms are described in [this google doc](https://docs.google.com/document/d/1MJeQreVlvfTfDb8kWwox5kaACd3dY4dVq62i6akbaaE/edit#heading=h.1gtnxmy3zejc). The "suite" of forms consists of the below:

- “V0 form” (no metadata)
- “V1-4 Safety form”
- “V1-3 Safety new members”
- “V1-7 Efficacy form”
- “V2-V13 Pregnancy follow up form (PFU)”

This document describes the metadata that the first 4 forms consume (the metadata consumed the the "Pregnancy followup form" will be described separately.

## Metadata tables

The 4 aforementioned forms consume "metadata", meaning csv files which are uploaded to ODK Central so as to accompany the XML form definitions. These metadata consist of two tables:

1. `individual_data.csv`
2. `household_data.csv`

These tables are updated frequently. An updated to a table should be accompanied by an XML form definition update and version increment, even if the changes are only to the metadata (the form increment/xml update forces a push of the metadata to the ODK Collect client application).

## Metadata variables

### `individual_data`

The individual data table, `individual_data.csv`, is a one row per individual dataframe consisting of the following variables:

```
firstname	
lastname	
fullname_id	
dob	
age	
sex	
hhid	
extid	
intervention	
starting_safety_status	
starting_pregnancy_status	
starting_weight	pk_preselected
```

### `household_data`

The individual data table, `individual_data.csv`, is a one row per household dataframe consisting of the following variables:

```
hhid	
roster	
num_members
```
