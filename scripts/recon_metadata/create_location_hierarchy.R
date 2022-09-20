library(dplyr)
library(gsheet)

# Read in location hierachy
babu <- gsheet::gsheet2tbl('https://docs.google.com/spreadsheets/d/1ZZE5YqIMJWQ05ihNN2COnfbpb2dmRprxgjXUS5iuZFI/edit#gid=547128098')

# Investigate duplicates
pd <- babu %>%
  group_by(community_health_unit) %>%
  summarise(wards = paste0(sort(unique(ward)), collapse = ';'),
            n_wards = length(unique(ward)),
            sub_counties = paste0(sort(unique(sub_county)), collapse = ';'),
            n_sub_counties = length(unique(sub_county))) %>%
  arrange(desc(n_wards))

# Create a unique list of community health units and write to csv
community_health_units <- babu %>%
  dplyr::distinct(community_health_unit) %>%
  arrange(community_health_unit)
readr::write_csv(community_health_units, 'community_health_units.csv')

# Create a unique list of wards
wards <- babu %>%
  dplyr::distinct(ward) %>%
  arrange(ward)
readr::write_csv(wards, 'wards.csv')

# Keep only the locations
locations <- babu %>% 
  dplyr::select(community_health_unit,
                ward, 
                sub_county) %>%
  arrange(ward, community_health_unit)
# Write a csv for the purpose of using in kenya recon forms
readr::write_csv(locations, 'locations.csv')
