# Generate visit conrol sheets for v0
# slack request: https://bohemiakenya.slack.com/archives/C04R44ATLDR/p1681312566394949
# google sheet with specs: https://docs.google.com/spreadsheets/d/1tqD-Gi2GTlPDJB61oTc-mdcvlsToTtfjLwWikB9Vo50/edit#gid=0


library(readr)
library(dplyr)
library(rgdal)
library(sp)

# Read in cleaned / curated recon data
## https://bohemiakenya.slack.com/archives/C042P3A05UP/p1679505186892229
recon <- read_csv('recon/curated_recon_household_data.csv')

# Get geographic data and randomization data
assignments <- read_csv('../../data_public/randomization/assignments.csv')
households <- rgdal::readOGR('../../data_public/spatial/households/', 'households')

# Get the raw/uncorrected ID into the households data
households@data <- left_join(households@data,
                             recon %>% dplyr::select(
                               hh_id = hh_id_clean,
                               hh_id_raw
                             ))
# Reformat columns
hh <- households@data %>%
  dplyr::select(cluster = cluster_n0,
                ward,
                community_unit = community0,
                village,
                map_recon_HHID = hh_id,
                painted_recon_HHID = hh_id_raw,
                roof_material = roof_type,
                wall_material = house_wal0)

# Remove non-study households
hh <- hh %>% filter(cluster > 0)

# Identify mismatches in IDS
mm <- hh
mm$mismatch <- mm$map_recon_HHID != mm$painted_recon_HHID
x <- mm %>%
  filter(mismatch) %>%
  group_by(clusters)

# Write a csv
write_csv(hh, 'hh.csv')

# Knit the visit control sheet
rmarkdown::render('rmds/v0vcs.Rmd')
