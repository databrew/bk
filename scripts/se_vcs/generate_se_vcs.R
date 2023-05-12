# Generate visit conrol sheets for v0
# slack request: https://bohemiakenya.slack.com/archives/C04R44ATLDR/p1681312566394949
# google sheet with specs: https://docs.google.com/spreadsheets/d/1tqD-Gi2GTlPDJB61oTc-mdcvlsToTtfjLwWikB9Vo50/edit#gid=0


library(readr)
library(dplyr)
library(rgdal)
library(sp)
library(cloudbrewr)

# login to aws via cloudbrewr
tryCatch({
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = 'production')

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})

# Read in cleaned / curated recon data: https://bohemiakenya.slack.com/archives/C042P3A05UP/p1679505186892229
if('recon_raw.RData' %in% dir()){
  load('recon_raw.RData')
} else {
  recon_raw <- cloudbrewr::aws_s3_get_table(
    bucket = 'databrew.org',
    key = 'raw-form/reconbhousehold.csv')
  # recon_raw <- read_csv('reconbhousehold.csv') # had to download from https://s3.console.aws.amazon.com/s3/buckets/databrew.org?region=us-east-1&tab=objects
  save(recon_raw, file = 'recon_raw.RData')
}

if('recon.RData' %in% dir()){
  load('recon.RData')
} else {
  recon <- cloudbrewr::aws_s3_get_table(
    bucket = 'databrew.org',
    key = 'clean-form/reconbhousehold/reconbhousehold.csv') %>%
    dplyr::select(hh_id_clean = hh_id,
                  ward,
                  community_health_unit,
                  village,
                  roof_type,
                  instanceID,
                  todays_date) %>%
    dplyr::left_join(recon_raw %>% dplyr::select(instanceID, hh_id_raw = hh_id)) %>%
    dplyr::select(instanceID, todays_date, hh_id_clean, hh_id_raw, ward,
                  community_health_unit, village, roof_type)
  # recon <- read_csv('reconbhousehold_clean.csv')
  save(recon, file = 'recon.RData')
}


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
if(FALSE){
  mm <- hh
  mm$mismatch <- mm$map_recon_HHID != mm$painted_recon_HHID
  x <- mm %>%
    filter(mismatch) %>%
    group_by(cluster) %>%
    tally %>%
    arrange(desc(n))
  hh <- hh %>% filter(cluster %in% c(3, 40))
  mm %>% filter(cluster %in% c(3, 40)) %>% filter(mismatch) %>% dplyr::select(cluster, contains('HHID'))
}

# Write a csv
write_csv(hh, 'hh.csv')

# Knit the visit control sheet
rmarkdown::render('rmds/v0vcs.Rmd')
