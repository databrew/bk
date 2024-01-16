# The Ento forms entoltparitywing and entorcoviposition will require metadata pulled from the entoltfield, entoltmorphid, entorcfield, and entorcmorphid datasets.
# This document https://docs.google.com/spreadsheets/d/1rw5NU7n3xfENVHs8HbBNwOxCM6B4udSUs0J9vWM-KgY/edit#gid=0 outlines the format of the metadata file needed. I have tried to provide an explanation of each column, though  in the likely event that clarification is needed, please let me know.

library(logger)
library(purrr)
library(dplyr)
library(cloudbrewr)
library(data.table)
library(sf)
library(sp)
library(lubridate)
library(readr)

# Define production
is_production <- TRUE
if(is_production){
  Sys.setenv(PIPELINE_STAGE = 'production') 
  raw_or_clean <- 'clean'
} else {
  Sys.setenv(PIPELINE_STAGE = 'develop')
  raw_or_clean <- 'raw'
}
env_pipeline_stage <- Sys.getenv("PIPELINE_STAGE")

# Log in
tryCatch({
  logger::log_info('Attempt AWS login')
  # login to AWS - this will be bypassed if executed in CI/CD environment
  cloudbrewr::aws_login(
    role_name = 'cloudbrewr-aws-role',
    profile_name =  'cloudbrewr-aws-role',
    pipeline_stage = env_pipeline_stage)

}, error = function(e){
  logger::log_error('AWS Login Failed')
  stop(e$message)
})


# Define datasets for which I'm retrieving data
datasets <- c(#'entoltfield',
              'entoltmorphid',
              # 'entorcfield',
              # 'entorcoviposition',
              'entorcmorphid')
datasets_names <- c(#'Ento Light trap collection field form',
                    'Ento Light trap morphological ID form',
                    # 'Ento Resting collections field form',
                    # 'Ento Resting Collections Oviposition form',
                    'Ento Resting collections morphological ID form')

# Loop through each dataset and retrieve
# bucket <- 'databrew.org'
# folder <- 'kwale'
bucket <- 'databrew.org'
if(is_production){
  folder <- 'kwale'
} else {
  folder <- 'kwale_ento_testing'
}

for(i in 1:length(datasets)){
  this_dataset <- datasets[i]
  object_keys <- glue::glue('/{folder}/{raw_or_clean}-form/{this_dataset}',
                            folder = folder,
                            this_dataset = this_dataset)
  output_dir <- glue::glue('{folder}/{raw_or_clean}-form/{this_dataset}',
                           folder = folder,
                           this_dataset = this_dataset)
  dir.create(object_keys, recursive = TRUE, showWarnings = FALSE)
  print(object_keys)
  cloudbrewr::aws_s3_bulk_get(
    bucket = bucket,
    prefix = as.character(object_keys),
    output_dir = output_dir
  )
}


# one csv file with two columns tube_id and collection_type would be easiest for me to manage. In the tube_id column, the tube QR code from:
#   entoltmorphid-repeat_tubes_dissected_unfed_funestus.csv
# entoltmorphid-repeat_tubes_dissected_unfed_gambiae.csv
# entorcmorphid-repeat_tube_funestus_gonotrophic.csv
# entorcmorphid-repeat_tube_gambiae_gonotrophic.csv
# and in the collection_type column: "LT" if the QR is from the entoltmorphid form and "RC" if the QR is from entorcmorphid form.
library(readr)
entoltmorphid <- read_csv('kwale/clean-form/entoltmorphid/entoltmorphid.csv')
entorcmorphid <- read_csv('kwale/clean-form/entorcmorphid/entorcmorphid.csv')
# entorcfield <- read_csv('kwale/clean-form/entorcfield/entorcfield.csv')
# entorcoviposition <- read_csv('kwale/clean-form/entorcoviposition/entorcoviposition.csv')


e1 <- read_csv('kwale/clean-form/entoltmorphid/entoltmorphid-repeat_tubes_dissected_unfed_funestus.csv') %>% left_join(entoltmorphid %>% dplyr::select(todays_date, KEY), by = c('PARENT_KEY' = 'KEY'))
e2 <- read_csv('kwale/clean-form/entoltmorphid/entoltmorphid-repeat_tubes_dissected_unfed_gambiae.csv') %>% left_join(entoltmorphid %>% dplyr::select(todays_date, KEY), by = c('PARENT_KEY' = 'KEY'))
e3 <- read_csv('kwale/clean-form/entorcmorphid/entorcmorphid-repeat_tube_funestus_gonotrophic.csv') %>% left_join(entorcmorphid %>% dplyr::select(todays_date, KEY), by = c('PARENT_KEY' = 'KEY'))
e4 <- read_csv('kwale/clean-form/entorcmorphid/entorcmorphid-repeat_tube_gambiae_gonotrophic.csv') %>% left_join(entorcmorphid %>% dplyr::select(todays_date, KEY), by = c('PARENT_KEY' = 'KEY'))

# Temp, remove bad instances
remove_bad <- function(z){z %>% filter(!PARENT_KEY %in% c("uuid:6ac0451a-4337-43ca-8e46-b2ed4e056e72", "uuid:8622a054-f295-4e53-af02-9e1d2d4fdb38", 
                                                          "uuid:cd616e21-8566-4d88-8ecf-5cf246a1e240"))}
e1 <- e1 %>% remove_bad()
e2 <- e2 %>% remove_bad()
e3 <- e3 %>% remove_bad()
e4 <- e4 %>% remove_bad()


out <- 
  bind_rows(
    e1 %>% dplyr::select(tube_id = tubes_dissected_unfed_funestus_qr, todays_date) %>%
      mutate(collection_type = 'LT', tube_id = as.character(tube_id)),
    e2 %>% dplyr::select(tube_id = tubes_dissected_unfed_gambiae_qr, todays_date) %>%
      mutate(collection_type = 'LT', tube_id = as.character(tube_id)),
    e3 %>% dplyr::select(tube_id = tubes_funestus_gonotrophic_qr, todays_date) %>%
      mutate(collection_type = 'RC', tube_id = as.character(tube_id)),
    e4 %>% dplyr::select(tube_id = tubes_gambiae_gonotrophic_qr, todays_date) %>%
      mutate(collection_type = 'RC', tube_id = as.character(tube_id))
  )

# Filter for a specific period based on Miguel's instructions
# https://bohemiakenya.slack.com/archives/C03DXF6SPC2/p1692871799819169?thread_ts=1692799112.440639&cid=C03DXF6SPC2
filter_by_dates <- TRUE
if(filter_by_dates){
  out <- out %>% filter(
    todays_date >= as.Date('2023-12-19'),
    todays_date <= as.Date('2023-12-21')
  )
  # out <- out %>%
  #   filter(todays_date %in% as.Date(c('2023-11-21',
  #                                     '2023-11-24',
  #                                     '2023-11-25')))
}

out <- out %>%
  dplyr::distinct(tube_id, .keep_all = TRUE) %>%
  arrange(tube_id)
write_csv(out, 'ento_metadata.csv')
