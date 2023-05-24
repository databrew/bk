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
Sys.setenv(PIPELINE_STAGE = 'develop') # change to production
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
              'entorcmorphid')
datasets_names <- c(#'Ento Light trap collection field form',
                    'Ento Light trap morphological ID form',
                    #'Ento Resting collections field form',
                    'Ento Resting collections morphological ID form')

# Loop through each dataset and retrieve
# bucket <- 'databrew.org'
# folder <- 'kwale'
bucket <- 'databrew.org'
folder <- 'kwale_ento_testing'

for(i in 1:length(datasets)){
  this_dataset <- datasets[i]
  object_keys <- glue::glue('/{folder}/raw-form/{this_dataset}',
                            folder = folder,
                            this_dataset = this_dataset)
  output_dir <- glue::glue('{folder}/raw-form/{this_dataset}',
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

e1 <- read_csv('kwale_ento_testing/raw-form/entoltmorphid/entoltmorphid-repeat_tubes_dissected_unfed_funestus.csv')
e2 <- read_csv('kwale_ento_testing/raw-form/entoltmorphid/entoltmorphid-repeat_tubes_dissected_unfed_gambiae.csv')
e3 <- read_csv('kwale_ento_testing/raw-form/entorcmorphid/entorcmorphid-repeat_tube_funestus_gonotrophic.csv')
e4 <- read_csv('kwale_ento_testing/raw-form/entorcmorphid/entorcmorphid-repeat_tube_gambiae_gonotrophic.csv')

out <- 
  bind_rows(
    e1 %>% dplyr::select(tube_id = tubes_dissected_unfed_funestus_qr) %>%
      mutate(collection_type = 'LT', tube_id = as.character(tube_id)),
    e2 %>% dplyr::select(tube_id = tubes_dissected_unfed_gambiae_qr) %>%
      mutate(collection_type = 'LT', tube_id = as.character(tube_id)),
    e3 %>% dplyr::select(tube_id = tubes_funestus_gonotrophic) %>%
      mutate(collection_type = 'RC', tube_id = as.character(tube_id)),
    e4 %>% dplyr::select(tube_id = tubes_gambiae_gonotrophic) %>%
      mutate(collection_type = 'RC', tube_id = as.character(tube_id))
  )
write_csv(out, 'metadata.csv')
