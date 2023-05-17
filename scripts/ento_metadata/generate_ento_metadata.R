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
bucket <- 'databrew-testing-databrew.org'
folder <- 'Kwale Ento Testing'

for(i in 1:length(datasets)){
  this_dataset <- datasets[i]
  this_data <- cloudbrewr::aws_s3_get_table(
    bucket = bucket,
    key = paste0(folder, 'clean-form/', this_dataset, '.csv'))
}

# # save dim table
# cloudbrewr::aws_s3_store(
#   bucket = bucket_name,
#   key = 'clean-form/dim-kwale-location-hierarchy/dim-kwale-location-hierarchy.csv',
#   filename = as.character(output_filename)
# )
# 
# # create log messages
# logger::log_success('Created dim table')