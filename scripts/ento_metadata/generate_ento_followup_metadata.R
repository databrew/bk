# https://trello.com/c/kgDp7cGZ/1882-ento-hhid-leid-metadata-for-follow-up-visit-forms
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
Sys.setenv(PIPELINE_STAGE = 'production') # change to production
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
datasets <- c('entohhfirstvisit', 'entolefirstvisit')

# Loop through each dataset and retrieve
# bucket <- 'databrew.org'
# folder <- 'kwale'
bucket <- 'databrew.org'
folder <- 'kwale'

for(i in 1:length(datasets)){
  this_dataset <- datasets[i]
  object_keys <- glue::glue('/{folder}/clean-form/{this_dataset}',
                            folder = folder,
                            this_dataset = this_dataset)
  output_dir <- glue::glue('{folder}/clean-form/{this_dataset}',
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

# entohhfirstvisit <- read_csv('kwale/clean-form/entohhfirstvisit/')
entoscreeningke <- read_csv('kwale/clean-form/entoscreeningke/entoscreeningke.csv')

# New instructions, 2023-05-25
# Please generate a list of Ento households and livestock enclosures from entohhfirstvisit and entolefirstvisit



  # A dynamic list of ‘active’ Ento households and livestock enclosures from entoscreeningke will need to be generated to be deployed with and used in the entohhfollowupke and entolefollowupke forms.
  # 
  # A hhid is ‘active' if it is NOT also present in the column orig_hhid of the entoscreeningke dataset. If a given HHID is contained in both the hhid and orig_hhid columns of entoscreeningke, it becomes ‘inactive'.
hhids <- sort(unique(entoscreeningke$hhid))
hhids <- hhids[!is.na(hhids)]
active_hhids <- hhids[!hhids %in% entoscreeningke$orig_hhid]
# Likewise, a leid is 'active’ if it is NOT also present in the  the column orig_le and is 'inactive’ if the same ID is contained in both columns.
leids <- sort(unique(entoscreeningke$leid))
leids <- leids[!is.na(leids)]
active_leids <- leids[!leids %in% entoscreeningke$orig_le]

# This metadata table of all active sites should contain 2 columns:
#   site_id which contains the hhid/leid
# type which contains ‘HH’ (for households) or ‘LE’ (for livestock enclosures)
metadata <- 
  bind_rows(
    tibble(site_id = as.character(active_hhids),
           type = 'HH'),
    tibble(site_id = as.character(active_leids),
           type = 'LE')
  )
write_csv(metadata, 'active_ids_metadata.csv')


# Xing to update follow up ODK forms:
#   entohhfollowup_kwale: The variable hhid_select should be a select_one type variable for fieldworkers to select an HHID from a list of active hhids.
# entolefollowup_kwale: The variable leid_select should be a select_one type variable for fieldworkers to select an LEID from a list of active leids.
