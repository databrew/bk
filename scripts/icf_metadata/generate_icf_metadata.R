# The ICF verification / rectification forms will require metadata files generated from data collected in forms v0demography, demography_icf_verification, and demography_icf_resolution.

# The specifications for the metadata file can be found on pages 5 and 6 here https://docs.google.com/document/d/16w7fjTfRJZZRWa99PM49hrk1Sr6IRInSmQTqz4YwVVQ/edit

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
is_production <- FALSE
Sys.setenv(PIPELINE_STAGE = ifelse(is_production, 'production', 'develop')) # change to production
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
datasets <- c('v0demography', 'demography_icf_verification', 'demography_icf_resolution')
datasets_names <- datasets
# Loop through each dataset and retrieve
# bucket <- 'databrew.org'
# folder <- 'kwale'
bucket <- 'databrew.org'
if(is_production){
  folder <- 'kwale'
} else {
  folder <- 'kwale_testing'
}

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

# Need to change the below from "kwale_testing" to "kwale"
v0demography <- read_csv('kwale_testing/raw-form/v0demography/v0demography.csv')
v0demography_repeat_individual <- read_csv('kwale_testing/raw-form/v0demography/v0demography-repeat_individual.csv')
demography_icf_verification <- read_csv('kwale_testing/raw-form/demography_icf_verification/demography_icf_verification.csv')
# demography_icf_resolution <- read_csv('kwale_testing/raw-form/demography_icf_resolution/') # not finished

# Create the metadata
individuals <- v0demography_repeat_individual %>% left_join(v0demography, by = c('PARENT_KEY' = 'KEY'))
household_heads <- individuals %>% 
  filter(`group_individual-group_basic_data-hh_head_yn` == 'yes') %>%
  dplyr::select(date = `group_meta-todays_date`,
                hhid, dob = `group_individual-group_basic_data-dob`,
                first_name = `group_individual-group_basic_data-firstname`,
                last_name = `group_individual-group_basic_data-lastname`) %>%
  dplyr::mutate(hh_head_fullname_dob = paste0(first_name, ' ', last_name, ' | ', dob)) %>%
  arrange(desc(date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  dplyr::select(hhid, hh_head_fullname_dob)
household_head_subs <- individuals %>% 
  filter(!is.na(`group_individual-group_basic_data-hh_head_sub_yn`)) %>%
  filter(`group_individual-group_basic_data-hh_head_sub_yn` == 'yes') %>%
  dplyr::select(hhid, date = `group_meta-todays_date`,
                dob = `group_individual-group_basic_data-dob`,
                first_name = `group_individual-group_basic_data-firstname`,
                last_name = `group_individual-group_basic_data-lastname`) %>%
  dplyr::mutate(hh_head_sub_fullname_dob = paste0(first_name, ' ', last_name, ' | ', dob)) %>%
  arrange(desc(date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  dplyr::select(hhid, hh_head_sub_fullname_dob)
icf_signers <- individuals %>%
  filter(!is.na(`group_individual-group_basic_data-person_signed_icf_yn`)) %>%
  filter(`group_individual-group_basic_data-person_signed_icf_yn` == 'yes') %>%
  dplyr::select(hhid, date = `group_meta-todays_date`,
                dob = `group_individual-group_basic_data-dob`,
                first_name = `group_individual-group_basic_data-firstname`,
                last_name = `group_individual-group_basic_data-lastname`) %>%
  dplyr::mutate(person_signed_icf = paste0(first_name, ' ', last_name, ' | ', dob)) %>%
  arrange(desc(date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  dplyr::select(hhid, person_signed_icf)


household_data <- individuals %>%
  dplyr::select(hhid,
                cl_id = `group_fw-wid`,
                fa_id = `group_fw-fa_id`,
                cluster = `group_location-cluster`,
                todays_date = `group_meta-todays_date`,
                num_members = `group_icf_completed_continue-group_demographics-num_hh_members`,
                hh_consent = icf_completed,
                # the below three variables are brought in through joins
                # hh_head_fullname_dob,
                # hh_head_sub_fullname_dob,
                # person_signed_icf,
                participant_extid = `group_individual-group_basic_data-extid`) %>%
  left_join(household_heads) %>%
  left_join(household_head_subs) %>%
  left_join(icf_signers) %>%
  mutate(hh_consent = ifelse(hh_consent == 1, 'Yes', 'No'))

# Get the variables from the demography_icf_verification form
div <- demography_icf_verification %>%
  mutate(consent_type = 'Household') %>%
  mutate(pages = gsub(' ', ', ', `group_pages-hh_errors_page_select`)) %>%
  mutate(errors = paste0(
    ifelse(`group_pages-hh_errors_page_select` %in% 'no', 1, 0),
    ifelse(`group_hh_icf_available-sign_hh_icf` %in% 'no', 2, 999),
    ifelse(`group_hh_icf_available-id_correct` %in% 'no', 3, 999),
    ifelse(`group_hh_icf_available-fw_sign` %in% 'no', 4, 999),
    ifelse(`group_hh_icf_available-fw_date` %in% 'no', 5, 999),
    ifelse(`group_hh_icf_available-fw_name` %in% 'no', 6, 999),
    ifelse(`group_hh_icf_available-participant_sign` %in% 'no', 7, 999),
    ifelse(`group_hh_icf_available-witness_sign` %in% 'no', 8, 999),
    ifelse(`group_hh_icf_available-participant_witness_name` %in% 'no', 9, 999),
    ifelse(`group_hh_icf_available-participant_witness_date` %in% 'no', 10, 999),
    ifelse(`group_hh_icf_available-thumbprint_clear` %in% 'no', 11, 999),
    ifelse(`group_hh_icf_available-dates_match` %in% 'no', 12, 999),
    ifelse(`group_hh_icf_available-handwriting_legible` %in% 'no', 13, 999),
    ifelse(`group_hh_icf_available-overwriting` %in% 'no', 14, 999),
    ifelse(`group_hh_icf_available-questions_answered` %in% 'no', 15, 999),
    ifelse(`group_hh_icf_available-correction_implemented` %in% 'no', 16, 999),
    ifelse(!is.na(nchar(`group_hh_icf_available-other_query`)), 17, 999),
    collapse = ', '
  )) %>%
  mutate(errors = gsub('999, ', '', errors)) %>%
  mutate(errors = gsub('999', '', errors)) %>%
  dplyr::select(
    hhid = `group_hh_info-hhid`,
    verification_date = `group_meta-todays_date`,
                consent_type,
                icf_status = `group_end_hh_status-hh_icf_stat`,
                errors,
                pages) %>%
  # Deduplicate
  arrange(desc(verification_date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE)


# Join the demography_icf_verification variables to the household_data
household_data <- left_join(household_data, div)

# Write a csv
write_csv(household_data, 'household_data.csv')
