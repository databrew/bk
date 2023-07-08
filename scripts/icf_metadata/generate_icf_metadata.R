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
is_production <- TRUE
form <- 'clean-form'
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
  object_keys <- glue::glue('/{folder}/{form}/{this_dataset}',
                            folder = folder,
                            this_dataset = this_dataset)
  output_dir <- glue::glue('{folder}/{form}/{this_dataset}',
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
v0demography <- read_csv(paste0(folder, '/', form, '/v0demography/v0demography.csv'))
v0demography_repeat_individual <- read_csv(paste0(folder, '/', form, '/v0demography/v0demography-repeat_individual.csv'))
tryCatch({
  demography_icf_verification <- read_csv(paste0(folder, '/', form, '/demography_icf_verification/demography_icf_verification.csv'))
},
error = {
  demography_icf_verification <- tibble() %>% mutate(todays_date = Sys.Date(),
                                                     hhid = '00000')
})
tryCatch({
  demography_icf_resolution <- read_csv(paste0(folder, '/', form, '/demography_icf_resolution')) # not finished
},
error = {
  demography_icf_resolution <- tibble()  %>% mutate(todays_date = Sys.Date(),
                                                    hhid = '00000')
})
    
names(demography_icf_resolution) <- unlist(lapply(strsplit(names(demography_icf_resolution), '-'), function(a){a[length(a)]})); 
demography_icf_resolution <- demography_icf_resolution[,!duplicated(names(demography_icf_resolution))]

names(demography_icf_verification) <- unlist(lapply(strsplit(names(demography_icf_verification), '-'), function(a){a[length(a)]}))
demography_icf_verification <- demography_icf_verification[,!duplicated(names(demography_icf_verification))]

names(v0demography) <- unlist(lapply(strsplit(names(v0demography), '-'), function(a){a[length(a)]}))
v0demography <- v0demography[,!duplicated(names(v0demography))]

names(v0demography_repeat_individual) <- unlist(lapply(strsplit(names(v0demography_repeat_individual), '-'), function(a){a[length(a)]}))
v0demography_repeat_individual <- v0demography_repeat_individual[,!duplicated(names(v0demography_repeat_individual))]

# Create the metadata
individuals <- v0demography_repeat_individual %>% left_join(v0demography, by = c('PARENT_KEY' = 'KEY'))
household_heads <- individuals %>% 
  filter(`hh_head_yn` == 'yes') %>%
  dplyr::select(date = `todays_date`,
                hhid, dob = `dob`,
                first_name = `firstname`,
                last_name = `lastname`) %>%
  dplyr::mutate(hh_head_fullname_dob = paste0(first_name, ' ', last_name, ' | ', dob)) %>%
  arrange(desc(date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  dplyr::select(hhid, hh_head_fullname_dob)
household_head_subs <- individuals %>% 
  filter(!is.na(`hh_head_sub_yn`)) %>%
  filter(`hh_head_sub_yn` == 'yes') %>%
  dplyr::select(hhid, date = `todays_date`,
                dob = `dob`,
                first_name = `firstname`,
                last_name = `lastname`) %>%
  dplyr::mutate(hh_head_sub_fullname_dob = paste0(first_name, ' ', last_name, ' | ', dob)) %>%
  arrange(desc(date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  dplyr::select(hhid, hh_head_sub_fullname_dob)
icf_signers <- individuals %>%
  filter(!is.na(`person_signed_icf_yn`)) %>%
  filter(`person_signed_icf_yn` == 'yes') %>%
  dplyr::select(hhid, date = `todays_date`,
                dob = `dob`,
                first_name = `firstname`,
                last_name = `lastname`,
                extid) %>%
  dplyr::mutate(person_signed_icf = paste0(first_name, ' ', last_name, ' | ', dob)) %>%
  arrange(desc(date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  dplyr::select(hhid, extid, person_signed_icf)


household_data <- individuals %>%
  filter(extid %in% icf_signers$extid) %>%
  dplyr::select(hhid,
                cl_id = `wid`,
                fa_id = `fa_id`,
                cluster = `cluster`,
                todays_date = `todays_date`,
                num_members = `num_hh_members`,
                hh_consent = icf_completed,
                # the below three variables are brought in through joins
                # hh_head_fullname_dob,
                # hh_head_sub_fullname_dob,
                # person_signed_icf,
                participant_extid = `extid`) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  left_join(household_heads) %>%
  left_join(household_head_subs) %>%
  left_join(icf_signers) %>%
  mutate(hh_consent = ifelse(hh_consent == 1, 'Yes', 'No'))

# Get the variables from the demography_icf_verification form
if(nrow(demography_icf_verification) > 0){
  div <- demography_icf_verification %>%
    mutate(consent_type = 'Household') %>%
    mutate(pages = gsub(' ', ', ', `hh_errors_page_select`)) %>%
    mutate(errors = paste0(
      ifelse(`hh_errors_page_select` %in% 'no', '1,', 999), 
      ifelse(`sign_hh_icf` %in% 'no', '2,', 999),
      ifelse(`id_correct` %in% 'no', '3,', 999),
      ifelse(`fw_sign` %in% 'no', '4,', 999),
      ifelse(`fw_date` %in% 'no', '5,', 999),
      ifelse(`fw_name` %in% 'no', '6,', 999),
      ifelse(`participant_sign` %in% 'no', '7,', 999),
      ifelse(`witness_sign` %in% 'no', '8,', 999),
      ifelse(`participant_witness_name` %in% 'no', '9,', 999),
      ifelse(`participant_witness_date` %in% 'no', '10,', 999),
      ifelse(`thumbprint_clear` %in% 'no', '11,', 999),
      ifelse(`dates_match` %in% 'no', '12,', 999),
      ifelse(`handwriting_legible` %in% 'no', '13,', 999),
      ifelse(`overwriting` %in% 'no', '14,', 999),
      ifelse(`questions_answered` %in% 'no', '15,', 999),
      ifelse(`correction_implemented` %in% 'no', '16,', 999),
      ifelse(!is.na(nchar(`other_query`)), '17', 999),
      collapse = NULL
    )) %>%
    mutate(errors = gsub('999, ', '', errors, fixed = TRUE)) %>%
    mutate(errors = gsub('999', '', errors, fixed = TRUE)) %>%
    mutate(errors = gsub(",$", "", errors)) %>%
    dplyr::select(
      hhid,
      verification_date = `todays_date`,
      consent_type,
      icf_status = `hh_icf_stat`,
      errors,
      pages) %>%
    # Deduplicate
    arrange(desc(verification_date)) %>%
    dplyr::distinct(hhid, .keep_all = TRUE)
} else {
  div <- tibble() %>% mutate(hhid = 00000)
}


# Replace the icf_status with the most recent icf_status from the "resolution" form
if(nrow(demography_icf_resolution) > 0 & nrow(div) > 0){
  for(i in 1:nrow(demography_icf_resolution)){
    this_hhid <- demography_icf_resolution$hhid[i]
    this_icf_status <- demography_icf_resolution$hh_icf_stat[i]
    if(this_hhid %in% div$hhid){
      this_div_date <- div %>% filter(hhid == this_hhid) %>% pull(verification_date)
      this_res_date <- demography_icf_resolution$todays_date[i]
      if(this_res_date >= this_div_date){
        div$icf_status[div$hhid == this_hhid] <- this_icf_status
      }
    }
  }
}


# Join the demography_icf_verification variables to the household_data
household_data <- left_join(household_data, div)

# Placeholder columns if no info is available
the_columns <- c('hhid',
                 'cl_id',
                 'fa_id',
                 'cluster',
                 'todays_date',
                 'num_members',
                 'hh_consent',
                 'hh_head_fullname_dob',
                 'hh_head_sub_fullname_dob',
                 'person_signed_icf',
                 'participant_extid',
                 'verification_date',
                 'consent_type',
                 'icf_status',
                 'errors',
                 'pages')
for(j in 1:length(the_columns)){
  this_column <- the_columns[j]
  if(!this_column %in% names(household_data)){
    household_data[,this_column] <- NA
  }
}

# Write a csv
write_csv(household_data, 'household_data.csv')
