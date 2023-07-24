# https://trello.com/c/QORpzA5d/1938-se-metadata-scripting
# https://docs.google.com/spreadsheets/d/1gff7p0MKejzllSEp7ONunpaSufvTWXxafktPK4xyCys/edit#gid=389444343

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
if(is_production){
  Sys.setenv(PIPELINE_STAGE = 'production') 
  # raw_or_clean <- 'clean'
} else {
  Sys.setenv(PIPELINE_STAGE = 'develop')
  # raw_or_clean <- 'raw'
}
raw_or_clean <- 'clean'
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
datasets <- c('v0demography', 'safetynew', 'safety', 'efficacy')
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

# Read in the datasets
middle_path <- glue::glue('{folder}/{raw_or_clean}-form/')
# Safety
safety <- read_csv(paste0(middle_path, 'safety/safety.csv'))
safety_repeat_drug <- read_csv(paste0(middle_path, 'safety/safety-repeat_drug.csv'))
safety_repeat_individual <- read_csv(paste0(middle_path, 'safety/safety-repeat_individual.csv'))
safety_repeat_ae_symptom <- read_csv(paste0(middle_path, 'safety/safety-repeat_ae_symptom.csv'))
# Safety new
safetynew <- read_csv(paste0(middle_path, 'safetynew/safetynew.csv'))
safetynew_repeat_individual <- read_csv(paste0(middle_path, 'safetynew/safetynew-repeat_individual.csv'))
#v0demography
v0demography <- read_csv(paste0(middle_path, 'v0demography/v0demography.csv'))
v0demography_repeat_individual <- read_csv(paste0(middle_path, 'v0demography/v0demography-repeat_individual.csv'))
# efficacy
efficacy <- read_csv(paste0(middle_path, 'efficacy/efficacy.csv'))

# Get the CHANGES (deaths, migrations, etc.) 
arrivals <- safetynew_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  left_join(safetynew %>% dplyr::select(hhid, KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>% mutate(type = 'Arrival')
departures <- safety_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  filter(person_left_household == 1| person_migrated == 1 | person_out_migrated == 1) %>%
  left_join(safety %>% dplyr::select(hhid, KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid)  %>%
    bind_rows(
      efficacy %>% 
        filter(!is.na(lastname), !is.na(dob)) %>%
        mutate(firstname = as.character(firstname), lastname = as.character(lastname),
               sex = as.character(sex), extid = as.character(extid)) %>%
        mutate(todays_date = lubridate::as_datetime(todays_date)) %>%
        mutate(dob = lubridate::as_datetime(dob)) %>%
        filter(!is.na(person_absent_reason)) %>%
        filter(person_absent_reason %in% c('Migrated', 'Died')) %>%
        dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid)
    ) %>% mutate(type = 'Departure')
events <- bind_rows(arrivals, departures) %>% arrange(todays_date)

# Get the starting roster
starting_roster <- v0demography_repeat_individual %>% 
  left_join(v0demography %>% dplyr::select(hhid, todays_date, KEY), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(remove = FALSE) %>%
  mutate(index = 1:nrow(.))

# Go through each departure and remove people
for(i in 1:nrow(departures)){
  this_departure <- departures[i,]
  this_removal <- starting_roster %>%
    filter(extid == this_departure$extid,
           hhid == this_departure$hhid)
  if(nrow(this_removal) > 0){
    message('OK: Departure of ', this_departure$extid, ' from household: ', this_departure$hhid)
    starting_roster$remove[starting_roster$index %in% this_removal$index] <- TRUE
  } else {
    message('NOT OK: Departure of ', this_departure$extid, ' from household: ', this_departure$hhid, '. This person was not at this house in the first place, so skipping.')
    
  }
}
starting_roster <- starting_roster %>% filter(!remove)

# Go through each arrival and add
roster <- bind_rows(
  starting_roster,
  arrivals
) %>%
  arrange(desc(todays_date)) %>%
  # keep only the most recent case
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  mutate(roster_name = paste0(firstname, ' ', lastname, ' (',
                              extid, ')')) %>%
  dplyr::select(hhid, extid, firstname, lastname, sex, dob, roster_name) 

# Prepare some external datasets
assignments <- read_csv('../../analyses/randomization/outputs/assignments.csv')
intervention_assignment <- read_csv('../../analyses/randomization/outputs/intervention_assignment.csv')

# Get household heads and geographic information
heads <- v0demography_repeat_individual %>% 
  filter(hh_head_yn == 'yes') %>%
  left_join(v0demography %>% dplyr::select(hhid, todays_date, KEY, village, ward), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid, village, ward) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(hhid, .keep_all = TRUE) %>%
  mutate(household_head = paste0(firstname, ' ', lastname)) %>%
  dplyr::select(hhid, household_head, village, ward)

# Get number of visits done
visits_done <- 
  safety %>%
  group_by(hhid) %>%
  summarise(visits_done = paste0(sort(unique(visit)), collapse = ', '))

# Build up the metadata, starting with the household IDs
households <- roster %>% 
  group_by(hhid) %>%
  summarise(roster = paste0(roster_name, collapse = ', '),
            num_members = n()) %>%
  # get cluster
  left_join(v0demography %>% 
              filter(!is.na(cluster)) %>%
              dplyr::distinct(hhid, .keep_all = TRUE) %>%
              dplyr::select(hhid, cluster)) %>%
  # get assignments
  left_join(assignments %>% dplyr::select(cluster = cluster_number,
                                          arm = assignment)) %>%
  # get interventions
  left_join(intervention_assignment) %>%
  # get household head and geographic info
  left_join(heads) %>%
  # get visits done
  left_join(visits_done)

# Households done, now get individuals

