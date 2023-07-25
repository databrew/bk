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
start_fresh <- TRUE

if(!start_fresh){
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
  datasets <- c('v0demography', 'safetynew', 'safety', 'efficacy', 'pfu',
                'pk_day0', 'pkdays123',
                'pkfollowup')
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
  # pregnancy follow-up
  pfu <- read_csv(paste0(middle_path, 'pfu/pfu.csv'))
  pfu_repeat_preg_symptom <- read_csv(paste0(middle_path, 'pfu/pfu-repeat_preg_symptom.csv'))
  # pk_day0
  pk_day0 <- read_csv(paste0(middle_path, 'pk_day0/pk_day0.csv'))
  # pkdays123
  pkdays123 <- read_csv(paste0(middle_path, 'pkdays123/pkdays123.csv'))
  # pkfollowup
  pkfollowup <- read_csv(paste0(middle_path, 'pkfollowup/pkfollowup.csv'))
  
  save(safety, safety_repeat_drug,
       safety_repeat_individual, safety_repeat_ae_symptom,
       safetynew, safetynew_repeat_individual,
       v0demography,
       v0demography_repeat_individual, efficacy,
       pfu, pfu_repeat_preg_symptom,
       pk_day0, pkdays123, pkfollowup,
       file = 'data.RData')
} else {
  load('data.RData')
}


# Get arrivals
arrivals <- safetynew_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  left_join(safetynew %>% dplyr::select(hhid, KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>% mutate(type = 'Arrival')

# Get departures
safety_departures <- safety_repeat_individual %>%
  filter(!is.na(lastname), !is.na(dob)) %>%
  filter(person_left_household == 1| person_migrated == 1 | person_out_migrated == 1) %>%
  left_join(safety %>% dplyr::select(hhid, KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid)
    
# Get efficacy departures (but ignore migrations, per project instructions)
# https://bohemiakenya.slack.com/archives/C042KSRLYUA/p1690186129913529?thread_ts=1689946560.024259&cid=C042KSRLYUA
efficacy_departures <- efficacy %>% 
  filter(!is.na(lastname), !is.na(dob)) %>%
  mutate(firstname = as.character(firstname), lastname = as.character(lastname),
         sex = as.character(sex), extid = as.character(extid)) %>%
  mutate(todays_date = lubridate::as_datetime(todays_date)) %>%
  mutate(dob = lubridate::as_datetime(dob)) %>%
  filter(!is.na(person_absent_reason)) %>%
  filter(person_absent_reason %in% c('Died')) %>%
  dplyr::select(hhid, todays_date, firstname, lastname, dob, sex, extid) %>% mutate(type = 'Departure')
departures <- bind_rows(safety_departures, efficacy_departures)


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
individuals <- roster %>%
  dplyr::mutate(fullname_dob = paste0(firstname, ' ', lastname, ' | ', dob)) %>%
  dplyr::rename(fullname_id = roster_name) %>%
  # get intervention, village, ward, cluster
  left_join(households %>% dplyr::select(hhid, intervention, village, ward, cluster))
# Get starting safety status
right <- 
  bind_rows(
    safety_repeat_individual %>%
      left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'safety'),
    safetynew_repeat_individual %>%
      left_join(safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'safetynew'),
    efficacy %>%
      dplyr::select(todays_date, extid, safety_status) %>% mutate(form = 'efficacy')
  ) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_safety_status = safety_status)
individuals <- left_join(individuals, right)
# Get starting weight
right <- 
  bind_rows(
    safety_repeat_individual %>% filter(!is.na(current_weight)) %>%
      left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'safety'),
    safetynew_repeat_individual %>% filter(!is.na(current_weight)) %>%
      left_join(safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'safetynew'),
    efficacy %>% filter(!is.na(current_weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = current_weight) %>% mutate(form = 'efficacy'),
    pfu %>% filter(!is.na(weight)) %>%
      dplyr::select(todays_date, extid, starting_weight = weight) %>% mutate(form = 'pfu')
  ) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_weight)
individuals <- left_join(individuals, right)

# Get starting height
right <- 
  bind_rows(
    safety_repeat_individual %>% 
      mutate(height = ifelse(is.na(height), height_short, height)) %>%
      filter(!is.na(height)) %>%
      left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'safety'),
    safetynew_repeat_individual %>% filter(!is.na(height)) %>%
      left_join(safetynew %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'safetynew'),
    efficacy %>% filter(!is.na(height)) %>%
      dplyr::select(todays_date, extid, starting_height = height) %>% mutate(form = 'efficacy')
  ) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, starting_height)
individuals <- left_join(individuals, right)

# Get pk_status
right <- bind_rows(
  safety_repeat_individual %>% 
    filter(!is.na(pk_status)) %>%
    left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
    dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'safety'),
  pk_day0 %>%
    filter(!is.na(pk_status)) %>%
    dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'pk_day0'),
  pkdays123 %>%
    filter(!is.na(pk_status)) %>%
    dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'pkdays123'),
  pkfollowup %>%
    filter(!is.na(pk_status)) %>%
    dplyr::select(todays_date, extid, pk_status) %>% mutate(form = 'pkfollowup'),
) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, pk_status)
individuals <- left_join(individuals, right)

# Get efficacy status (placeholder) ################################################
individuals$efficacy_preselected <- sample(c(NA, 0, 1), nrow(individuals), replace = TRUE)

# Get some further efficacy status variables
# starting_efficacy_status
right <- 
  efficacy %>% arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(starting_efficacy_status = efficacy_status) %>%
  dplyr::select(extid, starting_efficacy_status)
individuals <- left_join(individuals, right)

# efficacy_absent_most_recent_visit
right <- 
  efficacy %>% arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(efficacy_absent_most_recent_visit = person_absent) %>%
  dplyr::select(extid, starting_efficacy_status)
individuals <- left_join(individuals, right) %>%
  mutate(efficacy_absent_most_recent_visit = ifelse(is.na(efficacy_absent_most_recent_visit), 0, efficacy_absent_most_recent_visit))

# efficacy_most_recent_visit_present
right <- 
  efficacy %>% arrange(desc(todays_date)) %>%
  filter(!is.na(person_present_continue)) %>%
  filter(person_present_continue == 1) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(efficacy_absent_most_recent_visit = as.numeric(gsub('V', '', visit))) %>%
  dplyr::select(extid, efficacy_most_recent_visit_present)
individuals <- left_join(individuals, right) 

# efficacy_visits_done
right <- efficacy %>%
  group_by(extid) %>%
  summarise(efficacy_visits_done = paste0(sort(unique(visit)), collapse = ', '))
individuals <- left_join(individuals, right) 

# starting_pregnancy_status
right <-   
  bind_rows(
    safety_repeat_individual %>% filter(!is.na(pregnancy_status)) %>%
      left_join(safety %>% dplyr::select(KEY, todays_date), by = c('PARENT_KEY' = 'KEY')) %>%
      dplyr::select(todays_date, extid, pregnancy_status) %>% mutate(form = 'safety'),
    pfu %>% filter(!is.na(pregnancy_status)) %>%
      dplyr::select(todays_date, extid, pregnancy_status) %>% mutate(form = 'pfu')
  ) %>%
  arrange(desc(todays_date)) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::select(extid, pregnancy_status)
individuals <- left_join(individuals, right)

# pregnancy_consecutive_absences	 ########################## placeholder
individuals$pregnancy_consecutive_absences <- sample(c(NA, 0:7), nrow(individuals), replace = TRUE)

  
#pregnancy_most_recent_visit_present	
right <- 
  pfu %>% arrange(desc(todays_date)) %>%
  filter(!is.na(person_present_continue)) %>%
  filter(person_present_continue == 1) %>%
  dplyr::distinct(extid, .keep_all = TRUE) %>%
  dplyr::mutate(pregnancy_most_recent_visit_present = as.numeric(gsub('V', '', visit))) %>%
  dplyr::select(extid, pregnancy_most_recent_visit_present)
individuals <- left_join(individuals, right) 

# pregnancy_visits_done
right <-  pfu %>%
  group_by(extid) %>%
  summarise(pregnancy_visits_done = paste0(sort(unique(visit)), collapse = ', '))
individuals <- left_join(individuals, right) 

if(!dir.exists('metadata')){
  dir.create('metadata')
}

write_csv(households, 'metadata/households.csv')
write_csv(individuals, 'metadata/individuals.csv')
